// services/label-washing-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const {
  getBlokLokasiFromKodeProduksi,
} = require('../../../core/shared/mesin-location-helper'); 

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
} = require('../../../core/shared/tutup-transaksi-guard');


// GET all header with pagination & search
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT 
      h.NoWashing,
      h.DateCreate,
      h.IdJenisPlastik,
      jp.Jenis AS NamaJenisPlastik,
      h.IdWarehouse,
      w.NamaWarehouse,
      h.Blok,                    -- ✅ ambil langsung dari header
      h.IdLokasi,                -- ✅ ambil langsung dari header
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,
      h.Density,
      h.Moisture,
      -- ambil NoProduksi & NamaMesin
      MAX(wpo.NoProduksi) AS NoProduksi,
      MAX(m.NamaMesin) AS NamaMesin,
      -- ambil NoBongkarSusun
      MAX(bso.NoBongkarSusun) AS NoBongkarSusun
    FROM Washing_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    LEFT JOIN Washing_d d ON h.NoWashing = d.NoWashing
    LEFT JOIN WashingProduksiOutput wpo ON wpo.NoWashing = h.NoWashing
    LEFT JOIN WashingProduksi_h wph ON wph.NoProduksi = wpo.NoProduksi
    LEFT JOIN MstMesin m ON m.IdMesin = wph.IdMesin
    LEFT JOIN BongkarSusunOutputWashing bso ON bso.NoWashing = h.NoWashing
    WHERE 1=1
      ${search ? `AND (h.NoWashing LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ''}
      AND EXISTS (SELECT 1 FROM Washing_d d2 WHERE d2.NoWashing = h.NoWashing AND d2.DateUsage IS NULL)
    GROUP BY 
      h.NoWashing, h.DateCreate, h.IdJenisPlastik, jp.Jenis, 
      h.IdWarehouse, w.NamaWarehouse, h.IdStatus, 
      h.Density, h.Moisture, h.Blok, h.IdLokasi
    ORDER BY h.NoWashing DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoWashing) as total
    FROM Washing_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    WHERE 1=1
      ${search ? `AND (h.NoWashing LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ''}
      AND EXISTS (SELECT 1 FROM Washing_d d2 WHERE d2.NoWashing = h.NoWashing AND d2.DateUsage IS NULL)
  `;

  request.input('offset', sql.Int, offset).input('limit', sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map(item => ({
    ...item,
  }));

  const total = countResult.recordset[0].total;

  return { data, total };
};


// GET details by NoWashing
exports.getWashingDetailByNoWashing = async (nowashing) => {
  const pool = await poolPromise;
  const result = await pool.request()
    .input('NoWashing', sql.VarChar, nowashing)
    .query(`
      SELECT *
      FROM Washing_d
      WHERE NoWashing = @NoWashing
      ORDER BY NoSak
    `);

  return result.recordset.map(item => ({
    ...item
    }));
};


// ... getAll & getWashingDetailByNoWashing tetap

// util zero-pad
function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// Generate NoWashing dengan format: "B." + 10 digit
async function generateNextNoWashing(tx, prefix = 'B.', width = 10) {
  const rq = new sql.Request(tx);
  const q = `
    -- Kunci range baca nomor terakhir agar tidak balapan
    SELECT TOP 1 h.NoWashing
    FROM Washing_h AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE h.NoWashing LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(h.NoWashing, LEN(@prefix) + 1, 50)) DESC, h.NoWashing DESC
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoWashing; // e.g. "B.0000031863"
    const numericPart = last.substring(prefix.length); // "0000031863"
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

exports.createWashingCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : [];

  const NoProduksi = payload?.NoProduksi?.toString().trim() || null;
  const NoBongkarSusun = payload?.NoBongkarSusun?.toString().trim() || null;

  // ---- Validasi dasar
  const badReq = (msg) => { const e = new Error(msg); e.statusCode = 400; return e; };
  if (!header.IdJenisPlastik) throw badReq('IdJenisPlastik wajib diisi');
  if (!header.IdWarehouse) throw badReq('IdWarehouse wajib diisi');
  if (!header.CreateBy) throw badReq('CreateBy wajib diisi');
  if (!Array.isArray(details) || details.length === 0) throw badReq('Details wajib berisi minimal 1 item');

  // Mutually exclusive check
  const hasProduksi = !!NoProduksi;
  const hasBongkar = !!NoBongkarSusun;
  if (hasProduksi && hasBongkar) {
    const err = new Error('NoProduksi dan NoBongkarSusun tidak boleh diisi bersamaan');
    err.statusCode = 400;
    throw err;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE)
    // RULE: trxDate <= lastClosed => reject
    // ===============================
    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate); // date-only
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create washing',
      useLock: true,
    });

  // 0) Auto-isi Blok & IdLokasi dari sumber kode (produksi / bongkar susun)
const needBlok = header.Blok == null || String(header.Blok).trim() === '';
const needLokasi = header.IdLokasi == null;

if (needBlok || needLokasi) {
  const kodeRef = hasProduksi
    ? NoProduksi
    : (hasBongkar ? NoBongkarSusun : null);

  console.log('[WASHING][AUTO-LOKASI] hasProduksi=', hasProduksi,
              'hasBongkar=', hasBongkar,
              'NoProduksi=', NoProduksi,
              'NoBongkarSusun=', NoBongkarSusun,
              'kodeRef=', kodeRef,
              'needBlok=', needBlok,
              'needLokasi=', needLokasi);

  let lokasi = null;

  if (kodeRef) {
    lokasi = await getBlokLokasiFromKodeProduksi({
      kode: kodeRef,      // ✅ PENTING: pakai "kode"
      runner: tx,
    });
  }

  console.log('[WASHING][AUTO-LOKASI] lokasi(result)=', lokasi);

  if (lokasi) {
    if (needBlok) header.Blok = lokasi.Blok;
    if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
  }

  console.log('[WASHING][AUTO-LOKASI] header(after)=', {
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
  });
}


    // 1) Generate NoWashing (abaikan NoWashing dari client kalau ada)
    const generatedNo = await generateNextNoWashing(tx, 'B.', 10);

    // 2) Double-check belum dipakai
    const exist = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, generatedNo)
      .query(`SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`);

    if (exist.recordset.length > 0) {
      const retryNo = await generateNextNoWashing(tx, 'B.', 10);
      const exist2 = await new sql.Request(tx)
        .input('NoWashing', sql.VarChar, retryNo)
        .query(`SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`);

      if (exist2.recordset.length > 0) {
        const err = new Error('Gagal generate NoWashing unik, coba lagi.');
        err.statusCode = 409;
        throw err;
      }
      header.NoWashing = retryNo;
    } else {
      header.NoWashing = generatedNo;
    }

    // 3) Insert header
    // NOTE: selalu pakai @DateCreate (effectiveDateCreate) supaya:
    // - tanggal yang dicek = tanggal yang disimpan
    // - tidak tergantung GETDATE() server
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO Washing_h (
        NoWashing, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Density, Moisture, Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
      )
      VALUES (
        @NoWashing, @IdJenisPlastik, @IdWarehouse,
        @DateCreate,
        @IdStatus, @CreateBy, @DateTimeCreate,
        @Density, @Moisture, @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `;

    const rqHeader = new sql.Request(tx);
    rqHeader
      .input('NoWashing', sql.VarChar, header.NoWashing)
      .input('IdJenisPlastik', sql.Int, header.IdJenisPlastik)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('DateCreate', sql.Date, effectiveDateCreate) // ✅ date-only & sudah lolos tutup transaksi
      .input('IdStatus', sql.Int, header.IdStatus ?? 1)
      .input('CreateBy', sql.VarChar, header.CreateBy)
      .input('DateTimeCreate', sql.DateTime, nowDateTime)
      .input('Density', sql.Decimal(10, 3), header.Density ?? null)
      .input('Moisture', sql.Decimal(10, 3), header.Moisture ?? null)
      .input('Density2', sql.Decimal(10, 3), header.Density2 ?? null)
      .input('Density3', sql.Decimal(10, 3), header.Density3 ?? null)
      .input('Moisture2', sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input('Moisture3', sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.Int, header.IdLokasi ?? null);

    await rqHeader.query(insertHeaderSql);

    // 4) Insert details
    const insertDetailSql = `
      INSERT INTO Washing_d (NoWashing, NoSak, Berat, DateUsage)
      VALUES (@NoWashing, @NoSak, @Berat, NULL)
    `;

    let detailCount = 0;
    for (const d of details) {
      await new sql.Request(tx)
        .input('NoWashing', sql.VarChar, header.NoWashing)
        .input('NoSak', sql.Int, d.NoSak)
        .input('Berat', sql.Decimal(18, 3), d.Berat ?? 0)
        .query(insertDetailSql);
      detailCount++;
    }

    // 5) Conditional output (mutually exclusive)
    let outputTarget = null;
    let outputCount = 0;

    if (hasProduksi) {
      const insertWpoSql = `
        INSERT INTO WashingProduksiOutput (NoProduksi, NoWashing, NoSak)
        VALUES (@NoProduksi, @NoWashing, @NoSak)
      `;

      for (const d of details) {
        await new sql.Request(tx)
          .input('NoProduksi', sql.VarChar, NoProduksi)
          .input('NoWashing', sql.VarChar, header.NoWashing)
          .input('NoSak', sql.Int, d.NoSak)
          .query(insertWpoSql);
        outputCount++;
      }
      outputTarget = 'WashingProduksiOutput';
    } else if (hasBongkar) {
      const insertBsoSql = `
        INSERT INTO BongkarSusunOutputWashing (NoBongkarSusun, NoWashing, NoSak)
        VALUES (@NoBongkarSusun, @NoWashing, @NoSak)
      `;

      for (const d of details) {
        await new sql.Request(tx)
          .input('NoBongkarSusun', sql.VarChar, NoBongkarSusun)
          .input('NoWashing', sql.VarChar, header.NoWashing)
          .input('NoSak', sql.Int, d.NoSak)
          .query(insertBsoSql);
        outputCount++;
      }
      outputTarget = 'BongkarSusunOutputWashing';
    }

    await tx.commit();

    return {
      header: {
        NoWashing: header.NoWashing,
        IdJenisPlastik: header.IdJenisPlastik,
        IdWarehouse: header.IdWarehouse,
        IdStatus: header.IdStatus ?? 1,
        CreateBy: header.CreateBy,
        DateCreate: formatYMD(effectiveDateCreate), // ✅ konsisten dengan rule tutup transaksi
        DateTimeCreate: nowDateTime,
        Density: header.Density ?? null,
        Moisture: header.Moisture ?? null,
        Density2: header.Density2 ?? null,
        Density3: header.Density3 ?? null,
        Moisture2: header.Moisture2 ?? null,
        Moisture3: header.Moisture3 ?? null,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
      },
      counts: {
        detailsInserted: detailCount,
        outputInserted: outputCount,
      },
      outputTarget,
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};



exports.updateWashingCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoWashing = payload?.NoWashing?.toString().trim();
  if (!NoWashing) {
    const e = new Error('NoWashing (path) wajib diisi');
    e.statusCode = 400; throw e;
  }

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : null; // null => tidak sentuh details

  const NoProduksi = payload?.NoProduksi?.toString().trim() || null;
  const NoBongkarSusun = payload?.NoBongkarSusun?.toString().trim() || null;

  const hasProduksi = !!NoProduksi;
  const hasBongkar  = !!NoBongkarSusun;
  if (hasProduksi && hasBongkar) {
    const e = new Error('NoProduksi dan NoBongkarSusun tidak boleh diisi bersamaan');
    e.statusCode = 400; throw e;
  }

  const badReq = (msg) => { const e = new Error(msg); e.statusCode = 400; return e; };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) Pastikan header exist + ambil DateCreate existing (LOCK)
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`
        SELECT TOP 1 NoWashing, DateCreate
        FROM dbo.Washing_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing
      `);

    if (exist.recordset.length === 0) {
      const e = new Error(`NoWashing ${NoWashing} tidak ditemukan`);
      e.statusCode = 404; throw e;
    }

    const existingDateCreate = exist.recordset[0]?.DateCreate; // Date dari DB (dokumen ini)
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (UPDATE)
    // RULE: trxDate <= lastClosed => reject
    // Pakai tanggal existing di DB (dokumen ini)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `update washing ${NoWashing}`,
      useLock: true,
    });

    // Kalau client mengirim DateCreate baru, cek juga (dan jangan izinkan null->GETDATE)
    let newDateOnly = null;
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null) {
        throw badReq('DateCreate tidak boleh null pada UPDATE.');
      }
      newDateOnly = toDateOnly(header.DateCreate);
      if (!newDateOnly) throw badReq('DateCreate tidak valid.');

      await assertNotLocked({
        date: newDateOnly,
        runner: tx,
        action: `update washing ${NoWashing} (change DateCreate)`,
        useLock: true,
      });
    }

    // 1) Update header (partial / dynamic)
    const setParts = [];
    const reqHeader = new sql.Request(tx).input('NoWashing', sql.VarChar, NoWashing);

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf('IdJenisPlastik', 'IdJenisPlastik', sql.Int, header.IdJenisPlastik);
    setIf('IdWarehouse',    'IdWarehouse',    sql.Int, header.IdWarehouse);

    // DateCreate: hanya di-set kalau client mengirim, dan kita pakai hasil parse (bukan GETDATE)
    if (header.DateCreate !== undefined) {
      setIf('DateCreate', 'DateCreate', sql.Date, newDateOnly);
    }

    setIf('IdStatus',   'IdStatus',   sql.Int, header.IdStatus);
    setIf('Density',    'Density',    sql.Decimal(10,3), header.Density ?? null);
    setIf('Moisture',   'Moisture',   sql.Decimal(10,3), header.Moisture ?? null);
    setIf('Density2',   'Density2',   sql.Decimal(10,3), header.Density2 ?? null);
    setIf('Density3',   'Density3',   sql.Decimal(10,3), header.Density3 ?? null);
    setIf('Moisture2',  'Moisture2',  sql.Decimal(10,3), header.Moisture2 ?? null);
    setIf('Moisture3',  'Moisture3',  sql.Decimal(10,3), header.Moisture3 ?? null);
    setIf('Blok',       'Blok',       sql.VarChar, header.Blok ?? null);
    setIf('IdLokasi',   'IdLokasi',   sql.VarChar, header.IdLokasi ?? null);

    if (setParts.length > 0) {
      const sqlUpdateHeader = `
        UPDATE dbo.Washing_h SET ${setParts.join(', ')}
        WHERE NoWashing = @NoWashing
      `;
      await reqHeader.query(sqlUpdateHeader);
    }

    // 2) Replace details (yang DateUsage IS NULL) — kalau dikirim
    let detailAffected = 0;
    if (details) {
      await new sql.Request(tx)
        .input('NoWashing', sql.VarChar, NoWashing)
        .query(`
          DELETE FROM dbo.Washing_d
          WHERE NoWashing = @NoWashing AND DateUsage IS NULL
        `);

      const insertDetailSql = `
        INSERT INTO dbo.Washing_d (NoWashing, NoSak, Berat, DateUsage, IdLokasi)
        VALUES (@NoWashing, @NoSak, @Berat, NULL, @IdLokasi)
      `;

      for (const d of details) {
        await new sql.Request(tx)
          .input('NoWashing', sql.VarChar, NoWashing)
          .input('NoSak', sql.Int, d.NoSak)
          .input('Berat', sql.Decimal(18,3), d.Berat ?? 0)
          .input('IdLokasi', sql.VarChar, d.IdLokasi ?? header.IdLokasi ?? null)
          .query(insertDetailSql);
        detailAffected++;
      }
    }

    // 3) Conditional outputs (tidak berubah logic)
    let outputTarget = null;
    let outputCount = 0;

    const sentAnyOutputField = (payload.hasOwnProperty('NoProduksi') || payload.hasOwnProperty('NoBongkarSusun'));
    if (sentAnyOutputField) {
      await new sql.Request(tx)
        .input('NoWashing', sql.VarChar, NoWashing)
        .query(`DELETE FROM dbo.WashingProduksiOutput WHERE NoWashing = @NoWashing`);

      await new sql.Request(tx)
        .input('NoWashing', sql.VarChar, NoWashing)
        .query(`DELETE FROM dbo.BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`);

      if (hasProduksi) {
        const dets = await new sql.Request(tx)
          .input('NoWashing', sql.VarChar, NoWashing)
          .query(`SELECT NoSak FROM dbo.Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL ORDER BY NoSak`);

        const insertWpoSql = `
          INSERT INTO dbo.WashingProduksiOutput (NoProduksi, NoWashing, NoSak)
          VALUES (@NoProduksi, @NoWashing, @NoSak)
        `;

        for (const row of dets.recordset) {
          await new sql.Request(tx)
            .input('NoProduksi', sql.VarChar, NoProduksi)
            .input('NoWashing', sql.VarChar, NoWashing)
            .input('NoSak', sql.Int, row.NoSak)
            .query(insertWpoSql);
          outputCount++;
        }
        outputTarget = 'WashingProduksiOutput';
      } else if (hasBongkar) {
        const dets = await new sql.Request(tx)
          .input('NoWashing', sql.VarChar, NoWashing)
          .query(`SELECT NoSak FROM dbo.Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL ORDER BY NoSak`);

        const insertBsoSql = `
          INSERT INTO dbo.BongkarSusunOutputWashing (NoBongkarSusun, NoWashing, NoSak)
          VALUES (@NoBongkarSusun, @NoWashing, @NoSak)
        `;

        for (const row of dets.recordset) {
          await new sql.Request(tx)
            .input('NoBongkarSusun', sql.VarChar, NoBongkarSusun)
            .input('NoWashing', sql.VarChar, NoWashing)
            .input('NoSak', sql.Int, row.NoSak)
            .query(insertBsoSql);
          outputCount++;
        }
        outputTarget = 'BongkarSusunOutputWashing';
      }
    }

    await tx.commit();

    return {
      header: {
        NoWashing,
        ...header,
        // optional: info tanggal dokumen
        existingDateCreate: formatYMD(existingDateOnly),
        ...(newDateOnly ? { newDateCreate: formatYMD(newDateOnly) } : {}),
      },
      counts: {
        detailsAffected: detailAffected,
        outputInserted: outputCount
      },
      outputTarget,
      note: details
        ? 'Details (yang DateUsage IS NULL) diganti sesuai payload.'
        : 'Details tidak diubah.'
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};




// Hapus 1 header + semua output + details (jika aman + tidak melewati tutup transaksi)
exports.deleteWashingCascade = async (nowashing) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoWashing = (nowashing || '').toString().trim();
  if (!NoWashing) {
    const e = new Error('NoWashing wajib diisi');
    e.statusCode = 400; throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) pastikan exist + lock + ambil DateCreate existing
    const headRes = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`
        SELECT TOP 1 NoWashing, DateCreate
        FROM dbo.Washing_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing
      `);

    if (headRes.recordset.length === 0) {
      const e = new Error(`NoWashing ${NoWashing} tidak ditemukan`);
      e.statusCode = 404; throw e;
    }

    const existingDateCreate = headRes.recordset[0]?.DateCreate;
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (DELETE)
    // RULE: trxDate <= lastClosed => reject
    // berdasarkan tanggal dokumen (DateCreate) yang tersimpan di DB
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `delete washing ${NoWashing}`,
      useLock: true,
    });

    // 1) cek apakah ada detail terpakai
    const used = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`
        SELECT TOP 1 1
        FROM dbo.Washing_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      const e = new Error('Tidak bisa hapus: terdapat detail yang sudah terpakai (DateUsage IS NOT NULL).');
      e.statusCode = 409; throw e;
    }

    // 2) hapus output dulu (hindari FK)
    await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM dbo.WashingProduksiOutput WHERE NoWashing = @NoWashing`);

    await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM dbo.BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`);

    // 3) hapus semua details (yg belum terpakai)
    const delDet = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM dbo.Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL`);

    // 4) hapus header
    const delHead = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM dbo.Washing_h WHERE NoWashing = @NoWashing`);

    await tx.commit();

    return {
      NoWashing,
      docDateCreate: formatYMD(existingDateOnly),
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: 'WashingProduksiOutput + BongkarSusunOutputWashing',
      },
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}

    // mapping FK error jika ada constraint lain di DB
    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || 'Gagal hapus karena constraint referensi (FK).';
    }
    throw e;
  }
};

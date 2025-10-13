// services/label-washing-service.js
const { sql, poolPromise } = require('../../../core/config/db');

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
      AND NOT EXISTS (SELECT 1 FROM Washing_d d2 WHERE d2.NoWashing = h.NoWashing AND d2.DateUsage IS NOT NULL)
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
      AND NOT EXISTS (SELECT 1 FROM Washing_d d2 WHERE d2.NoWashing = h.NoWashing AND d2.DateUsage IS NOT NULL)
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
      WHERE NoWashing = @NoWashing AND DateUsage IS NULL
      ORDER BY NoSak
    `);

  return result.recordset.map(item => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) })
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

  // ---- Validasi dasar (NoWashing tidak wajib dari client, karena kita generate)
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
    // Pakai isolation level SERIALIZABLE agar generator aman dari race condition
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 1) Generate NoWashing (abaikan NoWashing dari client kalau ada)
    const generatedNo = await generateNextNoWashing(tx, 'B.', 10);

    // 2) Double-check belum dipakai (harusnya aman karena holdlock, tapi kita cek lagi)
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck.input('NoWashing', sql.VarChar, generatedNo)
      .query(`SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`);
    if (exist.recordset.length > 0) {
      // sangat kecil kemungkinannya, tapi kalau kejadian—ulangi sekali
      const retryNo = await generateNextNoWashing(tx, 'B.', 10);
      const exist2 = await new sql.Request(tx).input('NoWashing', sql.VarChar, retryNo)
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
    const nowDateOnly = header.DateCreate || null; // jika null -> pakai GETDATE() (date only)
    const nowDateTime = new Date();               // DateTimeCreate

    const insertHeaderSql = `
      INSERT INTO Washing_h (
        NoWashing, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Density, Moisture, Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
      )
      VALUES (
        @NoWashing, @IdJenisPlastik, @IdWarehouse,
        ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
        @IdStatus, @CreateBy, @DateTimeCreate,
        @Density, @Moisture, @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `;

    const rqHeader = new sql.Request(tx);
    rqHeader
      .input('NoWashing', sql.VarChar, header.NoWashing)
      .input('IdJenisPlastik', sql.Int, header.IdJenisPlastik)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1) // default PASS=1
      .input('CreateBy', sql.VarChar, header.CreateBy)
      .input('DateTimeCreate', sql.DateTime, nowDateTime)
      .input('Density', sql.Decimal(10, 3), header.Density ?? null)
      .input('Moisture', sql.Decimal(10, 3), header.Moisture ?? null)
      .input('Density2', sql.Decimal(10, 3), header.Density2 ?? null)
      .input('Density3', sql.Decimal(10, 3), header.Density3 ?? null)
      .input('Moisture2', sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input('Moisture3', sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.VarChar, header.IdLokasi ?? null);

    if (nowDateOnly) rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
    await rqHeader.query(insertHeaderSql);

    // 4) Insert details
    const insertDetailSql = `
      INSERT INTO Washing_d (NoWashing, NoSak, Berat, DateUsage, IdLokasi)
      VALUES (@NoWashing, @NoSak, @Berat, NULL, @IdLokasi)
    `;
    let detailCount = 0;
    for (const d of details) {
      const rqDet = new sql.Request(tx);
      await rqDet
        .input('NoWashing', sql.VarChar, header.NoWashing)
        .input('NoSak', sql.Int, d.NoSak)
        .input('Berat', sql.Decimal(18, 3), d.Berat ?? 0)
        .input('IdLokasi', sql.VarChar, d.IdLokasi ?? header.IdLokasi ?? null)
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
        const rqWpo = new sql.Request(tx);
        await rqWpo
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
        const rqBso = new sql.Request(tx);
        await rqBso
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
        DateCreate: nowDateOnly || 'GETDATE()',
        DateTimeCreate: nowDateTime,
        Density: header.Density ?? null,
        Moisture: header.Moisture ?? null,
        Density2: header.Density2 ?? null,
        Density3: header.Density3 ?? null,
        Moisture2: header.Moisture2 ?? null,
        Moisture3: header.Moisture3 ?? null,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null
      },
      counts: {
        detailsInserted: detailCount,
        outputInserted: outputCount
      },
      outputTarget
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
  const details = Array.isArray(payload?.details) ? payload.details : null; // null berarti tidak sentuh details

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

    // 0) Pastikan header exist
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck.input('NoWashing', sql.VarChar, NoWashing)
      .query(`SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`);
    if (exist.recordset.length === 0) {
      const e = new Error(`NoWashing ${NoWashing} tidak ditemukan`);
      e.statusCode = 404; throw e;
    }

    // 1) Update header (partial)
    // Build dynamic SET berdasarkan kolom yang dikirim
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
    if (header.DateCreate !== undefined) {
      // null => set ke GETDATE() (date only)
      if (header.DateCreate === null) {
        setParts.push('DateCreate = CONVERT(date, GETDATE())');
      } else {
        setIf('DateCreate', 'DateCreate', sql.Date, new Date(header.DateCreate));
      }
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

    // audit trail
    // if (payload.UpdateBy) {
    //   setParts.push('UpdateBy = @UpdateBy');
    //   setParts.push('DateTimeUpdate = @DateTimeUpdate');
    //   reqHeader.input('UpdateBy', sql.VarChar, payload.UpdateBy);
    //   reqHeader.input('DateTimeUpdate', sql.DateTime, new Date());
    // }

    if (setParts.length > 0) {
      const sqlUpdateHeader = `
        UPDATE Washing_h SET ${setParts.join(', ')}
        WHERE NoWashing = @NoWashing
      `;
      await reqHeader.query(sqlUpdateHeader);
    }

    // 2) Update details (replace yang DateUsage IS NULL) — kalau dikirim
    let detailAffected = 0;
    if (details) {
      // Pastikan tidak ada detail "terpakai" yang ikut diubah/hilang
      // Strategi: hapus semua detail yang DateUsage IS NULL, sisakan yang sudah terpakai
      const rqDel = new sql.Request(tx);
      await rqDel.input('NoWashing', sql.VarChar, NoWashing).query(`
        DELETE FROM Washing_d
        WHERE NoWashing = @NoWashing AND DateUsage IS NULL
      `);

      const insertDetailSql = `
        INSERT INTO Washing_d (NoWashing, NoSak, Berat, DateUsage, IdLokasi)
        VALUES (@NoWashing, @NoSak, @Berat, NULL, @IdLokasi)
      `;
      for (const d of details) {
        const rqDet = new sql.Request(tx);
        await rqDet
          .input('NoWashing', sql.VarChar, NoWashing)
          .input('NoSak', sql.Int, d.NoSak)
          .input('Berat', sql.Decimal(18,3), d.Berat ?? 0)
          .input('IdLokasi', sql.VarChar, d.IdLokasi ?? header.IdLokasi ?? null)
          .query(insertDetailSql);
        detailAffected++;
      }
    }

    // 3) Conditional outputs
    // Reset keduanya dulu, lalu isi sesuai payload (jika ada).
    // Catatan: kalau payload TIDAK mengirim NoProduksi & NoBongkarSusun sama sekali,
    // kita **tidak** menyentuh output yang existing (biarkan apa adanya).
    let outputTarget = null;
    let outputCount = 0;

    const sentAnyOutputField = (payload.hasOwnProperty('NoProduksi') || payload.hasOwnProperty('NoBongkarSusun'));

    if (sentAnyOutputField) {
      // bersihkan dulu
      const rqDelOut1 = new sql.Request(tx);
      await rqDelOut1.input('NoWashing', sql.VarChar, NoWashing)
        .query(`DELETE FROM WashingProduksiOutput WHERE NoWashing = @NoWashing`);
      const rqDelOut2 = new sql.Request(tx);
      await rqDelOut2.input('NoWashing', sql.VarChar, NoWashing)
        .query(`DELETE FROM BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`);

      if (hasProduksi) {
        // insert per detail yang DateUsage IS NULL (agar konsisten dengan create)
        const dets = await new sql.Request(tx)
          .input('NoWashing', sql.VarChar, NoWashing)
          .query(`SELECT NoSak FROM Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL ORDER BY NoSak`);
        const insertWpoSql = `
          INSERT INTO WashingProduksiOutput (NoProduksi, NoWashing, NoSak)
          VALUES (@NoProduksi, @NoWashing, @NoSak)
        `;
        for (const row of dets.recordset) {
          const rqWpo = new sql.Request(tx);
          await rqWpo
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
          .query(`SELECT NoSak FROM Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL ORDER BY NoSak`);
        const insertBsoSql = `
          INSERT INTO BongkarSusunOutputWashing (NoBongkarSusun, NoWashing, NoSak)
          VALUES (@NoBongkarSusun, @NoWashing, @NoSak)
        `;
        for (const row of dets.recordset) {
          const rqBso = new sql.Request(tx);
          await rqBso
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
      header: { NoWashing, ...header },
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



// Hapus 1 header + semua output + details (jika aman)
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

    // pastikan exist + lock
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`);
    if (exist.recordset.length === 0) {
      const e = new Error(`NoWashing ${NoWashing} tidak ditemukan`);
      e.statusCode = 404; throw e;
    }

    // cek apakah ada detail terpakai
    const rqUsed = new sql.Request(tx);
    const used = await rqUsed
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`
        SELECT TOP 1 1
        FROM Washing_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing AND DateUsage IS NOT NULL
      `);
    if (used.recordset.length > 0) {
      const e = new Error('Tidak bisa hapus: terdapat detail yang sudah terpakai (DateUsage IS NOT NULL).');
      e.statusCode = 409; throw e;
    }

    // hapus output dulu (hindari FK)
    await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM WashingProduksiOutput WHERE NoWashing = @NoWashing`);

    await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`);

    // hapus semua details (yg belum terpakai)
    const delDet = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL`);

    // hapus header
    const delHead = await new sql.Request(tx)
      .input('NoWashing', sql.VarChar, NoWashing)
      .query(`DELETE FROM Washing_h WHERE NoWashing = @NoWashing`);

    await tx.commit();

    return {
      NoWashing,
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: 'WashingProduksiOutput + BongkarSusunOutputWashing'
      }
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
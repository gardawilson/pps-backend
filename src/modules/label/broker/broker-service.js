// services/broker-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const {
  getBlokLokasiFromKodeProduksi,
} = require('../../../core/shared/mesin-location-helper'); 

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     // ✅ sekarang sudah lastClosed-based
  formatYMD,
} = require('../../../core/shared/tutup-transaksi-guard');



// GET all header Broker with pagination & search (mirror of Washing.getAll)
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      h.NoBroker,
      h.DateCreate,
      h.IdJenisPlastik,
      jp.Jenis AS NamaJenisPlastik,
      h.IdWarehouse,
      w.NamaWarehouse,
      h.Blok,                   -- dari header
      h.IdLokasi,               -- dari header
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,

      -- kolom kualitas/notes
      h.Density,
      h.Moisture,
      h.MaxMeltTemp,
      h.MinMeltTemp,
      h.MFI,
      h.VisualNote,
      h.Density2,
      h.Density3,
      h.Moisture2,
      h.Moisture3,

      -- 🔎 Tambahan sesuai permintaan
      MAX(bpo.NoProduksi)         AS NoProduksi,        -- dari BrokerProduksiOutput
      MAX(m.NamaMesin)            AS NamaMesin,         -- via BrokerProduksi_h → MstMesin
      MAX(bsob.NoBongkarSusun)    AS NoBongkarSusun    -- dari BongkarSusunOutputBroker
    FROM Broker_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse    w  ON w.IdWarehouse     = h.IdWarehouse

    -- Header → Output Produksi (ambil NoProduksi)
    LEFT JOIN dbo.BrokerProduksiOutput bpo
      ON bpo.NoBroker = h.NoBroker
    -- Output → Header Produksi (ambil IdMesin)
    LEFT JOIN dbo.BrokerProduksi_h bp
      ON bp.NoProduksi = bpo.NoProduksi
    -- Mesin (ambil NamaMesin)
    LEFT JOIN dbo.MstMesin m
      ON m.IdMesin = bp.IdMesin

    -- Bongkar Susun (ambil NoBongkarSusun)
    LEFT JOIN dbo.BongkarSusunOutputBroker bsob
      ON bsob.NoBroker = h.NoBroker

    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBroker LIKE @search
               OR jp.Jenis LIKE @search
               OR w.NamaWarehouse LIKE @search
               OR bpo.NoProduksi LIKE @search
               OR m.NamaMesin LIKE @search
               OR bsob.NoBongkarSusun LIKE @search
             )`
          : ''
      }
      AND EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NULL
      )
    GROUP BY
      h.NoBroker, h.DateCreate, h.IdJenisPlastik, jp.Jenis,
      h.IdWarehouse, w.NamaWarehouse, h.IdStatus,
      h.Density, h.Moisture, h.MaxMeltTemp, h.MinMeltTemp, h.MFI, h.VisualNote,
      h.Density2, h.Density3, h.Moisture2, h.Moisture3,
      h.Blok, h.IdLokasi
    ORDER BY h.NoBroker DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoBroker) AS total
    FROM Broker_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse    w  ON w.IdWarehouse     = h.IdWarehouse
    LEFT JOIN dbo.BrokerProduksiOutput bpo
      ON bpo.NoBroker = h.NoBroker
    LEFT JOIN dbo.BrokerProduksi_h bp
      ON bp.NoProduksi = bpo.NoProduksi
    LEFT JOIN dbo.MstMesin m
      ON m.IdMesin = bp.IdMesin
    LEFT JOIN dbo.BongkarSusunOutputBroker bsob
      ON bsob.NoBroker = h.NoBroker
    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBroker LIKE @search
               OR jp.Jenis LIKE @search
               OR w.NamaWarehouse LIKE @search
               OR bpo.NoProduksi LIKE @search
               OR m.NamaMesin LIKE @search
               OR bsob.NoBongkarSusun LIKE @search
             )`
          : ''
      }
      AND EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NULL
      )
  `;

  request.input('offset', sql.Int, offset).input('limit', sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map(item => ({ ...item }));
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};


// GET details by NoBroker (mirror Washing.getWashingDetailByNoWashing)
exports.getBrokerDetailByNoBroker = async (nobroker) => {
  const pool = await poolPromise;

  const result = await pool.request()
    .input('NoBroker', sql.VarChar, nobroker)
    .query(`
      SELECT
        d.NoBroker,
        d.NoSak,
        -- Jika IsPartial = 1, maka Berat dikurangi total dari BrokerPartial
        CASE 
          WHEN d.IsPartial = 1 THEN 
            d.Berat - ISNULL((
              SELECT SUM(p.Berat)
              FROM dbo.BrokerPartial p
              WHERE p.NoBroker = d.NoBroker
                AND p.NoSak = d.NoSak
            ), 0)
          ELSE d.Berat
        END AS Berat,
        d.DateUsage,
        d.IsPartial,
        d.IdLokasi
      FROM dbo.Broker_d d
      WHERE d.NoBroker = @NoBroker
      ORDER BY d.NoSak
    `);

  // Optional: format tanggal agar rapi di frontend
  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? '0' + n : n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  };

  return result.recordset.map(item => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) }),
  }));
};


// -------- utils
function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

/**
 * Generate next NoBroker using prefix (default 'E.') and numeric tail.
 * Examples in your data show "E.487834". You can pick a width:
 * - If you want fixed-width: width=10 => "E.0000487835"
 * - If you want non-padded like your sample: pass width=0 (we’ll keep raw increment)
 */
/**
 * Generate next NoBroker like: D.0000000001
 */
async function generateNextNoBroker(tx, { prefix = 'D.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoBroker
    FROM dbo.Broker_h AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE h.NoBroker LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(h.NoBroker, LEN(@prefix) + 1, 50)) DESC, h.NoBroker DESC
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoBroker;          // e.g. "D.0000000001"
    const numericPart = last.substring(prefix.length); // "0000000001"
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;                         // increment by 1
  return prefix + padLeft(next, width);             // e.g. "D.0000000002"
}

exports.createBrokerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : [];

  const NoProduksi = payload?.NoProduksi?.toString().trim() || null;
  const NoBongkarSusun = payload?.NoBongkarSusun?.toString().trim() || null;

  const badReq = (msg) => { const e = new Error(msg); e.statusCode = 400; return e; };
  if (!header.IdJenisPlastik) throw badReq('IdJenisPlastik is required');
  if (!header.IdWarehouse) throw badReq('IdWarehouse is required');
  if (!header.CreateBy) throw badReq('CreateBy is required');
  if (!Array.isArray(details) || details.length === 0) throw badReq('Details must contain at least 1 item');

  const hasProduksi = !!NoProduksi;
  const hasBongkar = !!NoBongkarSusun;
  if (hasProduksi && hasBongkar) throw badReq('NoProduksi and NoBongkarSusun cannot both be provided');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // [A] TUTUP TRANSAKSI CHECK (CREATE)
    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate);
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create broker',
      useLock: true,
    });

    // 0) Auto-isi Blok & IdLokasi dari kode produksi / bongkar susun
    const needBlok = header.Blok == null || String(header.Blok).trim() === '';
    const needLokasi = header.IdLokasi == null; // lebih aman daripada !header.IdLokasi

    if (needBlok || needLokasi) {
      let lokasi = null;

      if (hasProduksi) {
        lokasi = await getBlokLokasiFromKodeProduksi({ kode: NoProduksi, runner: tx });
      } else if (hasBongkar) {
        // BG juga lewat fungsi yang sama, karena config kamu STATIC mapping BG.
        lokasi = await getBlokLokasiFromKodeProduksi({ kode: NoBongkarSusun, runner: tx });
      }

      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    // 1) Generate NoBroker
    const generatedNo = await generateNextNoBroker(tx, { prefix: 'D.', width: 10 });

    const exist = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, generatedNo)
      .query(`SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK) WHERE NoBroker = @NoBroker`);

    header.NoBroker = (exist.recordset.length > 0)
      ? await generateNextNoBroker(tx, { prefix: 'E.', width: 6 })
      : generatedNo;

    // 2) Insert header
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO dbo.Broker_h (
        NoBroker, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Density, Moisture, MaxMeltTemp, MinMeltTemp, MFI, VisualNote,
        Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
      ) VALUES (
        @NoBroker, @IdJenisPlastik, @IdWarehouse,
        @DateCreate,
        @IdStatus, @CreateBy, @DateTimeCreate,
        @Density, @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @VisualNote,
        @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `;

    await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, header.NoBroker)
      .input('IdJenisPlastik', sql.Int, header.IdJenisPlastik)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('DateCreate', sql.Date, effectiveDateCreate) // ✅ checked date == saved date
      .input('IdStatus', sql.Int, header.IdStatus ?? 1)
      .input('CreateBy', sql.VarChar, header.CreateBy)
      .input('DateTimeCreate', sql.DateTime, nowDateTime)
      .input('Density', sql.Decimal(10, 3), header.Density ?? null)
      .input('Moisture', sql.Decimal(10, 3), header.Moisture ?? null)
      .input('MaxMeltTemp', sql.Decimal(10, 3), header.MaxMeltTemp ?? null)
      .input('MinMeltTemp', sql.Decimal(10, 3), header.MinMeltTemp ?? null)
      .input('MFI', sql.Decimal(10, 3), header.MFI ?? null)
      .input('VisualNote', sql.VarChar, header.VisualNote ?? null)
      .input('Density2', sql.Decimal(10, 3), header.Density2 ?? null)
      .input('Density3', sql.Decimal(10, 3), header.Density3 ?? null)
      .input('Moisture2', sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input('Moisture3', sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
      .query(insertHeaderSql);

    // 3) Insert details
    const insertDetailSql = `
      INSERT INTO dbo.Broker_d (NoBroker, NoSak, Berat, DateUsage, IsPartial)
      VALUES (@NoBroker, @NoSak, @Berat, NULL, @IsPartial)
    `;

    let detailCount = 0;
    for (const d of details) {
      await new sql.Request(tx)
        .input('NoBroker', sql.VarChar, header.NoBroker)
        .input('NoSak', sql.Int, d.NoSak)
        .input('Berat', sql.Decimal(18, 3), d.Berat ?? 0)
        .input('IsPartial', sql.Int, d.IsPartial ?? 0)
        .query(insertDetailSql);
      detailCount++;
    }

    // 4) Optional outputs
    let outputTarget = null;
    let outputCount = 0;

    if (hasProduksi) {
      const insertProdSql = `
        INSERT INTO dbo.BrokerProduksiOutput (NoProduksi, NoBroker, NoSak)
        VALUES (@NoProduksi, @NoBroker, @NoSak)
      `;
      for (const d of details) {
        await new sql.Request(tx)
          .input('NoProduksi', sql.VarChar, NoProduksi)
          .input('NoBroker', sql.VarChar, header.NoBroker)
          .input('NoSak', sql.Int, d.NoSak)
          .query(insertProdSql);
        outputCount++;
      }
      outputTarget = 'BrokerProduksiOutput';
    } else if (hasBongkar) {
      const insertBsoSql = `
        INSERT INTO dbo.BongkarSusunOutputBroker (NoBongkarSusun, NoBroker, NoSak)
        VALUES (@NoBongkarSusun, @NoBroker, @NoSak)
      `;
      for (const d of details) {
        await new sql.Request(tx)
          .input('NoBongkarSusun', sql.VarChar, NoBongkarSusun)
          .input('NoBroker', sql.VarChar, header.NoBroker)
          .input('NoSak', sql.Int, d.NoSak)
          .query(insertBsoSql);
        outputCount++;
      }
      outputTarget = 'BongkarSusunOutputBroker';
    }

    await tx.commit();

    return {
      header: {
        NoBroker: header.NoBroker,
        IdJenisPlastik: header.IdJenisPlastik,
        IdWarehouse: header.IdWarehouse,
        IdStatus: header.IdStatus ?? 1,
        CreateBy: header.CreateBy,
        DateCreate: formatYMD(effectiveDateCreate),
        DateTimeCreate: nowDateTime,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
      },
      counts: { detailsInserted: detailCount, outputInserted: outputCount },
      outputTarget,
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};


exports.updateBrokerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBroker = payload?.NoBroker?.toString().trim();
  if (!NoBroker) {
    const e = new Error('NoBroker (path) is required');
    e.statusCode = 400; throw e;
  }

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : null;

  const NoProduksi = payload?.NoProduksi?.toString().trim() || null;
  const NoBongkarSusun = payload?.NoBongkarSusun?.toString().trim() || null;

  const hasProduksi = !!NoProduksi;
  const hasBongkar  = !!NoBongkarSusun;
  if (hasProduksi && hasBongkar) {
    const e = new Error('NoProduksi and NoBongkarSusun cannot both be provided');
    e.statusCode = 400; throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // [A] Ambil DateCreate existing + lock header row
    // ===============================
    const rqHead = new sql.Request(tx);
    const headRes = await rqHead
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        SELECT TOP 1 NoBroker, CONVERT(date, DateCreate) AS DateCreate
        FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker
      `);

    if (headRes.recordset.length === 0) {
      const e = new Error(`NoBroker ${NoBroker} not found`);
      e.statusCode = 404; throw e;
    }

    const dbDateCreate = headRes.recordset[0].DateCreate; // Date object (date-only from SQL)
    const oldDate = toDateOnly(dbDateCreate);

    // ===============================
    // [B] Tutup transaksi check untuk UPDATE
    // 1) Selalu cek tanggal data lama (oldDate) -> boleh diedit?
    // 2) Kalau user mengubah DateCreate -> cek juga tanggal baru (newDate)
    // ===============================
    await assertNotLocked({
      date: oldDate,
      runner: tx,
      action: `update broker ${NoBroker} (tanggal lama ${formatYMD(oldDate)})`,
      useLock: true,
    });

    // Determine apakah user request ubah DateCreate
    // - header.DateCreate === undefined : tidak ubah
    // - header.DateCreate === null      : minta set ke "today"
    // - header.DateCreate adalah string : minta set ke tanggal itu
    let newDate = null;
    let willUpdateDateCreate = false;

    if (Object.prototype.hasOwnProperty.call(header, 'DateCreate')) {
      willUpdateDateCreate = true;

      if (header.DateCreate === null) {
        // "set to today" (date-only)
        newDate = resolveEffectiveDateForCreate(null); // today date-only
      } else {
        newDate = toDateOnly(header.DateCreate);
        if (!newDate) {
          const e = new Error('DateCreate invalid');
          e.statusCode = 400; throw e;
        }
      }

      // cek tanggal baru juga (target)
      await assertNotLocked({
        date: newDate,
        runner: tx,
        action: `update broker ${NoBroker} (tanggal baru ${formatYMD(newDate)})`,
        useLock: true,
      });
    }

    // ===============================
    // [C] Update header (partial/dynamic)
    // ===============================
    const setParts = [];
    const reqHeader = new sql.Request(tx).input('NoBroker', sql.VarChar, NoBroker);

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf('IdJenisPlastik', 'IdJenisPlastik', sql.Int, header.IdJenisPlastik);
    setIf('IdWarehouse',    'IdWarehouse',    sql.Int, header.IdWarehouse);

    // DateCreate handling (tanpa GETDATE() langsung)
    if (willUpdateDateCreate) {
      setParts.push('DateCreate = @DateCreate');
      reqHeader.input('DateCreate', sql.Date, newDate); // ✅ sudah lolos tutup transaksi
    }

    setIf('IdStatus',     'IdStatus',     sql.Int, header.IdStatus);
    setIf('Density',      'Density',      sql.Decimal(10,3), header.Density ?? null);
    setIf('Moisture',     'Moisture',     sql.Decimal(10,3), header.Moisture ?? null);
    setIf('MaxMeltTemp',  'MaxMeltTemp',  sql.Decimal(10,3), header.MaxMeltTemp ?? null);
    setIf('MinMeltTemp',  'MinMeltTemp',  sql.Decimal(10,3), header.MinMeltTemp ?? null);
    setIf('MFI',          'MFI',          sql.Decimal(10,3), header.MFI ?? null);
    setIf('VisualNote',   'VisualNote',   sql.VarChar, header.VisualNote ?? null);
    setIf('Density2',     'Density2',     sql.Decimal(10,3), header.Density2 ?? null);
    setIf('Density3',     'Density3',     sql.Decimal(10,3), header.Density3 ?? null);
    setIf('Moisture2',    'Moisture2',    sql.Decimal(10,3), header.Moisture2 ?? null);
    setIf('Moisture3',    'Moisture3',    sql.Decimal(10,3), header.Moisture3 ?? null);
    setIf('Blok',         'Blok',         sql.VarChar, header.Blok ?? null);
    setIf('IdLokasi',     'IdLokasi',     sql.VarChar, header.IdLokasi ?? null);

    if (setParts.length > 0) {
      const sqlUpdateHeader = `
        UPDATE dbo.Broker_h SET ${setParts.join(', ')}
        WHERE NoBroker = @NoBroker
      `;
      await reqHeader.query(sqlUpdateHeader);
    }

    // ===============================
    // [D] Replace details (only if details sent) for rows with DateUsage IS NULL
    // ===============================
    let detailAffected = 0;
    if (details) {
      await new sql.Request(tx)
        .input('NoBroker', sql.VarChar, NoBroker)
        .query(`
          DELETE FROM dbo.Broker_d
          WHERE NoBroker = @NoBroker AND DateUsage IS NULL
        `);

      const insertDetailSql = `
        INSERT INTO dbo.Broker_d (NoBroker, NoSak, Berat, DateUsage, IsPartial, IdLokasi)
        VALUES (@NoBroker, @NoSak, @Berat, NULL, @IsPartial, @IdLokasi)
      `;

      for (const d of details) {
        await new sql.Request(tx)
          .input('NoBroker', sql.VarChar, NoBroker)
          .input('NoSak', sql.Int, d.NoSak)
          .input('Berat', sql.Decimal(18,3), d.Berat ?? 0)
          .input('IsPartial', sql.Int, d.IsPartial ?? 0)
          .input('IdLokasi', sql.VarChar, d.IdLokasi ?? header.IdLokasi ?? null)
          .query(insertDetailSql);

        detailAffected++;
      }
    }

    // ===============================
    // [E] Conditional outputs
    // ===============================
    let outputTarget = null;
    let outputCount = 0;
    const sentAnyOutputField =
      (Object.prototype.hasOwnProperty.call(payload, 'NoProduksi') ||
       Object.prototype.hasOwnProperty.call(payload, 'NoBongkarSusun'));

    if (sentAnyOutputField) {
      await new sql.Request(tx)
        .input('NoBroker', sql.VarChar, NoBroker)
        .query(`DELETE FROM dbo.BrokerProduksiOutput WHERE NoBroker = @NoBroker`);

      await new sql.Request(tx)
        .input('NoBroker', sql.VarChar, NoBroker)
        .query(`DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBroker = @NoBroker`);

      if (hasProduksi) {
        const dets = await new sql.Request(tx)
          .input('NoBroker', sql.VarChar, NoBroker)
          .query(`SELECT NoSak FROM dbo.Broker_d WHERE NoBroker = @NoBroker AND DateUsage IS NULL ORDER BY NoSak`);

        const insertProdSql = `
          INSERT INTO dbo.BrokerProduksiOutput (NoProduksi, NoBroker, NoSak)
          VALUES (@NoProduksi, @NoBroker, @NoSak)
        `;

        for (const row of dets.recordset) {
          await new sql.Request(tx)
            .input('NoProduksi', sql.VarChar, NoProduksi)
            .input('NoBroker', sql.VarChar, NoBroker)
            .input('NoSak', sql.Int, row.NoSak)
            .query(insertProdSql);
          outputCount++;
        }
        outputTarget = 'BrokerProduksiOutput';
      } else if (hasBongkar) {
        const dets = await new sql.Request(tx)
          .input('NoBroker', sql.VarChar, NoBroker)
          .query(`SELECT NoSak FROM dbo.Broker_d WHERE NoBroker = @NoBroker AND DateUsage IS NULL ORDER BY NoSak`);

        const insertBsoSql = `
          INSERT INTO dbo.BongkarSusunOutputBroker (NoBongkarSusun, NoBroker, NoSak)
          VALUES (@NoBongkarSusun, @NoBroker, @NoSak)
        `;

        for (const row of dets.recordset) {
          await new sql.Request(tx)
            .input('NoBongkarSusun', sql.VarChar, NoBongkarSusun)
            .input('NoBroker', sql.VarChar, NoBroker)
            .input('NoSak', sql.Int, row.NoSak)
            .query(insertBsoSql);
          outputCount++;
        }
        outputTarget = 'BongkarSusunOutputBroker';
      }
    }

    await tx.commit();

    return {
      header: {
        NoBroker,
        ...header,
        // optional: return effective DateCreate if changed
        ...(willUpdateDateCreate ? { DateCreate: formatYMD(newDate) } : {}),
      },
      counts: {
        detailsAffected: detailAffected,
        outputInserted: outputCount,
      },
      outputTarget,
      note: details
        ? 'Details with DateUsage IS NULL were replaced according to payload.'
        : 'Details were not modified.',
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};



// Delete 1 header + outputs + details (safe)
// Delete 1 Broker header + outputs + details + partials (safe)
exports.deleteBrokerCascade = async (nobroker) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBroker = (nobroker || '').toString().trim();
  if (!NoBroker) {
    const e = new Error('NoBroker is required');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // [A] Ambil header + DateCreate + lock row
    // ===============================
    const rqHead = new sql.Request(tx);
    const headRes = await rqHead
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        SELECT TOP 1
          NoBroker,
          CONVERT(date, DateCreate) AS DateCreate
        FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker
      `);

    if (headRes.recordset.length === 0) {
      const e = new Error(`NoBroker ${NoBroker} not found`);
      e.statusCode = 404;
      throw e;
    }

    const oldDate = toDateOnly(headRes.recordset[0].DateCreate);

    // ===============================
    // [B] TUTUP TRANSAKSI CHECK (DELETE)
    // RULE: trxDate <= lastClosed => reject
    // ===============================
    await assertNotLocked({
      date: oldDate,
      runner: tx,
      action: `delete broker ${NoBroker} (tanggal ${formatYMD(oldDate)})`,
      useLock: true,
    });

    // ===============================
    // [C] Block if any detail is already used
    // ===============================
    const used = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        SELECT TOP 1 1
        FROM dbo.Broker_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      const e = new Error(
        'Cannot delete: some details are already used (DateUsage IS NOT NULL).'
      );
      e.statusCode = 409;
      throw e;
    }

    // ===============================
    // [D] Delete outputs first (avoid FK)
    // ===============================
    await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE FROM dbo.BrokerProduksiOutput
        WHERE NoBroker = @NoBroker
      `);

    await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE FROM dbo.BongkarSusunOutputBroker
        WHERE NoBroker = @NoBroker
      `);

    // ===============================
    // [E] Delete partial INPUT usages that reference BrokerPartial for this NoBroker
    // ===============================
    const delBrokerInputPartial = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE bip
        FROM dbo.BrokerProduksiInputBrokerPartial AS bip
        INNER JOIN dbo.BrokerPartial AS bp
          ON bp.NoBrokerPartial = bip.NoBrokerPartial
        WHERE bp.NoBroker = @NoBroker
      `);

    const delMixerInputPartial = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE mip
        FROM dbo.MixerProduksiInputBrokerPartial AS mip
        INNER JOIN dbo.BrokerPartial AS bp
          ON bp.NoBrokerPartial = mip.NoBrokerPartial
        WHERE bp.NoBroker = @NoBroker
      `);

    // ===============================
    // [F] Delete partial rows themselves
    // ===============================
    const delPartial = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE FROM dbo.BrokerPartial
        WHERE NoBroker = @NoBroker
      `);

    // ===============================
    // [G] Delete details (only the ones not used)
    // ===============================
    const delDet = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE FROM dbo.Broker_d
        WHERE NoBroker = @NoBroker AND DateUsage IS NULL
      `);

    // ===============================
    // [H] Delete header
    // ===============================
    const delHead = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE FROM dbo.Broker_h
        WHERE NoBroker = @NoBroker
      `);

    await tx.commit();

    return {
      NoBroker,
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: 'BrokerProduksiOutput + BongkarSusunOutputBroker',
        partials: {
          brokerPartial: delPartial.rowsAffected?.[0] ?? 0,
          brokerInputPartial: delBrokerInputPartial.rowsAffected?.[0] ?? 0,
          mixerInputPartial: delMixerInputPartial.rowsAffected?.[0] ?? 0,
        },
      },
      period: formatYMD(oldDate),
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}

    // Map FK constraint error
    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || 'Delete failed due to foreign key constraint.';
    }
    throw e;
  }
};




exports.getPartialInfoByBrokerAndSak = async (nobroker, nosak) => {
  const pool = await poolPromise;
  const req = pool.request()
    .input('NoBroker', sql.VarChar, nobroker)
    .input('NoSak', sql.Int, nosak);

  const query = `
    SELECT
      p.NoBrokerPartial,
      p.NoBroker,
      p.NoSak,
      p.Berat,                          -- partial weight
      mpi.NoProduksi,                   -- produksi number (if exists)
      mph.TglProduksi,                  -- production date
      mph.IdMesin,                      -- machine id
      mm.NamaMesin,                     -- machine name from MstMesin
      mph.IdOperator,
      mph.Jam,
      mph.Shift
    FROM dbo.BrokerPartial p
    LEFT JOIN dbo.MixerProduksiInputBrokerPartial mpi
      ON mpi.NoBrokerPartial = p.NoBrokerPartial
    LEFT JOIN dbo.MixerProduksi_h mph
      ON mph.NoProduksi = mpi.NoProduksi
    LEFT JOIN dbo.MstMesin mm
      ON mph.IdMesin = mm.IdMesin
    WHERE p.NoBroker = @NoBroker
      AND p.NoSak = @NoSak
    ORDER BY p.NoBrokerPartial ASC
  `;

  const result = await req.query(query);

  // Compute total partial weightd
  const totalPartialWeight = result.recordset.reduce((sum, row) => {
    const w = typeof row.Berat === 'number' ? row.Berat : Number(row.Berat) || 0;
    return sum + w;
  }, 0);

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? '0' + n : '' + n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = result.recordset.map((r) => ({
    NoBrokerPartial: r.NoBrokerPartial,
    NoBroker: r.NoBroker,
    NoSak: r.NoSak,
    Berat: r.Berat,
    NoProduksi: r.NoProduksi || null,
    TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
    IdMesin: r.IdMesin || null,
    NamaMesin: r.NamaMesin || null,
    IdOperator: r.IdOperator || null,
    Jam: r.Jam || null,
    Shift: r.Shift || null
  }));

  return { totalPartialWeight, rows };
};
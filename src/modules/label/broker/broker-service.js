// services/broker-service.js
const { sql, poolPromise } = require('../../../core/config/db');

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
      AND NOT EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NOT NULL
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
        AND d.DateUsage IS NULL
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

  // ---- validation
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

    // 1) Generate NoBroker (ignore client-provided NoBroker if any)
    const generatedNo = await generateNextNoBroker(tx, { prefix: 'D.', width: 10 }); // adjust width if you prefer
    // Double-check uniqueness
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBroker', sql.VarChar, generatedNo)
      .query(`SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK) WHERE NoBroker = @NoBroker`);
    header.NoBroker = (exist.recordset.length > 0)
      ? await generateNextNoBroker(tx, { prefix: 'E.', width: 6 })
      : generatedNo;

    // 2) Insert header
    const nowDateOnly = header.DateCreate || null; // if null -> use GETDATE() (date only)
    const nowDateTime = new Date();               // DateTimeCreate

    const insertHeaderSql = `
      INSERT INTO dbo.Broker_h (
        NoBroker, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Density, Moisture, MaxMeltTemp, MinMeltTemp, MFI, VisualNote,
        Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
      ) VALUES (
        @NoBroker, @IdJenisPlastik, @IdWarehouse,
        ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
        @IdStatus, @CreateBy, @DateTimeCreate,
        @Density, @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @VisualNote,
        @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `;

    const rqHeader = new sql.Request(tx);
    rqHeader
      .input('NoBroker', sql.VarChar, header.NoBroker)
      .input('IdJenisPlastik', sql.Int, header.IdJenisPlastik)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1) // default PASS=1
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
      .input('IdLokasi', sql.VarChar, header.IdLokasi ?? null);

    if (nowDateOnly) rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
    await rqHeader.query(insertHeaderSql);

    // 3) Insert details
    const insertDetailSql = `
      INSERT INTO dbo.Broker_d (NoBroker, NoSak, Berat, DateUsage, IsPartial, IdLokasi)
      VALUES (@NoBroker, @NoSak, @Berat, NULL, @IsPartial, @IdLokasi)
    `;
    let detailCount = 0;
    for (const d of details) {
      const rqDet = new sql.Request(tx);
      await rqDet
        .input('NoBroker', sql.VarChar, header.NoBroker)
        .input('NoSak', sql.Int, d.NoSak)
        .input('Berat', sql.Decimal(18, 3), d.Berat ?? 0)
        .input('IsPartial', sql.Int, d.IsPartial ?? 0) // default 0 if not provided
        .input('IdLokasi', sql.VarChar, d.IdLokasi ?? header.IdLokasi ?? null)
        .query(insertDetailSql);
      detailCount++;
    }

    // 4) Optional outputs (mutually exclusive)
    let outputTarget = null;
    let outputCount = 0;

    if (hasProduksi) {
      const insertProdSql = `
        INSERT INTO dbo.BrokerProduksiOutput (NoProduksi, NoBroker, NoSak)
        VALUES (@NoProduksi, @NoBroker, @NoSak)
      `;
      for (const d of details) {
        const rqProd = new sql.Request(tx);
        await rqProd
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
        const rqBso = new sql.Request(tx);
        await rqBso
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
        DateCreate: nowDateOnly || 'GETDATE()',
        DateTimeCreate: nowDateTime,
        Density: header.Density ?? null,
        Moisture: header.Moisture ?? null,
        MaxMeltTemp: header.MaxMeltTemp ?? null,
        MinMeltTemp: header.MinMeltTemp ?? null,
        MFI: header.MFI ?? null,
        VisualNote: header.VisualNote ?? null,
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


exports.updateBrokerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBroker = payload?.NoBroker?.toString().trim();
  if (!NoBroker) {
    const e = new Error('NoBroker (path) is required');
    e.statusCode = 400; throw e;
  }

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : null; // null => don’t touch details

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

    // 0) Ensure header exists
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK) WHERE NoBroker = @NoBroker`);
    if (exist.recordset.length === 0) {
      const e = new Error(`NoBroker ${NoBroker} not found`);
      e.statusCode = 404; throw e;
    }

    // 1) Update header (partial/dynamic)
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
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null) {
        setParts.push('DateCreate = CONVERT(date, GETDATE())');
      } else {
        setIf('DateCreate', 'DateCreate', sql.Date, new Date(header.DateCreate));
      }
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

    // Optional audit:
    // if (payload.UpdateBy) {
    //   setParts.push('UpdateBy = @UpdateBy');
    //   setParts.push('DateTimeUpdate = @DateTimeUpdate');
    //   reqHeader.input('UpdateBy', sql.VarChar, payload.UpdateBy);
    //   reqHeader.input('DateTimeUpdate', sql.DateTime, new Date());
    // }

    if (setParts.length > 0) {
      const sqlUpdateHeader = `
        UPDATE dbo.Broker_h SET ${setParts.join(', ')}
        WHERE NoBroker = @NoBroker
      `;
      await reqHeader.query(sqlUpdateHeader);
    }

    // 2) Replace details (only if details sent) for rows with DateUsage IS NULL
    let detailAffected = 0;
    if (details) {
      const rqDel = new sql.Request(tx);
      await rqDel
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
        const rqDet = new sql.Request(tx);
        await rqDet
          .input('NoBroker', sql.VarChar, NoBroker)
          .input('NoSak', sql.Int, d.NoSak)
          .input('Berat', sql.Decimal(18,3), d.Berat ?? 0)
          .input('IsPartial', sql.Int, d.IsPartial ?? 0)
          .input('IdLokasi', sql.VarChar, d.IdLokasi ?? header.IdLokasi ?? null)
          .query(insertDetailSql);
        detailAffected++;
      }
    }

    // 3) Conditional outputs
    // Only touch outputs if client sends either NoProduksi or NoBongkarSusun.
    let outputTarget = null;
    let outputCount = 0;
    const sentAnyOutputField = (payload.hasOwnProperty('NoProduksi') || payload.hasOwnProperty('NoBongkarSusun'));

    if (sentAnyOutputField) {
      // Clear both first
      await new sql.Request(tx)
        .input('NoBroker', sql.VarChar, NoBroker)
        .query(`DELETE FROM dbo.BrokerProduksiOutput WHERE NoBroker = @NoBroker`);
      await new sql.Request(tx)
        .input('NoBroker', sql.VarChar, NoBroker)
        .query(`DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBroker = @NoBroker`);

      // Re-insert depending on which field was sent
      if (hasProduksi) {
        // Insert per current details with DateUsage IS NULL (consistent with create)
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
      header: { NoBroker, ...header },
      counts: {
        detailsAffected: detailAffected,
        outputInserted: outputCount
      },
      outputTarget,
      note: details
        ? 'Details with DateUsage IS NULL were replaced according to payload.'
        : 'Details were not modified.'
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};


// Delete 1 header + outputs + details (safe)
exports.deleteBrokerCascade = async (nobroker) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBroker = (nobroker || '').toString().trim();
  if (!NoBroker) {
    const e = new Error('NoBroker is required');
    e.statusCode = 400; throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) Ensure header exists + lock
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        SELECT 1
        FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker
      `);
    if (exist.recordset.length === 0) {
      const e = new Error(`NoBroker ${NoBroker} not found`);
      e.statusCode = 404; throw e;
    }

    // 1) Block if any detail is already used
    const rqUsed = new sql.Request(tx);
    const used = await rqUsed
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        SELECT TOP 1 1
        FROM dbo.Broker_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker AND DateUsage IS NOT NULL
      `);
    if (used.recordset.length > 0) {
      const e = new Error('Cannot delete: some details are already used (DateUsage IS NOT NULL).');
      e.statusCode = 409; throw e;
    }

    // 2) Delete outputs first (avoid FK)
    await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`DELETE FROM dbo.BrokerProduksiOutput WHERE NoBroker = @NoBroker`);

    await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBroker = @NoBroker`);

    // 3) Delete details (only the ones not used)
    const delDet = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`
        DELETE FROM dbo.Broker_d
        WHERE NoBroker = @NoBroker AND DateUsage IS NULL
      `);

    // 4) Delete header
    const delHead = await new sql.Request(tx)
      .input('NoBroker', sql.VarChar, NoBroker)
      .query(`DELETE FROM dbo.Broker_h WHERE NoBroker = @NoBroker`);

    await tx.commit();

    return {
      NoBroker,
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: 'BrokerProduksiOutput + BongkarSusunOutputBroker',
      },
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
}


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
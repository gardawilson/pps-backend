// services/crusher-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const {
  getBlokLokasiFromKodeProduksi,
} = require('../../../core/shared/mesin-location-helper'); 


/**
 * Tables used:
 * - dbo.Crusher c
 * - dbo.MstCrusher mc          (IdCrusher -> NamaCrusher)
 * - dbo.MstWarehouse w         (IdWarehouse -> NamaWarehouse)
 * - dbo.CrusherProduksiOutput cpo        (NoCrusher -> NoCrusherProduksi)
 * - dbo.CrusherProduksi_h ch             (NoCrusherProduksi -> IdMesin)
 * - dbo.MstMesin m                       (IdMesin -> NamaMesin)
 * - dbo.BongkarSusunOutputCrusher bs     (NoCrusher -> NoBongkarSusun)
 */
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const whereSearch = search
    ? `
      AND (
        c.NoCrusher LIKE @search
        OR c.Blok LIKE @search
        OR CONVERT(VARCHAR(20), c.IdLokasi) LIKE @search
        OR CONVERT(VARCHAR(20), c.IdWarehouse) LIKE @search
        OR ISNULL(w.NamaWarehouse,'') LIKE @search
        OR ISNULL(mc.NamaCrusher,'') LIKE @search
        OR ISNULL(cpo.NoCrusherProduksi,'') LIKE @search
        OR ISNULL(m.NamaMesin,'') LIKE @search
        OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
      )
    `
    : '';

  const baseQuery = `
    SELECT
      c.NoCrusher,
      c.DateCreate,
      c.IdCrusher,
      mc.NamaCrusher,
      c.IdWarehouse,
      w.NamaWarehouse,
      c.Blok,
      c.IdLokasi,
      c.Berat,
      CASE
        WHEN c.IdStatus = 1 THEN 'PASS'
        WHEN c.IdStatus = 0 THEN 'HOLD'
        ELSE ''
      END AS StatusText,

      -- Joins collapsed (if multiple rows exist, pick one deterministically)
      MAX(cpo.NoCrusherProduksi) AS CrusherNoProduksi,
      MAX(m.NamaMesin)          AS CrusherNamaMesin,
      MAX(bs.NoBongkarSusun)    AS NoBongkarSusun

    FROM [dbo].[Crusher] c
    LEFT JOIN [dbo].[MstCrusher] mc
      ON mc.IdCrusher = c.IdCrusher
    LEFT JOIN [dbo].[MstWarehouse] w
      ON w.IdWarehouse = c.IdWarehouse

    -- Produksi chain
    LEFT JOIN [dbo].[CrusherProduksiOutput] cpo
      ON cpo.NoCrusher = c.NoCrusher
    LEFT JOIN [dbo].[CrusherProduksi_h] ch
      ON ch.NoCrusherProduksi = cpo.NoCrusherProduksi
    LEFT JOIN [dbo].[MstMesin] m
      ON m.IdMesin = ch.IdMesin

    -- Bongkar Susun
    LEFT JOIN [dbo].[BongkarSusunOutputCrusher] bs
      ON bs.NoCrusher = c.NoCrusher

    WHERE 1=1
      AND c.DateUsage IS NULL
      ${whereSearch}

    GROUP BY
      c.NoCrusher, c.DateCreate, c.IdCrusher, mc.NamaCrusher,
      c.IdWarehouse, w.NamaWarehouse, c.Blok, c.IdLokasi, c.Berat, c.IdStatus

    ORDER BY c.NoCrusher DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT c.NoCrusher) AS total
    FROM [dbo].[Crusher] c
    LEFT JOIN [dbo].[MstCrusher] mc
      ON mc.IdCrusher = c.IdCrusher
    LEFT JOIN [dbo].[MstWarehouse] w
      ON w.IdWarehouse = c.IdWarehouse
    LEFT JOIN [dbo].[CrusherProduksiOutput] cpo
      ON cpo.NoCrusher = c.NoCrusher
    LEFT JOIN [dbo].[CrusherProduksi_h] ch
      ON ch.NoCrusherProduksi = cpo.NoCrusherProduksi
    LEFT JOIN [dbo].[MstMesin] m
      ON m.IdMesin = ch.IdMesin
    LEFT JOIN [dbo].[BongkarSusunOutputCrusher] bs
      ON bs.NoCrusher = c.NoCrusher
    WHERE 1=1
      AND c.DateUsage IS NULL
      ${whereSearch}
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit',  sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data  = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};



function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// Generate next NoCrusher: e.g. 'F.0000000002'
async function generateNextNoCrusher(tx, { prefix = 'F.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 c.NoCrusher
    FROM [dbo].[Crusher] AS c WITH (UPDLOCK, HOLDLOCK)
    WHERE c.NoCrusher LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(c.NoCrusher, LEN(@prefix) + 1, 50)) DESC,
             c.NoCrusher DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoCrusher; // e.g. "F.0000000001"
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

exports.createCrusherCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const processedCode = (payload?.ProcessedCode || '').toString().trim(); // '', 'G.****', 'BG.****'

  // ---- validation
  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };
  if (!header.IdCrusher)   throw badReq('IdCrusher is required');
  if (!header.IdWarehouse) throw badReq('IdWarehouse is required');
  if (!header.CreateBy)    throw badReq('CreateBy is required');

  // Identify target from ProcessedCode (optional)
  const hasProcessed = processedCode.length > 0;
  let processedType = null; // 'PRODUKSI' | 'BONGKAR'
  if (hasProcessed) {
    if (processedCode.startsWith('G.'))       processedType = 'PRODUKSI'; // CrusherProduksiOutput
    else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';  // BongkarSusunOutputCrusher
    else throw badReq('ProcessedCode prefix not recognized (use G. or BG.)');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) Auto-isi Blok & IdLokasi dari kode produksi / bongkar susun (jika header belum isi)
    if (!header.Blok || !header.IdLokasi) {
      if (processedCode) {
        const lokasi = await getBlokLokasiFromKodeProduksi({
          kode: processedCode,
          runner: tx,
        });

        if (lokasi) {
          if (!header.Blok) header.Blok = lokasi.Blok;
          if (!header.IdLokasi) header.IdLokasi = lokasi.IdLokasi;
        }
      } 
    }

    // 1) Generate NoCrusher
    const generatedNo = await generateNextNoCrusher(tx, { prefix: 'F.', width: 10 });

    // Double-check uniqueness
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoCrusher', sql.VarChar, generatedNo)
      .query(`SELECT 1 FROM [dbo].[Crusher] WITH (UPDLOCK, HOLDLOCK) WHERE NoCrusher = @NoCrusher`);

    const noCrusher = (exist.recordset.length > 0)
      ? await generateNextNoCrusher(tx, { prefix: 'F.', width: 10 }) // extremely rare
      : generatedNo;

    // 2) Insert header into dbo.Crusher
    const nowDateOnly = header.DateCreate || null; // if null -> GETDATE() (date)
    const insertHeaderSql = `
      INSERT INTO [dbo].[Crusher] (
        NoCrusher, DateCreate, IdCrusher, IdWarehouse, DateUsage,
        Berat, IdStatus, Blok, IdLokasi, CreateBy, DateTimeCreate
      )
      VALUES (
        @NoCrusher,
        ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
        @IdCrusher, @IdWarehouse, NULL,
        @Berat, @IdStatus, @Blok, @IdLokasi, @CreateBy, GETDATE()
      );
    `;

    const rqHeader = new sql.Request(tx);
    rqHeader
      .input('NoCrusher', sql.VarChar, noCrusher)
      .input('IdCrusher', sql.Int, header.IdCrusher)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1) // default 1
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
      .input('CreateBy', sql.VarChar, header.CreateBy);

    if (nowDateOnly) rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
    await rqHeader.query(insertHeaderSql);

    // 3) Optional: insert mapping based on ProcessedCode prefix
    let mappingTable = null;
    if (processedType === 'PRODUKSI') {
      // G. → CrusherProduksiOutput (NoCrusherProduksi, NoCrusher)
      const q = `
        INSERT INTO [dbo].[CrusherProduksiOutput] (NoCrusherProduksi, NoCrusher)
        VALUES (@Processed, @NoCrusher);
      `;
      await new sql.Request(tx)
        .input('Processed', sql.VarChar, processedCode)
        .input('NoCrusher', sql.VarChar, noCrusher)
        .query(q);
      mappingTable = 'CrusherProduksiOutput';
    } else if (processedType === 'BONGKAR') {
      // BG. → BongkarSusunOutputCrusher (NoBongkarSusun, NoCrusher)
      const q = `
        INSERT INTO [dbo].[BongkarSusunOutputCrusher] (NoBongkarSusun, NoCrusher)
        VALUES (@Processed, @NoCrusher);
      `;
      await new sql.Request(tx)
        .input('Processed', sql.VarChar, processedCode)
        .input('NoCrusher', sql.VarChar, noCrusher)
        .query(q);
      mappingTable = 'BongkarSusunOutputCrusher';
    }

    await tx.commit();

    return {
      header: {
        NoCrusher: noCrusher,
        DateCreate: nowDateOnly || 'GETDATE()',
        IdCrusher: header.IdCrusher,
        IdWarehouse: header.IdWarehouse,
        Berat: header.Berat ?? null,
        IdStatus: header.IdStatus ?? 1,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
        CreateBy: header.CreateBy,
        DateTimeCreate: 'GETDATE()',
      },
      processed: {
        code: processedCode || null,
        type: processedType,       // PRODUKSI / BONGKAR / null
        mappingTable,              // which table got the insert, if any
      },
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};



exports.updateCrusher = async (noCrusher, payload = {}) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  // Whitelist fields allowed to update
  const fields = [
    { key: 'DateCreate',  type: sql.Date },
    { key: 'IdCrusher',   type: sql.Int },
    { key: 'IdWarehouse', type: sql.Int },
    { key: 'DateUsage',   type: sql.Date },
    { key: 'Berat',       type: sql.Decimal(18, 3) },
    { key: 'IdStatus',    type: sql.Int },
    { key: 'Blok',        type: sql.VarChar },
    { key: 'IdLokasi',    type: sql.VarChar },
  ];

  const toUpdate = fields.filter(f => payload[f.key] !== undefined);
  if (toUpdate.length === 0) {
    const e = new Error('No valid fields to update');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // Ensure row exists and lock it
    const exists = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, noCrusher)
      .query(`
        SELECT 1
        FROM [dbo].[Crusher] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher
      `);

    if (exists.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Crusher not found: ${noCrusher}`);
      e.statusCode = 404;
      throw e;
    }

    // Build SET clause and bind params
    const setClauses = [];
    const rq = new sql.Request(tx);
    rq.input('NoCrusher', sql.VarChar, noCrusher);

    for (const f of toUpdate) {
      const param = `p_${f.key}`;
      setClauses.push(`[${f.key}] = @${param}`);

      // Handle types & nulls
      if (f.type === sql.Date) {
        // allow null to clear date
        if (payload[f.key] === null) {
          rq.input(param, f.type, null);
        } else {
          rq.input(param, f.type, new Date(payload[f.key]));
        }
      } else if (f.type.declaration?.startsWith('decimal')) {
        rq.input(param, f.type, payload[f.key] === null ? null : Number(payload[f.key]));
      } else {
        rq.input(param, f.type, payload[f.key]);
      }
    }

    await rq.query(`
      UPDATE [dbo].[Crusher]
      SET ${setClauses.join(', ')}
      WHERE NoCrusher = @NoCrusher;
    `);

    await tx.commit();

    return { updated: true, updatedFields: toUpdate.map(f => f.key) };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};




exports.deleteCrusherCascade = async (noCrusher) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // Step 0: lock/ensure exists early (optional but helpful)
    const exist = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, noCrusher)
      .query(`
        SELECT 1
        FROM [dbo].[Crusher] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher
      `);

    if (exist.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Crusher not found: ${noCrusher}`);
      e.statusCode = 404;
      throw e;
    }

    // Step 1: delete mappings if exist
    const mappingQueries = [
      // G.* → mapping table: CrusherProduksiOutput
      `DELETE FROM [dbo].[CrusherProduksiOutput] WHERE NoCrusher = @NoCrusher`,
      // BG.* → mapping table: BongkarSusunOutputCrusher
      `DELETE FROM [dbo].[BongkarSusunOutputCrusher] WHERE NoCrusher = @NoCrusher`,
    ];

    for (const q of mappingQueries) {
      await new sql.Request(tx)
        .input('NoCrusher', sql.VarChar, noCrusher)
        .query(q);
    }

    // Step 2: delete header
    const delHeader = `
      DELETE FROM [dbo].[Crusher]
      WHERE NoCrusher = @NoCrusher;
    `;
    const result = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, noCrusher)
      .query(delHeader);

    await tx.commit();

    if ((result.rowsAffected?.[0] ?? 0) === 0) {
      const e = new Error(`Crusher not found: ${noCrusher}`);
      e.statusCode = 404;
      throw e;
    }

    return { deleted: true, noCrusher };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};


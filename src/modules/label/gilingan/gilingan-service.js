// services/labels/gilingan-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const {
  getBlokLokasiFromKodeProduksi,
} = require('../../../core/shared/mesin-location-helper'); 


exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
      SELECT
        g.NoGilingan,
        g.DateCreate,
        g.IdGilingan,
        mg.NamaGilingan,

        -- 🔹 Berat sudah dikurangi partial (jika IsPartial = 1)
        CASE 
          WHEN g.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(g.Berat, 0) - ISNULL(MAX(gp.TotalPartial), 0) < 0 
                THEN 0
              ELSE ISNULL(g.Berat, 0) - ISNULL(MAX(gp.TotalPartial), 0)
            END
          ELSE ISNULL(g.Berat, 0)
        END AS Berat,

        g.IsPartial,
        g.IdStatus,
        CASE 
          WHEN g.IdStatus = 1 THEN 'PASS'
          WHEN g.IdStatus = 0 THEN 'HOLD'
          ELSE ''
        END AS StatusText,
        g.Blok,
        g.IdLokasi,

        -- 🔗 Production chain: GilinganProduksiOutput → GilinganProduksi_h → MstMesin
        MAX(gpo.NoProduksi)     AS GilinganNoProduksi,
        MAX(m.NamaMesin)        AS GilinganNamaMesin,

        -- 🔗 Bongkar Susun mapping
        MAX(bs.NoBongkarSusun)  AS NoBongkarSusun

      FROM [dbo].[Gilingan] g
      LEFT JOIN [dbo].[MstGilingan] mg
             ON mg.IdGilingan = g.IdGilingan

      -- 🔹 Aggregate partial per NoGilingan
      LEFT JOIN (
        SELECT
          NoGilingan,
          SUM(ISNULL(Berat, 0)) AS TotalPartial
        FROM [dbo].[GilinganPartial]
        GROUP BY NoGilingan
      ) gp
        ON gp.NoGilingan = g.NoGilingan

      -- Gilingan production chain
      LEFT JOIN [dbo].[GilinganProduksiOutput] gpo
             ON gpo.NoGilingan = g.NoGilingan
      LEFT JOIN [dbo].[GilinganProduksi_h] gh
             ON gh.NoProduksi = gpo.NoProduksi
      LEFT JOIN [dbo].[MstMesin] m
             ON m.IdMesin = gh.IdMesin

      -- Bongkar Susun mapping
      LEFT JOIN [dbo].[BongkarSusunOutputGilingan] bs
             ON bs.NoGilingan = g.NoGilingan

      WHERE 1=1
        AND g.DateUsage IS NULL
        ${
          search
            ? `AND (
                 g.NoGilingan LIKE @search
                 OR g.Blok LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdLokasi) LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdGilingan) LIKE @search
                 OR ISNULL(mg.NamaGilingan,'') LIKE @search
                 OR ISNULL(gpo.NoProduksi,'') LIKE @search
                 OR ISNULL(m.NamaMesin,'') LIKE @search
                 OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
               )`
            : ''
        }
      GROUP BY
        g.NoGilingan,
        g.DateCreate,
        g.IdGilingan,
        mg.NamaGilingan,
        g.Berat,
        g.IsPartial,
        g.IdStatus,
        g.Blok,
        g.IdLokasi
      ORDER BY g.NoGilingan DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
    `;

  const countQuery = `
      SELECT COUNT(DISTINCT g.NoGilingan) AS total
      FROM [dbo].[Gilingan] g
      LEFT JOIN [dbo].[MstGilingan] mg
             ON mg.IdGilingan = g.IdGilingan
      LEFT JOIN [dbo].[GilinganProduksiOutput] gpo
             ON gpo.NoGilingan = g.NoGilingan
      LEFT JOIN [dbo].[GilinganProduksi_h] gh
             ON gh.NoProduksi = gpo.NoProduksi
      LEFT JOIN [dbo].[MstMesin] m
             ON m.IdMesin = gh.IdMesin
      LEFT JOIN [dbo].[BongkarSusunOutputGilingan] bs
             ON bs.NoGilingan = g.NoGilingan
      WHERE 1=1
        AND g.DateUsage IS NULL
        ${
          search
            ? `AND (
                 g.NoGilingan LIKE @search
                 OR g.Blok LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdLokasi) LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdGilingan) LIKE @search
                 OR ISNULL(mg.NamaGilingan,'') LIKE @search
                 OR ISNULL(gpo.NoProduksi,'') LIKE @search
                 OR ISNULL(m.NamaMesin,'') LIKE @search
                 OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
               )`
            : ''
        }
    `;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);
  if (search) {
    request.input('search', sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};



function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// Generate next NoGilingan: e.g. 'V.0000000002'
async function generateNextNoGilingan(tx, { prefix = 'V.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 g.NoGilingan
    FROM [dbo].[Gilingan] AS g WITH (UPDLOCK, HOLDLOCK)
    WHERE g.NoGilingan LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(g.NoGilingan, LEN(@prefix) + 1, 50)) DESC,
             g.NoGilingan DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoGilingan; // e.g. "V.0000000001"
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

exports.createGilingan = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = (payload?.outputCode || '').toString().trim(); // 'W.****' or 'BG.****'

  // ---- validation
  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  if (!header.IdGilingan) {
    throw badReq('IdGilingan is required');
  }

  // ❗ NOW REQUIRED: must link to either production or bongkar
  if (!outputCode) {
    throw badReq('outputCode is required (must be W. or BG. prefix)');
  }

  let outputType = null; // 'PRODUKSI' | 'BONGKAR'
  if (outputCode.startsWith('W.')) {
    outputType = 'PRODUKSI'; // GilinganProduksiOutput
  } else if (outputCode.startsWith('BG.')) {
    outputType = 'BONGKAR';  // BongkarSusunOutputGilingan
  } else {
    throw badReq('outputCode prefix not recognized (use W. or BG.)');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

        // 0) Auto-isi Blok & IdLokasi dari kode produksi / bongkar susun (jika header belum isi)
        if (!header.Blok || !header.IdLokasi) {
          if (outputCode) {
            const lokasi = await getBlokLokasiFromKodeProduksi({
              kode: outputCode,
              runner: tx,
            });
    
            if (lokasi) {
              if (!header.Blok) header.Blok = lokasi.Blok;
              if (!header.IdLokasi) header.IdLokasi = lokasi.IdLokasi;
            }
          } 
        }

    // 1) Generate NoGilingan
    const generatedNo = await generateNextNoGilingan(tx, {
      prefix: 'V.',
      width: 10,
    });

    // Double-check uniqueness (very rare)
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoGilingan', sql.VarChar, generatedNo)
      .query(`
        SELECT 1
        FROM [dbo].[Gilingan] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    const noGilingan =
      exist.recordset.length > 0
        ? await generateNextNoGilingan(tx, { prefix: 'V.', width: 10 })
        : generatedNo;

   // 2) Insert header into dbo.Gilingan
const nowDateOnly = header.DateCreate || null; // if null -> GETDATE() (date)
const insertHeaderSql = `
  INSERT INTO [dbo].[Gilingan] (
    NoGilingan,
    DateCreate,
    IdGilingan,
    DateUsage,
    Berat,
    IsPartial,
    IdStatus,
    Blok,
    IdLokasi
  )
  VALUES (
    @NoGilingan,
    ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
    @IdGilingan,
    NULL,
    @Berat,
    @IsPartial,
    @IdStatus,
    @Blok,
    @IdLokasi
  );
`;

const rqHeader = new sql.Request(tx);

// normalize IdLokasi safely
const rawIdLokasi = header.IdLokasi;
let idLokasiVal = null;
if (rawIdLokasi !== undefined && rawIdLokasi !== null) {
  idLokasiVal = String(rawIdLokasi).trim();
  if (idLokasiVal.length === 0) {
    idLokasiVal = null;
  }
}

rqHeader
  .input('NoGilingan', sql.VarChar, noGilingan)
  .input('IdGilingan', sql.Int, header.IdGilingan)
  .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
  .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
  .input('IdStatus', sql.Int, header.IdStatus ?? 1)
  .input('Blok', sql.VarChar, header.Blok ?? null)
  .input('IdLokasi', sql.Int, idLokasiVal);

if (nowDateOnly) {
  rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
}

await rqHeader.query(insertHeaderSql);

    // 3) REQUIRED mapping based on outputType
    let mappingTable = null;
    if (outputType === 'PRODUKSI') {
      // W. → GilinganProduksiOutput (NoProduksi, NoGilingan, Berat)
      const q = `
        INSERT INTO [dbo].[GilinganProduksiOutput] (NoProduksi, NoGilingan, Berat)
        VALUES (@OutputCode, @NoGilingan, @Berat);
      `;
      await new sql.Request(tx)
        .input('OutputCode', sql.VarChar, outputCode)
        .input('NoGilingan', sql.VarChar, noGilingan)
        .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
        .query(q);
      mappingTable = 'GilinganProduksiOutput';
    } else if (outputType === 'BONGKAR') {
      // BG. → BongkarSusunOutputGilingan (NoBongkarSusun, NoGilingan)
      const q = `
        INSERT INTO [dbo].[BongkarSusunOutputGilingan] (NoBongkarSusun, NoGilingan)
        VALUES (@OutputCode, @NoGilingan);
      `;
      await new sql.Request(tx)
        .input('OutputCode', sql.VarChar, outputCode)
        .input('NoGilingan', sql.VarChar, noGilingan)
        .query(q);
      mappingTable = 'BongkarSusunOutputGilingan';
    }

    await tx.commit();

    return {
      header: {
        NoGilingan: noGilingan,
        DateCreate: nowDateOnly || 'GETDATE()',
        IdGilingan: header.IdGilingan,
        Berat: header.Berat ?? null,
        IsPartial: header.IsPartial ?? 0,
        IdStatus: header.IdStatus ?? 1,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
      },
      output: {
        code: outputCode,
        type: outputType,   // 'PRODUKSI' / 'BONGKAR'
        mappingTable,       // table used
      },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};



exports.updateGilingan = async (noGilingan, payload = {}) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  // Whitelist fields allowed to update
  const fields = [
    { key: 'DateCreate', type: sql.Date },
    { key: 'IdGilingan', type: sql.Int },
    { key: 'DateUsage', type: sql.Date },
    { key: 'Berat', type: sql.Decimal(18, 3) },
    { key: 'IsPartial', type: sql.Bit },
    { key: 'IdStatus', type: sql.Int },
    { key: 'Blok', type: sql.VarChar },
    { key: 'IdLokasi', type: sql.VarChar }, // ✅ now VarChar, not Int
  ];

  const toUpdate = fields.filter((f) => payload[f.key] !== undefined);
  if (toUpdate.length === 0) {
    const e = new Error('No valid fields to update');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // Ensure row exists and lock it
    const exists = await new sql.Request(tx)
      .input('NoGilingan', sql.VarChar, noGilingan)
      .query(`
        SELECT 1
        FROM [dbo].[Gilingan] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    if (exists.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Gilingan not found: ${noGilingan}`);
      e.statusCode = 404;
      throw e;
    }

    // Build SET clause and bind params
    const setClauses = [];
    const rq = new sql.Request(tx);
    rq.input('NoGilingan', sql.VarChar, noGilingan);

    for (const f of toUpdate) {
      const param = `p_${f.key}`;
      setClauses.push(`[${f.key}] = @${param}`);

      const val = payload[f.key];

      if (f.type === sql.Date) {
        // allow null to clear date
        if (val === null) {
          rq.input(param, f.type, null);
        } else {
          rq.input(param, f.type, new Date(val));
        }
      } else if (f.type.declaration?.startsWith('decimal')) {
        rq.input(
          param,
          f.type,
          val === null ? null : Number(val)
        );
      } else if (f.key === 'IdLokasi') {
        // ✅ normalize IdLokasi as VarChar
        let idLokasiVal = null;
        if (val !== undefined && val !== null) {
          const s = String(val).trim();
          idLokasiVal = s.length === 0 ? null : s;
        }
        rq.input(param, f.type, idLokasiVal);
      } else {
        rq.input(param, f.type, val);
      }
    }

    await rq.query(`
      UPDATE [dbo].[Gilingan]
      SET ${setClauses.join(', ')}
      WHERE NoGilingan = @NoGilingan;
    `);

    await tx.commit();

    return { updated: true, updatedFields: toUpdate.map((f) => f.key) };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw err;
  }
};


  exports.deleteGilinganCascade = async (noGilingan) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);
  
      // Step 0: ensure header exists + lock it
      const exist = await new sql.Request(tx)
        .input('NoGilingan', sql.VarChar, noGilingan)
        .query(`
          SELECT 1
          FROM [dbo].[Gilingan] WITH (UPDLOCK, HOLDLOCK)
          WHERE NoGilingan = @NoGilingan
        `);
  
      if (exist.recordset.length === 0) {
        await tx.rollback();
        const e = new Error(`Gilingan not found: ${noGilingan}`);
        e.statusCode = 404;
        throw e;
      }
  
      // Step 1: delete PARTIALS first
      const delPartial = `
        DELETE FROM [dbo].[GilinganPartial]
        WHERE NoGilingan = @NoGilingan;
      `;
      await new sql.Request(tx)
        .input('NoGilingan', sql.VarChar, noGilingan)
        .query(delPartial);
  
      // Step 2: delete OUTPUT mappings
      const mappingQueries = [
        // From Bongkar Susun → Gilingan
        `
          DELETE FROM [dbo].[BongkarSusunOutputGilingan]
          WHERE NoGilingan = @NoGilingan
        `,
        // From Produksi → Gilingan
        `
          DELETE FROM [dbo].[GilinganProduksiOutput]
          WHERE NoGilingan = @NoGilingan
        `,
      ];
  
      for (const q of mappingQueries) {
        await new sql.Request(tx)
          .input('NoGilingan', sql.VarChar, noGilingan)
          .query(q);
      }
  
      // Step 3: delete header
      const delHeader = `
        DELETE FROM [dbo].[Gilingan]
        WHERE NoGilingan = @NoGilingan;
      `;
      const result = await new sql.Request(tx)
        .input('NoGilingan', sql.VarChar, noGilingan)
        .query(delHeader);
  
      await tx.commit();
  
      if ((result.rowsAffected?.[0] ?? 0) === 0) {
        const e = new Error(`Gilingan not found: ${noGilingan}`);
        e.statusCode = 404;
        throw e;
      }
  
      return {
        deleted: true,
        noGilingan,
      };
    } catch (err) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw err;
    }
  };


  exports.getPartialInfoByGilingan = async (nogilingan) => {
    const pool = await poolPromise;
  
    const req = pool
      .request()
      .input('NoGilingan', sql.VarChar, nogilingan);
  
    const query = `
      ;WITH BasePartial AS (
        SELECT
          gp.NoGilinganPartial,
          gp.NoGilingan,
          gp.Berat
        FROM dbo.GilinganPartial gp
        WHERE gp.NoGilingan = @NoGilingan
      ),
      Consumed AS (
        SELECT
          b.NoGilinganPartial,
          'BROKER' AS SourceType,
          b.NoProduksi
        FROM dbo.BrokerProduksiInputGilinganPartial b
  
        UNION ALL
  
        SELECT
          i.NoGilinganPartial,
          'INJECT' AS SourceType,
          i.NoProduksi
        FROM dbo.InjectProduksiInputGilinganPartial i
  
        UNION ALL
  
        SELECT
          m.NoGilinganPartial,
          'MIXER' AS SourceType,
          m.NoProduksi
        FROM dbo.MixerProduksiInputGilinganPartial m
  
        UNION ALL
  
        SELECT
          w.NoGilinganPartial,
          'WASHING' AS SourceType,
          w.NoProduksi
        FROM dbo.WashingProduksiInputGilinganPartial w
      )
      SELECT
        bp.NoGilinganPartial,
        bp.NoGilingan,
        bp.Berat,                 -- partial weight
  
        c.SourceType,             -- BROKER / INJECT / MIXER / WASHING / NULL
        c.NoProduksi,
  
        -- unified production fields from the 4 header tables
        COALESCE(bph.TglProduksi, iph.TglProduksi, mph.TglProduksi, wph.TglProduksi) AS TglProduksi,
        COALESCE(bph.IdMesin,     iph.IdMesin,     mph.IdMesin,     wph.IdMesin)     AS IdMesin,
        COALESCE(bph.IdOperator,  iph.IdOperator,  mph.IdOperator,  wph.IdOperator)  AS IdOperator,
        COALESCE(bph.Jam,         iph.Jam,         mph.Jam,         wph.JamKerja)    AS Jam,
        COALESCE(bph.Shift,       iph.Shift,       mph.Shift,       wph.Shift)       AS Shift,
  
        mm.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoGilinganPartial = bp.NoGilinganPartial
  
      -- Production headers for each flow
      LEFT JOIN dbo.BrokerProduksi_h bph
        ON c.SourceType = 'BROKER'
       AND bph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.InjectProduksi_h iph
        ON c.SourceType = 'INJECT'
       AND iph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MixerProduksi_h mph
        ON c.SourceType = 'MIXER'
       AND mph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.WashingProduksi_h wph
        ON c.SourceType = 'WASHING'
       AND wph.NoProduksi = c.NoProduksi
  
      -- Machine name
      LEFT JOIN dbo.MstMesin mm
        ON mm.IdMesin = COALESCE(bph.IdMesin, iph.IdMesin, mph.IdMesin, wph.IdMesin)
  
      ORDER BY
        bp.NoGilinganPartial ASC,
        c.SourceType ASC,
        c.NoProduksi ASC;
    `;
  
    const result = await req.query(query);
  
    // total partial weight (unique per NoGilinganPartial)
    const seen = new Set();
    let totalPartialWeight = 0;
  
    for (const row of result.recordset) {
      const key = row.NoGilinganPartial;
      if (!seen.has(key)) {
        seen.add(key);
        const w =
          typeof row.Berat === 'number'
            ? row.Berat
            : Number(row.Berat) || 0;
        totalPartialWeight += w;
      }
    }
  
    const formatDate = (date) => {
      if (!date) return null;
      const d = new Date(date);
      const pad = (n) => (n < 10 ? '0' + n : '' + n);
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    };
  
    const rows = result.recordset.map((r) => ({
      NoGilinganPartial: r.NoGilinganPartial,
      NoGilingan: r.NoGilingan,
      Berat: r.Berat,
  
      SourceType: r.SourceType || null,      // 'BROKER' | 'INJECT' | 'MIXER' | 'WASHING' | null
      NoProduksi: r.NoProduksi || null,
  
      TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
      IdMesin: r.IdMesin || null,
      NamaMesin: r.NamaMesin || null,
      IdOperator: r.IdOperator || null,
      Jam: r.Jam || null,
      Shift: r.Shift || null,
    }));
  
    return { totalPartialWeight, rows };
  };
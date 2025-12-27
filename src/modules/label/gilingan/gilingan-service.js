// services/labels/gilingan-service.js
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

  if (!header.IdGilingan) throw badReq('IdGilingan is required');

  // must link to either production or bongkar
  if (!outputCode) throw badReq('outputCode is required (must be W. or BG. prefix)');

  let outputType = null; // 'PRODUKSI' | 'BONGKAR'
  if (outputCode.startsWith('W.')) outputType = 'PRODUKSI';
  else if (outputCode.startsWith('BG.')) outputType = 'BONGKAR';
  else throw badReq('outputCode prefix not recognized (use W. or BG.)');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE) - UTC
    // ===============================
    const nowDateOnly = resolveEffectiveDateForCreate(header.DateCreate); // ✅ UTC date-only
    await assertNotLocked({
      date: nowDateOnly,
      runner: tx,
      action: 'create gilingan',
      useLock: true,
    });

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
    const generatedNo = await generateNextNoGilingan(tx, { prefix: 'V.', width: 10 });

    // Double-check uniqueness (very rare)
    const exist = await new sql.Request(tx)
      .input('NoGilingan', sql.VarChar, generatedNo)
      .query(`
        SELECT 1
        FROM [dbo].[Gilingan] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    const noGilingan = (exist.recordset.length > 0)
      ? await generateNextNoGilingan(tx, { prefix: 'V.', width: 10 })
      : generatedNo;

    // 2) Insert header into dbo.Gilingan
    // ✅ selalu pakai @DateCreate (UTC) agar tidak shift -1 hari
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
        @DateCreate,
        @IdGilingan,
        NULL,
        @Berat,
        @IsPartial,
        @IdStatus,
        @Blok,
        @IdLokasi
      );
    `;

    // normalize IdLokasi safely
    const rawIdLokasi = header.IdLokasi;
    let idLokasiVal = null;
    if (rawIdLokasi !== undefined && rawIdLokasi !== null) {
      const s = String(rawIdLokasi).trim();
      idLokasiVal = s.length ? Number(s) : null;
      if (s.length && Number.isNaN(idLokasiVal)) {
        const e = new Error('IdLokasi must be a number');
        e.statusCode = 400;
        e.meta = { field: 'IdLokasi', value: rawIdLokasi };
        throw e;
      }
    }

    const rqHeader = new sql.Request(tx);
    rqHeader
      .input('NoGilingan', sql.VarChar, noGilingan)
      .input('DateCreate', sql.Date, nowDateOnly) // ✅ UTC date-only
      .input('IdGilingan', sql.Int, header.IdGilingan)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1)
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.Int, idLokasiVal);

    await rqHeader.query(insertHeaderSql);

    // 3) REQUIRED mapping based on outputType
    let mappingTable = null;

    if (outputType === 'PRODUKSI') {
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
        DateCreate: formatYMD(nowDateOnly), // ✅ konsisten UTC string
        IdGilingan: header.IdGilingan,
        Berat: header.Berat ?? null,
        IsPartial: header.IsPartial ?? 0,
        IdStatus: header.IdStatus ?? 1,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
      },
      output: {
        code: outputCode,
        type: outputType,
        mappingTable,
      },
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};



exports.updateGilingan = async (noGilingan, payload = {}) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  // Whitelist fields allowed to update
  const fields = [
    { key: 'DateCreate', type: sql.Date, isDateOnly: true },
    { key: 'IdGilingan', type: sql.Int },
    { key: 'DateUsage',  type: sql.Date, isDateOnly: true },
    { key: 'Berat',      type: sql.Decimal(18, 3) },
    { key: 'IsPartial',  type: sql.Bit },
    { key: 'IdStatus',   type: sql.Int },
    { key: 'Blok',       type: sql.VarChar },
    { key: 'IdLokasi',   type: sql.VarChar }, // ✅ VarChar sesuai kode kamu
  ];

  const toUpdate = fields.filter((f) => payload[f.key] !== undefined);
  if (toUpdate.length === 0) {
    const e = new Error('No valid fields to update');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // Lock row + ambil DateCreate existing (optional, tapi bagus untuk audit/rule lain)
    const head = await new sql.Request(tx)
      .input('NoGilingan', sql.VarChar, noGilingan)
      .query(`
        SELECT TOP 1 NoGilingan, CONVERT(date, DateCreate) AS DateCreate
        FROM [dbo].[Gilingan] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    if (head.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Gilingan not found: ${noGilingan}`);
      e.statusCode = 404;
      throw e;
    }

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (UPDATE)
    // ===============================
    if (payload.DateCreate !== undefined) {
      const d = (payload.DateCreate === null || payload.DateCreate === '')
        ? null
        : toDateOnly(payload.DateCreate);

      if (d) {
        await assertNotLocked({
          date: d,
          runner: tx,
          action: 'update gilingan (DateCreate)',
          useLock: true,
        });
      }
      // kalau kamu tidak mau DateCreate jadi null, bisa throw di sini.
    }

    if (payload.DateUsage !== undefined) {
      const d = (payload.DateUsage === null || payload.DateUsage === '')
        ? null
        : toDateOnly(payload.DateUsage);

      if (d) {
        await assertNotLocked({
          date: d,
          runner: tx,
          action: 'update gilingan (DateUsage)',
          useLock: true,
        });
      }
    }

    // Build SET clause and bind params
    const setClauses = [];
    const rq = new sql.Request(tx);
    rq.input('NoGilingan', sql.VarChar, noGilingan);

    for (const f of toUpdate) {
      const param = `p_${f.key}`;
      setClauses.push(`[${f.key}] = @${param}`);

      const val = payload[f.key];

      // DATE fields -> UTC date-only (anti shift -1)
      if (f.isDateOnly) {
        if (val === null || val === '') {
          rq.input(param, f.type, null);
        } else {
          const d = toDateOnly(val);
          if (!d) {
            const e = new Error(`Invalid date for ${f.key}`);
            e.statusCode = 400;
            e.meta = { field: f.key, value: val };
            throw e;
          }
          rq.input(param, f.type, d); // ✅ UTC date-only
        }
        continue;
      }

      // DECIMAL
      if (f.type?.declaration?.startsWith('decimal')) {
        if (val === null || val === '') {
          rq.input(param, f.type, null);
        } else {
          const num = Number(val);
          if (Number.isNaN(num)) {
            const e = new Error(`Invalid number for ${f.key}`);
            e.statusCode = 400;
            e.meta = { field: f.key, value: val };
            throw e;
          }
          rq.input(param, f.type, num);
        }
        continue;
      }

      // IdLokasi as VarChar normalization
      if (f.key === 'IdLokasi') {
        let idLokasiVal = null;
        if (val !== undefined && val !== null) {
          const s = String(val).trim();
          idLokasiVal = s.length === 0 ? null : s;
        }
        rq.input(param, f.type, idLokasiVal);
        continue;
      }

      // default
      rq.input(param, f.type, val);
    }

    await rq.query(`
      UPDATE [dbo].[Gilingan]
      SET ${setClauses.join(', ')}
      WHERE NoGilingan = @NoGilingan;
    `);

    await tx.commit();
    return { updated: true, updatedFields: toUpdate.map((f) => f.key) };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};



  exports.deleteGilinganCascade = async (noGilingan) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const badReq = (msg) => { const e = new Error(msg); e.statusCode = 400; return e; };
  if (!noGilingan || !String(noGilingan).trim()) throw badReq('noGilingan wajib');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // ===============================
    // 0) Lock + ambil DateCreate untuk rule tutup transaksi
    // ===============================
    const head = await new sql.Request(tx)
      .input('NoGilingan', sql.VarChar, noGilingan)
      .query(`
        SELECT TOP 1 NoGilingan, CONVERT(date, DateCreate) AS DateCreate
        FROM [dbo].[Gilingan] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    if (head.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Gilingan not found: ${noGilingan}`);
      e.statusCode = 404;
      throw e;
    }

    const trxDate = head.recordset[0]?.DateCreate ? toDateOnly(head.recordset[0].DateCreate) : null;

    // ===============================
    // 1) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: 'delete gilingan',
      useLock: true,
    });

    // ===============================
    // 2) delete PARTIALS first
    // ===============================
    await new sql.Request(tx)
      .input('NoGilingan', sql.VarChar, noGilingan)
      .query(`
        DELETE FROM [dbo].[GilinganPartial]
        WHERE NoGilingan = @NoGilingan;
      `);

    // ===============================
    // 3) delete OUTPUT mappings
    // ===============================
    const mappingQueries = [
      `DELETE FROM [dbo].[BongkarSusunOutputGilingan] WHERE NoGilingan = @NoGilingan`,
      `DELETE FROM [dbo].[GilinganProduksiOutput]       WHERE NoGilingan = @NoGilingan`,
    ];

    for (const q of mappingQueries) {
      await new sql.Request(tx)
        .input('NoGilingan', sql.VarChar, noGilingan)
        .query(q);
    }

    // ===============================
    // 4) delete header
    // ===============================
    const result = await new sql.Request(tx)
      .input('NoGilingan', sql.VarChar, noGilingan)
      .query(`
        DELETE FROM [dbo].[Gilingan]
        WHERE NoGilingan = @NoGilingan;
      `);

    if ((result.rowsAffected?.[0] ?? 0) === 0) {
      await tx.rollback();
      const e = new Error(`Gilingan not found: ${noGilingan}`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();
    return { deleted: true, noGilingan };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
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
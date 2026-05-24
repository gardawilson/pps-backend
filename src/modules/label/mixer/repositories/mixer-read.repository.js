const { sql, poolPromise } = require("../../../../core/config/db");

async function getAllMixerHeaders({
  page,
  limit,
  search,
  includeUsed = false,
}) {
  const pool = await poolPromise;
  const offset = (page - 1) * limit;
  const dateUsageFilter = includeUsed
    ? ""
    : `AND EXISTS (
        SELECT 1
        FROM dbo.Mixer_d d2
        WHERE d2.NoMixer = h.NoMixer
          AND d2.DateUsage IS NULL
      )`;

  const searchClause = search
    ? `
      AND (
        h.NoMixer LIKE @search
        OR mx.Jenis LIKE @search
        OR h.Blok LIKE @search
        OR CAST(h.IdLokasi AS VARCHAR(20)) LIKE @search
        OR EXISTS (
          SELECT 1
          FROM dbo.MixerProduksiOutput mpo
          INNER JOIN dbo.MixerProduksi_h mph ON mph.NoProduksi = mpo.NoProduksi
          LEFT JOIN dbo.MstMesin m ON m.IdMesin = mph.IdMesin
          WHERE mpo.NoMixer = h.NoMixer
            AND (mpo.NoProduksi LIKE @search OR m.NamaMesin LIKE @search)
        )
        OR EXISTS (
          SELECT 1
          FROM dbo.BongkarSusunOutputMixer bsom
          WHERE bsom.NoMixer = h.NoMixer
            AND bsom.NoBongkarSusun LIKE @search
        )
        OR EXISTS (
          SELECT 1
          FROM dbo.InjectProduksiOutputMixer ipom
          INNER JOIN dbo.InjectProduksi_h iph ON iph.NoProduksi = ipom.NoProduksi
          LEFT JOIN dbo.MstMesin mi ON mi.IdMesin = iph.IdMesin
          WHERE ipom.NoMixer = h.NoMixer
            AND (ipom.NoProduksi LIKE @search OR mi.NamaMesin LIKE @search)
        )
      )
    `
    : "";

  const baseQuery = `
    SELECT h.NoMixer, h.DateCreate, h.IdMixer, mx.Jenis AS NamaMixer, h.IdStatus,
      CASE WHEN h.IdStatus = 1 THEN 'PASS' WHEN h.IdStatus = 0 THEN 'HOLD' ELSE '' END AS StatusText,
      h.Moisture, h.MaxMeltTemp, h.MinMeltTemp, h.MFI, h.Moisture2, h.Moisture3,
      CASE WHEN EXISTS (SELECT 1 FROM dbo.Mixer_d d3 WHERE d3.NoMixer = h.NoMixer AND d3.DateUsage IS NULL) THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS Used,
      ISNULL(CAST(h.HasBeenPrinted AS int), 0) AS HasBeenPrinted, h.Blok, h.IdLokasi,
      outInfo.OutputType, outInfo.OutputCode, outInfo.OutputNamaMesin
    FROM dbo.Mixer_h h
    INNER JOIN dbo.MstMixer mx ON mx.IdMixer = h.IdMixer
    OUTER APPLY (
      SELECT TOP (1) src.OutputType, src.OutputCode, src.OutputNamaMesin
      FROM (
        SELECT 'MIXER_PRODUKSI' AS OutputType, mpo.NoProduksi AS OutputCode, m.NamaMesin AS OutputNamaMesin, 1 AS Priority
        FROM dbo.MixerProduksiOutput mpo
        INNER JOIN dbo.MixerProduksi_h mph ON mph.NoProduksi = mpo.NoProduksi
        LEFT JOIN dbo.MstMesin m ON m.IdMesin = mph.IdMesin
        WHERE mpo.NoMixer = h.NoMixer
        UNION ALL
        SELECT 'INJECT_PRODUKSI', ipom.NoProduksi, mi.NamaMesin, 2
        FROM dbo.InjectProduksiOutputMixer ipom
        INNER JOIN dbo.InjectProduksi_h iph ON iph.NoProduksi = ipom.NoProduksi
        LEFT JOIN dbo.MstMesin mi ON mi.IdMesin = iph.IdMesin
        WHERE ipom.NoMixer = h.NoMixer
        UNION ALL
        SELECT 'BONGKAR_SUSUN', bsom.NoBongkarSusun, 'Bongkar Susun', 3
        FROM dbo.BongkarSusunOutputMixer bsom
        WHERE bsom.NoMixer = h.NoMixer
      ) AS src
      WHERE src.OutputCode IS NOT NULL
      ORDER BY src.Priority, src.OutputCode
    ) AS outInfo
    WHERE 1 = 1 ${searchClause} ${dateUsageFilter}
    ORDER BY h.NoMixer DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoMixer) AS total
    FROM dbo.Mixer_h h
    INNER JOIN dbo.MstMixer mx ON mx.IdMixer = h.IdMixer
    WHERE 1 = 1 ${searchClause} ${dateUsageFilter};
  `;

  const reqData = pool.request();
  reqData.input("offset", sql.Int, offset);
  reqData.input("limit", sql.Int, limit);
  if (search) reqData.input("search", sql.VarChar, `%${search}%`);
  const dataResult = await reqData.query(baseQuery);

  const reqCount = pool.request();
  if (search) reqCount.input("search", sql.VarChar, `%${search}%`);
  const countResult = await reqCount.query(countQuery);

  return {
    data: dataResult.recordset?.map((item) => ({ ...item })) ?? [],
    total: countResult.recordset?.[0]?.total ?? 0,
  };
}

async function getMixerHeaderByNoMixer(noMixer) {
  const pool = await poolPromise;
  return pool.request().input("NoMixer", sql.VarChar(50), noMixer).query(`
    SELECT
      h.NoMixer, h.DateCreate, h.IdMixer, mx.Jenis, mx.Jenis AS NamaMixer,
      MAX(CAST(ISNULL(d.IsPartial, 0) AS int)) AS IsPartial,
      COUNT(d.NoSak) AS JumlahSak,
      SUM(d.Berat) - ISNULL(SUM(mp.Berat), 0) AS SisaBerat,
      h.CreateBy, ISNULL(CAST(h.HasBeenPrinted AS int), 0) AS HasBeenPrinted,
      COALESCE(outInfo.OutputNamaMesin, '-') AS Mesin,
      COALESCE(CAST(outInfo.Shift AS VARCHAR(10)), '-') AS Shift
    FROM dbo.Mixer_h h
    JOIN dbo.MstMixer mx ON mx.IdMixer = h.IdMixer
    JOIN dbo.Mixer_d d ON d.NoMixer = h.NoMixer AND d.DateUsage IS NULL
    LEFT JOIN dbo.MixerPartial mp ON mp.NoMixer = h.NoMixer AND mp.NoSak = d.NoSak
    OUTER APPLY (
      SELECT TOP (1) src.OutputNamaMesin, src.Shift
      FROM (
        SELECT m.NamaMesin AS OutputNamaMesin, mph.Shift AS Shift, 1 AS Priority
        FROM dbo.MixerProduksiOutput mpo
        JOIN dbo.MixerProduksi_h mph ON mph.NoProduksi = mpo.NoProduksi
        LEFT JOIN dbo.MstMesin m ON m.IdMesin = mph.IdMesin
        WHERE mpo.NoMixer = h.NoMixer
        UNION ALL
        SELECT mi.NamaMesin, iph.Shift, 2
        FROM dbo.InjectProduksiOutputMixer ipom
        JOIN dbo.InjectProduksi_h iph ON iph.NoProduksi = ipom.NoProduksi
        LEFT JOIN dbo.MstMesin mi ON mi.IdMesin = iph.IdMesin
        WHERE ipom.NoMixer = h.NoMixer
        UNION ALL
        SELECT bsom.NoBongkarSusun, NULL, 3
        FROM dbo.BongkarSusunOutputMixer bsom
        WHERE bsom.NoMixer = h.NoMixer
      ) AS src
      WHERE src.OutputNamaMesin IS NOT NULL
      ORDER BY src.Priority
    ) AS outInfo
    WHERE h.NoMixer = @NoMixer
    GROUP BY h.NoMixer, h.DateCreate, h.IdMixer, mx.Jenis, h.CreateBy, h.HasBeenPrinted, outInfo.OutputNamaMesin, outInfo.Shift
  `);
}

async function getMixerDetailsByNoMixer(noMixer) {
  const pool = await poolPromise;
  const result = await pool.request().input("NoMixer", sql.VarChar, noMixer)
    .query(`
    SELECT d.NoMixer, d.NoSak,
      CASE WHEN d.IsPartial = 1 THEN
        d.Berat - ISNULL((SELECT SUM(p.Berat) FROM dbo.MixerPartial p WHERE p.NoMixer = d.NoMixer AND p.NoSak = d.NoSak), 0)
      ELSE d.Berat END AS Berat,
      d.DateUsage, d.IsPartial
    FROM dbo.Mixer_d d
    WHERE d.NoMixer = @NoMixer
    ORDER BY d.NoSak;
  `);

  const formatDateTime = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? `0${n}` : String(n));
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  };

  return result.recordset.map((item) => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDateTime(item.DateUsage) }),
  }));
}

async function getPartialInfoRowsByMixerAndSak(nomixer, nosak) {
  const pool = await poolPromise;
  const req = pool
    .request()
    .input("NoMixer", sql.VarChar, nomixer)
    .input("NoSak", sql.Int, nosak);

  const result = await req.query(`
    ;WITH BasePartial AS (
      SELECT mp.NoMixerPartial, mp.NoMixer, mp.NoSak, mp.Berat
      FROM dbo.MixerPartial mp
      WHERE mp.NoMixer = @NoMixer AND mp.NoSak = @NoSak
    ),
    Consumed AS (
      SELECT b.NoMixerPartial, 'BROKER' AS SourceType, b.NoProduksi FROM dbo.BrokerProduksiInputMixerPartial b
      UNION ALL
      SELECT i.NoMixerPartial, 'INJECT' AS SourceType, i.NoProduksi FROM dbo.InjectProduksiInputMixerPartial i
      UNION ALL
      SELECT m.NoMixerPartial, 'MIXER' AS SourceType, m.NoProduksi FROM dbo.MixerProduksiInputMixerPartial m
    )
    SELECT
      bp.NoMixerPartial, bp.NoMixer, bp.NoSak, bp.Berat,
      c.SourceType, c.NoProduksi,
      COALESCE(bph.TglProduksi, iph.TglProduksi, mph.TglProduksi) AS TglProduksi,
      COALESCE(bph.IdMesin, iph.IdMesin, mph.IdMesin) AS IdMesin,
      COALESCE(bph.IdOperator, iph.IdOperator, mph.IdOperator) AS IdOperator,
      COALESCE(bph.Jam, iph.Jam, mph.Jam) AS Jam,
      COALESCE(bph.Shift, iph.Shift, mph.Shift) AS Shift,
      mm.NamaMesin
    FROM BasePartial bp
    LEFT JOIN Consumed c ON c.NoMixerPartial = bp.NoMixerPartial
    LEFT JOIN dbo.BrokerProduksi_h bph ON c.SourceType = 'BROKER' AND bph.NoProduksi = c.NoProduksi
    LEFT JOIN dbo.InjectProduksi_h iph ON c.SourceType = 'INJECT' AND iph.NoProduksi = c.NoProduksi
    LEFT JOIN dbo.MixerProduksi_h mph ON c.SourceType = 'MIXER' AND mph.NoProduksi = c.NoProduksi
    LEFT JOIN dbo.MstMesin mm ON mm.IdMesin = COALESCE(bph.IdMesin, iph.IdMesin, mph.IdMesin)
    ORDER BY bp.NoMixerPartial ASC, c.SourceType ASC, c.NoProduksi ASC;
  `);
  return result.recordset || [];
}

async function getMixerHeaderForUpdate(tx, noMixer) {
  const res = await new sql.Request(tx).input(
    "NoMixer",
    sql.VarChar(50),
    noMixer,
  ).query(`
      SELECT TOP 1 NoMixer, CONVERT(date, DateCreate) AS DateCreate
      FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK)
      WHERE NoMixer = @NoMixer
    `);
  return res.recordset?.[0] || null;
}

async function isMixerFromBongkarSusun(tx, noMixer) {
  const res = await new sql.Request(tx)
    .input("NoMixer", sql.VarChar(50), noMixer)
    .query(
      `SELECT TOP 1 1 FROM dbo.BongkarSusunOutputMixer WHERE NoMixer = @NoMixer`,
    );
  return res.recordset.length > 0;
}

async function isNoMixerExists(tx, noMixer) {
  const res = await new sql.Request(tx)
    .input("NoMixer", sql.VarChar(50), noMixer)
    .query(
      `SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK) WHERE NoMixer = @NoMixer`,
    );
  return res.recordset.length > 0;
}

async function hasUsedDetails(tx, noMixer) {
  const res = await new sql.Request(tx).input(
    "NoMixer",
    sql.VarChar(50),
    noMixer,
  ).query(`
    SELECT TOP 1 1
    FROM dbo.Mixer_d WITH (UPDLOCK, HOLDLOCK)
    WHERE NoMixer = @NoMixer AND DateUsage IS NOT NULL
  `);
  return res.recordset.length > 0;
}

module.exports = {
  getAllMixerHeaders,
  getMixerHeaderByNoMixer,
  getMixerDetailsByNoMixer,
  getPartialInfoRowsByMixerAndSak,
  getMixerHeaderForUpdate,
  isMixerFromBongkarSusun,
  isNoMixerExists,
  hasUsedDetails,
};

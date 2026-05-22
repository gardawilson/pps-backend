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
          INNER JOIN dbo.MixerProduksi_h mph
            ON mph.NoProduksi = mpo.NoProduksi
          LEFT JOIN dbo.MstMesin m
            ON m.IdMesin = mph.IdMesin
          WHERE mpo.NoMixer = h.NoMixer
            AND (
              mpo.NoProduksi LIKE @search
              OR m.NamaMesin LIKE @search
            )
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
          INNER JOIN dbo.InjectP-roduksi_h iph
            ON iph.NoProduksi = ipom.NoProduksi
          LEFT JOIN dbo.MstMesin mi
            ON mi.IdMesin = iph.IdMesin
          WHERE ipom.NoMixer = h.NoMixer
            AND (
              ipom.NoProduksi LIKE @search
              OR mi.NamaMesin LIKE @search
            )
        )
      )
    `
    : "";

  const baseQuery = `
    SELECT
      h.NoMixer,
      h.DateCreate,
      h.IdMixer,
      mx.Jenis AS NamaMixer,
      h.IdStatus,
      CASE
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE ''
      END AS StatusText,
      h.Moisture,
      h.MaxMeltTemp,
      h.MinMeltTemp,
      h.MFI,
      h.Moisture2,
      h.Moisture3,
      CASE
        WHEN EXISTS (
          SELECT 1
          FROM dbo.Mixer_d d3
          WHERE d3.NoMixer = h.NoMixer
            AND d3.DateUsage IS NULL
        ) THEN CAST(0 AS bit)
        ELSE CAST(1 AS bit)
      END AS Used,
      ISNULL(CAST(h.HasBeenPrinted AS int), 0) AS HasBeenPrinted,
      h.Blok,
      h.IdLokasi,
      outInfo.OutputType,
      outInfo.OutputCode,
      outInfo.OutputNamaMesin
    FROM dbo.Mixer_h h
    INNER JOIN dbo.MstMixer mx
      ON mx.IdMixer = h.IdMixer
    OUTER APPLY (
      SELECT TOP (1)
        src.OutputType,
        src.OutputCode,
        src.OutputNamaMesin
      FROM (
        SELECT
          'MIXER_PRODUKSI' AS OutputType,
          mpo.NoProduksi   AS OutputCode,
          m.NamaMesin      AS OutputNamaMesin,
          1                AS Priority
        FROM dbo.MixerProduksiOutput mpo
        INNER JOIN dbo.MixerProduksi_h mph
          ON mph.NoProduksi = mpo.NoProduksi
        LEFT JOIN dbo.MstMesin m
          ON m.IdMesin = mph.IdMesin
        WHERE mpo.NoMixer = h.NoMixer

        UNION ALL

        SELECT
          'INJECT_PRODUKSI'        AS OutputType,
          ipom.NoProduksi          AS OutputCode,
          mi.NamaMesin             AS OutputNamaMesin,
          2                        AS Priority
        FROM dbo.InjectProduksiOutputMixer ipom
        INNER JOIN dbo.InjectProduksi_h iph
          ON iph.NoProduksi = ipom.NoProduksi
        LEFT JOIN dbo.MstMesin mi
          ON mi.IdMesin = iph.IdMesin
        WHERE ipom.NoMixer = h.NoMixer

        UNION ALL

        SELECT
          'BONGKAR_SUSUN'          AS OutputType,
          bsom.NoBongkarSusun      AS OutputCode,
          'Bongkar Susun'          AS OutputNamaMesin,
          3                        AS Priority
        FROM dbo.BongkarSusunOutputMixer bsom
        WHERE bsom.NoMixer = h.NoMixer
      ) AS src
      WHERE src.OutputCode IS NOT NULL
      ORDER BY src.Priority, src.OutputCode
    ) AS outInfo
    WHERE 1 = 1
      ${searchClause}
      ${dateUsageFilter}
    ORDER BY h.NoMixer DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoMixer) AS total
    FROM dbo.Mixer_h h
    INNER JOIN dbo.MstMixer mx
      ON mx.IdMixer = h.IdMixer
    WHERE 1 = 1
      ${searchClause}
      ${dateUsageFilter};
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
        h.NoMixer,
        h.DateCreate,
        h.IdMixer,
        mx.Jenis,
        mx.Jenis AS NamaMixer,
        MAX(CAST(ISNULL(d.IsPartial, 0) AS int)) AS IsPartial,
        COUNT(d.NoSak) AS JumlahSak,
        SUM(d.Berat) - ISNULL(SUM(mp.Berat), 0) AS SisaBerat,
        h.CreateBy,
        ISNULL(CAST(h.HasBeenPrinted AS int), 0) AS HasBeenPrinted,
        COALESCE(outInfo.OutputNamaMesin, '-') AS Mesin,
        COALESCE(CAST(outInfo.Shift AS VARCHAR(10)), '-') AS Shift
      FROM dbo.Mixer_h h
      JOIN dbo.MstMixer mx
        ON mx.IdMixer = h.IdMixer
      JOIN dbo.Mixer_d d
        ON d.NoMixer = h.NoMixer AND d.DateUsage IS NULL
      LEFT JOIN dbo.MixerPartial mp
        ON mp.NoMixer = h.NoMixer AND mp.NoSak = d.NoSak
      OUTER APPLY (
        SELECT TOP (1)
          src.OutputNamaMesin,
          src.Shift
        FROM (
          SELECT
            m.NamaMesin AS OutputNamaMesin,
            mph.Shift AS Shift,
            1 AS Priority
          FROM dbo.MixerProduksiOutput mpo
          JOIN dbo.MixerProduksi_h mph ON mph.NoProduksi = mpo.NoProduksi
          LEFT JOIN dbo.MstMesin m ON m.IdMesin = mph.IdMesin
          WHERE mpo.NoMixer = h.NoMixer
          UNION ALL
          SELECT
            mi.NamaMesin AS OutputNamaMesin,
            iph.Shift AS Shift,
            2 AS Priority
          FROM dbo.InjectProduksiOutputMixer ipom
          JOIN dbo.InjectProduksi_h iph ON iph.NoProduksi = ipom.NoProduksi
          LEFT JOIN dbo.MstMesin mi ON mi.IdMesin = iph.IdMesin
          WHERE ipom.NoMixer = h.NoMixer
          UNION ALL
          SELECT
            bsom.NoBongkarSusun AS OutputNamaMesin,
            NULL AS Shift,
            3 AS Priority
          FROM dbo.BongkarSusunOutputMixer bsom
          WHERE bsom.NoMixer = h.NoMixer
        ) AS src
        WHERE src.OutputNamaMesin IS NOT NULL
        ORDER BY src.Priority
      ) AS outInfo
      WHERE h.NoMixer = @NoMixer
      GROUP BY
        h.NoMixer,
        h.DateCreate,
        h.IdMixer,
        mx.Jenis,
        h.CreateBy,
        h.HasBeenPrinted,
        outInfo.OutputNamaMesin,
        outInfo.Shift
    `);
}

module.exports = {
  getAllMixerHeaders,
  getMixerHeaderByNoMixer,
};

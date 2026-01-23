// services/bahan-baku-service.js
const { sql, poolPromise } = require('../../../core/config/db');

// GET all header BahanBaku with pagination & search
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      h.NoBahanBaku,
      h.IdSupplier,
      s.NmSupplier AS NamaSupplier,
      h.NoPlat,
      h.DateCreate,
      h.CreateBy,
      h.DateTimeCreate
    FROM dbo.BahanBaku_h h
    LEFT JOIN dbo.MstSupplier s
      ON s.IdSupplier = h.IdSupplier
    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBahanBaku LIKE @search
               OR h.NoPlat LIKE @search
               OR h.CreateBy LIKE @search
               OR CAST(h.IdSupplier AS varchar(50)) LIKE @search
               OR s.NmSupplier LIKE @search
             )`
          : ''
      }
    ORDER BY h.NoBahanBaku DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(1) AS total
    FROM dbo.BahanBaku_h h
    LEFT JOIN dbo.MstSupplier s
      ON s.IdSupplier = h.IdSupplier
    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBahanBaku LIKE @search
               OR h.NoPlat LIKE @search
               OR h.CreateBy LIKE @search
               OR CAST(h.IdSupplier AS varchar(50)) LIKE @search
               OR s.NmSupplier LIKE @search
             )`
          : ''
      }
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map((r) => ({ ...r }));
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};

exports.getPalletByNoBahanBaku = async (nobahanbaku) => {
  const pool = await poolPromise;

  const result = await pool.request()
    .input('NoBahanBaku', sql.VarChar, nobahanbaku)
    .query(`
      SELECT
        p.NoBahanBaku,
        p.NoPallet,
        p.IdJenisPlastik,
        jp.Jenis AS NamaJenisPlastik,
        p.IdWarehouse,
        w.NamaWarehouse,
        p.Keterangan,
        p.IdStatus,
        CASE 
          WHEN p.IdStatus = 1 THEN 'PASS'
          WHEN p.IdStatus = 0 THEN 'HOLD'
          ELSE ''
        END AS StatusText,

        p.Moisture,
        p.MeltingIndex,
        p.Elasticity,
        p.Tenggelam,
        p.Density,
        p.Density2,
        p.Density3,

        p.Blok,
        p.IdLokasi,

        -- ✅ ACTUAL (tidak peduli DateUsage & IsPartial)
        ISNULL(dAgg.SakActual, 0)   AS SakActual,
        ISNULL(dAgg.BeratActual, 0) AS BeratActual,

        -- ✅ SISA (hanya DateUsage IS NULL, partial dikurangkan)
        ISNULL(dAgg.SakSisa, 0)     AS SakSisa,
        ISNULL(dAgg.BeratSisa, 0)   AS BeratSisa,

        -- ✅ Flag: IsEmpty = 1 jika semua detail sudah DateUsage terisi
        CAST(
          CASE
            WHEN ISNULL(dAgg.TotalDetail, 0) = 0 THEN 0
            WHEN ISNULL(dAgg.SakSisa, 0) = 0 THEN 1
            ELSE 0
          END
        AS bit) AS IsEmpty

      FROM dbo.BahanBakuPallet_h p
      LEFT JOIN dbo.MstJenisPlastik jp ON jp.IdJenisPlastik = p.IdJenisPlastik
      LEFT JOIN dbo.MstWarehouse w     ON w.IdWarehouse     = p.IdWarehouse

      OUTER APPLY (
        SELECT
          COUNT(1) AS TotalDetail,

          -- ACTUAL
          COUNT(1) AS SakActual,
          SUM(ISNULL(d.Berat, 0)) AS BeratActual,

          -- SISA
          SUM(CASE WHEN d.DateUsage IS NULL THEN 1 ELSE 0 END) AS SakSisa,

          SUM(
            CASE
              WHEN d.DateUsage IS NOT NULL THEN 0
              ELSE
                CASE
                  WHEN d.IsPartial = 1 THEN
                    CASE 
                      WHEN (ISNULL(d.Berat,0) - ISNULL(ps.PartialBerat,0)) < 0 THEN 0
                      ELSE (ISNULL(d.Berat,0) - ISNULL(ps.PartialBerat,0))
                    END
                  ELSE ISNULL(d.Berat,0)
                END
            END
          ) AS BeratSisa

        FROM dbo.BahanBaku_d d
        LEFT JOIN (
          SELECT
            NoBahanBaku,
            NoPallet,
            NoSak,
            SUM(Berat) AS PartialBerat
          FROM dbo.BahanBakuPartial
          GROUP BY NoBahanBaku, NoPallet, NoSak
        ) ps
          ON ps.NoBahanBaku = d.NoBahanBaku
         AND ps.NoPallet    = d.NoPallet
         AND ps.NoSak       = d.NoSak

        WHERE d.NoBahanBaku = p.NoBahanBaku
          AND d.NoPallet    = p.NoPallet
      ) dAgg

      WHERE p.NoBahanBaku = @NoBahanBaku
      ORDER BY p.NoPallet;
    `);

  const toInt = (v) => (typeof v === 'number' ? v : parseInt(v ?? '0', 10) || 0);
  const toNum = (v) => (typeof v === 'number' ? v : parseFloat(v ?? '0') || 0);

  return result.recordset.map(r => ({
    ...r,
    IsEmpty: r.IsEmpty === true || r.IsEmpty === 1,

    SakActual: toInt(r.SakActual),
    SakSisa: toInt(r.SakSisa),

    BeratActual: toNum(r.BeratActual),
    BeratSisa: toNum(r.BeratSisa),
  }));
};





exports.getDetailByNoBahanBakuAndNoPallet = async ({ nobahanbaku, nopallet }) => {
  const pool = await poolPromise;

  const result = await pool.request()
    .input('NoBahanBaku', sql.VarChar, nobahanbaku)
    .input('NoPallet', sql.VarChar, nopallet)
    .query(`
      SELECT
        d.NoBahanBaku,
        d.NoPallet,
        d.NoSak,
        d.TimeCreate,

        -- Jika IsPartial = 1, maka Berat dikurangi total dari BahanBakuPartial
        CASE
          WHEN d.IsPartial = 1 THEN
            d.Berat - ISNULL((
              SELECT SUM(p.Berat)
              FROM dbo.BahanBakuPartial p
              WHERE p.NoBahanBaku = d.NoBahanBaku
                AND p.NoPallet    = d.NoPallet
                AND p.NoSak       = d.NoSak
            ), 0)
          ELSE d.Berat
        END AS Berat,

        d.BeratAct,
        d.DateUsage,
        d.IsLembab,
        d.IsPartial,
        d.IdLokasi
      FROM dbo.BahanBaku_d d
      WHERE d.NoBahanBaku = @NoBahanBaku
        AND d.NoPallet    = @NoPallet
      ORDER BY d.NoSak;
    `);

  // optional: rapikan DateUsage agar konsisten di FE (mirip broker)
  const formatDate = (date) => {
    if (!date) return null;
    const x = new Date(date);
    const pad = (n) => (n < 10 ? '0' + n : n);
    return `${x.getFullYear()}-${pad(x.getMonth() + 1)}-${pad(x.getDate())} ${pad(x.getHours())}:${pad(x.getMinutes())}:${pad(x.getSeconds())}`;
  };

  return result.recordset.map((item) => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) }),
  }));
};
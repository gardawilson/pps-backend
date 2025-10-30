const { sql, poolPromise } = require('../../../core/config/db');

/**
 * Assumptions (rename if different in your DB):
 * - Header table:       [dbo].[Crusher]
 * - Master Crusher:     [dbo].[MstCrusher]       (IdCrusher, NamaCrusher)
 * - Master Warehouse:   [dbo].[MstWarehouse]     (IdWarehouse, NamaWarehouse)
 *
 * Filtering:
 * - Only items with DateUsage IS NULL (belum terpakai)
 * - Search across: NoCrusher, Blok, IdLokasi, IdWarehouse, NamaWarehouse, NamaCrusher
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
      END AS StatusText
    FROM [dbo].[Crusher] c
    LEFT JOIN [dbo].[MstCrusher] mc
      ON mc.IdCrusher = c.IdCrusher
    LEFT JOIN [dbo].[MstWarehouse] w
      ON w.IdWarehouse = c.IdWarehouse
    WHERE 1=1
      AND c.DateUsage IS NULL
      ${whereSearch}
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

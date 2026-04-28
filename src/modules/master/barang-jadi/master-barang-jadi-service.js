const { sql, poolPromise } = require("../../../core/config/db");

async function getAllActive({ page = 1, pageSize = 20, search = "" } = {}) {
  const pool = await poolPromise;
  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(100, Number(pageSize) || 20));
  const offset = (p - 1) * ps;
  const searchTerm = String(search || "").trim();

  const whereClause = `
    WHERE ISNULL(Enable, 1) = 1
      AND (@search = '' OR NamaBJ LIKE '%' + @search + '%')
  `;

  const countResult = await pool
    .request()
    .input("search", sql.NVarChar(200), searchTerm).query(`
      SELECT COUNT(1) AS total
      FROM [dbo].[MstBarangJadi]
      ${whereClause};
    `);

  const total = countResult.recordset?.[0]?.total || 0;
  if (total === 0) return { data: [], total };

  const dataResult = await pool
    .request()
    .input("search", sql.NVarChar(200), searchTerm)
    .input("offset", sql.Int, offset)
    .input("pageSize", sql.Int, ps).query(`
    SELECT
      IdBJ AS idJenis,
      NamaBJ AS namaJenis
    FROM [dbo].[MstBarangJadi]
    ${whereClause}
    ORDER BY NamaBJ ASC
    OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY;
  `);

  return { data: dataResult.recordset || [], total };
}

module.exports = { getAllActive };

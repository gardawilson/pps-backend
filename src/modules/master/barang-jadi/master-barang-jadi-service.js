const { sql, poolPromise } = require("../../../core/config/db");

async function getAllActive({ search = "" } = {}) {
  const pool = await poolPromise;
  const searchTerm = String(search || "").trim();

  const dataResult = await pool
    .request()
    .input("search", sql.NVarChar(200), searchTerm).query(`
    SELECT
      IdBJ AS idJenis,
      NamaBJ AS namaJenis
    FROM [dbo].[MstBarangJadi]
    WHERE ISNULL(Enable, 1) = 1
      AND (@search = '' OR NamaBJ LIKE '%' + @search + '%')
    ORDER BY NamaBJ ASC;
  `);

  return dataResult.recordset || [];
}

module.exports = { getAllActive };

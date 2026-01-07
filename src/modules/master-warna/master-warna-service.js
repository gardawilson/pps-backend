const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdWarna,
      Warna,
      ISNULL(Enable, 1) AS Enable
    FROM [dbo].[MstWarna]
    WHERE ISNULL(Enable, 1) = 1
    ORDER BY Warna ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getAllActive };

const { sql, poolPromise } = require('../../core/config/db');

async function getAllBlok() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT TOP (1000) Blok, IdWarehouse
    FROM PPS_TEST2.dbo.MstBlok
    ORDER BY Blok ASC
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllBlok };

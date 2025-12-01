// src/modules/master/gilingan-type-service.js
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdGilingan,
      NamaGilingan,
      SaldoAwal,
      Enable
    FROM [dbo].[MstGilingan]
    WHERE Enable = 1
    ORDER BY NamaGilingan ASC;
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllActive };

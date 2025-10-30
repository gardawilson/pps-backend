// src/modules/master/jenis-bonggolan-service.js
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      IdBonggolan,
      NamaBonggolan,
      Enable
    FROM [PPS_TEST2].[dbo].[MstBonggolan]
    WHERE Enable = 1
    ORDER BY NamaBonggolan ASC;
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllActive };

// src/modules/master/reject-master-service.js
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdReject,
      NamaReject,
      SaldoAwal,
      Enable,
      TglSaldoAwal,
      ItemCode
    FROM [dbo].[MstReject]
    WHERE Enable = 1
    ORDER BY NamaReject ASC;
  `;

  // NOTE:
  // - Kalau connection default kamu bukan PPS_TEST3,
  //   dan ingin paksa ke DB itu, ganti FROM menjadi:
  //   FROM [PPS_TEST3].[dbo].[MstReject]

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllActive };

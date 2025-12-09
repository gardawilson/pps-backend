// src/modules/master/packing-master-service.js
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdBJ,
      NamaBJ,
      IdUOM,
      IdBJType,
      TglSaldoAwal,
      BeratSTD,
      Enable,
      ItemCode,
      PcsPerLabel,
      IdTypeSubBarang
    FROM [dbo].[MstBarangJadi]
    WHERE Enable = 1
    ORDER BY NamaBJ ASC;
  `;

  // NOTE:
  // - Kalau connection default kamu bukan PPS_TEST3,
  //   dan ingin paksa ke DB itu, ganti FROM menjadi:
  //   FROM [PPS_TEST3].[dbo].[MstBarangJadi]

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllActive };

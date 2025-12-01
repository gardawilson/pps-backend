// src/modules/master/mixer-type-service.js
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      IdMixer,
      Jenis,
      Enable
    FROM [dbo].[MstMixer]
    WHERE Enable = 1
    ORDER BY Jenis ASC;
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllActive };

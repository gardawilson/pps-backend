const { sql, poolPromise } = require('../../core/config/db');

async function getAllJenisPlastikAktif() {
  const pool = await poolPromise;       // ✅ ambil pool global sekali
  const request = pool.request();

  const query = `
    SELECT 
      IdJenisPlastik,
      Jenis,
      Enable,
      DisableMinMax,
      Moisture,
      MeltingIndex,
      Elasticity,
      IsReject,
      IdWarna
    FROM MstJenisPlastik
    WHERE Enable = 1
    ORDER BY Jenis ASC
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllJenisPlastikAktif };

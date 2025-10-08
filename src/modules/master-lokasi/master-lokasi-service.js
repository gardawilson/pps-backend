const { sql, poolPromise } = require('../../core/config/db');

async function getAllLokasiAktif() {
  const pool = await poolPromise;       // ✅ ambil pool global sekali
  const request = pool.request();

  const query = `
    SELECT IdLokasi, Blok, Enable
    FROM MstLokasi 
    WHERE Enable = 1
    ORDER BY Blok ASC
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllLokasiAktif };

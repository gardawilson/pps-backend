const { sql, connectDb } = require('../db');

async function getAllLokasiAktif() {
  let pool;
  try {
    pool = await connectDb();
    const request = new sql.Request(pool);

    const query = `
      SELECT IdLokasi, Blok, Enable
      FROM MstLokasi 
      WHERE Enable = 1
      ORDER BY Blok ASC
    `;

    const result = await request.query(query);
    return result.recordset;
  } finally {
    if (pool) await pool.close();  // pastikan hanya close jika pool berhasil
  }
}

module.exports = { getAllLokasiAktif };

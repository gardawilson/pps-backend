const { sql, poolPromise } = require('../../core/config/db');

// Ambil semua WashingProduksi_h dengan pagination
async function getAllProduksi({ page = 1, limit = 50 }) {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const query = `
    SELECT 
      NoProduksi,
      IdOperator,
      IdMesin,
      TglProduksi,
      JamKerja,
      Shift,
      CreateBy,
      CheckBy1,
      CheckBy2,
      ApproveBy,
      JmlhAnggota,
      Hadir,
      HourMeter
    FROM WashingProduksi_h
    ORDER BY NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
  `;

  const countQuery = `SELECT COUNT(*) as total FROM WashingProduksi_h`;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);

  const [data, count] = await Promise.all([
    request.query(query),
    pool.request().query(countQuery)
  ]);

  return {
    data: data.recordset,
    total: count.recordset[0].total
  };
}

// Ambil satu by NoProduksi
async function getById(noProduksi) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('NoProduksi', sql.VarChar, noProduksi);

  const query = `
    SELECT 
      NoProduksi,
      IdOperator,
      IdMesin,
      TglProduksi,
      JamKerja,
      Shift,
      CreateBy,
      CheckBy1,
      CheckBy2,
      ApproveBy,
      JmlhAnggota,
      Hadir,
      HourMeter
    FROM WashingProduksi_h
    WHERE NoProduksi = @NoProduksi
  `;

  const result = await request.query(query);
  return result.recordset[0] || null;
}

module.exports = { getAllProduksi, getById };

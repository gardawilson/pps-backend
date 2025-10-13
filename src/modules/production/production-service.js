// services/production-service.js
const { sql, poolPromise } = require('../../core/config/db');

async function getProduksiByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();
  const query = `
    SELECT 
      h.NoProduksi, h.IdOperator, h.IdMesin, m.NamaMesin,
      h.TglProduksi, h.JamKerja, h.Shift, h.CreateBy,
      h.CheckBy1, h.CheckBy2, h.ApproveBy,
      h.JmlhAnggota, h.Hadir, h.HourMeter
    FROM WashingProduksi_h h
    LEFT JOIN MstMesin m ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @date
    ORDER BY h.JamKerja ASC;
  `;
  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getProduksiByDate };   // ⬅️ pastikan ini ada

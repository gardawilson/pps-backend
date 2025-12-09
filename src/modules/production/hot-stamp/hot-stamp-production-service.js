// services/hotstamping-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

async function getProduksiByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      h.NoProduksi,
      h.Tanggal,
      h.IdMesin,
      m.NamaMesin,
      h.IdOperator,
      o.NamaOperator,       -- sesuaikan kalau nama kolom beda
      h.Shift,
      h.JamKerja,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter
    FROM [dbo].[HotStamping_h] h
    LEFT JOIN [dbo].[MstMesin] m
      ON h.IdMesin = m.IdMesin
    LEFT JOIN [dbo].[MstOperator] o
      ON h.IdOperator = o.IdOperator
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.JamKerja ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getProduksiByDate };

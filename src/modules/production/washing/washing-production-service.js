// services/production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

async function getAllProduksi(page = 1, pageSize = 20) {
  const pool = await poolPromise;

  const offset = (page - 1) * pageSize;

  // 1) Total baris (tanpa JOIN supaya ringan)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM PPS_TEST2.dbo.WashingProduksi_h WITH (NOLOCK);
  `;
  const countRes = await pool.request().query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) return { data: [], total: 0 };

  // 2) Data halaman + JOIN ke master mesin & operator
  const request = pool.request();
  const dataQry = `
    SELECT
      h.NoProduksi,
      h.IdOperator,
      op.NamaOperator,
      h.IdMesin,
      ms.NamaMesin,
      h.TglProduksi,
      h.JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter
    FROM PPS_TEST2.dbo.WashingProduksi_h h WITH (NOLOCK)
    LEFT JOIN PPS_TEST2.dbo.MstMesin ms     WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN PPS_TEST2.dbo.MstOperator op  WITH (NOLOCK) ON op.IdOperator = h.IdOperator
    ORDER BY h.TglProduksi DESC, h.JamKerja ASC, h.NoProduksi ASC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit',  sql.Int, pageSize);

  const dataRes = await request.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

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

module.exports = { getProduksiByDate, getAllProduksi };   // ⬅️ pastikan ini ada

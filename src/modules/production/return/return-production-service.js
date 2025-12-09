// services/return-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

async function getReturnsByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      h.NoRetur,
      h.Invoice,
      h.Tanggal,
      h.IdPembeli,
      p.NamaPembeli,
      h.NoBJSortir
    FROM [dbo].[BJRetur_h] h
    LEFT JOIN [dbo].[MstPembeli] p
      ON h.IdPembeli = p.IdPembeli
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.NoRetur ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getReturnsByDate };

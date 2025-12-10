// services/sortir-reject-service.js
const { sql, poolPromise } = require('../../../core/config/db');

async function getSortirRejectByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      h.NoBJSortir,
      h.TglBJSortir,
      h.IdUsername
    FROM [dbo].[BJSortirReject_h] h
    WHERE CONVERT(date, h.TglBJSortir) = @date
    ORDER BY h.NoBJSortir ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

module.exports = {
  getSortirRejectByDate,
};

// services/bongkar-susun-service.js
const { sql, poolPromise } = require('../../core/config/db');

async function getByDate(date /* 'YYYY-MM-DD' */) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      NoBongkarSusun,
      Tanggal,
      IdUsername,
      Note
    FROM BongkarSusun_h
    WHERE CONVERT(date, Tanggal) = @date
    ORDER BY Tanggal DESC;
  `;

  request.input('date', sql.Date, date);

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getByDate };

const { poolPromise, sql } = require("../../../core/config/db");

async function getShiftHoursByDateAndShift({ tanggal, shift }) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input("Tanggal", sql.Date, tanggal);
  request.input("NoShift", sql.Int, shift);

  const query = `
    ;WITH LatestSet AS (
      SELECT TOP 1
        h.IdShiftHourSet,
        h.ValidFrmDate
      FROM dbo.MstShiftHourSet h WITH (NOLOCK)
      WHERE CONVERT(date, h.ValidFrmDate) <= @Tanggal
      ORDER BY CONVERT(date, h.ValidFrmDate) DESC, h.IdShiftHourSet DESC
    )
    SELECT
      ls.IdShiftHourSet,
      ls.ValidFrmDate,
      d.NoShift,
      CONVERT(varchar(8), d.HourStart, 108) AS HourStart,
      CONVERT(varchar(8), d.HourEnd, 108) AS HourEnd
    FROM LatestSet ls
    INNER JOIN dbo.MstShiftHourSet_d d WITH (NOLOCK)
      ON d.IdShiftHourSet = ls.IdShiftHourSet
     AND d.NoShift = @NoShift;
  `;

  const result = await request.query(query);
  return result.recordset?.[0] || null;
}

module.exports = { getShiftHoursByDateAndShift };

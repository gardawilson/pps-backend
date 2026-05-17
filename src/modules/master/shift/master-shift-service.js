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

async function getCurrentShift() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    ;WITH CurrentCtx AS (
      SELECT
        CONVERT(date, GETDATE()) AS CurrentDate,
        CAST(GETDATE() AS time(0)) AS CurrentTime
    ),
    LatestSet AS (
      SELECT TOP 1
        h.IdShiftHourSet,
        h.ValidFrmDate
      FROM dbo.MstShiftHourSet h WITH (NOLOCK)
      CROSS JOIN CurrentCtx c
      WHERE CONVERT(date, h.ValidFrmDate) <= c.CurrentDate
      ORDER BY CONVERT(date, h.ValidFrmDate) DESC, h.IdShiftHourSet DESC
    )
    SELECT TOP 1
      ls.IdShiftHourSet,
      ls.ValidFrmDate,
      d.NoShift,
      CONVERT(varchar(8), d.HourStart, 108) AS HourStart,
      CONVERT(varchar(8), d.HourEnd, 108) AS HourEnd,
      CONVERT(varchar(10), c.CurrentDate, 23) AS CurrentDate,
      CONVERT(varchar(8), c.CurrentTime, 108) AS CurrentTime
    FROM LatestSet ls
    INNER JOIN dbo.MstShiftHourSet_d d WITH (NOLOCK)
      ON d.IdShiftHourSet = ls.IdShiftHourSet
    CROSS JOIN CurrentCtx c
    WHERE
      (
        d.HourStart <= d.HourEnd
        AND c.CurrentTime >= CAST(d.HourStart AS time(0))
        AND c.CurrentTime < CAST(d.HourEnd AS time(0))
      )
      OR
      (
        d.HourStart > d.HourEnd
        AND (
          c.CurrentTime >= CAST(d.HourStart AS time(0))
          OR c.CurrentTime < CAST(d.HourEnd AS time(0))
        )
      )
    ORDER BY d.NoShift ASC;
  `;

  const result = await request.query(query);
  return result.recordset?.[0] || null;
}

module.exports = { getShiftHoursByDateAndShift, getCurrentShift };

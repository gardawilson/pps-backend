const { poolPromise } = require("../../core/config/db");

/**
 * Get MstMesin rows filtered by IdBagianMesin (integer).
 * - Exact match on IdBagianMesin.
 * - By default returns only active (Enable=1); pass includeDisabled=1 to include all.
 */
async function getByIdBagian({ idBagianMesin, includeDisabled = false }) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input("IdBagianMesin", idBagianMesin);

  const whereEnable = includeDisabled ? "1=1" : "ISNULL(Enable, 1) = 1";

  const query = `
    SELECT
      IdMesin,
      NamaMesin,
      Bagian,
      DefaultOperator,
      Enable,
      Kapasitas,
      IdUOM,
      ShotWeightPS,
      KlemLebar,
      KlemPanjang,
      IdBagianMesin,
      Target
    FROM [dbo].[MstMesin]
    WHERE ${whereEnable}
      AND IdBagianMesin = @IdBagianMesin
    ORDER BY NamaMesin ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

async function getBrokerByNoProduksi({
  idBagianMesin = 2,
  includeDisabled = true,
}) {
  const pool = await poolPromise;
  const request = pool.request();
  request.input("IdBagianMesin", idBagianMesin);

  const whereEnable = includeDisabled ? "1=1" : "ISNULL(m.Enable, 1) = 1";

  const query = `
    ;WITH CurrentCtx AS (
      SELECT
        CONVERT(date, GETDATE()) AS CurrentDate,
        CAST(GETDATE() AS time(0)) AS CurrentTime
    ),
    LatestShiftSet AS (
      SELECT TOP 1
        h.IdShiftHourSet,
        h.ValidFrmDate
      FROM dbo.MstShiftHourSet h WITH (NOLOCK)
      CROSS JOIN CurrentCtx c
      WHERE CONVERT(date, h.ValidFrmDate) <= c.CurrentDate
      ORDER BY CONVERT(date, h.ValidFrmDate) DESC, h.IdShiftHourSet DESC
    ),
    ActiveShift AS (
      SELECT TOP 1
        d.NoShift,
        d.HourStart,
        d.HourEnd,
        ls.ValidFrmDate
      FROM LatestShiftSet ls
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
      ORDER BY d.NoShift ASC
    )
    SELECT
      m.IdMesin,
      m.NamaMesin,
      m.Bagian,
      h.NoProduksi,
      CONVERT(date, h.TglProduksi) AS TglProduksi,
      h.OutputJenisId,
      br.Nama AS OutputJenisNama,
      br.ItemCode AS OutputJenisItemCode,
      JSON_QUERY(
        COALESCE(
          (
            SELECT od.IdOperator AS [value]
            FROM dbo.BrokerProduksiOperator_d od WITH (NOLOCK)
            WHERE od.NoProduksi = h.NoProduksi
            ORDER BY od.IdOperator
            FOR JSON PATH
          ),
          '[]'
        )
      ) AS IdOperators,
      COALESCE(
        (
          SELECT STRING_AGG(op.NamaOperator, ', ')
          FROM dbo.BrokerProduksiOperator_d od WITH (NOLOCK)
          INNER JOIN dbo.MstOperator op WITH (NOLOCK)
            ON op.IdOperator = od.IdOperator
          WHERE od.NoProduksi = h.NoProduksi
        ),
        ''
      ) AS Operators,
      h.Shift,
      CONVERT(varchar(8), h.HourStart, 108) AS HourStart,
      CONVERT(varchar(8), h.HourEnd, 108) AS HourEnd,
      m.Target,
      CONVERT(varchar(10), c.CurrentDate, 23) AS CurrentDate,
      CONVERT(varchar(8), c.CurrentTime, 108) AS CurrentTime,
      s.NoShift AS ActiveShift,
      CONVERT(varchar(8), s.HourStart, 108) AS ActiveShiftHourStart,
      CONVERT(varchar(8), s.HourEnd, 108) AS ActiveShiftHourEnd,
      s.ValidFrmDate AS ActiveShiftValidFrmDate
    FROM dbo.MstMesin m WITH (NOLOCK)
    OUTER APPLY (
      SELECT TOP 1
        bh.NoProduksi,
        bh.TglProduksi,
        bh.OutputJenisId,
        bh.Shift,
        bh.HourStart,
        bh.HourEnd
      FROM dbo.BrokerProduksi_h bh WITH (NOLOCK)
      CROSS JOIN CurrentCtx c
      WHERE bh.IdMesin = m.IdMesin
        AND CONVERT(date, bh.TglProduksi) = c.CurrentDate
        AND bh.Shift = (SELECT TOP 1 NoShift FROM ActiveShift)
        AND (
          (
            bh.HourStart <= bh.HourEnd
            AND c.CurrentTime >= CAST(bh.HourStart AS time(0))
            AND c.CurrentTime < CAST(bh.HourEnd AS time(0))
          )
          OR
          (
            bh.HourStart > bh.HourEnd
            AND (
              c.CurrentTime >= CAST(bh.HourStart AS time(0))
              OR c.CurrentTime < CAST(bh.HourEnd AS time(0))
            )
          )
        )
      ORDER BY bh.HourStart DESC, bh.NoProduksi DESC
    ) h
    LEFT JOIN dbo.MstBroker br WITH (NOLOCK)
      ON br.IdBroker = h.OutputJenisId
    OUTER APPLY (SELECT TOP 1 * FROM ActiveShift) s
    CROSS JOIN CurrentCtx c
    WHERE ${whereEnable}
      AND m.IdBagianMesin = @IdBagianMesin
    ORDER BY m.NamaMesin ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getByIdBagian, getBrokerByNoProduksi };

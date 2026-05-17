const { poolPromise } = require('../../core/config/db');

/**
 * Get MstMesin rows filtered by IdBagianMesin (integer).
 * - Exact match on IdBagianMesin.
 * - By default returns only active (Enable=1); pass includeDisabled=1 to include all.
 */
async function getByIdBagian({ idBagianMesin, includeDisabled = false }) {
  const pool = await poolPromise;
  const request = pool.request();
  const requestNoProduksi = pool.request();

  request.input('IdBagianMesin', idBagianMesin);
  requestNoProduksi.input('IdBagianMesin', idBagianMesin);

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(m.Enable, 1) = 1';

  const query = `
    SELECT
      m.IdMesin,
      m.NamaMesin,
      m.Bagian,
      bp.NoProduksi,
      bp.OutputJenisId,
      brProd.Nama AS OutputJenisNama,
      brProd.ItemCode AS OutputJenisItemCode,
      opProd.NamaOperator AS Operator,
      m.Target,
      bp.Shift,
      CONVERT(varchar(8), bp.HourStart, 108) AS HourStart,
      CONVERT(varchar(8), bp.HourEnd, 108) AS HourEnd
    FROM [dbo].[MstMesin] m
    OUTER APPLY (
      SELECT TOP 1
        h.NoProduksi,
        h.IdOperator,
        h.OutputJenisId,
        h.Shift,
        h.HourStart,
        h.HourEnd
      FROM dbo.BrokerProduksi_h h WITH (NOLOCK)
      WHERE h.IdMesin = m.IdMesin
        AND CONVERT(date, h.TglProduksi) = CONVERT(date, GETDATE())
      ORDER BY h.TglProduksi DESC, h.NoProduksi DESC
    ) bp
    LEFT JOIN dbo.MstOperator opProd WITH (NOLOCK)
      ON opProd.IdOperator = bp.IdOperator
    LEFT JOIN dbo.MstBroker brProd WITH (NOLOCK)
      ON brProd.IdBroker = bp.OutputJenisId
    WHERE ${whereEnable}
      AND m.IdBagianMesin = @IdBagianMesin
    ORDER BY m.NamaMesin ASC;
  `;

  const result = await request.query(query);
  const mesinRows = result.recordset || [];

  const noProduksiQuery = `
    SELECT
      m.IdMesin,
      h.NoProduksi,
      h.OutputJenisId,
      br.Nama AS OutputJenisNama,
      br.ItemCode AS OutputJenisItemCode
    FROM [dbo].[MstMesin] m
    LEFT JOIN dbo.BrokerProduksi_h h WITH (NOLOCK)
      ON h.IdMesin = m.IdMesin
      AND CONVERT(date, h.TglProduksi) = CONVERT(date, GETDATE())
      AND h.NoProduksi IS NOT NULL
    LEFT JOIN dbo.MstBroker br WITH (NOLOCK)
      ON br.IdBroker = h.OutputJenisId
    WHERE ${whereEnable}
      AND m.IdBagianMesin = @IdBagianMesin
    ORDER BY m.IdMesin ASC, h.NoProduksi ASC;
  `;

  const resultNoProduksi = await requestNoProduksi.query(noProduksiQuery);
  const noProduksiRows = resultNoProduksi.recordset || [];

  const noProduksiMap = new Map();
  for (const row of noProduksiRows) {
    if (!row.NoProduksi) continue;
    const list = noProduksiMap.get(row.IdMesin) || [];
    list.push({
      NoProduksi: row.NoProduksi,
      OutputJenisId: row.OutputJenisId,
      OutputJenisNama: row.OutputJenisNama,
      OutputJenisItemCode: row.OutputJenisItemCode,
    });
    noProduksiMap.set(row.IdMesin, list);
  }

  return mesinRows.map((row) => {
    const noProduksiList = noProduksiMap.get(row.IdMesin) || [];
    return {
      ...row,
      NoProduksiValues: noProduksiList.map((item) => item.NoProduksi),
      NoProduksiList: noProduksiList,
      TotalNoProduksi: noProduksiList.length,
    };
  });
}

async function getBrokerByNoProduksi({ idBagianMesin = 2, includeDisabled = true }) {
  const pool = await poolPromise;
  const request = pool.request();
  request.input('IdBagianMesin', idBagianMesin);

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(m.Enable, 1) = 1';

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
      h.IdOperator,
      op.NamaOperator AS Operator,
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
        bh.IdOperator,
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
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK)
      ON op.IdOperator = h.IdOperator
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

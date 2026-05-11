const { poolPromise } = require('../../core/config/db');

/**
 * Get MstMesin rows filtered by IdBagianMesin (integer).
 * - Exact match on IdBagianMesin.
 * - By default returns only active (Enable=1); pass includeDisabled=1 to include all.
 */
async function getByIdBagian({ idBagianMesin, includeDisabled = false }) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('IdBagianMesin', idBagianMesin);

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(m.Enable, 1) = 1';

  const query = `
    SELECT
      m.IdMesin,
      m.NamaMesin,
      m.Bagian,
      bp.NoProduksi,
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
    WHERE ${whereEnable}
      AND m.IdBagianMesin = @IdBagianMesin
    ORDER BY m.NamaMesin ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getByIdBagian };

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

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(Enable, 1) = 1';

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

module.exports = { getByIdBagian };

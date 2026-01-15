// master-warehouse-service.js
const { poolPromise, sql } = require('../../../core/config/db');

async function listAll({
  includeDisabled = false,
  q = '',
  orderBy = 'NamaWarehouse',
  orderDir = 'ASC',
}) {
  const pool = await poolPromise;
  const request = pool.request();

  // whitelist biar aman dari SQL injection di ORDER BY
  const allowedOrderBy = new Set(['NamaWarehouse', 'IdWarehouse', 'Enable']);
  const orderCol = allowedOrderBy.has(orderBy) ? orderBy : 'NamaWarehouse';
  const dir = orderDir === 'DESC' ? 'DESC' : 'ASC';

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(Enable, 1) = 1';
  const hasSearch = q && q.trim().length > 0;

  let where = whereEnable;
  if (hasSearch) {
    where += ' AND (NamaWarehouse LIKE @q)';
    // typed param optional:
    // request.input('q', sql.VarChar(100), `%${q}%`);
    request.input('q', `%${q}%`);
  }

  const query = `
    SELECT
      IdWarehouse,
      NamaWarehouse,
      Enable
    FROM [dbo].[MstWarehouse]
    WHERE ${where}
    ORDER BY ${orderCol} ${dir};
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { listAll };

const { poolPromise } = require('../../core/config/db');

/**
 * Return ALL operators (no pagination).
 * - Active only by default (ISNULL(Enable,1)=1); pass includeDisabled=1 to include all.
 * - q: search by NamaOperator (LIKE %q%)
 * - orderBy: NamaOperator | IdOperator | Enable (whitelist)
 * - orderDir: ASC | DESC
 */
async function listAll({
  includeDisabled = false,
  q = '',
  orderBy = 'NamaOperator',
  orderDir = 'ASC',
}) {
  const pool = await poolPromise;
  const request = pool.request();

  const allowedOrderBy = new Set(['NamaOperator', 'IdOperator', 'Enable']);
  const orderCol = allowedOrderBy.has(orderBy) ? orderBy : 'NamaOperator';
  const dir = orderDir === 'DESC' ? 'DESC' : 'ASC';

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(Enable, 1) = 1';
  const hasSearch = q && q.trim().length > 0;

  let where = whereEnable;
  if (hasSearch) {
    where += ' AND (NamaOperator LIKE @q)';
    request.input('q', `%${q}%`);
  }

  const sql = `
    SELECT
      IdOperator,
      NamaOperator,
      Enable
    FROM [dbo].[MstOperator]
    WHERE ${where}
    ORDER BY ${orderCol} ${dir};
  `;

  const result = await request.query(sql);
  return result.recordset || [];
}

module.exports = { listAll };

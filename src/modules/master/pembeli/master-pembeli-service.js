// master-pembeli-service.js
const { poolPromise, sql } = require('../../../core/config/db'); 
// NOTE: if your db export does NOT include `sql`, remove it.
// For request.input with mssql you can omit type: request.input('q', value)

async function listAll({
  includeDisabled = false,
  q = '',
  orderBy = 'NamaPembeli',
  orderDir = 'ASC',
}) {
  const pool = await poolPromise;
  const request = pool.request();

  const allowedOrderBy = new Set(['NamaPembeli', 'IdPembeli', 'Enable']);
  const orderCol = allowedOrderBy.has(orderBy) ? orderBy : 'NamaPembeli';
  const dir = orderDir === 'DESC' ? 'DESC' : 'ASC';

  const whereEnable = includeDisabled ? '1=1' : 'ISNULL(Enable, 1) = 1';
  const hasSearch = q && q.trim().length > 0;

  let where = whereEnable;
  if (hasSearch) {
    where += ' AND (NamaPembeli LIKE @q)';
    // if you want typed param, use:
    // request.input('q', sql.VarChar(100), `%${q}%`);
    request.input('q', `%${q}%`);
  }

  const query = `
    SELECT
      IdPembeli,
      NamaPembeli,
      Enable
    FROM [dbo].[MstPembeli]
    WHERE ${where}
    ORDER BY ${orderCol} ${dir};
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { listAll };

// core/utils/get-user-permissions.js
const { sql, poolPromise } = require('../config/db');

/**
 * Ambil semua permission (NoPermission) berdasarkan IdUsername.
 * Digunakan di middleware attachPermissions dan saat login.
 */
async function getUserPermissions(idUsername) {
  if (!idUsername) return [];

  const pool = await poolPromise;
  const request = pool.request();
  request.input('IdUsername', sql.Int, idUsername);

  const query = `
    SELECT DISTINCT gp.NoPermission
    FROM dbo.MstUserGroupMember gm
    INNER JOIN dbo.MstUserGroupPermission gp
      ON gp.IdUGroup = gm.IdUGroup
    WHERE gm.IdUsername = @IdUsername
      AND gp.Allow = 1
  `;

  const result = await request.query(query);
  return result.recordset.map(r => r.NoPermission);
}

module.exports = getUserPermissions;

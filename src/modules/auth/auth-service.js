const { sql, poolPromise } = require('../../core/config/db');
const { hashPassword } = require('../../core/utils/crypto-helper');

async function verifyUser(username, password) {
  const pool = await poolPromise; // ✅ ambil pool global
  const hashedPassword = hashPassword(password);

  const result = await pool.request()
    .input('username', sql.VarChar, username)
    .input('password', sql.VarChar, hashedPassword)
    .query(`
      SELECT COUNT(*) AS count
      FROM MstUsername
      WHERE Username = @username AND Password = @password
    `);

  return result.recordset[0].count > 0;
}

module.exports = { verifyUser };

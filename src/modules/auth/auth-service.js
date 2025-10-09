const { sql, poolPromise } = require('../../core/config/db');
const { hashPassword } = require('../../core/utils/crypto-helper');

/**
 * ✅ Verifikasi user dan kembalikan data lengkapnya
 */
async function verifyUser(username, password) {
  const pool = await poolPromise;
  const hashedPassword = hashPassword(password);

  const result = await pool.request()
    .input('username', sql.VarChar, username)
    .input('password', sql.VarChar, hashedPassword)
    .query(`
      SELECT TOP 1 
        IdUsername,
        Username,
        FName,
        LName,
        Status,
        IsEnable
      FROM dbo.MstUsername
      WHERE Username = @username AND Password = @password
    `);

  // 🔹 kalau tidak ditemukan
  if (result.recordset.length === 0) {
    return null;
  }

  // 🔹 return data user (bisa langsung dimasukkan ke JWT)
  return result.recordset[0];
}

module.exports = { verifyUser };

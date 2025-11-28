const { sql, poolPromise } = require('../../core/config/db');
const { hashPassword } = require('../../core/utils/crypto-helper');

/**
 * ✅ Verifikasi user dan kembalikan data lengkapnya dengan error detail
 * 🔒 Security: Jangan berikan info apakah username ada atau tidak, hanya "credentials invalid"
 */
async function verifyUser(username, password) {
  const pool = await poolPromise;
  const hashedPassword = hashPassword(password);
  
  // 🔹 Query langsung dengan username DAN password
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

  // 🔹 Kalau tidak ditemukan (bisa username salah ATAU password salah)
  // Security best practice: jangan bedakan "user not found" vs "wrong password"
  if (result.recordset.length === 0) {
    return { 
      success: false, 
      errorType: 'invalid_credentials',
      message: 'Username atau password salah'
    };
  }

  const user = result.recordset[0];

  // 🔹 BARU CEK status setelah credentials valid
  // if (!user.IsEnable || user.Status !== 'Active') {
  //   return { 
  //     success: false, 
  //     errorType: 'user_inactive',
  //     message: 'Akun Anda tidak aktif. Hubungi administrator.'
  //   };
  // }

  // 🔹 Sukses - return data user
  return {
    success: true,
    user: {
      IdUsername: user.IdUsername,
      Username: user.Username,
      FName: user.FName,
      LName: user.LName,
      Status: user.Status,
      IsEnable: user.IsEnable
    }
  };
}

module.exports = { verifyUser };
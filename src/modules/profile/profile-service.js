const { sql, poolPromise } = require('../../core/config/db');
const { hashPassword } = require('../../core/utils/crypto-helper');

const getProfileService = async (username) => {
  const pool = await poolPromise;

  const result = await pool.request()
    .input('username', sql.VarChar, username)
    .query(`
      SELECT TOP 1 [Username], [FName], [LName], [Password]
      FROM MstUsername
      WHERE Username = @username
    `);

  return result.recordset[0] || null;
};

const changePasswordService = async (username, oldPassword, newPassword) => {
  const pool = await poolPromise;

  const hashedOld = hashPassword(oldPassword);
  const hashedNew = hashPassword(newPassword);

  const check = await pool.request()
    .input('username', sql.VarChar, username)
    .input('oldPassword', sql.VarChar, hashedOld)
    .query(`
      SELECT COUNT(*) AS count 
      FROM MstUsername 
      WHERE Username = @username AND Password = @oldPassword
    `);

  if (check.recordset[0].count === 0) {
    throw new Error('Password lama tidak cocok.');
  }

  await pool.request()
    .input('username', sql.VarChar, username)
    .input('newPassword', sql.VarChar, hashedNew)
    .query(`
      UPDATE MstUsername 
      SET Password = @newPassword 
      WHERE Username = @username
    `);
};

module.exports = { getProfileService, changePasswordService };

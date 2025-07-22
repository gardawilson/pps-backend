const { sql, connectDb } = require('../db');
const { hashPassword } = require('../utils/crypto-helper');

const getProfileService = async (username) => {
  await connectDb();

  const result = await sql.query`
    SELECT TOP 1 [Username], [FName], [LName], [Password]
    FROM MstUsername
    WHERE Username = ${username}
  `;

  return result.recordset[0] || null;
};

const changePasswordService = async (username, oldPassword, newPassword) => {
  await connectDb();

  const hashedOld = hashPassword(oldPassword);
  const hashedNew = hashPassword(newPassword);

  const check = await sql.query`
    SELECT COUNT(*) AS count FROM MstUsername WHERE Username = ${username} AND Password = ${hashedOld}
  `;

  if (check.recordset[0].count === 0) {
    throw new Error('Password lama tidak cocok.');
  }

  await sql.query`
    UPDATE MstUsername SET Password = ${hashedNew} WHERE Username = ${username}
  `;
};

module.exports = { getProfileService, changePasswordService };

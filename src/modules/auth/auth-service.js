const { sql, connectDb } = require('../../core/config/db');
const { hashPassword } = require('../../core/utils/crypto-helper');

async function verifyUser(username, password) {
  await connectDb();
  const hashedPassword = hashPassword(password);

  const result = await sql.query`
    SELECT COUNT(*) AS count FROM MstUsername WHERE Username = ${username} AND Password = ${hashedPassword}
  `;

  return result.recordset[0].count > 0;
}

module.exports = { verifyUser };

const crypto = require('crypto');

function hashPassword(password) {
  try {
    const md5 = crypto.createHash('md5').update(password).digest();
    const key = Buffer.concat([md5, md5.slice(0, 8)]);
    const cipher = crypto.createCipheriv('des-ede3', key, null);

    let encrypted = cipher.update(password, 'utf8', 'base64');
    encrypted += cipher.final('base64');

    return encrypted;
  } catch (err) {
    console.error('Error hashing password:', err);
    return null;
  }
}

module.exports = { hashPassword };

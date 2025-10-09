require('dotenv').config(); // Memuat file .env

const jwt = require('jsonwebtoken');
const secretKey = process.env.SECRET_KEY;

// Middleware untuk memverifikasi token dan mengekstrak data user
const verifyToken = (req, res, next) => {
  const token = req.header('Authorization')?.replace('Bearer ', '');

  if (!token) {
    return res.status(401).json({ message: 'Access Denied, token required.' });
  }

  try {
    // 🔹 Verifikasi dan decode token
    const decoded = jwt.verify(token, secretKey);

    // 🔹 Simpan info user ke request (biar bisa dipakai di controller)
    req.username = decoded.username;     // misal "admin"
    req.idUsername = decoded.idUsername; // misal 5

    next(); // lanjut ke handler berikutnya
  } catch (error) {
    return res.status(401).json({ message: 'Invalid or expired token.' });
  }
};

module.exports = verifyToken;

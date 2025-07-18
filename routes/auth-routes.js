require('dotenv').config();  // Menggunakan dotenv untuk memuat file .env

const express = require('express');
const crypto = require('crypto');  // Import crypto untuk hashing
const jwt = require('jsonwebtoken');  // Import jsonwebtoken untuk JWT
const router = express.Router();

const { sql, connectDb } = require('../db');  // Import koneksi DB

// Fungsi untuk mengenkripsi password (TripleDES)
function hashPassword(password) {
  try {
    const md5 = crypto.createHash('md5').update(password).digest();
    const key = Buffer.concat([md5, md5.slice(0, 8)]); // 16-byte -> 24-byte
    const cipher = crypto.createCipheriv('des-ede3', key, null);

    let encrypted = cipher.update(password, 'utf8', 'base64');
    encrypted += cipher.final('base64');

    return encrypted;
  } catch (err) {
    console.error('Error in hashing password:', err);
    return null;
  }
}

// Middleware untuk parsing JSON
router.use(express.json());

router.post('/login', async (req, res) => {
  const { username, password } = req.body;

  try {
    await connectDb();  // Koneksi ke database

    const hashedPassword = hashPassword(password);

    const result = await sql.query`
      SELECT COUNT(*) AS count FROM MstUsername WHERE Username = ${username} AND Password = ${hashedPassword}
    `;

    if (result.recordset[0].count > 0) {
      // Membuat JWT token
      const payload = { username };  // Payload hanya berisi username
      const secretKey = process.env.SECRET_KEY;  

      // Membuat token yang berlaku selama 1 jam
      const token = jwt.sign(payload, secretKey, { expiresIn: '12h' });

      // Kirimkan token ke client
      res.status(200).json({
        success: true,
        message: 'Login berhasil',
        token: token
      });
    } else {
      res.status(400).json({ success: false, message: 'Username atau password salah' });
    }
  } catch (err) {
    console.error('Error during login:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan di server' });
  }
});

module.exports = router;

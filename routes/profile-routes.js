require('dotenv').config();  // Menggunakan dotenv untuk memuat file .env

const express = require('express');
const crypto = require('crypto');  // Untuk hashing password
const jwt = require('jsonwebtoken');  // Import jsonwebtoken untuk memverifikasi token
const { sql, connectDb } = require('../db');  // Import koneksi DB

const router = express.Router();

// Fungsi untuk mengenkripsi password (TripleDES) seperti yang dilakukan pada authRoutes
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

// Middleware untuk memverifikasi token JWT
function verifyToken(req, res, next) {
  const token = req.header('Authorization')?.replace('Bearer ', '');  // Mengambil token dari header Authorization

  if (!token) {
    return res.status(401).json({ success: false, message: 'Akses ditolak. Token tidak ditemukan.' });
  }

  try {
    const secretKey = process.env.SECRET_KEY;
    const decoded = jwt.verify(token, secretKey);  // Verifikasi token

    req.user = decoded;  // Menyimpan data user yang terdekripsi dalam objek req
    next();
  } catch (err) {
    console.error('Token verification error:', err);
    res.status(401).json({ success: false, message: 'Token tidak valid.' });
  }
}

// Middleware untuk parsing JSON
router.use(express.json());


// Rute untuk mengambil profil berdasarkan username yang login
router.get('/profile', verifyToken, async (req, res) => {
  const { username } = req.user;  // Mendapatkan username dari token yang sudah didekodekan

  try {
    await connectDb();  // Koneksi ke database

    // Mengambil data profil dari tabel MstUsername berdasarkan username
    const result = await sql.query`
        SELECT TOP 1 [Username], [FName], [LName], [Password] 
        FROM [WPS_Test].[dbo].[MstUsername] 
        WHERE Username = ${username}
      `;

    if (result.recordset.length > 0) {
      const userProfile = result.recordset[0];  // Ambil data profil user pertama

      res.status(200).json({
        success: true,
        message: 'Profil ditemukan',
        data: userProfile
      });
    } else {
      res.status(404).json({ success: false, message: 'Profil tidak ditemukan' });
    }
  } catch (err) {
    console.error('Error during fetching profile:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan di server' });
  }
});


// Rute untuk mengganti password
router.post('/change-password', verifyToken, async (req, res) => {
  const { username } = req.user;  // Mendapatkan username dari token yang sudah didekodekan
  const { oldPassword, newPassword, confirmPassword } = req.body;  // Mendapatkan password lama, baru, dan konfirmasi password dari request body

  // Pastikan oldPassword, newPassword, dan confirmPassword diisi
  if (!oldPassword || !newPassword || !confirmPassword) {
    return res.status(400).json({ success: false, message: 'Semua password harus diisi.' });
  }

  // Memastikan password baru dan konfirmasi password cocok
  if (newPassword !== confirmPassword) {
    return res.status(400).json({ success: false, message: 'Password baru dan konfirmasi password tidak cocok.' });
  }

  try {
    await connectDb();  // Koneksi ke database

    // Enkripsi password lama
    const hashedOldPassword = hashPassword(oldPassword);

    // Memastikan password lama yang dimasukkan sesuai dengan yang ada di database
    const result = await sql.query`
        SELECT COUNT(*) AS count FROM MstUsername WHERE Username = ${username} AND Password = ${hashedOldPassword}
      `;

    if (result.recordset[0].count === 0) {
      return res.status(400).json({ success: false, message: 'Password lama tidak cocok.' });
    }

    // Enkripsi password baru
    const hashedNewPassword = hashPassword(newPassword);

    // Update password baru di database
    await sql.query`
        UPDATE MstUsername SET Password = ${hashedNewPassword} WHERE Username = ${username}
      `;

    res.status(200).json({
      success: true,
      message: 'Password berhasil diganti.'
    });
  } catch (err) {
    console.error('Error during password change:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan di server' });
  }
});


module.exports = router;

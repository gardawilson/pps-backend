require('dotenv').config();  // Memuat file .env

const sql = require('mssql');

// Konfigurasi koneksi ke SQL Server
const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  port: parseInt(process.env.DB_PORT, 10),
  database: process.env.DB_DATABASE,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

// Fungsi untuk mendapatkan koneksi pool
const connectDb = async () => {
  try {
    const pool = await sql.connect(dbConfig);
    // console.log('✅ Koneksi ke database berhasil');
    return pool; // ⬅️ penting: return pool instance
  } catch (err) {
    console.error('❌ Koneksi ke database gagal:', err.message);
    throw err;
  }
};

module.exports = { connectDb, sql };

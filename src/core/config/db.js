require('dotenv').config();  // Menggunakan dotenv untuk memuat file .env

const sql = require('mssql');

// Konfigurasi koneksi ke SQL Server menggunakan variabel .env
const dbConfig = {
  user: process.env.DB_USER,  // Menggunakan variabel dari .env
  password: process.env.DB_PASSWORD,  // Menggunakan variabel dari .env
  server: process.env.DB_SERVER,  // Menggunakan variabel dari .env
  port: parseInt(process.env.DB_PORT),  // Menggunakan variabel dari .env
  database: process.env.DB_DATABASE,  // Menggunakan variabel dari .env
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

// Fungsi untuk mendapatkan koneksi
const connectDb = async () => {
  try {
    await sql.connect(dbConfig);
    // console.log('Koneksi ke database berhasil');
  } catch (err) {
    // console.error('Koneksi ke database gagal', err);
  }
};

module.exports = { connectDb, sql };

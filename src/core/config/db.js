require("dotenv").config();
const sql = require("mssql");

// Konfigurasi koneksi ke SQL Server
const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  port: parseInt(process.env.DB_PORT, 10),
  database: process.env.DB_DATABASE,
  options: {
    encrypt: true, // true untuk Azure / SSL
    trustServerCertificate: true, // true untuk local/self-signed
  },
  pool: {
    max: 10, // jumlah koneksi maksimum
    min: 0, // minimum koneksi
    idleTimeoutMillis: 30000, // tutup koneksi idle setelah 30 detik
  },
};

// Buat pool global (sekali saja)
const poolPromise = new sql.ConnectionPool(dbConfig)
  .connect()
  .then((pool) => {
    console.log("✅ Koneksi DB berhasil");
    return pool;
  })
  .catch((err) => {
    console.error("❌ Koneksi DB gagal:", err.message);
    throw err;
  });

// Graceful shutdown saat server berhenti
process.on("SIGINT", async () => {
  try {
    const pool = await poolPromise;
    await pool.close();
    console.log("🛑 Pool DB ditutup dengan aman");
    process.exit(0);
  } catch (err) {
    console.error("❌ Gagal menutup pool DB:", err.message);
    process.exit(1);
  }
});

module.exports = {
  sql,
  poolPromise,
};

require('dotenv').config();  // Memuat file .env
const express = require('express');
const cors = require('cors');  // Menggunakan CORS untuk menangani permintaan lintas asal
const bodyParser = require('body-parser');
const { connectDb } = require('./db');  // Menghubungkan ke database
const http = require('http');  // Untuk membuat server HTTP
const WebSocket = require('ws');  // Menggunakan WebSocket
const authRoutes = require('./routes/auth-routes');  // Rute untuk autentikasi
const stockOpnameRoutes = require('./routes/stock-opname-routes');  // Rute untuk Stock Opname
const profileRoutes = require('./routes/profile-routes');  // Rute untuk Akun
const mstLokasiRoutes = require('./routes/master-lokasi-routes');



const app = express();
const server = http.createServer(app);  // Membuat server HTTP menggunakan express
const wss = new WebSocket.Server({ server });  // Membuat WebSocket server

const port = process.env.PORT || 8000;  // Menggunakan port dari .env atau default 5000

// Middleware untuk parsing JSON dari body request
app.use(express.json());

// Middleware untuk menangani CORS
app.use(cors());

// Middleware untuk parsing JSON
app.use(bodyParser.json());

// Menggunakan rute autentikasi dan stock opname
app.use('/api', authRoutes);  // Rute autentikasi
app.use('/api', stockOpnameRoutes);  // Rute stock opname
app.use('/api', profileRoutes);  // Rute stock opname
app.use('/api', mstLokasiRoutes);

// WebSocket connection handling
wss.on('connection', (ws) => {
  console.log('Client connected via WebSocket');

  // Mengirim pesan ke klien setelah koneksi
  ws.send('Welcome to WebSocket server!');


  // Menangani koneksi yang terputus
  ws.on('close', () => {
    console.log('Client disconnected');
  });
});

// Menjalankan server pada port yang sudah ditentukan
server.listen(port, () => {
  console.log(`Server berjalan di http://localhost:${port}`);
  connectDb(); // Pastikan koneksi ke database berhasil
});

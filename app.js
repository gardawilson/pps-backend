require('dotenv').config(); // Memuat file .env
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const http = require('http');
const socketIO = require('socket.io'); // Ganti dari 'ws' ke 'socket.io'

const { connectDb } = require('./db');
const authRoutes = require('./routes/auth-routes');
const stockOpnameRoutes = require('./routes/stock-opname-routes');
const profileRoutes = require('./routes/profile-routes');
const mstLokasiRoutes = require('./routes/master-lokasi-routes');
const initSocket = require('./socket'); // Gunakan file socket.js

const app = express();
const server = http.createServer(app); // Gunakan http server untuk socket.io
const io = socketIO(server, {
  cors: {
    origin: '*', // Ganti sesuai kebutuhan
    methods: ['GET', 'POST'],
  }
});

// Inisialisasi Socket.IO handler
initSocket(io);

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(express.json());

// Routes
app.use('/api', authRoutes);
app.use('/api', stockOpnameRoutes);
app.use('/api', profileRoutes);
app.use('/api', mstLokasiRoutes);

// Start server
const port = process.env.PORT || 7500;
server.listen(port, () => {
  console.log(`🚀 Server berjalan di http://localhost:${port}`);
  connectDb(); // Pastikan koneksi ke DB
});

require('dotenv').config(); // Memuat file .env
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const http = require('http');
const socketIO = require('socket.io');

// Import konfigurasi database
const { connectDb } = require('./src/core/config/db');

// Import routes dengan path yang benar
const authRoutes = require('./src/modules/auth/auth-routes');
const stockOpnameRoutes = require('./src/modules/stock-opname/stock-opname-routes');
const profileRoutes = require('./src/modules/profile/profile-routes');
const mstLokasiRoutes = require('./src/modules/master-lokasi/master-lokasi-routes');
const detailLabelRoutes = require('./src/modules/label/label-detail-routes');


// Import socket handler
const initSocket = require('./src/core/socket/index'); // Sesuai struktur folder

// Import middleware
const verifyToken = require('./src/core/middleware/verify-token');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
  cors: {
    origin: process.env.FRONTEND_URL || '*', 
    methods: ['GET', 'POST'],
    credentials: true
  }
});

// Inisialisasi Socket.IO handler
initSocket(io);

// Global middleware
app.use(cors({
  origin: process.env.FRONTEND_URL || '*',
  credentials: true
}));
app.use(bodyParser.json({ limit: '10mb' })); // Tambah limit untuk file upload
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'OK', 
    message: 'Server is running',
    timestamp: new Date().toISOString()
  });
});

// API Routes
app.use('/api', authRoutes);
app.use('/api', stockOpnameRoutes);
app.use('/api', profileRoutes);
app.use('/api', mstLokasiRoutes);
app.use('/api', detailLabelRoutes);


// Protected routes (contoh jika ada route yang perlu authentication)
// app.use('/api/protected', verifyToken, protectedRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Error:', err.stack);
  res.status(500).json({
    success: false,
    message: 'Something went wrong!',
    error: process.env.NODE_ENV === 'development' ? err.message : 'Internal Server Error'
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    message: 'Route not found'
  });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  server.close(() => {
    console.log('Process terminated');
  });
});

// Start server
const port = process.env.PORT || 7500;
server.listen(port, () => {
  console.log(`🚀 Server berjalan di http://localhost:${port}`);
  console.log(`📝 Health check: http://localhost:${port}/health`);
  
  // Connect to database
  connectDb();
});

module.exports = app;
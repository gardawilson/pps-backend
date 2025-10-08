require('dotenv').config();
const http = require('http');
const socketIO = require('socket.io');
const app = require('./src/app');
const { poolPromise } = require('./src/core/config/db'); // pakai poolPromise, bukan connectDb
const initSocket = require('./src/core/socket');
const getLocalIp = require('./src/core/utils/get-local-ip'); 

const port = process.env.PORT || 7500;
const server = http.createServer(app);

// Konfigurasi Socket.IO
const io = socketIO(server, {
  cors: {
    origin: process.env.FRONTEND_URL || '*',
    methods: ['GET', 'POST'],
    credentials: true
  }
});
initSocket(io);

// Start server
server.listen(port, () => {
  const ip = getLocalIp();
  console.log('✅ Server berjalan:');
  console.log(`   Local:   http://localhost:${port}`);
  console.log(`   Network: http://${ip}:${port}`);
});

// Graceful shutdown
async function shutdown() {
  console.log('🛑 Shutting down gracefully...');
  server.close(async () => {
    try {
      const pool = await poolPromise;
      await pool.close();
      console.log('✅ Database pool closed');
    } catch (err) {
      console.error('❌ Error closing DB pool:', err.message);
    }
    process.exit(0);
  });
}

// Handle signals
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

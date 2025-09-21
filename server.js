require('dotenv').config();
const http = require('http');
const socketIO = require('socket.io');
const app = require('./src/app');
const { connectDb } = require('./src/core/config/db');
const initSocket = require('./src/core/socket');
const getLocalIp = require('./src/core/utils/get-local-ip'); // ✅ import

const port = process.env.PORT || 7500;
const server = http.createServer(app);

const io = socketIO(server, {
  cors: {
    origin: process.env.FRONTEND_URL || '*',
    methods: ['GET', 'POST'],
    credentials: true
  }
});
initSocket(io);

server.listen(port, () => {
  const ip = getLocalIp();
  console.log('✅ Server berjalan:');
  console.log(`   Local:   http://localhost:${port}`);
  console.log(`   Network: http://${ip}:${port}`);
  
  connectDb();
});

process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  server.close(() => {
    console.log('Process terminated');
  });
});

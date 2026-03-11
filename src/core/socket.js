const { setIo } = require('./utils/socket-instance');
const printLock = require('./utils/print-lock');

function initSocket(io) {
  setIo(io);

  io.on('connection', (socket) => {
    console.info(`[Socket.io] Client connected: ${socket.id}`);

    // Kirim snapshot lock yang sedang aktif saat client pertama konek
    // → Flutter langsung tahu lock mana yang sedang aktif tanpa perlu hit REST
    const activeLocks = printLock.getAllLocks();
    socket.emit('initial_locks', activeLocks);

    socket.on('disconnect', (reason) => {
      console.info(`[Socket.io] Client disconnected: ${socket.id} (${reason})`);
    });
  });
}

module.exports = initSocket;

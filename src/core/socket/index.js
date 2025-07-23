module.exports = function(io) {
    io.on('connection', (socket) => {
      console.log('🔌 Socket terhubung:', socket.id);
  
      // Contoh kirim pesan ke client
      socket.emit('server_message', 'Terhubung ke server via Socket.IO!');
  
      // Tangani event dari client
      socket.on('client_message', (data) => {
        console.log('📨 Pesan dari client:', data);
      });
  
      socket.on('disconnect', () => {
        console.log('❌ Socket terputus:', socket.id);
      });
    });
  
    // Simpan ke global agar bisa dipakai di route lain
    global.io = io;
  };
  
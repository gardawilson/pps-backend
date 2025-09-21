const os = require('os');

function getLocalIp() {
  const interfaces = os.networkInterfaces();
  for (let k in interfaces) {
    for (let iface of interfaces[k]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address; // return IP pertama yang ketemu
      }
    }
  }
  return 'localhost'; // fallback
}

module.exports = getLocalIp;

/**
 * Socket.io singleton instance.
 * Diisi oleh initSocket() di server startup, dipakai di mana saja via getIo().
 */

let _io = null;

function setIo(io) {
  _io = io;
}

function getIo() {
  return _io;
}

module.exports = { setIo, getIo };

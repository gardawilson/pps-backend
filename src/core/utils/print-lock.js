/**
 * In-memory Print Lock Manager
 *
 * Mencegah 2 user mencetak label yang sama secara bersamaan.
 * Lock otomatis expired setelah TTL untuk antisipasi user yang keluar
 * dari preview tanpa release lock.
 *
 * Key: noLabel langsung (sudah unik global karena prefix berbeda per kategori)
 *   e.g. "B.0000000003" (washing), "D.0000000001" (broker), "H.0000000001" (mixer)
 */

const { getIo } = require("./socket-instance");

const DEFAULT_TTL_MS = 3 * 60 * 1000; // 3 menit

/** @type {Map<string, { lockedBy: number, username: string, lockedAt: Date, expiresAt: Date }>} */
const locks = new Map();

/** Broadcast event ke semua client yang terhubung via Socket.io */
function broadcast(event, data) {
  const io = getIo();
  if (io) io.emit(event, data);
}

/**
 * Coba acquire lock untuk label tertentu.
 * @returns {{ ok: true } | { ok: false, reason: string, lockedBy: string, lockedAt: Date, expiresAt: Date }}
 */
function acquireLock(noLabel, actorId, username, ttlMs = DEFAULT_TTL_MS) {
  const now = new Date();
  const existing = locks.get(noLabel);

  if (existing) {
    if (existing.expiresAt > now) {
      // Lock masih aktif — tolak jika bukan pemiliknya
      if (existing.lockedBy !== actorId) {
        return {
          ok: false,
          reason: `Label sedang dibuka untuk print oleh ${existing.username}`,
          lockedBy: existing.username,
          lockedAt: existing.lockedAt,
          expiresAt: existing.expiresAt,
        };
      }
      // Pemilik yang sama → perpanjang lock (idempotent), tidak perlu broadcast ulang
      existing.expiresAt = new Date(now.getTime() + ttlMs);
      return { ok: true };
    }
    // Lock expired → hapus dan lanjut
    locks.delete(noLabel);
  }

  const lockedAt = now;
  const expiresAt = new Date(now.getTime() + ttlMs);

  locks.set(noLabel, {
    lockedBy: actorId,
    username: username || `user#${actorId}`,
    lockedAt,
    expiresAt,
  });

  // Broadcast ke semua client → UI otomatis tampilkan lock
  broadcast("lock_acquired", {
    noLabel,
    lockedBy: username || `user#${actorId}`,
    lockedAt,
    expiresAt,
  });

  return { ok: true };
}

/**
 * Release lock. Hanya pemilik lock yang bisa release (kecuali force=true).
 * @returns {{ ok: true } | { ok: false, reason: string }}
 */
function releaseLock(noLabel, actorId, force = false) {
  const existing = locks.get(noLabel);

  if (!existing) return { ok: true };

  if (!force && existing.lockedBy !== actorId) {
    return {
      ok: false,
      reason: `Lock dimiliki oleh ${existing.username}, bukan Anda`,
    };
  }

  locks.delete(noLabel);

  // Broadcast ke semua client → UI otomatis hapus lock indicator
  broadcast("lock_released", { noLabel });

  return { ok: true };
}

/**
 * Ambil semua lock yang masih aktif (belum expired).
 * Sekaligus membersihkan lock yang sudah expired.
 */
function getAllLocks() {
  const now = new Date();
  const result = [];

  for (const [noLabel, data] of locks.entries()) {
    if (data.expiresAt <= now) {
      locks.delete(noLabel);
      continue;
    }
    result.push({
      noLabel,
      lockedBy: data.lockedBy,
      username: data.username,
      lockedAt: data.lockedAt,
      expiresAt: data.expiresAt,
      remainingMs: data.expiresAt.getTime() - now.getTime(),
    });
  }

  return result;
}

module.exports = { acquireLock, releaseLock, getAllLocks };

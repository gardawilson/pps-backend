const printLock = require("../../../core/utils/print-lock");
const {
  getActorId,
  getActorUsername,
} = require("../../../core/utils/http-context");

exports.getAllLocks = (_req, res) => {
  const locks = printLock.getAllLocks();
  return res
    .status(200)
    .json({ success: true, total: locks.length, data: locks });
};

exports.acquire = (req, res) => {
  const noLabel = String(req.params.noLabel || "").trim();
  if (!noLabel) {
    return res
      .status(400)
      .json({ success: false, message: "noLabel wajib diisi" });
  }

  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({ success: false, message: "Unauthorized" });
  }

  const username = getActorUsername(req) || `user#${actorId}`;
  const result = printLock.acquireLock(noLabel, actorId, username);

  if (!result.ok) {
    console.warn(
      `[PrintLock] BLOCKED acquire — noLabel=${noLabel} oleh ${username} (id=${actorId}) | ditolak: sudah di-lock oleh ${result.lockedBy} hingga ${result.expiresAt}`,
    );
    return res.status(409).json({
      success: false,
      message: result.reason,
      lockedBy: result.lockedBy,
      lockedAt: result.lockedAt,
      expiresAt: result.expiresAt,
    });
  }

  console.info(
    `[PrintLock] ACQUIRED — noLabel=${noLabel} oleh ${username} (id=${actorId})`,
  );
  return res
    .status(200)
    .json({ success: true, message: "Print lock berhasil diperoleh" });
};

exports.release = (req, res) => {
  const noLabel = String(req.params.noLabel || "").trim();

  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({ success: false, message: "Unauthorized" });
  }

  const username = getActorUsername(req) || `user#${actorId}`;
  const result = printLock.releaseLock(noLabel, actorId);

  if (!result.ok) {
    console.warn(
      `[PrintLock] BLOCKED release — noLabel=${noLabel} oleh ${username} (id=${actorId}) | ${result.reason}`,
    );
    return res.status(403).json({ success: false, message: result.reason });
  }

  console.info(
    `[PrintLock] RELEASED — noLabel=${noLabel} oleh ${username} (id=${actorId})`,
  );
  return res
    .status(200)
    .json({ success: true, message: "Print lock berhasil dilepas" });
};

const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("./http-context");

function buildAuditContext(req, res) {
  const actorId = getActorId(req);
  if (!actorId) {
    const err = new Error("Unauthorized (idUsername missing)");
    err.statusCode = 401;
    throw err;
  }

  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";

  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId && res) res.setHeader("x-request-id", requestId);

  return { actorId, actorUsername, requestId };
}

module.exports = { buildAuditContext };

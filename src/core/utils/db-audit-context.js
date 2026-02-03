// src/core/utils/db-audit-context.js
const sql = require("mssql");

/**
 * Set audit context ke SESSION_CONTEXT pada connection yang sama (req harus dibuat dari tx!)
 * @param {sql.Request} req - harus new sql.Request(tx)
 * @param {object} ctx
 * @param {number} ctx.actorId
 * @param {string} ctx.actorUsername
 * @param {string} ctx.requestId
 */
exports.applyAuditContext = async (
  req,
  { actorId, actorUsername, requestId } = {},
) => {
  const actorIdNum = Number(actorId);
  const actorIdSafe =
    Number.isFinite(actorIdNum) && actorIdNum > 0
      ? Math.trunc(actorIdNum)
      : null;

  if (!actorIdSafe)
    throw badReq(
      "actorId kosong / tidak valid. Controller harus inject actorId dari token.",
    );

  const actorSafe = String(actorUsername || "").trim() || null;
  const ridSafe =
    String(requestId || "").trim() ||
    `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  req.input("actorId", sql.Int, actorIdSafe);
  req.input("actor", sql.NVarChar(128), actorSafe);
  req.input("rid", sql.NVarChar(64), ridSafe);

  await req.query(`
    EXEC sys.sp_set_session_context @key=N'actor_id',  @value=@actorId, @read_only=0;
    EXEC sys.sp_set_session_context @key=N'actor',     @value=@actor,   @read_only=0;
    EXEC sys.sp_set_session_context @key=N'request_id',@value=@rid,     @read_only=0;
  `);

  // optional: return normalized ctx
  return { actorId: actorIdSafe, actorUsername: actorSafe, requestId: ridSafe };
};

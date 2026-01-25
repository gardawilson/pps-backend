// src/core/utils/http-context.js

function getActorId(req) {
  const n = Number(req?.idUsername);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function getActorUsername(req) {
  return String(req?.username || '').trim() || null;
}

function makeRequestId(req) {
  const fromHeader =
    String(req?.headers?.['x-request-id'] || '').trim() ||
    String(req?.headers?.['x-correlation-id'] || '').trim();

  if (fromHeader) return fromHeader;

  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

module.exports = { getActorId, getActorUsername, makeRequestId };

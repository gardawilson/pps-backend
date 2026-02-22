// Utility helpers for numeric inputs
const { badReq } = require("./http-error") || require("../utils/http-error");

const normalizeDecimalField = (raw, fieldName) => {
  if (raw === undefined) return undefined;
  if (raw === null) return null;
  const s = String(raw).trim();
  if (s === "" || s === "-") return null;
  const n = Number(s);
  if (!Number.isFinite(n)) {
    throw badReq(`${fieldName} tidak valid: ${raw}`);
  }
  return n;
};

module.exports = { normalizeDecimalField };

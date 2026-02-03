// ============================================================
// utils/parse.js
// ============================================================

// ----------------------
// CREATE helpers
// ----------------------
const toIntCreate = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : Math.trunc(n);
};

const toFloatCreate = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
};

const toBitCreate = (v) => {
  if (v === true || v === 1 || v === "1" || v === "true") return 1;
  if (v === false || v === 0 || v === "0" || v === "false") return 0;
  return null;
};

// ----------------------
// UPDATE helpers (undef = field not sent)
// ----------------------
const toIntUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : Math.trunc(n);
};

const toFloatUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
};

const toBitUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null || v === "") return null;
  if (v === true || v === 1 || v === "1" || v === "true") return 1;
  if (v === false || v === 0 || v === "0" || v === "false") return 0;
  return null;
};

const toStrUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null) return null;
  return String(v);
};

// ----------------------
// General helpers (bisa dipakai di Create / Update)
// ----------------------
const toInt = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : Math.trunc(n);
};

const toFloat = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
};

const normalizeTime = (v) => {
  if (v === undefined) return undefined;
  if (v === null) return null;
  const s = String(v).trim();
  return s ? s : null;
};

// ----------------------
// BIT helpers
// ----------------------
const toBit = (v) => {
  if (v === undefined || v === null || v === "") return null;
  if (typeof v === "boolean") return v ? 1 : 0;
  const n = parseInt(v, 10);
  return isNaN(n) ? null : n ? 1 : 0;
};

// ----------------------
// Jam INT helper ("HH:mm" / "HH:mm:ss" -> HH)
// ----------------------
const toJamInt = (v) => {
  if (v === undefined || v === null || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? Math.trunc(v) : null;
  const s = String(v).trim();
  if (!s) return null;
  const m = s.match(/^(\d{1,2})(?::\d{2})?(?::\d{2})?$/);
  if (m) return parseInt(m[1], 10);
  const n = parseInt(s, 10);
  return isNaN(n) ? null : n;
};

module.exports = {
  // general
  toInt,
  toFloat,
  normalizeTime,
  toBit,
  toJamInt,

  // CREATE helpers
  toIntCreate,
  toFloatCreate,
  toBitCreate,

  // UPDATE helpers
  toIntUndef,
  toFloatUndef,
  toBitUndef,
  toStrUndef,
};

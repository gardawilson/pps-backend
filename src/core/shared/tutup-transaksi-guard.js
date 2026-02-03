// src/core/shared/tutup-transaksi.js
// ✅ UTC-only implementation + helper config-based doc date lookup
const { sql, poolPromise } = require("../config/db");

// Optional config (kalau file config belum dibuat, helper config-based akan throw dengan pesan jelas)
let TUTUP_TRANSAKSI_SOURCES = null;
try {
  // 👉 buat file ini: src/core/config/tutup-transaksi-config.js
  // module.exports = { TUTUP_TRANSAKSI_SOURCES: { ... } }
  ({ TUTUP_TRANSAKSI_SOURCES } = require("../config/tutup-transaksi-config"));
} catch (_) {
  TUTUP_TRANSAKSI_SOURCES = null;
}

async function getRequest(runner) {
  const r = typeof runner?.then === "function" ? await runner : runner;
  if (r instanceof sql.Request) return r;
  if (r instanceof sql.Transaction) return new sql.Request(r);
  if (r?.request) return r.request();
  const pool = await poolPromise;
  return pool.request();
}

/**
 * Normalize input into a "date-only" JS Date that represents UTC midnight (00:00:00Z).
 * This avoids timezone shifting when passing to mssql/sql.Date.
 */
function toDateOnly(value) {
  if (!value) return null;

  // If Date object: take its UTC Y/M/D as date-only
  if (value instanceof Date && !isNaN(value.getTime())) {
    return new Date(
      Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()),
    );
  }

  const s = String(value).trim();
  if (!s) return null;

  // Handle "YYYY-MM-DD" safely (no timezone ambiguity)
  const s10 = s.length >= 10 ? s.substring(0, 10) : s;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s10);
  if (m) {
    const y = Number(m[1]),
      mo = Number(m[2]),
      d = Number(m[3]);
    const dt = new Date(Date.UTC(y, mo - 1, d)); // ✅ UTC midnight
    if (!isNaN(dt.getTime())) return dt;
  }

  // Otherwise parse as Date (ISO datetime etc.), then normalize to UTC date-only
  const dt2 = new Date(s);
  if (!isNaN(dt2.getTime())) {
    return new Date(
      Date.UTC(dt2.getUTCFullYear(), dt2.getUTCMonth(), dt2.getUTCDate()),
    );
  }

  return null;
}

/**
 * Format YYYY-MM-DD using UTC getters (konsisten dengan toDateOnly)
 */
function formatYMD(d) {
  if (!d) return null;
  const pad = (n) => (n < 10 ? `0${n}` : `${n}`);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

/**
 * lastClosed = max(PeriodHarian) where Lock=1
 * NOTE: PeriodHarian dari SQL (date/datetime) -> normalize to UTC date-only
 */
async function getLastClosedPeriod({ runner, useLock = false } = {}) {
  const request = await getRequest(runner);
  const hint = useLock ? "WITH (UPDLOCK, HOLDLOCK)" : "WITH (NOLOCK)";

  const q = `
    SELECT TOP 1 Id, CONVERT(date, PeriodHarian) AS PeriodHarian, [Lock]
    FROM dbo.MstTutupTransaksiHarian ${hint}
    WHERE [Lock] = 1
    ORDER BY CONVERT(date, PeriodHarian) DESC, Id DESC
  `;

  const res = await request.query(q);
  const row = res.recordset?.[0] || null;

  const lastClosed = row?.PeriodHarian ? toDateOnly(row.PeriodHarian) : null;

  return { lastClosed, row };
}

/**
 * RULE: trxDate must be > lastClosed
 * Semua compare dilakukan dalam UTC date-only.
 */
async function assertDateAfterLastClosed({
  date,
  runner,
  action = "transaction",
  useLock = false,
} = {}) {
  const trxDate = toDateOnly(date);
  if (!trxDate) return { ok: true, trxDate: null, lastClosed: null, row: null };

  const { lastClosed, row } = await getLastClosedPeriod({ runner, useLock });

  if (lastClosed && trxDate.getTime() <= lastClosed.getTime()) {
    // nextAllowed = lastClosed + 1 day (UTC)
    const nextAllowed = new Date(lastClosed.getTime() + 24 * 60 * 60 * 1000);

    const e = new Error(
      `Tidak bisa ${action}: transaksi tanggal ${formatYMD(trxDate)} sudah ditutup (last closed: ${formatYMD(lastClosed)}). ` +
        `Silakan input minimal tanggal ${formatYMD(nextAllowed)}.`,
    );
    e.statusCode = 423;
    e.code = "TUTUP_TRANSAKSI_LOCKED";
    e.meta = {
      trxDate: formatYMD(trxDate),
      lastClosed: formatYMD(lastClosed),
      row,
    };
    throw e;
  }

  return { ok: true, trxDate, lastClosed, row };
}

/**
 * Backward-compatible name (biar kode kamu tetap sama):
 * assertNotLocked = assertDateAfterLastClosed
 */
async function assertNotLocked(p = {}) {
  return assertDateAfterLastClosed(p);
}

/**
 * Effective date for create:
 * - if bodyDate provided => UTC date-only
 * - else => "today" based on current UTC date (bukan WIB), to keep UTC consistency end-to-end
 */
function resolveEffectiveDateForCreate(bodyDate) {
  const d = toDateOnly(bodyDate) ?? toDateOnly(new Date());
  return d;
}

/**
 * Helper: ambil tanggal dokumen (date-only) dari tabel tertentu secara GENERIC.
 * Cocok untuk menghindari perulangan query "SELECT DateCreate FROM ... WHERE ..."
 *
 * @param {Object} p
 * @param {sql.Transaction|sql.Request|Pool} p.runner - tx/request/pool
 * @param {string} p.table - contoh: 'dbo.Washing_h'
 * @param {string} p.codeColumn - contoh: 'NoWashing'
 * @param {string} p.dateColumn - contoh: 'DateCreate'
 * @param {string|number} p.codeValue - contoh: 'B.0000000123'
 * @param {boolean} p.useLock - default true (UPDLOCK, HOLDLOCK)
 * @param {boolean} p.throwIfNotFound - default true
 */
async function loadDocDateOnlyFromTable({
  runner,
  table,
  codeColumn,
  dateColumn,
  codeValue,
  useLock = true,
  throwIfNotFound = true,
} = {}) {
  if (!table) {
    const e = new Error("table wajib diisi");
    e.statusCode = 500;
    throw e;
  }
  if (!codeColumn) {
    const e = new Error("codeColumn wajib diisi");
    e.statusCode = 500;
    throw e;
  }
  if (!dateColumn) {
    const e = new Error("dateColumn wajib diisi");
    e.statusCode = 500;
    throw e;
  }
  if (
    codeValue === undefined ||
    codeValue === null ||
    String(codeValue).trim() === ""
  ) {
    const e = new Error("codeValue wajib diisi");
    e.statusCode = 400;
    throw e;
  }

  const request = await getRequest(runner);
  const hint = useLock ? "WITH (UPDLOCK, HOLDLOCK)" : "WITH (NOLOCK)";

  // NOTE: table/column tidak bisa jadi parameter, jadi harus dari config/static.
  // Pastikan table/column berasal dari kode internal (bukan input user mentah).
  const q = `
    SELECT TOP 1
      ${codeColumn} AS CodeValue,
      CONVERT(date, ${dateColumn}) AS DocDate
    FROM ${table} ${hint}
    WHERE ${codeColumn} = @code
  `;

  const res = await request
    .input("code", sql.VarChar, String(codeValue))
    .query(q);
  const row = res.recordset?.[0] || null;

  if (!row) {
    if (!throwIfNotFound) return { found: false, docDateOnly: null, row: null };

    const e = new Error(
      `Dokumen tidak ditemukan: ${table}.${codeColumn} = ${String(codeValue)}`,
    );
    e.statusCode = 404;
    e.code = "DOC_NOT_FOUND";
    e.meta = { table, codeColumn, dateColumn, codeValue: String(codeValue) };
    throw e;
  }

  const docDateOnly = row.DocDate ? toDateOnly(row.DocDate) : null;
  return { found: true, docDateOnly, row };
}

/**
 * Helper (recommended): ambil tanggal dokumen berdasarkan CONFIG key.
 * Ini yang bikin CRUD kamu gak perlu ulang query ambil tanggal.
 *
 * @param {Object} p
 * @param {string} p.entityKey - key config, contoh: 'washingLabel'
 * @param {string|number} p.codeValue - nilai kode, contoh: NoWashing
 * @param {sql.Transaction|sql.Request|Pool} p.runner - tx/request/pool
 * @param {boolean} p.useLock - default true
 * @param {boolean} p.throwIfNotFound - default true
 */
async function loadDocDateOnlyFromConfig({
  entityKey,
  codeValue,
  runner,
  useLock = true,
  throwIfNotFound = true,
} = {}) {
  if (!entityKey) {
    const e = new Error("entityKey wajib diisi");
    e.statusCode = 500;
    throw e;
  }
  if (!TUTUP_TRANSAKSI_SOURCES) {
    const e = new Error(
      `TUTUP_TRANSAKSI_SOURCES belum tersedia. Buat file: src/core/config/tutup-transaksi-config.js`,
    );
    e.statusCode = 500;
    e.code = "TUTUP_TRANSAKSI_CONFIG_MISSING";
    throw e;
  }

  const cfg = TUTUP_TRANSAKSI_SOURCES[entityKey];
  if (!cfg) {
    const e = new Error(
      `Config tutup transaksi tidak ditemukan untuk entityKey=${entityKey}`,
    );
    e.statusCode = 500;
    e.code = "TUTUP_TRANSAKSI_CONFIG_NOT_FOUND";
    e.meta = { entityKey, availableKeys: Object.keys(TUTUP_TRANSAKSI_SOURCES) };
    throw e;
  }

  // Support alias field names for flexibility
  const table = cfg.table;
  const codeColumn = cfg.codeColumn;
  const dateColumn = cfg.dateColumn;

  return loadDocDateOnlyFromTable({
    runner,
    table,
    codeColumn,
    dateColumn,
    codeValue,
    useLock,
    throwIfNotFound,
  });
}

module.exports = {
  // existing exports
  toDateOnly,
  formatYMD,
  resolveEffectiveDateForCreate,
  getLastClosedPeriod,
  assertDateAfterLastClosed,
  assertNotLocked,

  // new exports (to avoid duplicated queries)
  loadDocDateOnlyFromTable,
  loadDocDateOnlyFromConfig,
};

// src/core/shared/tutup-transaksi.js
// ✅ UTC-only implementation (nama function & export tetap sama)
const { sql, poolPromise } = require('../config/db');

async function getRequest(runner) {
  const r = (typeof runner?.then === 'function') ? await runner : runner;
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
    return new Date(Date.UTC(
      value.getUTCFullYear(),
      value.getUTCMonth(),
      value.getUTCDate()
    ));
  }

  const s = String(value).trim();
  if (!s) return null;

  // Handle "YYYY-MM-DD" safely (no timezone ambiguity)
  const s10 = s.length >= 10 ? s.substring(0, 10) : s;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s10);
  if (m) {
    const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
    const dt = new Date(Date.UTC(y, mo - 1, d)); // ✅ UTC midnight
    if (!isNaN(dt.getTime())) return dt;
  }

  // Otherwise parse as Date (ISO datetime etc.), then normalize to UTC date-only
  const dt2 = new Date(s);
  if (!isNaN(dt2.getTime())) {
    return new Date(Date.UTC(
      dt2.getUTCFullYear(),
      dt2.getUTCMonth(),
      dt2.getUTCDate()
    ));
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
  const hint = useLock ? 'WITH (UPDLOCK, HOLDLOCK)' : 'WITH (NOLOCK)';

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
async function assertDateAfterLastClosed({ date, runner, action = 'transaction', useLock = false } = {}) {
  const trxDate = toDateOnly(date);
  if (!trxDate) return { ok: true, trxDate: null, lastClosed: null, row: null };

  const { lastClosed, row } = await getLastClosedPeriod({ runner, useLock });

  if (lastClosed && trxDate.getTime() <= lastClosed.getTime()) {
    // nextAllowed = lastClosed + 1 day (UTC)
    const nextAllowed = new Date(lastClosed.getTime() + 24 * 60 * 60 * 1000);

    const e = new Error(
      `Tidak bisa ${action}: transaksi tanggal ${formatYMD(trxDate)} sudah ditutup (last closed: ${formatYMD(lastClosed)}). ` +
      `Silakan input minimal tanggal ${formatYMD(nextAllowed)}.`
    );
    e.statusCode = 423;
    e.code = 'TUTUP_TRANSAKSI_LOCKED';
    e.meta = { trxDate: formatYMD(trxDate), lastClosed: formatYMD(lastClosed), row };
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

module.exports = {
  toDateOnly,
  formatYMD,
  resolveEffectiveDateForCreate,
  getLastClosedPeriod,
  assertDateAfterLastClosed,
  assertNotLocked,
};

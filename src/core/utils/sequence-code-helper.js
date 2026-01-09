// src/core/utils/sequence-code-helper.js
const { sql } = require('../config/db');

function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

function escapeIdent(name) {
  // very strict: allow only letters, numbers, underscore, dot, brackets
  // to reduce injection risk because table/column can't be parameterized
  const s = String(name || '').trim();
  if (!/^[A-Za-z0-9_.\[\]]+$/.test(s)) {
    throw new Error(`Invalid identifier: ${name}`);
  }
  return s;
}

/**
 * Generic sequential code generator.
 *
 * Examples:
 * - table=dbo.PasangKunci_h, column=NoProduksi, prefix='BI.', width=10 -> BI.0000000001
 * - table=dbo.Spanner_h, column=NoProduksi, prefix='BJ.', width=10 -> BJ.0000000001
 * - table=dbo.Mixer_h, column=NoMixer,   prefix='MX.', width=10 -> MX.0000000001
 *
 * Options:
 * - runnerTx: sql.Transaction (recommended)
 * - tableName: e.g. 'dbo.PasangKunci_h'
 * - columnName: e.g. 'NoProduksi' / 'NoMixer'
 * - prefix: e.g. 'BI.' (can be '' if no prefix)
 * - width: digits width for numeric part
 * - extraWhereSql: optional additional SQL (safe static string) like "AND IdWarehouse=@IdWarehouse"
 * - extraInputs: optional fn(req) to bind extra parameters to request
 */
async function generateNextCode(tx, {
  tableName,
  columnName,
  prefix = '',
  width = 10,
  extraWhereSql = '',
  extraInputs = null,
} = {}) {
  if (!tx) throw new Error('tx is required');
  if (!tableName) throw new Error('tableName is required');
  if (!columnName) throw new Error('columnName is required');

  const table = escapeIdent(tableName);
  const col = escapeIdent(columnName);

  const rq = new sql.Request(tx);
  rq.input('prefix', sql.VarChar(50), prefix);

  if (typeof extraInputs === 'function') {
    extraInputs(rq);
  }

  const q = `
    SELECT TOP 1 h.${col} AS Code
    FROM ${table} AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE (@prefix = '' OR h.${col} LIKE @prefix + '%')
    ${extraWhereSql ? ` ${extraWhereSql} ` : ''}
    ORDER BY
      TRY_CONVERT(BIGINT, SUBSTRING(h.${col}, LEN(@prefix) + 1, 50)) DESC,
      h.${col} DESC;
  `;

  const r = await rq.query(q);

  let lastNum = 0;
  if (r.recordset?.length) {
    const last = String(r.recordset[0].Code || '');
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }

  return prefix + padLeft(lastNum + 1, width);
}

module.exports = { generateNextCode };

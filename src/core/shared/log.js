// src/core/shared/log.js
const { sql, poolPromise } = require('../config/db'); // sesuaikan jika path db berbeda

// helper: INT atau NULL
const toIntOrNull = (v) =>
  (v === null || v === undefined || isNaN(Number(v))) ? null : Number(v);

// helper: dapatkan sql.Request dari berbagai "runner"
async function getRequest(runner) {
  const r = (typeof runner?.then === 'function') ? await runner : runner;
  if (r instanceof sql.Request) return r;         // sudah request
  if (r instanceof sql.Transaction) return new sql.Request(r); // transaksi aktif
  if (r?.request) return r.request();             // pool
  const pool = await poolPromise;                 // fallback: pool global
  return pool.request();
}

/**
 * Insert ke dbo.LogMappingLokasi
 * @param {Object} p
 * @param {string|null} p.noLabel
 * @param {string|null} p.beforeBlok
 * @param {number|null} p.beforeIdLokasi
 * @param {string|null} p.afterBlok
 * @param {number|null} p.afterIdLokasi
 * @param {number|null} p.idUsername
 * @param {sql.Transaction|sql.Request|sql.ConnectionPool|Promise} [p.runner] - opsional, untuk ikut transaksi
 */
async function insertLogMappingLokasi(p) {
  const {
    noLabel, beforeBlok, beforeIdLokasi,
    afterBlok, afterIdLokasi, idUsername, isSO,
    runner
  } = p;

  const _beforeId = toIntOrNull(beforeIdLokasi);
  const _afterId  = toIntOrNull(afterIdLokasi);

  const query = `
    INSERT INTO dbo.LogMappingLokasi (
      IdUsername, Tgl, NoLabel, BeforeBlok, BeforeIdLokasi, AfterBlok, AfterIdLokasi, IsSO
    )
    OUTPUT INSERTED.IdLog, INSERTED.Tgl
    VALUES (
      @IdUsername, GETDATE(), @NoLabel, @BeforeBlok, @BeforeIdLokasi, @AfterBlok, @AfterIdLokasi, @IsSO
    )
  `;

  try {
    const request = await getRequest(runner);
    request.input('IdUsername',     sql.Int,        idUsername ?? null);
    request.input('NoLabel',        sql.VarChar(50), noLabel ?? null);
    request.input('BeforeBlok',     sql.VarChar(3),  beforeBlok ?? null);
    request.input('BeforeIdLokasi', sql.Int,         _beforeId);
    request.input('AfterBlok',      sql.VarChar(3),  afterBlok ?? null);
    request.input('AfterIdLokasi',  sql.Int,         _afterId);
    request.input('IsSO',  sql.Bit,         isSO) ?? 0;


    const result = await request.query(query);
    const inserted = result.recordset?.[0] || {};
    const idLog = inserted.IdLog ?? null;
    const tglInserted = inserted.Tgl ?? null;

    console.info(`✅ Mapping ${idUsername}: ${beforeBlok}:${_beforeId} -> ${afterBlok}:${_afterId} (${idLog})`);
    return { success: true, message: 'Log mapping lokasi berhasil ditambahkan', idLog, tgl: tglInserted };
  } catch (err) {
    console.error('❌ Failed to insert LogMappingLokasi', {
      err: err.message,
      idUsername, noLabel, beforeBlok, beforeIdLokasi, afterBlok, afterIdLokasi
    });
    return { success: false, message: err.message };
  }
}

module.exports = {
  insertLogMappingLokasi,
};

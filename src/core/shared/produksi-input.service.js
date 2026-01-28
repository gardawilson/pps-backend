// src/core/shared/produksi-input.service.js

const { sql, poolPromise } = require('../config/db');
const { generatePartialsInsertSQL } = require('../utils/produksi-partial-sql.generator');
const { generateInputsAttachSQL } = require('../utils/produksi-input-sql.generator');
const { generateInputsDeleteSQL, generatePartialsDeleteSQL } = require('../utils/produksi-delete-sql.generator');
const { loadDocDateOnlyFromConfig, assertNotLocked } = require('../shared/tutup-transaksi-guard');
const { badReq } = require('../utils/http-error');
const {
  PARTIAL_CONFIGS,
  INPUT_LABELS,
  INPUT_CONFIGS,
} = require('../config/produksi-input-mapping.config');

// ✅ NEW: audit context helper
const { applyAuditContext } = require('../utils/db-audit-context'); // sesuaikan path jika beda

function _log(tag, msg, extra) {
  if (extra !== undefined) console.log(`[${tag}] ${msg}`, extra);
  else console.log(`[${tag}] ${msg}`);
}
function _logErr(tag, msg, err) {
  console.error(`[${tag}] ${msg}`);
  if (err) console.error(err);
}

/**
 * ✅ REVISED: tambah param ctx
 * ctx wajib: { actorId, actorUsername, requestId }
 */
async function upsertInputsAndPartials(produksiType, noProduksi, payload, ctx) {
  const TAG = 'produksi-input';
  const startedAt = Date.now();

  let tx = null;
  let began = false;
  const norm = (a) => (Array.isArray(a) ? a : []);

  // ✅ normalize ctx (avoid null)
  const actorIdNum = Number(ctx?.actorId);
  const actorUsername = String(ctx?.actorUsername || ctx?.actor || '').trim() || null;
  let requestId = String(ctx?.requestId || '').trim();

  // kalau requestId kosong, bikin fallback (biar nggak NULL)
  if (!requestId) requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  try {
    if (!noProduksi) throw badReq('noProduksi wajib');
    if (!produksiType) throw badReq('produksiType wajib');
    if (!payload || typeof payload !== 'object') throw badReq('payload wajib object');

    // ✅ ctx wajib untuk audit
    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq('ctx.actorId wajib (controller harus inject dari token)');
    }

    const pool = await poolPromise;
    tx = new sql.Transaction(pool);

    _log(TAG, `upsert start type=${produksiType} no=${noProduksi}`);

    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    began = true;

    // =====================================================
    // ✅ [AUDIT CTX] set SESSION_CONTEXT di connection milik TX
    // =====================================================
    await applyAuditContext(new sql.Request(tx), {
      actorId: Math.trunc(actorIdNum),
      actor: actorUsername, // simpan string juga (kalau trigger butuh)
      requestId,
    });

    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: produksiType,
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: `upsert ${produksiType} inputs/partials`,
      useLock: true,
    });

    const partialTypes = Object.keys(payload)
      .filter((key) => key.endsWith('PartialNew') && norm(payload[key]).length > 0)
      .map((key) => key.replace('PartialNew', ''));

    let partials = { summary: {}, createdLists: {} };
    if (partialTypes.length > 0) {
      partials = await _insertPartialsWithTx(tx, produksiType, noProduksi, payload, partialTypes);
    }

    const inputTypes = Object.keys(payload)
      .filter((key) => !key.endsWith('PartialNew') && norm(payload[key]).length > 0)
      .filter((key) => INPUT_CONFIGS?.[produksiType]?.[key]);

    let attachments = {};
    let invalidRows = {};
    if (inputTypes.length > 0) {
      const r = await _insertInputsWithTx(tx, produksiType, noProduksi, payload, inputTypes);
      attachments = r.attachments;
      invalidRows = r.invalidRows;
    }

    await tx.commit();

    // ✅ OPTIONAL: clear context (hindari “lengket” kalau connection reused)
    try {
      await new sql.Request(pool)
        .input('rid', sql.NVarChar(64), requestId)
        .query(`
          -- best-effort: clear session context on THIS request connection
          EXEC sys.sp_set_session_context @key=N'actor_id',  @value=NULL, @read_only=0;
          EXEC sys.sp_set_session_context @key=N'actor',     @value=NULL, @read_only=0;
          EXEC sys.sp_set_session_context @key=N'request_id',@value=NULL, @read_only=0;
        `);
    } catch (_) {
      // ignore
    }

    const result = _buildResponse(noProduksi, attachments, partials, payload, invalidRows);

    // ✅ attach audit meta for debugging (optional)
    result.meta = result.meta || {};
    result.meta.audit = { actorId: Math.trunc(actorIdNum), actorUsername, requestId };

    _log(TAG, `upsert success type=${produksiType} no=${noProduksi} in ${Date.now() - startedAt}ms`);
    return result;
  } catch (err) {
    _logErr(TAG, `upsert error type=${produksiType} no=${noProduksi} after ${Date.now() - startedAt}ms`, err);

    if (tx && began) {
      try { await tx.rollback(); } catch (rbErr) { _logErr(TAG, 'rollback error', rbErr); }
    }
    throw err;
  }
}

async function _insertPartialsWithTx(tx, produksiType, noProduksi, payload, partialTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(payload));

  const sqlQuery = generatePartialsInsertSQL(produksiType, partialTypes);
  const rs = await req.query(sqlQuery);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  const createdLists = {};
  partialTypes.forEach((type, idx) => {
    const config = PARTIAL_CONFIGS?.[type];
    const requestKey = `${type}PartialNew`;
    if (config) {
      createdLists[requestKey] = (rs.recordsets?.[idx + 1] || []).map((r) => r[config.partialColumn]);
    } else {
      createdLists[requestKey] = (rs.recordsets?.[idx + 1] || []).map((r) => r.Code || r.code);
    }
  });

  return { summary, createdLists };
}

async function _insertInputsWithTx(tx, produksiType, noProduksi, payload, inputTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(payload));

  const sqlQuery = generateInputsAttachSQL(produksiType, inputTypes);
  const rs = await req.query(sqlQuery);

  const attachments = {};
  for (const row of rs.recordset || []) {
    attachments[row.Section] = {
      inserted: row.Inserted,
      skipped: row.Skipped,
      invalid: row.Invalid,
    };
  }

  const invalidRows = {};
  const invalidRs = rs.recordsets?.[1];
  if (Array.isArray(invalidRs) && invalidRs.length > 0) {
    for (const r of invalidRs) {
      const section = r.Section || r.section || 'unknown';
      if (!invalidRows[section]) invalidRows[section] = [];
      invalidRows[section].push(r);
    }
  }

  return { attachments, invalidRows };
}

function _buildResponse(noProduksi, attachments, partials, requestBody, invalidRows = {}) {
  const totalInserted = Object.values(attachments).reduce((sum, item) => sum + (item.inserted || 0), 0);
  const totalSkipped  = Object.values(attachments).reduce((sum, item) => sum + (item.skipped || 0), 0);
  const totalInvalid  = Object.values(attachments).reduce((sum, item) => sum + (item.invalid || 0), 0);

  const totalPartialsCreated = Object.values(partials.summary || {}).reduce(
    (sum, item) => sum + (item.created || 0),
    0
  );

  const hasInvalid = totalInvalid > 0;
  const hasNoSuccess = totalInserted === 0 && totalPartialsCreated === 0;

  const response = {
    noProduksi,
    summary: {
      totalInserted,
      totalSkipped,
      totalInvalid,
      totalPartialsCreated,
    },
    details: {
      inputs: _buildInputDetails(attachments, requestBody, invalidRows),
      partials: _buildPartialDetails(partials, requestBody),
    },
    createdPartials: partials.createdLists || {},
  };

  return {
    success: !hasInvalid && !hasNoSuccess,
    message: hasInvalid ? 'Beberapa data tidak valid' : undefined,
    hasWarnings: totalSkipped > 0,
    data: response,
  };
}

function _buildInputDetails(attachments, requestBody, invalidRowsBySection = {}) {
  const details = [];

  for (const [key, result] of Object.entries(attachments || {})) {
    const requestedCount = Array.isArray(requestBody?.[key]) ? requestBody[key].length : 0;
    if (requestedCount === 0) continue;

    const label = INPUT_LABELS?.[key] || key;
    const invalid = result.invalid || 0;

    details.push({
      section: key,
      label,
      requested: requestedCount,
      inserted: result.inserted || 0,
      skipped: result.skipped || 0,
      invalid,
      status: invalid > 0 ? 'error' : (result.skipped || 0) > 0 ? 'warning' : 'success',
      message: _buildSectionMessage(label, result),
      invalidRows: Array.isArray(invalidRowsBySection?.[key]) ? invalidRowsBySection[key] : [],
    });
  }

  return details;
}

function _buildPartialDetails(partials, requestBody) {
  const details = [];
  const createdLists = partials?.createdLists || {};
  const summaryObj = partials?.summary || {};
  const requestedPartialKeys = Object.keys(requestBody || {}).filter((k) => k.endsWith('PartialNew'));

  for (const requestKey of requestedPartialKeys) {
    const type = requestKey.replace('PartialNew', '');
    const requestedCount = Array.isArray(requestBody?.[requestKey]) ? requestBody[requestKey].length : 0;
    if (requestedCount === 0) continue;

    const candidates = [requestKey, `${type}Partial`, type];
    let created = 0;
    for (const c of candidates) {
      if (summaryObj?.[c]?.created != null) { created = summaryObj[c].created || 0; break; }
    }

    const label = `${INPUT_LABELS?.[type] || type} Partial`;

    details.push({
      section: requestKey,
      label,
      requested: requestedCount,
      created,
      status: created === requestedCount ? 'success' : created > 0 ? 'warning' : 'error',
      message: `${created} dari ${requestedCount} ${label} berhasil dibuat`,
      codes: Array.isArray(createdLists?.[requestKey]) ? createdLists[requestKey] : [],
    });
  }

  return details;
}

function _buildSectionMessage(label, result) {
  const parts = [];
  const inserted = result?.inserted || 0;
  const skipped = result?.skipped || 0;
  const invalid = result?.invalid || 0;

  if (inserted > 0) parts.push(`${inserted} berhasil ditambahkan`);
  if (skipped > 0) parts.push(`${skipped} sudah ada (dilewati)`);
  if (invalid > 0) parts.push(`${invalid} tidak valid (tidak ditemukan)`);

  if (parts.length === 0) return `Tidak ada ${label} yang diproses`;
  return `${label}: ${parts.join(', ')}`;
}


async function deleteInputsAndPartials(produksiType, noProduksi, payload, ctx) {
  const TAG = 'produksi-input-delete';
  const startedAt = Date.now();

  let tx = null;
  let began = false;
  const norm = (a) => (Array.isArray(a) ? a : []);

  // ✅ normalize ctx (avoid null)
  const actorIdNum = Number(ctx?.actorId);
  const actorUsername = String(ctx?.actorUsername || ctx?.actor || '').trim() || null;
  let requestId = String(ctx?.requestId || '').trim();

  // kalau requestId kosong, bikin fallback (biar nggak NULL)
  if (!requestId) requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  try {
    if (!noProduksi) throw badReq('noProduksi wajib');
    if (!produksiType) throw badReq('produksiType wajib');
    if (!payload || typeof payload !== 'object') throw badReq('payload wajib object');

    // ✅ ctx wajib untuk audit
    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq('ctx.actorId wajib (controller harus inject dari token)');
    }

    const pool = await poolPromise;
    tx = new sql.Transaction(pool);

    _log(TAG, `delete start type=${produksiType} no=${noProduksi}`);

    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    began = true;

    // =====================================================
    // ✅ [AUDIT CTX] set SESSION_CONTEXT di connection milik TX
    // =====================================================
    await applyAuditContext(new sql.Request(tx), {
      actorId: Math.trunc(actorIdNum),
      actor: actorUsername,
      requestId,
    });

    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: produksiType,
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: `delete ${produksiType} inputs/partials`,
      useLock: true,
    });

    // Determine which partial types & input types are requested
    const requestedPartialTypes = Object.keys(payload)
      .filter((key) => key.endsWith('Partial') && norm(payload[key]).length > 0)
      .map((key) => key.replace('Partial', ''));

    const requestedInputTypes = Object.keys(payload)
      .filter((key) => !key.endsWith('Partial') && norm(payload[key]).length > 0)
      .filter((key) => INPUT_CONFIGS?.[produksiType]?.[key]);

    let partialsResult = { summary: {} };
    if (requestedPartialTypes.length > 0) {
      partialsResult = await _deletePartialsWithTx(
        tx, 
        produksiType, 
        noProduksi, 
        payload, 
        requestedPartialTypes
      );
    }

    let inputsResult = {};
    if (requestedInputTypes.length > 0) {
      inputsResult = await _deleteInputsWithTx(
        tx, 
        produksiType, 
        noProduksi, 
        payload, 
        requestedInputTypes
      );
    }

    await tx.commit();

    // ✅ OPTIONAL: clear context (hindari "lengket" kalau connection reused)
    try {
      await new sql.Request(pool).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id',  @value=NULL, @read_only=0;
        EXEC sys.sp_set_session_context @key=N'actor',     @value=NULL, @read_only=0;
        EXEC sys.sp_set_session_context @key=N'request_id',@value=NULL, @read_only=0;
      `);
    } catch (_) {
      // ignore
    }

    const result = _buildDeleteResponse(noProduksi, inputsResult, partialsResult, payload);

    // ✅ attach audit meta for debugging (optional)
    result.meta = result.meta || {};
    result.meta.audit = { actorId: Math.trunc(actorIdNum), actorUsername, requestId };

    _log(TAG, `delete success type=${produksiType} no=${noProduksi} in ${Date.now() - startedAt}ms`);
    return result;
  } catch (err) {
    _logErr(TAG, `delete error type=${produksiType} no=${noProduksi} after ${Date.now() - startedAt}ms`, err);

    if (tx && began) {
      try { await tx.rollback(); } catch (rbErr) { _logErr(TAG, 'rollback error', rbErr); }
    }
    throw err;
  }
}

// ✅ NEW: Delete inputs using config-driven SQL generator
async function _deleteInputsWithTx(tx, produksiType, noProduksi, payload, requestedTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(payload));

  const sqlQuery = generateInputsDeleteSQL(produksiType, requestedTypes);
  const rs = await req.query(sqlQuery);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = {
      deleted: row.Deleted,
      notFound: row.NotFound,
    };
  }
  return out;
}

// ✅ NEW: Delete partials using config-driven SQL generator
async function _deletePartialsWithTx(tx, produksiType, noProduksi, payload, requestedTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(payload));

  const sqlQuery = generatePartialsDeleteSQL(produksiType, requestedTypes);
  const rs = await req.query(sqlQuery);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = {
      deleted: row.Deleted,
      notFound: row.NotFound,
    };
  }

  return { summary };
}

// ✅ Helper: Build delete response
function _buildDeleteResponse(noProduksi, inputsResult, partialsResult, requestBody) {
  const totalDeleted = Object.values(inputsResult).reduce((sum, item) => sum + (item.deleted || 0), 0);
  const totalNotFound = Object.values(inputsResult).reduce((sum, item) => sum + (item.notFound || 0), 0);

  const totalPartialsDeleted = Object.values(partialsResult.summary || {}).reduce(
    (sum, item) => sum + (item.deleted || 0),
    0
  );
  const totalPartialsNotFound = Object.values(partialsResult.summary || {}).reduce(
    (sum, item) => sum + (item.notFound || 0),
    0
  );

  const hasNotFound = totalNotFound > 0 || totalPartialsNotFound > 0;
  const hasNoSuccess = totalDeleted === 0 && totalPartialsDeleted === 0;

  const response = {
    noProduksi,
    summary: {
      totalDeleted,
      totalNotFound,
      totalPartialsDeleted,
      totalPartialsNotFound,
    },
    details: {
      inputs: _buildDeleteInputDetails(inputsResult, requestBody),
      partials: _buildDeletePartialDetails(partialsResult, requestBody),
    },
  };

  return {
    success: !hasNoSuccess,
    hasWarnings: hasNotFound,
    data: response,
  };
}

// ✅ Helper: Build input details for delete
function _buildDeleteInputDetails(results, requestBody) {
  const details = [];

  for (const [key, result] of Object.entries(results || {})) {
    const requestedCount = Array.isArray(requestBody?.[key]) ? requestBody[key].length : 0;
    if (requestedCount === 0) continue;

    const label = INPUT_LABELS?.[key] || key;

    details.push({
      section: key,
      label,
      requested: requestedCount,
      deleted: result.deleted || 0,
      notFound: result.notFound || 0,
      status: result.notFound > 0 ? 'warning' : 'success',
      message: `${label}: ${result.deleted || 0} berhasil dihapus${result.notFound > 0 ? `, ${result.notFound} tidak ditemukan` : ''}`,
    });
  }

  return details;
}

// ✅ Helper: Build partial details for delete
function _buildDeletePartialDetails(partialsResult, requestBody) {
  const details = [];
  const summaryObj = partialsResult?.summary || {};
  const requestedPartialKeys = Object.keys(requestBody || {}).filter((k) => k.endsWith('Partial'));

  for (const requestKey of requestedPartialKeys) {
    const type = requestKey.replace('Partial', '');
    const requestedCount = Array.isArray(requestBody?.[requestKey]) ? requestBody[requestKey].length : 0;
    if (requestedCount === 0) continue;

    const result = summaryObj?.[requestKey] || { deleted: 0, notFound: 0 };
    const label = `${INPUT_LABELS?.[type] || type} Partial`;

    details.push({
      section: requestKey,
      label,
      requested: requestedCount,
      deleted: result.deleted || 0,
      notFound: result.notFound || 0,
      status: result.notFound > 0 ? 'warning' : 'success',
      message: `${label}: ${result.deleted || 0} berhasil dihapus${result.notFound > 0 ? `, ${result.notFound} tidak ditemukan` : ''}`,
    });
  }

  return details;
}

module.exports = { upsertInputsAndPartials, deleteInputsAndPartials };

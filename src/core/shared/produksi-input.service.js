// src/core/shared/produksi-input.service.js

const { sql, poolPromise } = require('../config/db');
const { generatePartialsInsertSQL } = require('../utils/produksi-partial-sql.generator');
const { generateInputsAttachSQL } = require('../utils/produksi-input-sql.generator');
const { generateInputsDeleteSQL, generatePartialsDeleteSQL } = require('../utils/produksi-delete-sql.generator');

const { generateUpsertInputsSQL } = require('../utils/produksi-upsert-sql.generator'); // ✅ UPSERT
const { generateUpsertInputsDeleteSQL } = require('../utils/produksi-upsert-delete-sql.generator'); // ✅ UPSERT DELETE

const { loadDocDateOnlyFromConfig, assertNotLocked } = require('../shared/tutup-transaksi-guard');
const { badReq } = require('../utils/http-error');
const {
  PARTIAL_CONFIGS,
  INPUT_LABELS,
  INPUT_CONFIGS,
  UPSERT_INPUT_CONFIGS, // ✅ NEW
} = require('../config/produksi-input-mapping.config');

const { applyAuditContext } = require('../utils/db-audit-context');

function _log(tag, msg, extra) {
  if (extra !== undefined) console.log(`[${tag}] ${msg}`, extra);
  else console.log(`[${tag}] ${msg}`);
}
function _logErr(tag, msg, err) {
  console.error(`[${tag}] ${msg}`);
  if (err) console.error(err);
}

const norm = (a) => (Array.isArray(a) ? a : []);

/**
 * ✅ Standard payload: "*Partial" (NO PartialNew from client)
 * ✅ Legacy generator expects "*PartialNew"
 * -> Build internal alias payload for partial insert only
 */
function _withPartialNewAliases(payload) {
  const p = { ...(payload || {}) };

  // ambil semua key standar "*Partial" yang punya isi
  for (const key of Object.keys(p)) {
    if (!key.endsWith('Partial')) continue;

    const arr = norm(p[key]);
    if (arr.length === 0) continue;

    const legacyKey = `${key}New`; // brokerPartial -> brokerPartialNew
    // hanya bikin alias kalau client belum kirim legacy
    if (!Array.isArray(p[legacyKey]) || p[legacyKey].length === 0) {
      p[legacyKey] = arr;
    }
  }

  return p;
}

/**
 * ✅ REVISED: support UPSERT inputs (cabinetMaterial, dll)
 * ctx wajib: { actorId, actorUsername, requestId }
 */
async function upsertInputsAndPartials(produksiType, noProduksi, payload, ctx) {
  const TAG = 'produksi-input';
  const startedAt = Date.now();

  let tx = null;
  let began = false;

  const actorIdNum = Number(ctx?.actorId);
  const actorUsername = String(ctx?.actorUsername || ctx?.actor || '').trim() || null;
  let requestId = String(ctx?.requestId || '').trim();
  if (!requestId) requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  try {
    if (!noProduksi) throw badReq('noProduksi wajib');
    if (!produksiType) throw badReq('produksiType wajib');
    if (!payload || typeof payload !== 'object') throw badReq('payload wajib object');

    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq('ctx.actorId wajib (controller harus inject dari token)');
    }

    const pool = await poolPromise;
    tx = new sql.Transaction(pool);

    _log(TAG, `upsert start type=${produksiType} no=${noProduksi}`);

    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    began = true;

    // AUDIT CTX
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
      action: `upsert ${produksiType} inputs/partials`,
      useLock: true,
    });

    // =====================================================
    // 0) Build internal payload alias untuk partial insert
    // =====================================================
    // client kirim "*Partial", kita buat "*PartialNew" internal supaya generator lama tetap jalan
    const payloadInternal = _withPartialNewAliases(payload);

    // =====================================================
    // 1) CREATE NEW PARTIALS (via legacy key "*PartialNew")
    // =====================================================
    const partialTypes = Object.keys(payloadInternal)
      .filter((key) => key.endsWith('PartialNew') && norm(payloadInternal[key]).length > 0)
      .map((key) => key.replace('PartialNew', ''));

    let partials = { summary: {}, createdLists: {} };
    if (partialTypes.length > 0) {
      partials = await _insertPartialsWithTx(tx, produksiType, noProduksi, payloadInternal, partialTypes);
    }

    // =====================================================
    // 2) CATEGORIZE INPUT TYPES (STANDARD vs UPSERT)
    // =====================================================
    const allInputKeys = Object.keys(payload)
      .filter((key) => norm(payload[key]).length > 0); // pakai payload asli (tanpa PartialNew)

    // Standard inputs (yang ada di INPUT_CONFIGS dan bukan UPSERT_INPUT_CONFIGS)
    const standardInputTypes = allInputKeys.filter(
      (key) => INPUT_CONFIGS?.[produksiType]?.[key] && !UPSERT_INPUT_CONFIGS?.[produksiType]?.[key]
    );

    // UPSERT inputs (yang ada di UPSERT_INPUT_CONFIGS)
    const upsertInputTypes = allInputKeys.filter(
      (key) => UPSERT_INPUT_CONFIGS?.[produksiType]?.[key]
    );

    let attachments = {};
    let invalidRows = {};

    // =====================================================
    // 3) PROCESS STANDARD INPUTS (attach)
    // =====================================================
    if (standardInputTypes.length > 0) {
      const r = await _insertInputsWithTx(tx, produksiType, noProduksi, payload, standardInputTypes);
      attachments = { ...attachments, ...r.attachments };
      invalidRows = { ...invalidRows, ...r.invalidRows };
    }

    // =====================================================
    // 4) PROCESS UPSERT INPUTS (cabinetMaterial, dll)
    // =====================================================
    if (upsertInputTypes.length > 0) {
      const r = await _insertUpsertInputsWithTx(tx, produksiType, noProduksi, payload, upsertInputTypes);
      attachments = { ...attachments, ...r.attachments };
    }

    await tx.commit();

    // clear context (best effort)
    try {
      await new sql.Request(pool).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id',  @value=NULL, @read_only=0;
        EXEC sys.sp_set_session_context @key=N'actor',     @value=NULL, @read_only=0;
        EXEC sys.sp_set_session_context @key=N'request_id',@value=NULL, @read_only=0;
      `);
    } catch (_) {}

    const result = _buildResponse(noProduksi, attachments, partials, payload, invalidRows);

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

// ✅ UPSERT inputs (cabinet material pattern)
async function _insertUpsertInputsWithTx(tx, produksiType, noProduksi, payload, upsertTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(payload));

  const sqlQuery = generateUpsertInputsSQL(produksiType, upsertTypes);
  const rs = await req.query(sqlQuery);

  const attachments = {};
  for (const row of rs.recordset || []) {
    attachments[row.Section] = {
      inserted: row.Inserted,
      updated: row.Updated || 0,
      skipped: row.Skipped,
      invalid: row.Invalid,
    };
  }

  return { attachments };
}

async function _insertPartialsWithTx(tx, produksiType, noProduksi, payloadInternal, partialTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(payloadInternal));

  const sqlQuery = generatePartialsInsertSQL(produksiType, partialTypes);
  const rs = await req.query(sqlQuery);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  const createdLists = {};
  partialTypes.forEach((type, idx) => {
    const config = PARTIAL_CONFIGS?.[type];
    const requestKeyLegacy = `${type}PartialNew`;     // generator legacy
    const requestKeyStd = `${type}Partial`;           // standar API (kita expose ini juga)
    const list = (rs.recordsets?.[idx + 1] || []);

    const codes = config
      ? list.map((r) => r[config.partialColumn])
      : list.map((r) => r.Code || r.code);

    // Simpan keduanya biar UI bisa baca yang standar, dan yang legacy masih ada kalau ada consumer lama
    createdLists[requestKeyStd] = codes;
    createdLists[requestKeyLegacy] = codes;
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
  const totalUpdated  = Object.values(attachments).reduce((sum, item) => sum + (item.updated || 0), 0);
  const totalSkipped  = Object.values(attachments).reduce((sum, item) => sum + (item.skipped || 0), 0);
  const totalInvalid  = Object.values(attachments).reduce((sum, item) => sum + (item.invalid || 0), 0);

  const totalPartialsCreated = Object.values(partials.summary || {}).reduce(
    (sum, item) => sum + (item.created || 0),
    0
  );

  const hasInvalid = totalInvalid > 0;
  const hasNoSuccess = (totalInserted + totalUpdated) === 0 && totalPartialsCreated === 0;

  const response = {
    noProduksi,
    summary: {
      totalInserted,
      totalUpdated,
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
      updated: result.updated || 0,
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

  // Standar: client kirim "*Partial"
  const requestedPartialKeys = Object.keys(requestBody || {}).filter((k) => k.endsWith('Partial'));

  for (const requestKey of requestedPartialKeys) {
    const type = requestKey.replace('Partial', '');
    const requestedCount = Array.isArray(requestBody?.[requestKey]) ? requestBody[requestKey].length : 0;
    if (requestedCount === 0) continue;

    // Summary dari generator kemungkinan pakai key legacy (typePartialNew)
    const legacyKey = `${type}PartialNew`;
    const candidates = [requestKey, legacyKey, type];

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
      // expose codes standar
      codes: Array.isArray(createdLists?.[requestKey]) ? createdLists[requestKey] : [],
    });
  }

  return details;
}

function _buildSectionMessage(label, result) {
  const parts = [];
  const inserted = result?.inserted || 0;
  const updated = result?.updated || 0;
  const skipped = result?.skipped || 0;
  const invalid = result?.invalid || 0;

  if (inserted > 0) parts.push(`${inserted} berhasil ditambahkan`);
  if (updated > 0) parts.push(`${updated} berhasil diupdate`);
  if (skipped > 0) parts.push(`${skipped} sudah ada (dilewati)`);
  if (invalid > 0) parts.push(`${invalid} tidak valid (tidak ditemukan)`);

  if (parts.length === 0) return `Tidak ada ${label} yang diproses`;
  return `${label}: ${parts.join(', ')}`;
}

// ==================== DELETE FUNCTIONS ====================

async function deleteInputsAndPartials(produksiType, noProduksi, payload, ctx) {
  const TAG = 'produksi-input-delete';
  const startedAt = Date.now();

  let tx = null;
  let began = false;

  const actorIdNum = Number(ctx?.actorId);
  const actorUsername = String(ctx?.actorUsername || ctx?.actor || '').trim() || null;
  let requestId = String(ctx?.requestId || '').trim();
  if (!requestId) requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  try {
    if (!noProduksi) throw badReq('noProduksi wajib');
    if (!produksiType) throw badReq('produksiType wajib');
    if (!payload || typeof payload !== 'object') throw badReq('payload wajib object');

    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq('ctx.actorId wajib (controller harus inject dari token)');
    }

    const pool = await poolPromise;
    tx = new sql.Transaction(pool);

    _log(TAG, `delete start type=${produksiType} no=${noProduksi}`);

    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    began = true;

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

    const requestedPartialTypes = Object.keys(payload)
      .filter((key) => key.endsWith('Partial') && norm(payload[key]).length > 0)
      .map((key) => key.replace('Partial', ''));

    const allInputKeys = Object.keys(payload).filter((key) => !key.endsWith('Partial') && norm(payload[key]).length > 0);

    const standardInputTypes = allInputKeys.filter(
      (key) => INPUT_CONFIGS?.[produksiType]?.[key] && !UPSERT_INPUT_CONFIGS?.[produksiType]?.[key]
    );

    const upsertInputTypes = allInputKeys.filter(
      (key) => UPSERT_INPUT_CONFIGS?.[produksiType]?.[key]
    );

    let partialsResult = { summary: {} };
    let inputsResult = {};

    if (requestedPartialTypes.length > 0) {
      partialsResult = await _deletePartialsWithTx(tx, produksiType, noProduksi, payload, requestedPartialTypes);
    }

    if (standardInputTypes.length > 0) {
      const r = await _deleteInputsWithTx(tx, produksiType, noProduksi, payload, standardInputTypes);
      inputsResult = { ...inputsResult, ...r };
    }

    if (upsertInputTypes.length > 0) {
      const r = await _deleteUpsertInputsWithTx(tx, produksiType, noProduksi, payload, upsertInputTypes);
      inputsResult = { ...inputsResult, ...r };
    }

    await tx.commit();

    // clear context (best effort)
    try {
      await new sql.Request(pool).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id',  @value=NULL, @read_only=0;
        EXEC sys.sp_set_session_context @key=N'actor',     @value=NULL, @read_only=0;
        EXEC sys.sp_set_session_context @key=N'request_id',@value=NULL, @read_only=0;
      `);
    } catch (_) {}

    const result = _buildDeleteResponse(noProduksi, inputsResult, partialsResult, payload);
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

// Delete UPSERT inputs
async function _deleteUpsertInputsWithTx(tx, produksiType, noProduksi, payload, upsertTypes) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(payload));

  const sqlQuery = generateUpsertInputsDeleteSQL(produksiType, upsertTypes);
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

const service = require('./crusher-production-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');



async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support both ?noCrusherProduksi= and ?search=
  const search =
    (typeof req.query.noCrusherProduksi === 'string' && req.query.noCrusherProduksi) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  const idMesinRaw =
    typeof req.query.idMesin === 'string' ? req.query.idMesin.trim() : '';
  const tanggalRaw =
    typeof req.query.tanggal === 'string' ? req.query.tanggal.trim() : '';
  const shiftRaw =
    typeof req.query.shift === 'string' ? req.query.shift.trim() : '';

  let idMesin = null;
  if (idMesinRaw) {
    const parsedIdMesin = Number(idMesinRaw);
    if (!Number.isInteger(parsedIdMesin) || parsedIdMesin <= 0) {
      return res.status(400).json({
        success: false,
        message: 'Query param idMesin harus integer positif',
      });
    }
    idMesin = parsedIdMesin;
  }

  const tanggalRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (tanggalRaw && !tanggalRegex.test(tanggalRaw)) {
    return res.status(400).json({
      success: false,
      message: 'Query param tanggal harus format YYYY-MM-DD',
    });
  }

  let shift = null;
  if (shiftRaw) {
    const parsedShift = Number(shiftRaw);
    if (!Number.isInteger(parsedShift) || parsedShift <= 0) {
      return res.status(400).json({
        success: false,
        message: 'Query param shift harus integer positif',
      });
    }
    shift = parsedShift;
  }

  try {
    const { data, total } = await service.getAllProduksi(
      page,
      pageSize,
      search,
      idMesin,
      tanggalRaw || null,
      shift,
    );

    return res.status(200).json({
      success: true,
      message: 'CrusherProduksi_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search, // echo back for client state
        idMesin,
        tanggal: tanggalRaw || null,
        shift,
      },
    });
  } catch (error) {
    console.error('Error fetching CrusherProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  // Optional filters
  const idMesin = req.query.idMesin ? parseInt(req.query.idMesin, 10) : null;
  const shift   = req.query.shift ? String(req.query.shift).trim() : null;

  console.log("🔍 Fetching CrusherProduksi_h | user:", username, "| date:", date, "| idMesin:", idMesin, "| shift:", shift);

  try {
    const data = await service.getProduksiByDate({ date, idMesin, shift });

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No CrusherProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date, idMesin, shift },
      });
    }

    return res.status(200).json({
      success: true,
      message: `CrusherProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date, idMesin, shift },
    });
  } catch (error) {
    console.error('Error fetching CrusherProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getCrusherMasters(req, res) {
  try {
    const data = await service.getCrusherMasters();
    return res.status(200).json({
      success: true,
      message: 'MstCrusher retrieved successfully',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstCrusher:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function createProduksi(req, res) {
  // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
  const body = req.body && typeof req.body === 'object' ? req.body : {};

  // ❌ jangan percaya audit fields dari client
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...b
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized (idUsername missing)',
    });
  }

  // ✅ username dari token / session
  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || 'system';

  // ✅ request id per HTTP request
  const requestId = String(makeRequestId(req) || '').trim();
  if (requestId) res.setHeader('x-request-id', requestId);

  // ===============================
  // Helper parsing (SAMA DENGAN WASHING)
  // ===============================
  const toInt = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : Math.trunc(n);
  };

  const toFloat = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  };

  const normalizeTime = (v) => {
    if (v === undefined) return undefined; // penting utk update / partial
    if (v === null) return null;
    const s = String(v).trim();
    return s ? s : null;
  };

  const toIntArray = (v) => {
    if (v === undefined || v === null || v === '') return [];
    const raw = Array.isArray(v) ? v : String(v).split(',');
    return [...new Set(
      raw
        .map((x) => Number(String(x).trim()))
        .filter((n) => Number.isFinite(n))
        .map((n) => Math.trunc(n))
        .filter((n) => n > 0),
    )];
  };
  const operatorIds = toIntArray(
    b.idOperators !== undefined ? b.idOperators : b.idOperator,
  );

  // ===============================
  // Payload business (tanpa audit)
  // ===============================
  const payload = {
    tanggal: b.tanggal,                 // 'YYYY-MM-DD'
    idMesin: toInt(b.idMesin),
    idOperators: operatorIds,
    idOperator: operatorIds[0] ?? null, // primary operator in header
    outputJenisId: toInt(b.outputJenisId),
    idRegu: toInt(b.idRegu),

    // crusher pakai jam / jamKerja (alias support)
    jam: b.jam ?? b.jamKerja,
    shift: toInt(b.shift),

    // audit/business fields (OVERWRITE)
    createBy: actorUsername,

    checkBy1: b.checkBy1 ?? null,
    checkBy2: b.checkBy2 ?? null,
    approveBy: b.approveBy ?? null,
    jmlhAnggota: toInt(b.jmlhAnggota),
    hadir: toInt(b.hadir),
    hourMeter: toFloat(b.hourMeter),

    hourStart: normalizeTime(b.hourStart) ?? null,
    hourEnd: normalizeTime(b.hourEnd) ?? null,
  };

  // ===============================
  // Optional quick validation (400 rapi)
  // ===============================
  const must = [];
  if (!payload.tanggal) must.push('tanggal');
  if (payload.idMesin == null) must.push('idMesin');
  if (!Array.isArray(payload.idOperators) || payload.idOperators.length === 0) {
    must.push('idOperator');
  }
  if (payload.outputJenisId == null) must.push('outputJenisId');
  if (payload.idRegu == null) must.push('idRegu');
  if (payload.jam == null) must.push('jam');
  if (payload.shift == null) must.push('shift');

  if (must.length) {
    return res.status(400).json({
      success: false,
      message: `Field wajib: ${must.join(', ')}`,
      error: { fields: must },
    });
  }

  try {
    // ✅ forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ signature service: (payload, ctx)
    const result = await service.createCrusherProduksi(payload, ctx);
    const header = result?.header ?? result;

    return res.status(201).json({
      success: true,
      message: 'Created',
      data: header,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error('[Crusher][createProduksi]', err);
    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? 'Internal Server Error' : (err.message || 'Error'),
      error: {
        message: err.message,
        details: process.env.NODE_ENV === 'development' ? err.stack : undefined,
      },
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  }
}

/**
 * PUT /api/produksi/crusher/:noCrusherProduksi
 * Update crusher production header
 */
async function updateProduksi(req, res) {
  // route param
  const noCrusherProduksi = req.params.noCrusherProduksi;
  if (!noCrusherProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noCrusherProduksi is required in route param',
    });
  }

  // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
  const body = req.body && typeof req.body === 'object' ? req.body : {};

  // ❌ jangan percaya audit fields dari client
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...b
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized (idUsername missing)',
    });
  }

  // ✅ username dari token / session
  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || 'system';

  // ✅ request id per HTTP request
  const requestId = String(makeRequestId(req) || '').trim();
  if (requestId) res.setHeader('x-request-id', requestId);

  // ===============================
  // Helper parsing (SAMA DENGAN WASHING)
  // ===============================
  const toInt = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : Math.trunc(n);
  };

  const toFloat = (v) => {
    if (v === undefined || v === null || v === '') return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  };

  const normalizeTime = (v) => {
    if (v === undefined) return undefined; // penting utk update partial
    if (v === null) return null;
    const s = String(v).trim();
    return s ? s : null;
  };

  // ===============================
  // Payload business (PARTIAL OK)
  // ===============================
  const payload = {
    tanggal: b.tanggal,                 // 'YYYY-MM-DD' (optional)
    idMesin: toInt(b.idMesin),
    idOperator: toInt(b.idOperator),

    // crusher pakai jam (atau jamKerja alias)
    jam: b.jam ?? b.jamKerja,
    shift: toInt(b.shift),

    checkBy1: b.checkBy1 ?? undefined,
    checkBy2: b.checkBy2 ?? undefined,
    approveBy: b.approveBy ?? undefined,
    jmlhAnggota: toInt(b.jmlhAnggota),
    hadir: toInt(b.hadir),
    hourMeter: toFloat(b.hourMeter),

    hourStart: normalizeTime(b.hourStart),
    hourEnd: normalizeTime(b.hourEnd),
  };

  try {
    // ✅ forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await service.updateCrusherProduksi(
      noCrusherProduksi,
      payload,
      ctx
    );
    const header = result?.header ?? result;

    return res.status(200).json({
      success: true,
      message: 'Updated',
      data: header,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error('[Crusher][updateProduksi]', err);
    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? 'Internal Server Error' : (err.message || 'Error'),
      error: {
        message: err.message,
        details: process.env.NODE_ENV === 'development' ? err.stack : undefined,
      },
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  }
}


/**
 * DELETE /api/produksi/crusher/:noCrusherProduksi
 * Delete crusher production header and all related inputs/partials
 */
async function deleteProduksi(req, res) {
  const noCrusherProduksi = req.params.noCrusherProduksi;
  if (!noCrusherProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noCrusherProduksi is required in route param',
    });
  }

  // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
  const body = req.body && typeof req.body === 'object' ? req.body : {};

  // ✅ jangan percaya audit fields dari client
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ..._b
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized (idUsername missing)',
    });
  }

  // username untuk audit actor string
  const actorUsername =
    getActorUsername(req) ||
    req.username ||
    req.user?.username ||
    'system';

  // request id per HTTP request
  const requestId = String(makeRequestId(req) || '').trim();
  if (requestId) res.setHeader('x-request-id', requestId);

  try {
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ signature service HARUS (noCrusherProduksi, ctx)
    const result = await service.deleteCrusherProduksi(
      noCrusherProduksi,
      ctx
    );

    return res.status(200).json({
      success: true,
      message: 'Deleted',
      data: result?.header ?? undefined,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error('[Crusher][deleteProduksi]', err);

    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message:
        status === 500
          ? 'Internal Server Error'
          : (err.message || 'Error'),
      error: {
        message: err.message,
        details:
          process.env.NODE_ENV === 'development'
            ? err.stack
            : undefined,
      },
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  }
}



async function getInputsByNoCrusherProduksi(req, res) {
  const noCrusherProduksi = (req.params.noCrusherProduksi || '').trim();
  if (!noCrusherProduksi) {
    return res.status(400).json({ 
      success: false, 
      message: 'noCrusherProduksi is required' 
    });
  }
  try {
    const data = await service.fetchInputs(noCrusherProduksi);
    return res.status(200).json({ 
      success: true, 
      message: 'Inputs retrieved', 
      data 
    });
  } catch (e) {
    console.error('[getInputsByNoCrusherProduksi]', e);
    return res.status(500).json({ 
      success: false, 
      message: 'Internal Server Error', 
      error: e.message 
    });
  }
}

async function getOutputsByNoCrusherProduksi(req, res) {
  const noCrusherProduksi = (req.params.noCrusherProduksi || '').trim();
  if (!noCrusherProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noCrusherProduksi is required'
    });
  }

  try {
    const data = await service.fetchOutputs(noCrusherProduksi);
    return res.status(200).json({
      success: true,
      message: 'Outputs retrieved',
      data
    });
  } catch (e) {
    console.error('[getOutputsByNoCrusherProduksi]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message
    });
  }
}


/**
 * GET /api/produksi/crusher/validate-label/:labelCode
 * Validate label for crusher production (only BB and Bonggolan)
 */
async function validateLabel(req, res) {
  const { labelCode } = req.params;

  // Validate input
  if (!labelCode || typeof labelCode !== 'string') {
    return res.status(400).json({
      success: false,
      message: 'Label number is required and must be a string',
    });
  }

  try {
    const result = await service.validateLabel(labelCode);

    if (!result.found) {
      return res.status(404).json({
        success: false,
        message: `Label ${labelCode} not found or already used`,
        prefix: result.prefix,
        tableName: result.tableName,
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Label validated successfully',
      prefix: result.prefix,
      tableName: result.tableName,
      totalRecords: result.count,
      data: result.data,
    });
  } catch (error) {
    console.error('Error validating label:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}



async function upsertInputsAndPartials(req, res) {
  const noProduksi = String(req.params.noCrusherProduksi  || '').trim();
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' },
    });
  }

  // ✅ pastikan body object
  const body = req.body && typeof req.body === 'object' ? req.body : {};

  // ✅ jangan percaya audit fields dari client
  // (biar client tidak bisa spoof requestId/actorId dan biar tidak bikin null/aneh)
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...payload
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized (idUsername missing)',
    });
  }

  // ✅ username untuk business fields / audit actor string
  const actorUsername = getActorUsername(req) || req.username || req.user?.username || 'system';

  // ✅ request id per HTTP request (kalau ada header ikut pakai)
  const requestId = String(makeRequestId(req) || '').trim();

  // optional: echo header for tracing
  if (requestId) res.setHeader('x-request-id', requestId);

  // optional validate: at least one input exists
  const hasInput = ['bb', 'bonggolan', 'bbPartial'].some((key) => Array.isArray(payload?.[key]) && payload[key].length > 0);

  // if (!hasInput) { ... } // kalau mau strict, aktifkan lagi

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await service.upsertInputsAndPartials(noProduksi, payload, ctx);

    // Support beberapa bentuk return (backward compatible)
    const success = result?.success !== undefined ? !!result.success : true;
    const hasWarnings = !!result?.hasWarnings;
    const data = result?.data ?? result;

    let statusCode = 200;
    let message = 'Inputs & partials processed successfully';

    if (!success) {
      const totalInvalid = Number(data?.summary?.totalInvalid ?? 0);
      const totalInserted = Number(data?.summary?.totalInserted ?? 0);
      const totalPartialsCreated = Number(data?.summary?.totalPartialsCreated ?? 0);

      if (totalInvalid > 0) {
        statusCode = 422;
        message = 'Beberapa data tidak valid';
      } else if (totalInserted === 0 && totalPartialsCreated === 0) {
        statusCode = 400;
        message = 'Tidak ada data yang berhasil diproses';
      }
    } else if (hasWarnings) {
      message = 'Inputs & partials processed with warnings';
    }

    return res.status(statusCode).json({
      success,
      message,
      data,
      meta: {
        noProduksi,
        hasInput,
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (e) {
    console.error('[upsertInputsAndPartials]', e);
    const status = e.statusCode || e.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? 'Internal Server Error' : e.message,
      error: {
        message: e.message,
        details: process.env.NODE_ENV === 'development' ? e.stack : undefined,
      },
    });
  }
}



async function deleteInputsAndPartials(req, res) {
  const noProduksi = String(req.params.noCrusherProduksi  || '').trim();
  
  if (!noProduksi) {
    return res.status(400).json({ 
      success: false, 
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' }
    });
  }

  // ✅ Strip client audit fields
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...payload
  } = req.body || {};

  // ✅ Get trusted audit context
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized (idUsername missing)',
    });
  }

  const actorUsername = getActorUsername(req) || req.username || req.user?.username || 'system';
  const requestId = String(makeRequestId(req) || '').trim();

  if (requestId) res.setHeader('x-request-id', requestId);

  // Validate input
  const hasInput = ['bb', 'bonggolan', 'bbPartial'].some(key => Array.isArray(payload?.[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: { message: 'Request body harus berisi minimal satu array input' }
    });
  }

  try {
    // ✅ Forward audit context
    const ctx = { actorId, actorUsername, requestId };
    
    const result = await service.deleteInputsAndPartials(noProduksi, payload, ctx);

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = 'Inputs & partials deleted successfully';

    if (!success) {
      statusCode = 404;
      message = 'Tidak ada data yang berhasil dihapus';
    } else if (hasWarnings) {
      message = 'Inputs & partials deleted with warnings';
    }

    return res.status(statusCode).json({
      success,
      message,
      data,
      meta: {
        noProduksi,
        hasInput,
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (e) {
    console.error('[deleteInputsAndPartials]', e);
    const status = e.statusCode || e.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? 'Internal Server Error' : e.message,
      error: {
        message: e.message,
        details: process.env.NODE_ENV === 'development' ? e.stack : undefined
      }
    });
  }
}

async function splitProduksiTime(req, res) {
  const normalizeSqlTimeToHms = (value) => {
    if (value == null) return value;
    if (value instanceof Date) {
      const hh = String(value.getUTCHours()).padStart(2, "0");
      const mm = String(value.getUTCMinutes()).padStart(2, "0");
      const ss = String(value.getUTCSeconds()).padStart(2, "0");
      return `${hh}:${mm}:${ss}`;
    }
    const raw = String(value).trim();
    const m = /(\d{2}):(\d{2}):(\d{2})/.exec(raw);
    return m ? `${m[1]}:${m[2]}:${m[3]}` : raw;
  };

  const idMesinRaw = String(req.params.idMesin || "").trim();
  const tanggal = String(req.params.tanggal || "").trim();

  const idMesin = Number(idMesinRaw);
  if (!Number.isInteger(idMesin) || idMesin <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "idMesin harus integer positif" });
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(tanggal)) {
    return res.status(400).json({
      success: false,
      message: "tanggal harus format YYYY-MM-DD",
    });
  }

  const body = req.body && typeof req.body === "object" ? req.body : {};
  const hourStart = String(body.hourStart || "").trim();
  const outputJenisId = Number(body.outputJenisId);

  if (!hourStart) {
    return res.status(400).json({
      success: false,
      message: "hourStart wajib diisi",
    });
  }
  const timeRegex = /^\d{2}:\d{2}(:\d{2})?$/;
  if (!timeRegex.test(hourStart)) {
    return res.status(400).json({
      success: false,
      message: "Format hourStart harus HH:mm atau HH:mm:ss",
    });
  }
  if (!Number.isInteger(outputJenisId) || outputJenisId <= 0) {
    return res.status(400).json({
      success: false,
      message: "outputJenisId wajib integer positif",
    });
  }

  const actorId = getActorId(req);
  if (!actorId) {
    return res
      .status(401)
      .json({ success: false, message: "Unauthorized (idUsername missing)" });
  }

  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";
  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId) res.setHeader("x-request-id", requestId);

  try {
    const ctx = { actorId, actorUsername, requestId };
    const result = await service.splitProduksiTime(
      { idMesin, tanggal },
      { hourStart, outputJenisId },
      ctx,
    );

    const header = result?.header
      ? {
          ...result.header,
          HourStart: normalizeSqlTimeToHms(result.header.HourStart),
          HourEnd: normalizeSqlTimeToHms(result.header.HourEnd),
        }
      : result?.header;

    return res.status(201).json({
      success: true,
      message: "Produksi berhasil di-split",
      data: {
        ...result,
        header,
      },
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[crusher.splitProduksiTime]", err);
    const status = err.statusCode || err.status || 500;
    return res.status(status).json({
      success: false,
      message:
        status === 500 ? "Internal Server Error" : err.message || "Error",
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
      },
    });
  }
}

module.exports = { getAllProduksi, getProduksiByDate, getCrusherMasters, createProduksi, updateProduksi, deleteProduksi, getInputsByNoCrusherProduksi, getOutputsByNoCrusherProduksi, upsertInputsAndPartials, validateLabel, deleteInputsAndPartials, splitProduksiTime };

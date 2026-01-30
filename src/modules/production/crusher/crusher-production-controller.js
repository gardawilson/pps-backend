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

  try {
    const { data, total } = await service.getAllProduksi(
      page,
      pageSize,
      search
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
  try {
    // dari verifyToken middleware
    const username = req.username || req.user?.username || 'system';

    // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
    const b = req.body || {};

    // helper kecil buat parse number
    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };
    const toFloat = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    const payload = {
      tanggal: b.tanggal,                         // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),                  // number
      idOperator: toInt(b.idOperator),            // number
      jam: toInt(b.jam) || toInt(b.jamKerja),     // number (alias support)
      shift: toInt(b.shift),                      // number
      createBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),
      hourStart: b.hourStart || null,             // ex: '08:00:00'
      hourEnd: b.hourEnd || null,                 // ex: '16:00:00'
    };

    const result = await service.createCrusherProduksi(payload);

    return res
      .status(201)
      .json({ success: true, message: 'Created', data: result.header });
  } catch (err) {
    const status = err.statusCode || 500;
    return res
      .status(status)
      .json({ success: false, message: err.message || 'Internal Error' });
  }
}


/**
 * PUT /api/produksi/crusher/:noCrusherProduksi
 * Update crusher production header
 */
async function updateProduksi(req, res) {
  try {
    const noCrusherProduksi = req.params.noCrusherProduksi;
    if (!noCrusherProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noCrusherProduksi is required in route param',
      });
    }

    // Get username from verifyToken middleware
    const username = req.username || req.user?.username || 'system';

    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };
    const toFloat = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    // Body can be partial, don't require all fields
    const payload = {
      tanggal: b.tanggal,                       // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      jam: b.jam,                               // will be parsed again
      shift: toInt(b.shift),
      updateBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),
      hourStart: b.hourStart || null,
      hourEnd: b.hourEnd || null,
    };

    const result = await service.updateCrusherProduksi(noCrusherProduksi, payload);

    return res.status(200).json({
      success: true,
      message: 'Updated',
      data: result.header,
    });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}



/**
 * DELETE /api/produksi/crusher/:noCrusherProduksi
 * Delete crusher production header and all related inputs/partials
 */
async function deleteProduksi(req, res) {
  try {
    const noCrusherProduksi = req.params.noCrusherProduksi;
    if (!noCrusherProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noCrusherProduksi is required in route param',
      });
    }

    await service.deleteCrusherProduksi(noCrusherProduksi);

    return res.status(200).json({
      success: true,
      message: 'Deleted',
    });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
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


module.exports = { getAllProduksi, getProduksiByDate, getCrusherMasters, createProduksi, updateProduksi, deleteProduksi, getInputsByNoCrusherProduksi, upsertInputsAndPartials, validateLabel, deleteInputsAndPartials };

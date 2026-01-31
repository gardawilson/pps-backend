// controllers/bj-jual-controller.js
const bjJualService = require('./bj-jual-service'); // sesuaikan path
const { getActorId, getActorUsername, makeRequestId } = require('../../core/utils/http-context');


// ✅ GET ALL (paging + search + optional date range)
async function getAllBJJual(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support both ?noBJJual= and ?search=
  const search =
    (typeof req.query.noBJJual === 'string' && req.query.noBJJual) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  // OPTIONAL date filter
  // Example: ?dateFrom=2025-12-01&dateTo=2025-12-31
  const dateFrom =
    (typeof req.query.dateFrom === 'string' && req.query.dateFrom) || null;
  const dateTo =
    (typeof req.query.dateTo === 'string' && req.query.dateTo) || null;

  try {
    const { data, total } = await bjJualService.getAllBJJual(
      page,
      pageSize,
      search,
      dateFrom,
      dateTo
    );

    return res.status(200).json({
      success: true,
      message: 'BJJual_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search,
        dateFrom,
        dateTo,
      },
    });
  } catch (error) {
    console.error('Error fetching BJJual_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function createBJJual(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : Math.trunc(n);
    };

    const payload = {
      tanggal: b.tanggal || b.tglJual || b.tgl || null, // required (pick one)
      idPembeli: toInt(b.idPembeli),                   // required
      remark: b.remark ?? null,                        // optional
      createBy: username,                              // optional if you later add column
    };

    const result = await bjJualService.createBJJual(payload);

    return res.status(201).json({
      success: true,
      message: 'BJJual_h created',
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

async function updateBJJual(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const noBJJual = String(req.params.noBJJual || '').trim();

    if (!noBJJual) {
      return res.status(400).json({ success: false, message: 'noBJJual wajib' });
    }

    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : Math.trunc(n);
    };

    const payload = {
      // optional
      tanggal: b.tanggal ?? b.tglJual ?? b.tgl ?? undefined, // ✅ undefined berarti "tidak dikirim"

      idPembeli: b.idPembeli !== undefined ? toInt(b.idPembeli) : undefined,
      remark: b.remark ?? undefined,

      updateBy: username,
    };

    const result = await bjJualService.updateBJJual(noBJJual, payload);

    return res.status(200).json({
      success: true,
      message: 'BJJual_h updated',
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

async function deleteBJJual(req, res) {
  try {
    const noBJJual = String(req.params.noBJJual || '').trim();

    if (!noBJJual) {
      return res.status(400).json({
        success: false,
        message: 'noBJJual is required in route param',
      });
    }

    await bjJualService.deleteBJJual(noBJJual);

    return res.status(200).json({ success: true, message: 'Deleted' });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}


async function getInputsByNoBJJual(req, res) {
  const noBJJual = String(req.params.noBJJual || '').trim();
  if (!noBJJual) {
    return res
      .status(400)
      .json({ success: false, message: 'noBJJual is required' });
  }

  try {
    const data = await bjJualService.fetchInputs(noBJJual);
    return res
      .status(200)
      .json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[bjJual.getInputsByNoBJJual]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
  }
}


async function upsertInputsAndPartials(req, res) {
  const noProduksi = String(req.params.noProduksi || '').trim();
  
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' },
    });
  }

  // ✅ Pastikan body object
  const body = req.body && typeof req.body === 'object' ? req.body : {};

  // ✅ Strip client audit fields (jangan percaya dari client)
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...payload
  } = body;

  // ✅ Get trusted audit context from token/session
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized (idUsername missing)',
    });
  }

  const actorUsername = getActorUsername(req) || req.username || req.user?.username || 'system';
  const requestId = String(makeRequestId(req) || '').trim();

  // Optional: echo header for tracing
  if (requestId) res.setHeader('x-request-id', requestId);

  // ✅ Validate: at least one input exists
  const hasInput = [
    'barangJadi',
    'furnitureWip',
    'cabinetMaterial',
    'barangJadiPartial',
    'furnitureWipPartial',
  ].some((key) => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0);


  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message: 'Request body harus berisi minimal satu array input yang tidak kosong',
      },
    });
  }

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await bjJualService.upsertInputsAndPartials(
      noProduksi,
      payload,
      ctx
    );

    // Support beberapa bentuk return (backward compatible)
    const success = result?.success !== undefined ? !!result.success : true;
    const hasWarnings = !!result?.hasWarnings;
    const data = result?.data ?? result;

    let statusCode = 200;
    let message = 'Inputs & partials processed successfully';

    if (!success) {
      const totalInvalid = Number(data?.summary?.totalInvalid ?? 0);
      const totalInserted = Number(data?.summary?.totalInserted ?? 0);
      const totalUpdated = Number(data?.summary?.totalUpdated ?? 0); // ✅ Support UPSERT
      const totalPartialsCreated = Number(data?.summary?.totalPartialsCreated ?? 0);

      if (totalInvalid > 0) {
        statusCode = 422;
        message = 'Beberapa data tidak valid';
      } else if ((totalInserted + totalUpdated) === 0 && totalPartialsCreated === 0) {
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
    console.error('[inject.upsertInputsAndPartials]', e);
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
  const noProduksi = String(req.params.noProduksi || '').trim();

  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' },
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

  // ✅ Validate input
  const hasInput = [
    'barangJadi',
    'furnitureWip',
    'cabinetMaterial',
    'barangJadiPartial',
    'furnitureWipPartial',
  ].some((key) => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: { message: 'Request body harus berisi minimal satu array input yang tidak kosong' },
    });
  }

  try {
    // ✅ Forward audit context
    const ctx = { actorId, actorUsername, requestId };

    const result = await bjJualService.deleteInputsAndPartials(
      noProduksi,
      payload,
      ctx
    );

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
    console.error('[inject.deleteInputsAndPartials]', e);
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


module.exports = {
  getAllBJJual,
  createBJJual,
  updateBJJual,
  deleteBJJual,
  getInputsByNoBJJual,
  upsertInputsAndPartials,
  deleteInputsAndPartials
};

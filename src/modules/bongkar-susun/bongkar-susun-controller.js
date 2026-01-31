// controllers/bongkar-susun-controller.js
const bongkarSusunService = require('./bongkar-susun-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../core/utils/http-context');


async function getByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah tervalidasi formatnya oleh route regex
  console.log(
    '🔍 Fetching BongkarSusun_h | Username:',
    username,
    '| date:',
    date
  );

  try {
    const data = await bongkarSusunService.getByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data BongkarSusun_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data BongkarSusun_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching BongkarSusun_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getAllBongkarSusun(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support ?noBongkarSusun= dan ?search=
  const search =
    (typeof req.query.noBongkarSusun === 'string' &&
      req.query.noBongkarSusun) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  try {
    const { data, total } = await bongkarSusunService.getAllBongkarSusun(
      page,
      pageSize,
      search
    );

    return res.status(200).json({
      success: true,
      message: 'BongkarSusun_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search,
      },
    });
  } catch (error) {
    console.error('Error fetching BongkarSusun_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function createBongkarSusun(req, res) {
  try {
    // ⬇️ Ambil username dari JWT (verifyToken)
    const username = req.username || req.user?.username || null;

    if (!username) {
      return res.status(401).json({
        success: false,
        message: 'Username tidak ditemukan di token (unauthorized)',
      });
    }

    const b = req.body || {};

    // ⬇️ Tidak pakai idUsername dari body lagi
    const payload = {
      tanggal: b.tanggal, // 'YYYY-MM-DD'
      username,           // akan di-resolve ke IdUsername di service
      note: b.note ?? null,
    };

    const result = await bongkarSusunService.createBongkarSusun(payload);

    return res
      .status(201)
      .json({ success: true, message: 'Created', data: result.header });
  } catch (err) {
    console.error('Error create BongkarSusun_h:', err);
    const status = err.statusCode || 500;
    return res
      .status(status)
      .json({ success: false, message: err.message || 'Internal Error' });
  }
}

async function updateBongkarSusun(req, res) {
  try {
    const noBongkarSusun = req.params.noBongkarSusun;
    if (!noBongkarSusun) {
      return res.status(400).json({
        success: false,
        message: 'noBongkarSusun wajib diisi di URL',
      });
    }

    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    // Karena UPDATE, semua field opsional.
    const payload = {};

    if (b.tanggal !== undefined) {
      payload.tanggal = b.tanggal; // 'YYYY-MM-DD'
    }

    // ⬇️ opsional: kalau suatu saat mau pakai, bisa kirim idUsername dari body.
    if (b.idUsername !== undefined) {
      payload.idUsername = toInt(b.idUsername);
    }

    if (b.note !== undefined) {
      payload.note = b.note === '' ? null : b.note;
    }

    const result = await bongkarSusunService.updateBongkarSusunCascade(
      noBongkarSusun,
      payload
    );

    return res.status(200).json({
      success: true,
      message: 'Updated',
      data: result.header,
    });
  } catch (err) {
    console.error('Error update BongkarSusun_h:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}

async function deleteBongkarSusun(req, res) {
  try {
    const noBongkarSusun = req.params.noBongkarSusun;
    if (!noBongkarSusun) {
      return res.status(400).json({
        success: false,
        message: 'noBongkarSusun wajib diisi di URL',
      });
    }

    await bongkarSusunService.deleteBongkarSusun(noBongkarSusun);

    return res.status(200).json({
      success: true,
      message: 'Deleted',
    });
  } catch (err) {
    console.error('Error delete BongkarSusun_h:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}

// controllers/bongkarSusunController.js
async function getInputsByNoBongkarSusun(req, res) {
  const noBongkarSusun = (req.params.noBongkarSusun || '').trim();
  if (!noBongkarSusun) {
    return res
      .status(400)
      .json({ success: false, message: 'noBongkarSusun is required' });
  }

  try {
    const data = await bongkarSusunService.fetchInputs(noBongkarSusun);
    return res
      .status(200)
      .json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[getInputsByNoBongkarSusun]', e);
    return res
      .status(500)
      .json({ success: false, message: 'Internal Server Error', error: e.message });
  }
}

async function validateLabel(req, res) {
  const { labelCode } = req.params;

  if (!labelCode || typeof labelCode !== 'string') {
    return res.status(400).json({
      success: false,
      message: 'Label number is required and must be a string',
    });
  }

  try {
    const result = await bongkarSusunService.validateLabelBongkarSusun(labelCode);

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
    console.error('Error validating label (BongkarSusun):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}


async function upsertInputs(req, res) {
  const noProduksi = String(req.params.noBongkarSusun || '').trim();
  
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
    'broker',
    'bb',
    'washing',
    'crusher',
    'gilingan',
    'mixer',
    'bonggolan',
    'furnitureWip',
    'barangJadi',
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

    const result = await bongkarSusunService.upsertInputs(
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


async function deleteInputs(req, res) {
  const noProduksi = String(req.params.noBongkarSusun || '').trim();

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
    'broker',
    'bb',
    'washing',
    'crusher',
    'gilingan',
    'mixer',
    'bonggolan',
    'furnitureWip',
    'barangJadi',
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

    const result = await bongkarSusunService.deleteInputs(
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
  getByDate,
  getAllBongkarSusun,
  createBongkarSusun,
  updateBongkarSusun,
  deleteBongkarSusun,
  getInputsByNoBongkarSusun,
  validateLabel,
  upsertInputs,
  deleteInputs
};

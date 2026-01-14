// controllers/bj-jual-controller.js
const bjJualService = require('./bj-jual-service'); // sesuaikan path

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


async function upsertInputs(req, res) {
  const noBJJual = String(req.params.noBJJual || '').trim();

  if (!noBJJual) {
    return res.status(400).json({
      success: false,
      message: 'noBJJual is required',
      error: {
        field: 'noBJJual',
        message: 'Parameter noBJJual tidak boleh kosong',
      },
    });
  }

  const payload = req.body || {};

  const hasInput = [
    'barangJadi',
    'barangJadiPartial',
    'barangJadiPartialNew',
    'furnitureWip',
    'furnitureWipPartial',
    'furnitureWipPartialNew',
    'cabinetMaterial',
  ].some((key) => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message:
          'Request body harus berisi minimal satu array input yang tidak kosong',
      },
    });
  }

  try {
    const result = await bjJualService.upsertInputsAndPartials(noBJJual, payload);

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = 'Inputs & partials processed successfully';

    if (!success) {
      if ((data?.summary?.totalInvalid || 0) > 0) {
        statusCode = 422;
        message = 'Beberapa data tidak valid';
      } else if (
        ((data?.summary?.totalInserted || 0) + (data?.summary?.totalUpdated || 0)) === 0 &&
        (data?.summary?.totalPartialsCreated || 0) === 0
      ) {
        statusCode = 400;
        message = 'Tidak ada data yang berhasil diproses';
      }
    } else if (hasWarnings) {
      message = 'Inputs & partials processed with warnings';
    }

    return res.status(statusCode).json({ success, message, data });
  } catch (e) {
    console.error('[bjJual.upsertInputs]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: {
        message: e.message,
        details: process.env.NODE_ENV === 'development' ? e.stack : undefined,
      },
    });
  }
}

async function deleteInputsAndPartials(req, res) {
  const noBJJual = String(req.params.noBJJual || '').trim();

  if (!noBJJual) {
    return res.status(400).json({
      success: false,
      message: 'noBJJual is required',
      error: { field: 'noBJJual', message: 'Parameter noBJJual tidak boleh kosong' },
    });
  }

  const payload = req.body || {};

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
    const result = await bjJualService.deleteInputsAndPartials(noBJJual, payload);

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = 'Inputs & partials deleted successfully';

    if (!success) {
      statusCode = 404;
      message = 'Tidak ada data yang berhasil dihapus';
    } else if (hasWarnings) {
      message = 'Inputs & partials deleted with warnings';
    }

    return res.status(statusCode).json({ success, message, data });
  } catch (e) {
    console.error('[bjJual.deleteInputsAndPartials]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
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
  upsertInputs,
  deleteInputsAndPartials
};

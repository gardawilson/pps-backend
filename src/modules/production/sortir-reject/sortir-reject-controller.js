// controllers/sortir-reject-controller.js
const sortirRejectService = require('./sortir-reject-service');


async function getAllSortirReject(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support ?noBJSortir= or ?search=
  const search =
    (typeof req.query.noBJSortir === 'string' && req.query.noBJSortir) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  // OPTIONAL date range
  // Example: ?dateFrom=2025-12-01&dateTo=2025-12-31
  const dateFrom =
    (typeof req.query.dateFrom === 'string' && req.query.dateFrom) || null;
  const dateTo =
    (typeof req.query.dateTo === 'string' && req.query.dateTo) || null;

  try {
    const { data, total } = await sortirRejectService.getAllSortirReject(
      page,
      pageSize,
      search,
      dateFrom,
      dateTo
    );

    return res.status(200).json({
      success: true,
      message: 'BJSortirReject_h retrieved successfully',
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
    console.error('Error fetching BJSortirReject_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}


async function getSortirRejectByDate(req, res) {
  const { username } = req; // dari verifyToken
  const date = req.params.date;

  console.log('🔍 Fetching BJSortirReject_h | Username:', username, '| date:', date);

  try {
    const data = await sortirRejectService.getSortirRejectByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No BJSortirReject_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `BJSortirReject_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching BJSortirReject_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function createSortirReject(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const idUsername = req.idUsername ?? req.user?.idUsername ?? null; // ✅ numeric from token
    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : Math.trunc(n);
    };

    const payload = {
      tglBJSortir: b.tglBJSortir,          // required (YYYY-MM-DD)
      idWarehouse: toInt(b.idWarehouse),   // required
      idUsername: toInt(b.idUsername) ?? idUsername, // ✅ required, numeric (default from token)
      _actor: username, // only for logs, not stored
    };

    const result = await sortirRejectService.createSortirReject(payload);

    return res.status(201).json({
      success: true,
      message: 'BJSortirReject_h created',
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

async function updateSortirReject(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const noBJSortir = String(req.params.noBJSortir || '').trim();

    if (!noBJSortir) {
      return res.status(400).json({ success: false, message: 'noBJSortir wajib' });
    }

    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : Math.trunc(n);
    };

    const payload = {
      // header fields (optional)
      tglBJSortir: b.tglBJSortir ?? undefined,      // ✅ distinguish not-sent vs null
      idWarehouse: b.idWarehouse ?? undefined,      // optional
      // idUsername normally should NOT be editable, but if you want allow:
      idUsername: b.idUsername ?? undefined,

      // inputs (optional) -> if provided, service will replace mapping
      barangJadi: Array.isArray(b.barangJadi) ? b.barangJadi : undefined,        // [{ noBJ: 'BJ.0000...' }, ...]
      furnitureWip: Array.isArray(b.furnitureWip) ? b.furnitureWip : undefined,  // [{ noFurnitureWIP: 'BB.0000...' }, ...]

      updateBy: username, // only for log
    };

    const result = await sortirRejectService.updateSortirReject(noBJSortir, payload);

    return res.status(200).json({
      success: true,
      message: 'BJSortirReject_h updated',
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

async function deleteSortirReject(req, res) {
  try {
    const noBJSortir = String(req.params.noBJSortir || '').trim();
    if (!noBJSortir) {
      return res.status(400).json({
        success: false,
        message: 'noBJSortir is required in route param',
      });
    }

    await sortirRejectService.deleteSortirReject(noBJSortir);

    return res.status(200).json({ success: true, message: 'Deleted' });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}



async function getInputsByNoBJSortir(req, res) {
  const noBJSortir = String(req.params.noBJSortir || '').trim();
  if (!noBJSortir) {
    return res
      .status(400)
      .json({ success: false, message: 'noBJSortir is required' });
  }

  try {
    const data = await sortirRejectService.fetchInputs(noBJSortir);
    return res
      .status(200)
      .json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[sortirReject.getInputsByNoBJSortir]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
  }
}


async function upsertInputs(req, res) {
  const noBJSortir = String(req.params.noBJSortir || '').trim();

  if (!noBJSortir) {
    return res.status(400).json({
      success: false,
      message: 'noBJSortir is required',
      error: {
        field: 'noBJSortir',
        message: 'Parameter noBJSortir tidak boleh kosong',
      },
    });
  }

  const payload = req.body || {};

  const hasInput = ['furnitureWip', 'cabinetMaterial', 'barangJadi'].some(
    (key) => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0
  );

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message:
          'Request body harus berisi minimal satu array input (furnitureWip, cabinetMaterial, barangJadi) yang tidak kosong',
      },
    });
  }

  try {
    const result = await sortirRejectService.upsertInputs(noBJSortir, payload);

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = 'Inputs processed successfully';

    if (!success) {
      if ((data?.summary?.totalInvalid || 0) > 0) {
        statusCode = 422;
        message = 'Beberapa data tidak valid';
      } else if (
        ((data?.summary?.totalInserted || 0) + (data?.summary?.totalUpdated || 0)) === 0
      ) {
        statusCode = 400;
        message = 'Tidak ada data yang berhasil diproses';
      }
    } else if (hasWarnings) {
      message = 'Inputs processed with warnings';
    }

    return res.status(statusCode).json({ success, message, data });
  } catch (e) {
    console.error('[sortirReject.upsertInputs]', e);
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

async function deleteInputs(req, res) {
  const noBJSortir = String(req.params.noBJSortir || '').trim();

  if (!noBJSortir) {
    return res.status(400).json({
      success: false,
      message: 'noBJSortir is required',
      error: { field: 'noBJSortir', message: 'Parameter noBJSortir tidak boleh kosong' },
    });
  }

  const payload = req.body || {};

  const hasInput = ['furnitureWip', 'cabinetMaterial', 'barangJadi'].some(
    (key) => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0
  );

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: { message: 'Request body harus berisi minimal satu array input yang tidak kosong' },
    });
  }

  try {
    const result = await sortirRejectService.deleteInputs(noBJSortir, payload);

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = 'Inputs deleted successfully';

    if (!success) {
      statusCode = 404;
      message = 'Tidak ada data yang berhasil dihapus';
    } else if (hasWarnings) {
      message = 'Inputs deleted with warnings';
    }

    return res.status(statusCode).json({ success, message, data });
  } catch (e) {
    console.error('[sortirReject.deleteInputs]', e);
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
  getAllSortirReject,
  getSortirRejectByDate,
  createSortirReject,
  updateSortirReject,
  deleteSortirReject,
  getInputsByNoBJSortir,
  upsertInputs,
  deleteInputs
};

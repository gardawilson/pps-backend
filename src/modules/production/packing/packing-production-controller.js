// controllers/packing-production-controller.js
const packingService = require('./packing-production-service');

// ✅ NEW: GET ALL (paging + search + optional date range)
async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support both ?noPacking= and ?search=
  const search =
    (typeof req.query.noPacking === 'string' && req.query.noPacking) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  // OPTIONAL (recommended): date filter
  // Example: ?dateFrom=2025-12-01&dateTo=2025-12-31
  const dateFrom =
    (typeof req.query.dateFrom === 'string' && req.query.dateFrom) || null;
  const dateTo =
    (typeof req.query.dateTo === 'string' && req.query.dateTo) || null;

  try {
    const { data, total } = await packingService.getAllProduksi(
      page,
      pageSize,
      search,
      dateFrom,
      dateTo
    );

    return res.status(200).json({
      success: true,
      message: 'PackingProduksi_h retrieved successfully',
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
    console.error('Error fetching PackingProduksi_h:', error);
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

  console.log('🔍 Fetching PackingProduksi_h | Username:', username, '| date:', date);

  try {
    const data = await packingService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No PackingProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `PackingProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching PackingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function createProduksi(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const b = req.body || {};

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

    const payload = {
      tglProduksi: b.tglProduksi,         // required
      idMesin: toInt(b.idMesin),          // required
      idOperator: toInt(b.idOperator),    // required
      shift: toInt(b.shift),              // required
      jamKerja: b.jamKerja ?? null,       // optional, can be computed
      hourMeter: toFloat(b.hourMeter),    // optional
      hourStart: b.hourStart || null,     // required
      hourEnd: b.hourEnd || null,         // required

      createBy: username,
      checkBy1: b.checkBy1 ?? null,
      checkBy2: b.checkBy2 ?? null,
      approveBy: b.approveBy ?? null,
    };

    const result = await packingService.createPackingProduksi(payload);

    return res.status(201).json({
      success: true,
      message: 'PackingProduksi_h created',
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


async function updateProduksi(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const noPacking = String(req.params.noPacking || '').trim();

    if (!noPacking) {
      return res.status(400).json({ success: false, message: 'noPacking wajib' });
    }

    const b = req.body || {};

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

    const payload = {
      tglProduksi: b.tglProduksi, // optional

      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      shift: toInt(b.shift),

      jamKerja: b.jamKerja, // optional

      hourMeter: toFloat(b.hourMeter),

      // ✅ distinguish "not sent" vs null
      hourStart: b.hourStart ?? undefined,
      hourEnd: b.hourEnd ?? undefined,

      updateBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
    };

    const result = await packingService.updatePackingProduksi(noPacking, payload);

    return res.status(200).json({
      success: true,
      message: 'PackingProduksi_h updated',
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


async function deleteProduksi(req, res) {
  try {
    const noPacking = String(req.params.noPacking || '').trim();
    if (!noPacking) {
      return res.status(400).json({
        success: false,
        message: 'noPacking is required in route param',
      });
    }

    await packingService.deletePackingProduksi(noPacking);

    return res.status(200).json({ success: true, message: 'Deleted' });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}


async function getInputsByNoPacking(req, res) {
  const noPacking = String(req.params.noPacking || '').trim();
  if (!noPacking) {
    return res
      .status(400)
      .json({ success: false, message: 'noPacking is required' });
  }

  try {
    const data = await packingService.fetchInputs(noPacking);
    return res
      .status(200)
      .json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[packing.getInputsByNoPacking]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
  }
}


async function upsertInputs(req, res) {
  const noPacking = String(req.params.noPacking || '').trim();

  if (!noPacking) {
    return res.status(400).json({
      success: false,
      message: 'noPacking is required',
      error: {
        field: 'noPacking',
        message: 'Parameter noPacking tidak boleh kosong',
      },
    });
  }

  const payload = req.body || {};

  const hasInput = [
    'furnitureWip',
    'cabinetMaterial',
    'furnitureWipPartialNew',
  ].some(
    (key) =>
      payload[key] && Array.isArray(payload[key]) && payload[key].length > 0
  );

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message:
          'Request body harus berisi minimal satu array input (furnitureWip, cabinetMaterial, furnitureWipPartialNew) yang tidak kosong',
      },
    });
  }

  try {
    const result = await packingService.upsertInputsAndPartials(noPacking, payload);

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
    console.error('[packing.upsertInputs]', e);
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
  const noPacking = String(req.params.noPacking || '').trim();

  if (!noPacking) {
    return res.status(400).json({
      success: false,
      message: 'noPacking is required',
      error: { field: 'noPacking', message: 'Parameter noPacking tidak boleh kosong' },
    });
  }

  const payload = req.body || {};

  const hasInput = ['furnitureWip', 'cabinetMaterial', 'furnitureWipPartial'].some(
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
    const result = await packingService.deleteInputsAndPartials(noPacking, payload);

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
    console.error('[packing.deleteInputsAndPartials]', e);
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


module.exports = { getAllProduksi, getProduksiByDate, createProduksi, updateProduksi, deleteProduksi, getInputsByNoPacking, upsertInputs, deleteInputsAndPartials };

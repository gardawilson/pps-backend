// controllers/gilingan-production-controller.js
const gilinganProduksiService = require('./gilingan-production-service');

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log('🔍 Fetching GilinganProduksi_h | Username:', username, '| date:', date);

  try {
    const data = await gilinganProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No GilinganProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `GilinganProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching GilinganProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}


async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support both ?noProduksi= and ?search=
  const search =
    (typeof req.query.noProduksi === 'string' && req.query.noProduksi) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  try {
    const { data, total } = await gilinganProduksiService.getAllProduksi(
      page,
      pageSize,
      search
    );

    return res.status(200).json({
      success: true,
      message: 'GilinganProduksi_h retrieved successfully',
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
    console.error('Error fetching GilinganProduksi_h:', error);
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

    const payload = {
      tglProduksi: b.tglProduksi,                 // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      shift: toInt(b.shift),
      createBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),

      hourStart: b.hourStart || null,             // 'HH:mm:ss'
      hourEnd: b.hourEnd || null,                 // 'HH:mm:ss'
    };

    const result = await gilinganProduksiService.createGilinganProduksi(payload);

    return res
      .status(201)
      .json({ success: true, message: 'GilinganProduksi_h created', data: result.header });
  } catch (err) {
    const status = err.statusCode || 500;
    return res
      .status(status)
      .json({ success: false, message: err.message || 'Internal Error' });
  }
}


async function updateProduksi(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const noProduksi = req.params.noProduksi; // /gilingan/produksi/:noProduksi

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

    const payload = {
      tglProduksi: b.tglProduksi,                 // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      // tidak ada jam
      shift: toInt(b.shift),
      // CreateBy biasanya tidak diubah saat update; kita pakai user sebagai "editor" kalau nanti ada kolom EditBy, dst.
      updateBy: username,                         // kalau mau dipakai nanti
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),

      hourStart: b.hourStart || null,             // 'HH:mm:ss'
      hourEnd: b.hourEnd || null                  // 'HH:mm:ss'
    };

    const result = await gilinganProduksiService.updateGilinganProduksi(
      noProduksi,
      payload
    );

    return res.status(200).json({
      success: true,
      message: 'GilinganProduksi_h updated',
      data: result.header,
    });
  } catch (err) {
    const status = err.statusCode || 500;
    return res
      .status(status)
      .json({ success: false, message: err.message || 'Internal Error' });
  }
}


async function deleteProduksi(req, res) {
  try {
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

    await gilinganProduksiService.deleteGilinganProduksi(noProduksi);

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


async function getInputsByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || '').trim();
  if (!noProduksi) {
    return res.status(400).json({ success: false, message: 'noProduksi is required' });
  }

  try {
    const data = await gilinganProduksiService.fetchInputs(noProduksi);
    return res.status(200).json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[gilingan.getInputsByNoProduksi]', e);
    return res.status(500).json({ success: false, message: 'Internal Server Error', error: e.message });
  }
}

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
    const result = await gilinganProduksiService.validateLabel(labelCode);

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
      data: result.data, // Now returns array of all matching records
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
  const noProduksi = String(req.params.noProduksi || '').trim();
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' }
    });
  }

  const payload = req.body || {};

  // (optional) validasi minimal 1 input
  // const hasInput = ['broker','bonggolan','crusher','reject','brokerPartialNew','rejectPartialNew']
  //   .some(k => Array.isArray(payload[k]) && payload[k].length > 0);

  try {
    const result = await gilinganProduksiService.upsertInputsAndPartials(noProduksi, payload);

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = 'Inputs & partials processed successfully';

    if (!success) {
      if (data.summary.totalInvalid > 0) {
        statusCode = 422;
        message = 'Beberapa data tidak valid';
      } else if (data.summary.totalInserted === 0 && data.summary.totalPartialsCreated === 0) {
        statusCode = 400;
        message = 'Tidak ada data yang berhasil diproses';
      }
    } else if (hasWarnings) {
      message = 'Inputs & partials processed with warnings';
    }

    return res.status(statusCode).json({ success, message, data });
  } catch (e) {
    console.error('[gilingan.upsertInputsAndPartials]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: {
        message: e.message,
        details: process.env.NODE_ENV === 'development' ? e.stack : undefined
      }
    });
  }
}

async function deleteInputsAndPartials(req, res) {
  const noProduksi = String(req.params.noProduksi || '').trim();

  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' }
    });
  }

  const payload = req.body || {};

  const hasInput = [
    'broker', 'bonggolan', 'crusher', 'reject',
    'brokerPartial', 'rejectPartial'
  ].some(k => Array.isArray(payload[k]) && payload[k].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: { message: 'Request body harus berisi minimal satu array input yang tidak kosong' }
    });
  }

  try {
    const result = await gilinganProduksiService.deleteInputsAndPartials(noProduksi, payload);
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
    console.error('[gilingan.deleteInputsAndPartials]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: {
        message: e.message,
        details: process.env.NODE_ENV === 'development' ? e.stack : undefined
      }
    });
  }
}


module.exports = { getProduksiByDate, getAllProduksi, createProduksi, updateProduksi, deleteProduksi, getInputsByNoProduksi, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials  };

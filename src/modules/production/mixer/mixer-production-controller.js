// controllers/mixer-production-controller.js
const mixerProduksiService = require('./mixer-production-service');

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log("🔍 Fetching MixerProduksi_h | Username:", username, "| date:", date);

  try {
    const data = await mixerProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No MixerProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `MixerProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching MixerProduksi_h:', error);
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
    const { data, total } = await mixerProduksiService.getAllProduksi(
      page,
      pageSize,
      search
    );

    return res.status(200).json({
      success: true,
      message: 'MixerProduksi_h retrieved successfully',
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
    console.error('Error fetching MixerProduksi_h:', error);
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
      jam: b.jam,                                 // number or 'HH:mm-HH:mm'
      shift: toInt(b.shift),
      createBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),
      hourStart: b.hourStart || null,             // '08:00:00'
      hourEnd: b.hourEnd || null,                 // '09:00:00'
    };

    const result = await mixerProduksiService.createMixerProduksi(payload);

    return res.status(201).json({
      success: true,
      message: 'Created',
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
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

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

    const has = (k) => Object.prototype.hasOwnProperty.call(b, k);

    const payload = {
      tglProduksi: has('tglProduksi') ? b.tglProduksi : undefined,
      idMesin: has('idMesin') ? toInt(b.idMesin) : undefined,
      idOperator: has('idOperator') ? toInt(b.idOperator) : undefined,
      jam: has('jam') ? b.jam : undefined,
      shift: has('shift') ? toInt(b.shift) : undefined,

      updateBy: username,

      checkBy1: has('checkBy1') ? b.checkBy1 : undefined,
      checkBy2: has('checkBy2') ? b.checkBy2 : undefined,
      approveBy: has('approveBy') ? b.approveBy : undefined,
      jmlhAnggota: has('jmlhAnggota') ? toInt(b.jmlhAnggota) : undefined,
      hadir: has('hadir') ? toInt(b.hadir) : undefined,
      hourMeter: has('hourMeter') ? toFloat(b.hourMeter) : undefined,

      hourStart: has('hourStart') ? (b.hourStart ?? null) : undefined,
      hourEnd: has('hourEnd') ? (b.hourEnd ?? null) : undefined,
    };

    const result = await mixerProduksiService.updateMixerProduksi(noProduksi, payload);

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


async function deleteProduksi(req, res) {
  try {
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

    await mixerProduksiService.deleteMixerProduksi(noProduksi);

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
    return res.status(400).json({ success:false, message:'noProduksi is required' });
  }

  try {
    const data = await mixerProduksiService.fetchInputs(noProduksi);
    return res.status(200).json({ success:true, message:'Inputs retrieved', data });
  } catch (e) {
    console.error('[mixer.getInputsByNoProduksi]', e);
    return res.status(500).json({ success:false, message:'Internal Server Error', error:e.message });
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
    const result = await mixerProduksiService.validateLabel(labelCode);

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
      error: {
        field: 'noProduksi',
        message: 'Parameter noProduksi tidak boleh kosong'
      }
    });
  }

  const payload = req.body || {};

  // Mixer hanya punya: broker, bb, gilingan, mixer + partialNew-nya
  const hasInput = [
    'broker', 'bb', 'gilingan', 'mixer',
    'bbPartialNew', 'brokerPartialNew', 'gilinganPartialNew', 'mixerPartialNew'
  ].some((key) => Array.isArray(payload[key]) && payload[key].length > 0);

  // (Opsional) kalau mau wajib minimal 1 input:
  // if (!hasInput) return res.status(400).json({...});

  try {
    const result = await mixerProduksiService.upsertInputsAndPartials(noProduksi, payload);
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
    console.error('[mixer.upsertInputsAndPartials]', e);
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

  // Mixer hanya punya 4 section + partialnya
  const hasInput = [
    'broker', 'bb', 'gilingan', 'mixer',
    'bbPartial', 'brokerPartial', 'gilinganPartial', 'mixerPartial'
  ].some((key) => Array.isArray(payload[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: { message: 'Request body harus berisi minimal satu array input yang tidak kosong' }
    });
  }

  try {
    const result = await mixerProduksiService.deleteInputsAndPartials(noProduksi, payload);
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
    console.error('[mixer.deleteInputsAndPartials]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: { message: e.message, details: process.env.NODE_ENV === 'development' ? e.stack : undefined }
    });
  }
}

module.exports = { getProduksiByDate, getAllProduksi, createProduksi, updateProduksi, deleteProduksi, getInputsByNoProduksi, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials };

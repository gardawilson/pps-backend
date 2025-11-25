// controllers/production-controller.js
const washingProduksiService = require('./washing-production-service');


// controller/washingProduksiController.js (misal)
async function getAllProduksi(req, res) {
  // pagination (default 20)
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100); // batasi max 100

  // support both ?noProduksi= and ?search=
  const search =
    (typeof req.query.noProduksi === 'string' && req.query.noProduksi) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  try {
    const { data, total } = await washingProduksiService.getAllProduksi(
      page,
      pageSize,
      search
    );

    return res.status(200).json({
      success: true,
      message: 'WashingProduksi_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search, // echo back untuk state di client (sama seperti broker)
      },
    });
  } catch (error) {
    console.error('Error fetching WashingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah match regex di route
  console.log("🔍 Fetching WashingProduksi_h | Username:", username, "| date:", date);

  try {
    const data = await washingProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data WashingProduksi_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date }
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data WashingProduksi_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date }
    });
  } catch (error) {
    console.error('Error fetching WashingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}



async function createProduksi(req, res) {
  try {
    // dari verifyToken middleware
    const username = req.username || req.user?.username || 'system';

    // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
    const b = req.body || {};

    // (opsional) debug log
    console.log('🟢 [Washing] raw body:', b);

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

    const normalizeTime = (v) => {
      if (!v) return null;
      const s = String(v).trim();
      if (!s) return null;
      // boleh kirim 'HH:mm' atau 'HH:mm:ss' dari Flutter,
      // di DB akan di-CAST ke time(7)
      return s;
    };

    const payload = {
      tglProduksi: b.tglProduksi,              // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),              // number
      idOperator: toInt(b.idOperator),        // number

      // ⬇️ beda nama di washing (JamKerja di table)
      jamKerja: b.jamKerja,                   // number atau 'HH:mm-HH:mm'

      shift: toInt(b.shift),                  // number
      createBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),        // Decimal(18,2)

      // ⬇️ baru: ikutkan ke service
      hourStart: normalizeTime(b.hourStart),  // mis. '06:00:00'
      hourEnd:   normalizeTime(b.hourEnd),    // mis. '14:00:00'
    };

    console.log('🟢 [Washing] payload to service:', payload);

    const result = await washingProduksiService.createWashingProduksi(payload);

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



async function updateProduksi(req, res) {
  try {
    const noProduksi = req.params.noProduksi; // dari URL
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

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

    // body boleh partial, jadi jangan wajibkan semua
    const payload = {
      tglProduksi: b.tglProduksi,         // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      jamKerja: b.jamKerja,               // ⚠️ PERBEDAAN: jamKerja (bukan jam)
      shift: toInt(b.shift),
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),
      hourStart: b.hourStart || null,
      hourEnd: b.hourEnd || null,
    };

    const result = await washingProduksiService.updateWashingProduksi(noProduksi, payload);

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
 * DELETE /api/production/washing/:noProduksi
 * Hapus header washing production beserta semua input-nya
 */
async function deleteProduksi(req, res) {
  try {
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

    await washingProduksiService.deleteWashingProduksi(noProduksi);

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
    return res
      .status(400)
      .json({ success: false, message: 'noProduksi is required' });
  }

  try {
    const data = await washingProduksiService.fetchInputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[washing.getInputsByNoProduksi]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
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
    const result = await washingProduksiService.validateLabel(labelCode);

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
      data: result.data, // Returns array of all matching records
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

  // Validate that at least one input is provided
  const hasInput = ['bb', 'bbPartialNew', 'washing', 'gilingan', 'gilinganPartialNew']
    .some(key => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message: 'Request body harus berisi minimal satu array input (bb, washing, gilingan, dll) yang tidak kosong'
      }
    });
  }

  try {
    const result = await washingProduksiService.upsertInputsAndPartials(noProduksi, payload);

    const { success, hasWarnings, data } = result;

    // Determine appropriate HTTP status code
    let statusCode = 200;
    let message = 'Inputs & partials processed successfully';

    if (!success) {
      if (data.summary.totalInvalid > 0) {
        statusCode = 422; // Unprocessable Entity - some data is invalid
        message = 'Beberapa data tidak valid';
      } else if (data.summary.totalInserted === 0 && data.summary.totalPartialsCreated === 0) {
        statusCode = 400; // Bad Request - nothing was processed
        message = 'Tidak ada data yang berhasil diproses';
      }
    } else if (hasWarnings) {
      message = 'Inputs & partials processed with warnings';
    }

    return res.status(statusCode).json({
      success,
      message,
      data,
    });
  } catch (e) {
    console.error('[upsertInputsAndPartials - Washing]', e);
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
      error: {
        field: 'noProduksi',
        message: 'Parameter noProduksi tidak boleh kosong'
      }
    });
  }

  const payload = req.body || {};

  // Validate that at least one input is provided
  const hasInput = ['bb', 'washing', 'gilingan', 'bbPartial', 'gilinganPartial']
    .some(key => payload[key] && Array.isArray(payload[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message: 'Request body harus berisi minimal satu array input yang tidak kosong'
      }
    });
  }

  try {
    const result = await washingProduksiService.deleteInputsAndPartials(noProduksi, payload);

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
    });
  } catch (e) {
    console.error('[deleteInputsAndPartials - Washing]', e);
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

module.exports = { getProduksiByDate , getAllProduksi, createProduksi, updateProduksi, deleteProduksi, getInputsByNoProduksi, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials};

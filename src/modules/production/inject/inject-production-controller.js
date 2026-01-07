// controllers/inject-production-controller.js
const injectProduksiService = require('./inject-production-service');

// ------------------------------------------------------------
// helpers (shared)
// ------------------------------------------------------------
const toIntCreate = (v) => {
  // CREATE: missing -> null (so service can validate required fields)
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : Math.trunc(n);
};

const toFloatCreate = (v) => {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
};

const toBitCreate = (v) => {
  if (v === true || v === 1 || v === '1' || v === 'true') return 1;
  if (v === false || v === 0 || v === '0' || v === 'false') return 0;
  return null;
};

const toIntUndef = (v) => {
  // UPDATE: undefined = not sent, null/'' = set null
  if (v === undefined) return undefined;
  if (v === null || v === '') return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : Math.trunc(n);
};

const toFloatUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null || v === '') return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
};

const toBitUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null || v === '') return null;
  if (v === true || v === 1 || v === '1' || v === 'true') return 1;
  if (v === false || v === 0 || v === '0' || v === 'false') return 0;
  return null;
};

const toJamInt = (v) => {
  // Jam column is INT in DB
  if (v === undefined) return undefined; // not sent (for update)
  if (v === null || v === '') return null;

  if (typeof v === 'number') return Number.isFinite(v) ? Math.trunc(v) : null;

  const s = String(v).trim();
  if (!s) return null;

  // "HH:mm" or "HH:mm:ss" -> HH
  const m = s.match(/^(\d{1,2})(?::\d{2})?(?::\d{2})?$/);
  if (m) return parseInt(m[1], 10);

  // "8" -> 8
  const n = parseInt(s, 10);
  return Number.isNaN(n) ? null : n;
};

const toStrUndef = (v) => {
  if (v === undefined) return undefined;
  if (v === null) return null;
  const s = String(v);
  return s;
};

// ------------------------------------------------------------
// ✅ GET ALL (paged)
// ------------------------------------------------------------
async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  const search =
    (typeof req.query.noProduksi === 'string' && req.query.noProduksi) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  try {
    const { data, total } = await injectProduksiService.getAllProduksi(page, pageSize, search);

    return res.status(200).json({
      success: true,
      message: 'InjectProduksi_h retrieved successfully',
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
    console.error('Error fetching InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// 🔹 GET InjectProduksi_h by date (YYYY-MM-DD)
// ------------------------------------------------------------
async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log('🔍 Fetching InjectProduksi_h | Username:', username, '| date:', date);

  try {
    const data = await injectProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No InjectProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `InjectProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// 🔹 GET FurnitureWIP info from InjectProduksi_h by NoProduksi
// ------------------------------------------------------------
async function getFurnitureWipByNoProduksi(req, res) {
  const { username } = req;
  const { noProduksi } = req.params;

  console.log(
    '🔍 Fetching FurnitureWIP from InjectProduksi_h | Username:',
    username,
    '| NoProduksi:',
    noProduksi
  );

  try {
    const rows = await injectProduksiService.getFurnitureWipListByNoProduksi(noProduksi);

    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No InjectProduksi_h / mapping FurnitureWIP found for NoProduksi ${noProduksi}`,
        data: { beratProdukHasilTimbang: null, items: [] },
        meta: { noProduksi },
      });
    }

    const beratProdukHasilTimbang = rows[0].BeratProdukHasilTimbang ?? null;

    const items = rows.map((r) => ({
      IdFurnitureWIP: r.IdFurnitureWIP,
      NamaFurnitureWIP: r.NamaFurnitureWIP,
    }));

    return res.status(200).json({
      success: true,
      message: `FurnitureWIP for NoProduksi ${noProduksi} retrieved successfully`,
      data: { beratProdukHasilTimbang, items },
      meta: { noProduksi },
    });
  } catch (error) {
    console.error('Error fetching FurnitureWIP from InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// 🔹 GET BarangJadi info (Packing) from InjectProduksi_h by NoProduksi
// ------------------------------------------------------------
async function getPackingByNoProduksi(req, res) {
  const { username } = req;
  const { noProduksi } = req.params;

  console.log(
    '🔍 Fetching BarangJadi (Packing) from InjectProduksi_h | Username:',
    username,
    '| NoProduksi:',
    noProduksi
  );

  try {
    const rows = await injectProduksiService.getPackingListByNoProduksi(noProduksi);

    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No InjectProduksi_h / mapping Produk (BarangJadi) found for NoProduksi ${noProduksi}`,
        data: { beratProdukHasilTimbang: null, items: [] },
        meta: { noProduksi },
      });
    }

    const beratProdukHasilTimbang = rows[0].BeratProdukHasilTimbang ?? null;

    const items = rows.map((r) => ({
      IdBJ: r.IdBJ,
      NamaBJ: r.NamaBJ,
    }));

    return res.status(200).json({
      success: true,
      message: `BarangJadi (Packing) for NoProduksi ${noProduksi} retrieved successfully`,
      data: { beratProdukHasilTimbang, items },
      meta: { noProduksi },
    });
  } catch (error) {
    console.error('Error fetching BarangJadi (Packing) from InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// ✅ POST Create InjectProduksi_h
// ------------------------------------------------------------
async function createProduksi(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const b = req.body || {};

    const payload = {
      tglProduksi: b.tglProduksi, // 'YYYY-MM-DD'
      idMesin: toIntCreate(b.idMesin),
      idOperator: toIntCreate(b.idOperator),
      shift: toIntCreate(b.shift),

      // Jam = INT (accept "08:00" -> 8)
      jam: toJamInt(b.jam) ?? null,

      jmlhAnggota: toIntCreate(b.jmlhAnggota),
      hadir: toIntCreate(b.hadir),

      idCetakan: toIntCreate(b.idCetakan),
      idWarna: toIntCreate(b.idWarna),

      enableOffset: toBitCreate(b.enableOffset) ?? 0,
      offsetCurrent: toIntCreate(b.offsetCurrent),
      offsetNext: toIntCreate(b.offsetNext),

      idFurnitureMaterial: toIntCreate(b.idFurnitureMaterial),

      hourMeter: toFloatCreate(b.hourMeter),
      beratProdukHasilTimbang: toFloatCreate(b.beratProdukHasilTimbang),

      hourStart: b.hourStart || null, // 'HH:mm' / 'HH:mm:ss'
      hourEnd: b.hourEnd || null,     // 'HH:mm' / 'HH:mm:ss'

      createBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
    };

    const result = await injectProduksiService.createInjectProduksi(payload);

    return res.status(201).json({
      success: true,
      message: 'InjectProduksi_h created',
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

// ------------------------------------------------------------
// ✅ PUT Update InjectProduksi_h (dynamic fields)
// ------------------------------------------------------------
async function updateProduksi(req, res) {
  try {
    const username = req.username || req.user?.username || 'system';
    const noProduksi = String(req.params.noProduksi || '').trim();

    if (!noProduksi) {
      return res.status(400).json({ success: false, message: 'noProduksi wajib' });
    }

    const b = req.body || {};

    // NOTE:
    // - undefined => field not sent (do not update)
    // - null      => sent null (set DB to NULL)
    const payload = {
      tglProduksi: b.tglProduksi, // undefined or 'YYYY-MM-DD' or null (if you allow null, service should reject)

      idMesin: toIntUndef(b.idMesin),
      idOperator: toIntUndef(b.idOperator),
      shift: toIntUndef(b.shift),

      jam: toJamInt(b.jam),

      jmlhAnggota: toIntUndef(b.jmlhAnggota),
      hadir: toIntUndef(b.hadir),

      idCetakan: toIntUndef(b.idCetakan),
      idWarna: toIntUndef(b.idWarna),

      enableOffset: toBitUndef(b.enableOffset),
      offsetCurrent: toIntUndef(b.offsetCurrent),
      offsetNext: toIntUndef(b.offsetNext),

      idFurnitureMaterial: toIntUndef(b.idFurnitureMaterial),

      hourMeter: toFloatUndef(b.hourMeter),
      beratProdukHasilTimbang: toFloatUndef(b.beratProdukHasilTimbang),

      hourStart: toStrUndef(b.hourStart),
      hourEnd: toStrUndef(b.hourEnd),

      updateBy: username,
      checkBy1: toStrUndef(b.checkBy1),
      checkBy2: toStrUndef(b.checkBy2),
      approveBy: toStrUndef(b.approveBy),
    };

    const result = await injectProduksiService.updateInjectProduksi(noProduksi, payload);

    return res.status(200).json({
      success: true,
      message: 'InjectProduksi_h updated',
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

// ------------------------------------------------------------
// ✅ DELETE InjectProduksi_h
// ------------------------------------------------------------
async function deleteProduksi(req, res) {
  try {
    const noProduksi = String(req.params.noProduksi || '').trim();
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

    await injectProduksiService.deleteInjectProduksi(noProduksi);

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
    const data = await injectProduksiService.fetchInputs(noProduksi);
    return res.status(200).json({ success: true, message: 'Inputs retrieved', data });
  } catch (e) {
    console.error('[inject.getInputsByNoProduksi]', e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || 'Internal Server Error',
    });
  }
}


async function validateLabel(req, res) {
  const labelCode = String(req.params.labelCode || '').trim();

  if (!labelCode) {
    return res.status(400).json({
      success: false,
      message: 'labelCode is required',
    });
  }

  try {
    const result = await injectProduksiService.validateLabel(labelCode);

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
  } catch (e) {
    console.error('[inject.validateLabel]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
  }
}


async function upsertInputs(req, res) {
  const noProduksi = String(req.params.noProduksi || '').trim();
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' },
    });
  }

  const payload = req.body || {};

  // ✅ inject inputs keys (tanpa cabinet material)
const hasInput = [
  'broker', 'mixer', 'gilingan', 'furnitureWip', 'cabinetMaterial',
  'brokerPartial', 'mixerPartial', 'gilinganPartial', 'furnitureWipPartial',
  'brokerPartialNew', 'mixerPartialNew', 'gilinganPartialNew', 'furnitureWipPartialNew',
].some(k => Array.isArray(payload?.[k]) && payload[k].length > 0);


  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: {
        message:
          'Request body harus berisi minimal satu array input yang tidak kosong (furnitureWip, broker, mixer, gilingan, atau partialNew)',
      },
    });
  }

  try {
    const result = await injectProduksiService.upsertInputsAndPartials(noProduksi, payload);
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
    console.error('[inject.upsertInputs]', e);
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
  const noProduksi = String(req.params.noProduksi || '').trim();

  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: 'noProduksi is required',
      error: { field: 'noProduksi', message: 'Parameter noProduksi tidak boleh kosong' },
    });
  }

  const payload = req.body || {};

  const hasInput = [
    'broker',
    'mixer',
    'gilingan',
    'furnitureWip',
    'cabinetMaterial',
    'brokerPartial',
    'mixerPartial',
    'gilinganPartial',
    'furnitureWipPartial',
  ].some((k) => Array.isArray(payload?.[k]) && payload[k].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: 'Tidak ada data input yang diberikan',
      error: { message: 'Request body harus berisi minimal satu array input yang tidak kosong' },
    });
  }

  try {
    const result = await injectProduksiService.deleteInputsAndPartials(noProduksi, payload);

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
    console.error('[inject.deleteInputsAndPartials]', e);
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
  getAllProduksi,
  getProduksiByDate,
  getFurnitureWipByNoProduksi,
  getPackingByNoProduksi,
  createProduksi,
  updateProduksi,
  deleteProduksi,
  getInputsByNoProduksi,
  validateLabel,
  upsertInputs,
  deleteInputsAndPartials
};

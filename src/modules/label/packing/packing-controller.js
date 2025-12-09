// routes/labels/packing-controller.js
const service = require('./packing-service'); 
// ⬆️ sesuaikan path dengan struktur project-mu

// GET /labels/packing?page=&limit=&search=
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await service.getAll({ page, limit, search });
    const totalPages = Math.max(Math.ceil(total / limit), 1);

    res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Packing List Error:', err);
    res
      .status(500)
      .json({ success: false, message: 'Terjadi kesalahan server' });
  }
};



/**
 * Expected body:
 * {
 *   "header": {
 *     "IdBJ": 1,                  // required
 *     "Pcs": 10,                  // optional
 *     "Berat": 25.5,              // optional
 *     "DateCreate": "2025-10-28", // optional (default GETDATE() on server)
 *     "Jam": "08:00",             // optional
 *     "IsPartial": 0,             // optional (default 0)
 *     "IdWarehouse": 3,           // optional
 *     "Blok": "A",                // optional
 *     "IdLokasi": "A1"            // optional
 *     // "CreateBy": "user"       // optional, default from token if available
 *   },
 *   "outputCode": "BD.0000001234"   // required: prefix-based source label
 *                                   // BD.=Packing, S.=Inject, BG.=Bongkar Susun, L.=Retur
 * }
 */
exports.create = async (req, res) => {
  try {
    const payload = req.body || {};

    // Otomatis isi CreateBy dari token kalau belum ada
    if (!payload?.header?.CreateBy && req.username) {
      payload.header = { ...(payload.header || {}), CreateBy: req.username };
    }

    const result = await service.createPacking(payload);

    const headers = Array.isArray(result?.headers) ? result.headers : [];
    const count =
      (result?.output && typeof result.output.count === 'number')
        ? result.output.count
        : (headers.length || 1);

    const msg =
      count > 1
        ? `${count} Packing / BarangJadi labels created successfully`
        : 'Packing / BarangJadi created successfully';

    return res.status(201).json({
      success: true,
      message: msg,
      data: result,
    });
  } catch (err) {
    console.error('Create Packing Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};



/**
 * PUT /labels/packing/:noBJ
 *
 * Expected body (contoh):
 * {
 *   "header": {
 *     "IdBJ": 1,                  // required
 *     "Pcs": 12,
 *     "Berat": 27.5,
 *     "DateCreate": "2025-12-05",
 *     "Jam": "09:00",
 *     "IsPartial": 0,
 *     "IdWarehouse": 3,
 *     "Blok": "A",
 *     "IdLokasi": "1"
 *   },
 *   "outputCode": "BD.0000001234"   // optional; jika diisi → ganti mapping
 * }
 */
exports.update = async (req, res) => {
  try {
    const noBJ = (req.params.noBJ || '').trim();
    if (!noBJ) {
      return res.status(400).json({
        success: false,
        message: 'NoBJ is required in URL parameter',
      });
    }

    const payload = req.body || {};

    // Auto isi CreateBy kalau mau kamu simpan user yang edit (opsional)
    if (!payload?.header?.CreateBy && req.username) {
      payload.header = { ...(payload.header || {}), CreateBy: req.username };
    }

    const result = await service.updatePacking(noBJ, payload);

    return res.status(200).json({
      success: true,
      message: 'Packing / BarangJadi updated successfully',
      data: result,
    });
  } catch (err) {
    console.error('Update Packing Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};


/**
 * DELETE /labels/packing/:noBJ
 */
exports.remove = async (req, res) => {
  try {
    const noBJ = (req.params.noBJ || '').trim();
    if (!noBJ) {
      return res.status(400).json({
        success: false,
        message: 'NoBJ is required in URL parameter',
      });
    }

    const result = await service.deletePacking(noBJ);

    return res.status(200).json({
      success: true,
      message: 'Packing / BarangJadi deleted successfully',
      data: result,
    });
  } catch (err) {
    console.error('Delete Packing Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};


// GET /labels/packing/partials/:nobj
exports.getPackingPartialInfo = async (req, res) => {
  const { nobj } = req.params;

  try {
    if (!nobj) {
      return res.status(400).json({
        success: false,
        message: 'NoBJ is required.',
      });
    }

    const data = await service.getPartialInfoByBJ(nobj);

    if (!data.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No partial data for NoBJ ${nobj}`,
        totalRows: 0,
        totalPartialPcs: 0,
        data: [],
        meta: { NoBJ: nobj },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Packing partial info retrieved successfully',
      totalRows: data.rows.length,
      totalPartialPcs: data.totalPartialPcs,
      data: data.rows,
      meta: { NoBJ: nobj },
    });
  } catch (err) {
    console.error('Get Packing Partial Info Error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
};

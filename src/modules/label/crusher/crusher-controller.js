const service = require('./crusher-service');

exports.getAll = async (req, res) => {
  try {
    const page   = parseInt(req.query.page, 10)  || 1;
    const limit  = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await service.getAll({ page, limit, search });
    const totalPages = Math.max(Math.ceil(total / limit), 1);

    res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Crusher List Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};



/**
 * Expected body:
 * {
 *   "header": {
 *     "IdCrusher": 1,           // required
 *     "IdWarehouse": 5,         // required
 *     "DateCreate": "2025-10-28", // optional (default GETDATE() on server)
 *     "Berat": 25.5,            // optional
 *     "IdStatus": 1,            // optional (default 1)
 *     "Blok": "A",              // optional
 *     "IdLokasi": "A1"          // optional
 *     // "CreateBy": "user"     // optional; will use token username if missing
 *   },
 *   "ProcessedCode": "G.00001234" | "BG.00001234" // optional
 * }
 */
exports.create = async (req, res) => {
  try {
    const payload = req.body || {};

    // Fill CreateBy from token if not provided
    if (!payload?.header?.CreateBy && req.username) {
      payload.header = { ...(payload.header || {}), CreateBy: req.username };
    }

    const result = await service.createCrusherCascade(payload);

    return res.status(201).json({
      success: true,
      message: 'Crusher created successfully',
      data: result,
    });
  } catch (err) {
    console.error('Create Crusher Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};



exports.update = async (req, res) => {
  try {
    const { noCrusher } = req.params;
    if (!noCrusher) {
      return res.status(400).json({ success: false, message: 'noCrusher parameter is required' });
    }

    const body = req.body || {};
    const result = await service.updateCrusher(noCrusher, body);

    return res.status(200).json({
      success: true,
      message: 'Crusher updated successfully',
      data: {
        noCrusher,
        updatedFields: result.updatedFields,
      },
    });
  } catch (err) {
    console.error('Update Crusher Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};



exports.delete = async (req, res) => {
  try {
    const { noCrusher } = req.params;

    if (!noCrusher) {
      return res.status(400).json({
        success: false,
        message: 'noCrusher parameter is required',
      });
    }

    const result = await service.deleteCrusherCascade(noCrusher);

    return res.status(200).json({
      success: true,
      message: 'Crusher deleted successfully',
      data: result,
    });
  } catch (err) {
    console.error('Delete Crusher Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};
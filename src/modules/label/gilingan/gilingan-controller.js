// routes/labels/gilingan-controller.js
const service = require('./gilingan-service');

// GET /labels/gilingan?page=&limit=&search=
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
    console.error('Get Gilingan List Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};




/**
 * Expected body:
 * {
 *   "header": {
 *     "IdGilingan": 1,              // required
 *     "DateCreate": "2025-10-28",   // optional (default GETDATE() on server)
 *     "Berat": 25.5,                // optional
 *     "IsPartial": 0,               // optional (default 0)
 *     "IdStatus": 1,                // optional (default 1)
 *     "Blok": "A",                  // optional
 *     "IdLokasi": "A1"              // optional
 *     // "CreateBy": "user"         // future: if column exists
 *   },
 *   // "outputCode": "W.0000004133" | "BG.0000004133"   // optional
 * }
 */
exports.create = async (req, res) => {
  try {
    const payload = req.body || {};

    // If later you add CreateBy column to Gilingan,
    // you can auto-fill from token here (similar to Crusher)
    // if (!payload?.header?.CreateBy && req.username) {
    //   payload.header = { ...(payload.header || {}), CreateBy: req.username };
    // }

    const result = await service.createGilingan(payload);

    return res.status(201).json({
      success: true,
      message: 'Gilingan created successfully',
      data: result,
    });
  } catch (err) {
    console.error('Create Gilingan Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};



exports.update = async (req, res) => {
  try {
    const { noGilingan } = req.params;
    if (!noGilingan) {
      return res.status(400).json({
        success: false,
        message: 'noGilingan parameter is required',
      });
    }

    const payload = req.body || {};

    // Accept both { header: {...} } and plain {...}
    const header = payload.header || payload;

    const result = await service.updateGilingan(noGilingan, header);

    return res.status(200).json({
      success: true,
      message: 'Gilingan updated successfully',
      data: {
        noGilingan,
        updatedFields: result.updatedFields,
      },
    });
  } catch (err) {
    console.error('Update Gilingan Error:', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};
  

// routes/labels/gilingan-controller.js

exports.delete = async (req, res) => {
    try {
      const { noGilingan } = req.params;
  
      if (!noGilingan) {
        return res.status(400).json({
          success: false,
          message: 'noGilingan parameter is required',
        });
      }
  
      const result = await service.deleteGilinganCascade(noGilingan);
  
      return res.status(200).json({
        success: true,
        message: 'Gilingan deleted successfully',
        data: result,
      });
    } catch (err) {
      console.error('Delete Gilingan Error:', err);
      return res.status(err.statusCode || 500).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };
  


  exports.getGilinganPartialInfo = async (req, res) => {
    const { nogilingan } = req.params;
  
    try {
      if (!nogilingan) {
        return res.status(400).json({
          success: false,
          message: 'NoGilingan is required.',
        });
      }
  
      const data = await service.getPartialInfoByGilingan(nogilingan);
  
      if (!data.rows || data.rows.length === 0) {
        return res.status(200).json({
          success: true,
          message: `No partial data for NoGilingan ${nogilingan}`,
          totalRows: 0,
          totalPartialWeight: 0,
          data: [],
          meta: { nogilingan },
        });
      }
  
      return res.status(200).json({
        success: true,
        message: 'Gilingan partial info retrieved successfully',
        totalRows: data.rows.length,
        totalPartialWeight: data.totalPartialWeight,
        data: data.rows,
        meta: { nogilingan },
      });
    } catch (err) {
      console.error('Get Gilingan Partial Info Error:', err);
      return res.status(500).json({
        success: false,
        message: 'Internal Server Error',
        error: err.message,
      });
    }
  };
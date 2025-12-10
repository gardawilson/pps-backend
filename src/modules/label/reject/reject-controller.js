// routes/labels/reject-controller.js
const service = require('./reject-service');

// GET /labels/reject?page=&limit=&search=
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
    console.error('Get Reject List Error:', err);
    res
      .status(500)
      .json({ success: false, message: 'Terjadi kesalahan server' });
  }
};



/**
 * Expected body:
 * {
 *   "header": {
 *     "IdReject": 1,               // required
 *     "Berat": 25.5,               // optional
 *     "DateCreate": "2025-10-28",  // optional (default GETDATE() on server)
 *     "Jam": "08:00",              // optional
 *     "IsPartial": 0,              // optional (default 0)
 *     "IdWarehouse": 3,            // optional
 *     "Blok": "A",                 // optional
 *     "IdLokasi": "A1"             // optional
 *     // "CreateBy": "user"        // optional, default dari token kalau ada
 *   },
 *   "outputCode": "S.0000001234"   // required: prefix-based source label
 *                                  // S.=Inject, BH.=HotStamping, BI.=PasangKunci,
 *                                  // BJ.=Spanner, J.=BJSortir
 * }
 */
exports.create = async (req, res) => {
    try {
      const payload = req.body || {};
  
      // Otomatis isi CreateBy dari token kalau belum ada
      if (!payload?.header?.CreateBy && req.username) {
        payload.header = { ...(payload.header || {}), CreateBy: req.username };
      }
  
      const result = await service.createReject(payload);
  
      const headers = Array.isArray(result?.headers) ? result.headers : [];
      const count =
        (result?.output && typeof result.output.count === 'number')
          ? result.output.count
          : (headers.length || 1);
  
      const msg =
        count > 1
          ? `${count} Reject labels created successfully`
          : 'Reject created successfully';
  
      return res.status(201).json({
        success: true,
        message: msg,
        data: result,
      });
    } catch (err) {
      console.error('Create Reject Error:', err);
      return res.status(err.statusCode || 500).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };



  /**
 * PUT /labels/reject/:noReject
 *
 * Body (semua optional, kecuali kalau mau ganti outputCode ya harus isi outputCode):
 * {
 *   "header": {
 *     "IdReject": 2,
 *     "Berat": 12.5,
 *     "DateCreate": "2025-12-10",
 *     "Jam": "09:00",
 *     "IsPartial": 1,
 *     "IdWarehouse": 4,
 *     "Blok": "B",
 *     "IdLokasi": "B1"
 *   },
 *   "outputCode": "BH.0000001234"   // optional: kalau diisi → ganti mapping sumber
 * }
 */
exports.update = async (req, res) => {
    try {
      const noReject = req.params.noReject;
      if (!noReject) {
        return res
          .status(400)
          .json({ success: false, message: 'noReject is required in route param' });
      }
  
      const payload = req.body || {};
  
      const result = await service.updateReject(noReject, payload);
  
      return res.status(200).json({
        success: true,
        message: 'Reject updated successfully',
        data: result,
      });
    } catch (err) {
      console.error('Update Reject Error:', err);
      return res.status(err.statusCode || 500).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };

  // DELETE /labels/reject/:noReject
exports.delete = async (req, res) => {
    try {
      const noReject = req.params.noReject;
      if (!noReject) {
        return res
          .status(400)
          .json({ success: false, message: 'noReject is required in route param' });
      }
  
      const result = await service.deleteReject(noReject);
  
      return res.status(200).json({
        success: true,
        message: `Reject ${noReject} deleted successfully`,
        data: result,
      });
    } catch (err) {
      console.error('Delete Reject Error:', err);
      return res.status(err.statusCode || 500).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };



  
// GET /labels/reject/partials/:noreject
exports.getRejectPartialInfo = async (req, res) => {
    const { noreject } = req.params;
  
    try {
      if (!noreject) {
        return res.status(400).json({
          success: false,
          message: 'NoReject is required.',
        });
      }
  
      const data = await service.getPartialInfoByReject(noreject);
  
      if (!data.rows || data.rows.length === 0) {
        return res.status(200).json({
          success: true,
          message: `No partial data for NoReject ${noreject}`,
          totalRows: 0,
          totalPartialBerat: 0,
          data: [],
          meta: { NoReject: noreject },
        });
      }
  
      return res.status(200).json({
        success: true,
        message: 'Reject partial info retrieved successfully',
        totalRows: data.rows.length,
        totalPartialBerat: data.totalPartialBerat,
        data: data.rows,
        meta: { NoReject: noreject },
      });
    } catch (err) {
      console.error('Get Reject Partial Info Error:', err);
      return res.status(500).json({
        success: false,
        message: 'Internal Server Error',
        error: err.message,
      });
    }
  };
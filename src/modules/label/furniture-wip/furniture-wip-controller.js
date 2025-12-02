// routes/labels/furniture-wip-controller.js
const service = require('./furniture-wip-service'); 
// ⬆️ sesuaikan path dengan struktur project-mu
// kalau sama seperti gilingan, mungkin: require('./furniture-wip-service');

// GET /labels/furniture-wip?page=&limit=&search=
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
    console.error('Get Furniture WIP List Error:', err);
    res
      .status(500)
      .json({ success: false, message: 'Terjadi kesalahan server' });
  }
};


/**
 * Expected body:
 * {
 *   "header": {
 *     "IdFurnitureWIP": 1,          // required
 *     "Pcs": 10,                    // optional
 *     "Berat": 25.5,                // optional
 *     "DateCreate": "2025-10-28",   // optional (default GETDATE() on server)
 *     "IsPartial": 0,               // optional (default 0)
 *     "IdWarna": 1,                 // optional
 *     "Blok": "A",                  // optional
 *     "IdLokasi": "A1"              // optional
 *     // "CreateBy": "user"         // optional, will default from token if available
 *   },
 *   "outputCode": "BH.0000001234"   // required: prefix-based source label
 * }
 */
exports.create = async (req, res) => {
    try {
      const payload = req.body || {};
  
      // Otomatis isi CreateBy dari token kalau belum ada
      if (!payload?.header?.CreateBy && req.username) {
        payload.header = { ...(payload.header || {}), CreateBy: req.username };
      }
  
      const result = await service.createFurnitureWip(payload);
  
      return res.status(201).json({
        success: true,
        message: 'Furniture WIP created successfully',
        data: result,
      });
    } catch (err) {
      console.error('Create Furniture WIP Error:', err);
      return res.status(err.statusCode || 500).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };




  
/**
 * PUT /labels/furniture-wip/:noFurnitureWip
 *
 * Body (partial update allowed):
 * {
 *   "header": {
 *     "IdFurnitureWIP": 1,   // optional
 *     "Pcs": 10,             // optional
 *     "Berat": 25.5,         // optional
 *     "IsPartial": 0,        // optional
 *     "IdWarna": 3,          // optional
 *     "Blok": "A1",          // optional
 *     "IdLokasi": "R01",     // optional
 *     "DateCreate": "2025-12-01", // optional
 *     "CreateBy": "ganda"    // optional
 *   },
 *   // "outputCode": "BH.0000001234"  // optional
 * }
 *
 * - If "outputCode" is NOT sent at all -> mapping tidak diubah.
 * - If "outputCode": "" (string kosong) -> mapping dihapus.
 * - If "outputCode": "BH.****" / "BI.****" / "BG.****" / "L.****" -> mapping diganti.
 */
exports.update = async (req, res) => {
    try {
      const noFurnitureWip = req.params.noFurnitureWip;
      if (!noFurnitureWip) {
        return res.status(400).json({
          success: false,
          message: 'noFurnitureWip is required in URL',
        });
      }
  
      const payload = req.body || {};
  
      // auto-fill CreateBy jika mau (optional)
      if (!payload?.header?.CreateBy && req.username) {
        payload.header = { ...(payload.header || {}), CreateBy: req.username };
      }
  
      const result = await service.updateFurnitureWip(noFurnitureWip, payload);
  
      return res.status(200).json({
        success: true,
        message: 'Furniture WIP updated successfully',
        data: result,
      });
    } catch (err) {
      console.error('Update Furniture WIP Error:', err);
      return res.status(err.statusCode || 500).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };



  // DELETE /labels/furniture-wip/:noFurnitureWip
exports.delete = async (req, res) => {
    const { noFurnitureWip } = req.params;
  
    if (!noFurnitureWip) {
      return res.status(400).json({
        success: false,
        message: 'NoFurnitureWIP is required in URL',
      });
    }
  
    try {
      const result = await service.deleteFurnitureWip(noFurnitureWip);
  
      return res.status(200).json({
        success: true,
        message: 'Furniture WIP deleted successfully',
        data: result, // { noFurnitureWip, deleted: true }
      });
    } catch (err) {
      console.error('Delete Furniture WIP Error:', err);
  
      if (err.statusCode === 404) {
        return res.status(404).json({
          success: false,
          message: err.message || 'Furniture WIP not found',
        });
      }
  
      return res.status(500).json({
        success: false,
        message: 'Internal Server Error',
      });
    }
  };


  // GET /labels/furniture-wip/partials/:nofurniturewip
exports.getFurnitureWipPartialInfo = async (req, res) => {
    const { nofurniturewip } = req.params;
  
    try {
      if (!nofurniturewip) {
        return res.status(400).json({
          success: false,
          message: 'NoFurnitureWIP is required.',
        });
      }
  
      const data = await service.getPartialInfoByFurnitureWip(nofurniturewip);
  
      if (!data.rows || data.rows.length === 0) {
        return res.status(200).json({
          success: true,
          message: `No partial data for NoFurnitureWIP ${nofurniturewip}`,
          totalRows: 0,
          totalPartialPcs: 0,
          data: [],
          meta: { noFurnitureWIP: nofurniturewip },
        });
      }
  
      return res.status(200).json({
        success: true,
        message: 'FurnitureWIP partial info retrieved successfully',
        totalRows: data.rows.length,
        totalPartialPcs: data.totalPartialPcs,
        data: data.rows,
        meta: { noFurnitureWIP: nofurniturewip },
      });
    } catch (err) {
      console.error('Get FurnitureWIP Partial Info Error:', err);
      return res.status(500).json({
        success: false,
        message: 'Internal Server Error',
        error: err.message,
      });
    }
  };
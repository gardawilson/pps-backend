// controllers/broker-controller.js

const brokerService = require('./broker-service');

exports.getAll = async (req, res) => {
    try {
      const page = parseInt(req.query.page, 10) || 1;
      const limit = parseInt(req.query.limit, 10) || 20;
      const search = (req.query.search || '').trim();
  
      const { data, total } = await brokerService.getAll({ page, limit, search });
      const totalPages = Math.ceil(total / limit);
  
      res.status(200).json({
        success: true,
        data,
        meta: { page, limit, total, totalPages },
      });
    } catch (err) {
      console.error('Get Broker List Error:', err);
      res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
    }
  };
  

  exports.getOne = async (req, res) => {
    const { nobroker } = req.params;
    try {
      const details = await brokerService.getBrokerDetailByNoBroker(nobroker);
  
      if (!details || details.length === 0) {
        return res.status(404).json({
          success: false,
          message: `Data tidak ditemukan untuk NoBroker ${nobroker}`
        });
      }
  
      return res.status(200).json({ success: true, data: { nobroker, details } });
    } catch (err) {
      console.error('Get Broker_d Error:', err);
      return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
    }
  };


  /**
 * Expected body:
 * {
 *   "header": {
 *     // NoBroker optional (we auto-generate, like washing)
 *     "IdJenisPlastik": 1,           // required
 *     "IdWarehouse": 2,              // required
 *     "DateCreate": "2025-10-15",    // optional (default GETDATE())
 *     "IdStatus": 1,                 // optional (default 1=PASS)
 *     "CreateBy": "x",           // required (we can take from token)
 *     "Density": 0.91, "Moisture": 0.3, "MaxMeltTemp": null, ...
 *     "Blok": "A", "IdLokasi": "A1"  // optional
 *   },
 *   "details": [
 *     { "NoSak": 1, "Berat": 25.6, "IdLokasi": "A1", "IsPartial": 0 },
 *     { "NoSak": 2, "Berat": 26.0, "IdLokasi": "A1", "IsPartial": 0 }
 *   ],
 *   // Conditional (mutually exclusive):
 *   "NoProduksi": "BR.00001234",     // either this
 *   // or
 *   "NoBongkarSusun": "BKS.0000456"  // or this (not both)
 * }
 */
exports.create = async (req, res) => {
  try {
    const payload = req.body;

    // Optional: set CreateBy from token if not provided
    if (!payload?.header?.CreateBy && req.username) {
      payload.header = { ...(payload.header || {}), CreateBy: req.username };
    }

    const result = await brokerService.createBrokerCascade(payload);

    return res.status(201).json({
      success: true,
      message: 'Broker created successfully',
      data: result
    });
  } catch (err) {
    console.error('Create Broker Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Server Error'
    });
  }
};


exports.update = async (req, res) => {
  const { nobroker } = req.params;
  try {
    /**
     * Expected body (like POST, but NoBroker from path):
     * {
     *   "header": {
     *     "IdJenisPlastik": 1,
     *     "IdWarehouse": 2,
     *     "DateCreate": "2025-10-15", // optional (null -> GETDATE())
     *     "IdStatus": 1,
     *     "Density": 0.91, "Moisture": 0.3,
     *     "MaxMeltTemp": null, "MinMeltTemp": null, "MFI": null, "VisualNote": "..."
     *     "Blok": "A", "IdLokasi": "A1"
     *   },
     *   "details": [
     *     { "NoSak": 1, "Berat": 25.6, "IdLokasi": "A1", "IsPartial": 0 },
     *     { "NoSak": 2, "Berat": 26.0, "IdLokasi": "A1", "IsPartial": 1 }
     *   ], // if sent: REPLACE all details with DateUsage IS NULL
     *
     *   // Conditional outputs (mutually exclusive, optional):
     *   "NoProduksi": "D.0000000123",
     *   "NoBongkarSusun": null
     * }
     */
    const payload = { ...req.body, NoBroker: nobroker };

    // Optional: audit
    if (req.username) {
      payload.UpdateBy = req.username;
    }

    const result = await brokerService.updateBrokerCascade(payload);

    return res.status(200).json({
      success: true,
      message: 'Broker updated successfully',
      data: result
    });
  } catch (err) {
    console.error('Update Broker Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Server Error'
    });
  }
};


exports.remove = async (req, res) => {
  const { nobroker } = req.params;
  try {
    const result = await brokerService.deleteBrokerCascade(nobroker);

    return res.status(200).json({
      success: true,
      message: `Broker ${nobroker} deleted successfully`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Broker Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
};


exports.getPartialInfo = async (req, res) => {
  const { nobroker, nosak } = req.params;

  try {
    if (!nobroker || !nosak) {
      return res.status(400).json({
        success: false,
        message: 'NoBroker and NoSak are required.',
      });
    }

    const data = await brokerService.getPartialInfoByBrokerAndSak(nobroker, Number(nosak));

    if (!data.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No partial data for NoBroker ${nobroker} / NoSak ${nosak}`,
        totalRows: 0,
        totalPartialWeight: 0,
        data: [],
        meta: { nobroker, nosak: Number(nosak) },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Partial info retrieved successfully',
      totalRows: data.rows.length,
      totalPartialWeight: data.totalPartialWeight,
      data: data.rows, // array of partial rows with produksi info
      meta: { nobroker, nosak: Number(nosak) },
    });
  } catch (err) {
    console.error('Get Partial Info Error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
};
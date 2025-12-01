// controllers/mixer-controller.js

const mixerService = require('./mixer-service');

exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await mixerService.getAll({ page, limit, search });
    const totalPages = Math.ceil(total / limit);

    res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Mixer List Error:', err);
    res.status(500).json({
      success: false,
      message: 'Terjadi kesalahan server',
    });
  }
};


// GET detail sak per NoMixer (mirror broker getOne)
exports.getOne = async (req, res) => {
    const { nomixer } = req.params;
  
    try {
      const details = await mixerService.getMixerDetailByNoMixer(nomixer);
  
      if (!details || details.length === 0) {
        return res.status(404).json({
          success: false,
          message: `Data tidak ditemukan untuk NoMixer ${nomixer}`,
        });
      }
  
      return res.status(200).json({
        success: true,
        data: { nomixer, details },
      });
    } catch (err) {
      console.error('Get Mixer_d Error:', err);
      return res
        .status(500)
        .json({ success: false, message: 'Terjadi kesalahan server' });
    }
  };



  
/**
 * Expected body:
 * {
 *   "header": {
 *     "IdMixer": 1,                 // required
 *     "DateCreate": "2025-11-28",   // optional (default GETDATE())
 *     "IdStatus": 1,                // optional (default 1=PASS)
 *     "CreateBy": "username",       // required (or taken from token)
 *     "Blok": "A",                  // optional
 *     "IdLokasi": "A1"              // optional (used as default for details)
 *   },
 *   "details": [
 *     { "NoSak": 1, "Berat": 25.6, "IdLokasi": "A1", "IsPartial": 0 },
 *     { "NoSak": 2, "Berat": 26.0, "IdLokasi": "A1", "IsPartial": 0 }
 *   ],
 *   "outputCode": "BG.0000000004"   // optional
 *   // - if starts with "BG."  → insert into BongkarSusunOutputMixer
 *   // - if starts with "I."   → insert into MixerProduksiOutput
 * }
 */
exports.create = async (req, res) => {
    try {
      const payload = req.body;
  
      // Optional: set CreateBy from token if not provided
      if (!payload?.header?.CreateBy && req.username) {
        payload.header = { ...(payload.header || {}), CreateBy: req.username };
      }
  
      const result = await mixerService.createMixerCascade(payload);
  
      return res.status(201).json({
        success: true,
        message: 'Mixer created successfully',
        data: result,
      });
    } catch (err) {
      console.error('Create Mixer Error:', err);
      const status = err.statusCode || 500;
      return res.status(status).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };



  /**
 * Expected body (like POST, but NoMixer from path):
 * {
 *   "header": {
 *     "IdMixer": 1,
 *     "DateCreate": "2025-11-28",  // optional (null -> GETDATE())
 *     "IdStatus": 1,
 *     "Moisture": 0.3,
 *     "MaxMeltTemp": null,
 *     "MinMeltTemp": null,
 *     "MFI": null,
 *     "Moisture2": null,
 *     "Moisture3": null,
 *     "Blok": "A",
 *     "IdLokasi": 1               // INT
 *   },
 *   "details": [
 *     { "NoSak": 1, "Berat": 25.6 },
 *     { "NoSak": 2, "Berat": 26.0 }
 *   ], // if sent: REPLACE all details with DateUsage IS NULL
 *
 *   // Optional, single field:
 *   "outputCode": "BG.0000000004"  // or "I.0000000002" or null/""
 *   // - if property is omitted -> outputs untouched
 *   // - if property exists but empty/null -> outputs cleared
 *   // - if "BG.*" -> write BongkarSusunOutputMixer
 *   // - if "I.*"  -> write MixerProduksiOutput
 * }
 */
exports.update = async (req, res) => {
    const { nomixer } = req.params;
  
    try {
      const payload = { ...req.body, NoMixer: nomixer };
  
      // Optional audit
      if (req.username) {
        payload.UpdateBy = req.username;
      }
  
      const result = await mixerService.updateMixerCascade(payload);
  
      return res.status(200).json({
        success: true,
        message: 'Mixer updated successfully',
        data: result,
      });
    } catch (err) {
      console.error('Update Mixer Error:', err);
      const status = err.statusCode || 500;
      return res.status(status).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };



  exports.remove = async (req, res) => {
    const { nomixer } = req.params;
  
    try {
      const result = await mixerService.deleteMixerCascade(nomixer);
  
      return res.status(200).json({
        success: true,
        message: `Mixer ${nomixer} deleted successfully`,
        data: result,
      });
    } catch (err) {
      console.error('Delete Mixer Error:', err);
      const status = err.statusCode || 500;
      return res.status(status).json({
        success: false,
        message: err.message || 'Internal Server Error',
      });
    }
  };


  // GET partial info for one Mixer + NoSak
exports.getPartialInfo = async (req, res) => {
    const { nomixer, nosak } = req.params;
  
    try {
      if (!nomixer || !nosak) {
        return res.status(400).json({
          success: false,
          message: 'NoMixer and NoSak are required.',
        });
      }
  
      const data = await mixerService.getPartialInfoByMixerAndSak(
        nomixer,
        Number(nosak)
      );
  
      if (!data.rows || data.rows.length === 0) {
        return res.status(200).json({
          success: true,
          message: `No partial data for NoMixer ${nomixer} / NoSak ${nosak}`,
          totalRows: 0,
          totalPartialWeight: 0,
          data: [],
          meta: { nomixer, nosak: Number(nosak) },
        });
      }
  
      return res.status(200).json({
        success: true,
        message: 'Partial info retrieved successfully',
        totalRows: data.rows.length,
        totalPartialWeight: data.totalPartialWeight,
        data: data.rows,
        meta: { nomixer, nosak: Number(nosak) },
      });
    } catch (err) {
      console.error('Get Mixer Partial Info Error:', err);
      return res.status(500).json({
        success: false,
        message: 'Internal Server Error',
        error: err.message,
      });
    }
  };
  
const labelWashingService = require('./washing-service');

// GET all header washing
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await labelWashingService.getAll({ page, limit, search });

    const totalPages = Math.ceil(total / limit);

    res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages }
    });
  } catch (err) {
    console.error('Get Washing List Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

// GET one header + details
exports.getOne = async (req, res) => {
  const { nowashing } = req.params;
  try {
    const details = await labelWashingService.getWashingDetailByNoWashing(nowashing);

    if (!details || details.length === 0) {
      return res.status(404).json({
        success: false,
        message: `Data tidak ditemukan untuk NoWashing ${nowashing}`
      });
    }

    res.status(200).json({ success: true, data: { nowashing, details } });
  } catch (err) {
    console.error('Get Washing_d Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};


exports.create = async (req, res) => {
  try {
    /**
     * Expected body:
     * {
     *   "header": {
     *     "NoWashing": "W.20251009-0001",   // wajib (kalau mau auto-gen, bisa nanti ditambah helper)
     *     "IdJenisPlastik": 1,              // wajib
     *     "IdWarehouse": 2,                 // wajib
     *     "DateCreate": "2025-10-09",       // opsional (default GETDATE())
     *     "IdStatus": 1,                    // opsional (default 1=PASS/0=HOLD sesuai sistemmu)
     *     "CreateBy": "ganda",              // wajib (ambil dari token juga boleh)
     *     "Density": 0.91, "Moisture": 0.3, "Density2": null, ...,
     *     "Blok": "A", "IdLokasi": "A1"     // opsional
     *   },
     *   "details": [
     *     { "NoSak": 1, "Berat": 25.6, "IdLokasi": "A1" },
     *     { "NoSak": 2, "Berat": 26.0, "IdLokasi": "A1" }
     *   ],
     *   // Conditional output (mutually exclusive):
     *   "NoProduksi": "C.0000002304",       // isi ini ATAU
     *   "NoBongkarSusun": null              // isi ini (bukan dua-duanya)
     * }
     */
    const payload = req.body;

    // optional: set CreateBy dari token
    if (!payload?.header?.CreateBy && req.username) {
      payload.header = { ...(payload.header || {}), CreateBy: req.username };
    }

    const result = await labelWashingService.createWashingCascade(payload);

    res.status(201).json({
      success: true,
      message: 'Washing berhasil dibuat',
      data: result
    });
  } catch (err) {
    console.error('Create Washing Error:', err);
    const status = err.statusCode || 500;
    res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server'
    });
  }
};


exports.update = async (req, res) => {
  const { nowashing } = req.params;
  try {
    /**
     * Expected body (mirip POST, tapi NoWashing ambil dari path):
     * {
     *   "header": {
     *     // kolom2 yang mau di-update (opsional jika tidak berubah)
     *     "IdJenisPlastik": 1,
     *     "IdWarehouse": 2,
     *     "DateCreate": "2025-10-09", // optional
     *     "IdStatus": 1,
     *     "Density": 0.91, "Moisture": 0.3, ...
     *     "Blok": "A", "IdLokasi": "A1"
     *   },
     *   "details": [
     *     { "NoSak": 1, "Berat": 25.6, "IdLokasi": "A1" },
     *     { "NoSak": 2, "Berat": 26.0, "IdLokasi": "A1" }
     *   ], // kalau dikirim: REPLACE semua detail yg DateUsage IS NULL
     *
     *   // Conditional output (mutually exclusive, opsional):
     *   "NoProduksi": "C.0000002304",
     *   "NoBongkarSusun": null
     * }
     */
    const payload = { ...req.body, NoWashing: nowashing };

    // optional: UpdateBy dari token
    if (req.username) {
      payload.UpdateBy = req.username;
    }

    const result = await labelWashingService.updateWashingCascade(payload);

    res.status(200).json({
      success: true,
      message: 'Washing berhasil diupdate',
      data: result
    });
  } catch (err) {
    console.error('Update Washing Error:', err);
    const status = err.statusCode || 500;
    res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server'
    });
  }
};


exports.remove = async (req, res) => {
  const { nowashing } = req.params;
  try {
    const result = await labelWashingService.deleteWashingCascade(nowashing);

    res.status(200).json({
      success: true,
      message: `Washing ${nowashing} berhasil dihapus`,
      data: result
    });
  } catch (err) {
    console.error('Delete Washing Error:', err);
    const status = err.statusCode || 500;
    res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server'
    });
  }
};
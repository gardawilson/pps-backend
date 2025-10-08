const labelWashingService = require('./label-washing-service');

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

// CREATE header washing
exports.createHeader = async (req, res) => {
  const { IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy } = req.body;

  if (!IdJenisPlastik || !IdWarehouse || !DateCreate || !IdStatus || !CreateBy) {
    return res.status(400).json({
      success: false,
      message: 'Field tidak lengkap',
      received: { IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy }
    });
  }

  try {
    const result = await labelWashingService.insertWashingData({ IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy });

    res.status(201).json({
      success: true,
      message: 'Header berhasil dibuat',
      data: result
    });
  } catch (err) {
    console.error('Insert Washing_h Error:', err);
    res.status(500).json({ success: false, message: 'Gagal membuat header', error: err.message });
  }
};

// CREATE details for a washing header
exports.createDetails = async (req, res) => {
  const { nowashing } = req.params;
  const details = req.body;

  if (!Array.isArray(details) || details.length === 0) {
    return res.status(400).json({
      success: false,
      message: 'Data harus berupa array dan tidak boleh kosong'
    });
  }

  try {
    const results = await labelWashingService.insertWashingDetailData(
      details.map(d => ({ ...d, NoWashing: nowashing }))
    );

    res.status(201).json({
      success: true,
      message: 'Detail berhasil ditambahkan',
      data: results
    });
  } catch (err) {
    console.error('Insert Washing_d Error:', err);
    res.status(500).json({ success: false, message: 'Gagal menambah detail', error: err.message });
  }
};

const maxSakService = require('./max-sak-service');

// GET list
exports.getAll = async (req, res) => {
  try {
    const page   = parseInt(req.query.page, 10)  || 1;
    const limit  = parseInt(req.query.limit, 10) || 50;
    const idBagian = req.query.idbagian ? parseInt(req.query.idbagian, 10) : null;

    const { data, total } = await maxSakService.getAll({ page, limit, idBagian });
    const totalPages = Math.ceil(total / limit);

    res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages }
    });
  } catch (err) {
    console.error('Get MstMaxSak Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

// GET one
exports.getOne = async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const item = await maxSakService.getOne(id);
    if (!item) {
      return res.status(404).json({ success: false, message: 'Data tidak ditemukan' });
    }
    res.status(200).json({ success: true, data: item });
  } catch (err) {
    console.error('GetOne MstMaxSak Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

// CREATE
exports.create = async (req, res) => {
  try {
    const payload = req.body; // { IdBagian, JlhSak, DefaultKG }
    const created = await maxSakService.create(payload);
    res.status(201).json({ success: true, message: 'Data berhasil dibuat', data: created });
  } catch (err) {
    console.error('Create MstMaxSak Error:', err);
    res.status(err.statusCode || 500).json({ success: false, message: err.message || 'Terjadi kesalahan server' });
  }
};

// UPDATE
exports.update = async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const payload = req.body; // { JlhSak, DefaultKG }
    const updated = await maxSakService.update(id, payload);
    res.status(200).json({ success: true, message: 'Data berhasil diperbarui', data: updated });
  } catch (err) {
    console.error('Update MstMaxSak Error:', err);
    res.status(err.statusCode || 500).json({ success: false, message: err.message || 'Terjadi kesalahan server' });
  }
};

// DELETE
exports.remove = async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    await maxSakService.remove(id);
    res.status(200).json({ success: true, message: 'Data berhasil dihapus' });
  } catch (err) {
    console.error('Delete MstMaxSak Error:', err);
    res.status(err.statusCode || 500).json({ success: false, message: err.message || 'Terjadi kesalahan server' });
  }
};

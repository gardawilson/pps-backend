// controllers/label-detail-controller.js
const { getDetailByNomorLabel } = require('./label-detail-service');

async function getLabelDetail(req, res) {
  const { nomorLabel } = req.params;

  try {
    const result = await getDetailByNomorLabel(nomorLabel);
    if (!result) {
      return res.status(404).json({ success: false, message: 'Label tidak ditemukan' });
    }

    return res.json({ success: true, data: result });
  } catch (err) {
    console.error('Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
}

module.exports = { getLabelDetail };

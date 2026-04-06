// master-regu-controller.js
const service = require('./master-regu-service');

async function list(req, res) {
  const q = (req.query.q || '').toString().trim();
  const orderBy = (req.query.orderBy || 'NamaRegu').toString();
  const orderDir =
    (req.query.orderDir || 'ASC').toString().toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
  const idBagian = req.query.idBagian || null;

  try {
    const rows = await service.listAll({ q, orderBy, orderDir, idBagian });
    return res.status(200).json({
      success: true,
      message: 'Data MstRegu berhasil diambil',
      totalData: rows.length,
      data: rows,
    });
  } catch (error) {
    console.error('Error listing MstRegu:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { list };

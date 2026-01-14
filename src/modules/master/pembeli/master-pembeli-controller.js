// master-pembeli-controller.js
const service = require('./master-pembeli-service');

async function list(req, res) {
  const includeDisabled = String(req.query.includeDisabled || '0') === '1';
  const q = (req.query.q || '').toString().trim();
  const orderBy = (req.query.orderBy || 'NamaPembeli').toString();
  const orderDir =
    (req.query.orderDir || 'ASC').toString().toUpperCase() === 'DESC' ? 'DESC' : 'ASC';

  try {
    const rows = await service.listAll({ includeDisabled, q, orderBy, orderDir });
    return res.status(200).json({
      success: true,
      message: 'Data MstPembeli berhasil diambil',
      includeDisabled,
      totalData: rows.length,
      data: rows,
    });
  } catch (error) {
    console.error('Error listing MstPembeli (no pagination):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { list };

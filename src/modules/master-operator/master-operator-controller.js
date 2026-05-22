// master-operator-controller.js
const service = require('./master-operator-service');

async function list(req, res) {
  const includeDisabled = String(req.query.includeDisabled || '0') === '1';
  const q = (req.query.q || '').toString().trim(); // still used to filter, not echoed
  const orderBy = (req.query.orderBy || 'NamaOperator').toString(); // used internally
  const orderDir =
    (req.query.orderDir || 'ASC').toString().toUpperCase() === 'DESC' ? 'DESC' : 'ASC';

  try {
    const rows = await service.listAll({ includeDisabled, q, orderBy, orderDir });
    return res.status(200).json({
      success: true,
      message: 'Data MstOperator berhasil diambil',
      includeDisabled,         // ✅ keep this
      totalData: rows.length,  // optional; remove if you don't want it
      data: rows,
    });
  } catch (error) {
    console.error('Error listing MstOperator (no pagination):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function listByIdRegu(req, res) {
  const idregu = (req.params.idregu || '').toString().trim();

  if (!idregu) {
    return res.status(400).json({
      success: false,
      message: 'Parameter idregu wajib diisi',
    });
  }

  try {
    const rows = await service.listByIdRegu(idregu);
    return res.status(200).json({
      success: true,
      message: 'Data operator berdasarkan idregu berhasil diambil',
      idregu,
      totalData: rows.length,
      data: rows,
    });
  } catch (error) {
    console.error('Error listing MstOperator by idregu:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { list, listByIdRegu };

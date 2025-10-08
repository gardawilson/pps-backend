const washingProduksiService = require('./washing-produksi-service');

// GET /api/washing-produksi
async function getAll(req, res) {
  try {
    const page = parseInt(req.query.page || '1', 10);
    const limit = parseInt(req.query.limit || '50', 10);

    const result = await washingProduksiService.getAllProduksi({ page, limit });

    res.json({
      success: true,
      message: 'Data WashingProduksi_h berhasil diambil',
      data: result.data,
      total: result.total
    });
  } catch (error) {
    console.error('Error getAll WashingProduksi:', error);
    res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

// GET /api/washing-produksi/:id
async function getById(req, res) {
  try {
    const { id } = req.params;
    const data = await washingProduksiService.getById(id);

    if (!data) {
      return res.status(404).json({
        success: false,
        message: `WashingProduksi_h dengan NoProduksi ${id} tidak ditemukan`
      });
    }

    res.json({
      success: true,
      message: 'Data WashingProduksi_h berhasil diambil',
      data
    });
  } catch (error) {
    console.error('Error getById WashingProduksi:', error);
    res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getAll, getById };

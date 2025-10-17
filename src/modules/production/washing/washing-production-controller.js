// controllers/production-controller.js
const washingProduksiService = require('./washing-production-service');

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah match regex di route
  console.log("🔍 Fetching WashingProduksi_h | Username:", username, "| date:", date);

  try {
    const data = await washingProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data WashingProduksi_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date }
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data WashingProduksi_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date }
    });
  } catch (error) {
    console.error('Error fetching WashingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getProduksiByDate };

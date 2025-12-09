// controllers/packing-production-controller.js
const packingService = require('./packing-production-service');

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;

  console.log('🔍 Fetching PackingProduksi_h | Username:', username, '| date:', date);

  try {
    const data = await packingService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No PackingProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `PackingProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching PackingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getProduksiByDate };

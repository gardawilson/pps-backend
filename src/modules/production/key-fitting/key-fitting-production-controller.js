// controllers/key-fitting-production-controller.js
const keyFittingService = require('./key-fitting-production-service');

async function getProductionByDate(req, res) {
  const { username } = req;
  const date = req.params.date;

  console.log('🔍 Fetching PasangKunci_h (Key Fitting) | Username:', username, '| date:', date);

  try {
    const data = await keyFittingService.getProductionByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No key fitting production data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Key fitting production data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching key fitting production:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getProductionByDate };

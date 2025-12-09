// controllers/spanner-production-controller.js
const spannerService = require('./spanner-production-service');

async function getProductionByDate(req, res) {
  const { username } = req;
  const date = req.params.date;

  console.log('🔍 Fetching Spanner_h | Username:', username, '| date:', date);

  try {
    const data = await spannerService.getProductionByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No spanner production data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Spanner production data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching Spanner_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getProductionByDate };

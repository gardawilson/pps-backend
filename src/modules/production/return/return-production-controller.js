// controllers/return-production-controller.js
const returnService = require('./return-production-service');

async function getReturnsByDate(req, res) {
  const { username } = req;
  const date = req.params.date;

  console.log('🔍 Fetching BJRetur_h | Username:', username, '| date:', date);

  try {
    const data = await returnService.getReturnsByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No return data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Return data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching BJRetur_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getReturnsByDate };

// controllers/sortir-reject-controller.js
const sortirRejectService = require('./sortir-reject-service');

async function getSortirRejectByDate(req, res) {
  const { username } = req; // dari verifyToken
  const date = req.params.date;

  console.log('🔍 Fetching BJSortirReject_h | Username:', username, '| date:', date);

  try {
    const data = await sortirRejectService.getSortirRejectByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No BJSortirReject_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `BJSortirReject_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching BJSortirReject_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = {
  getSortirRejectByDate,
};

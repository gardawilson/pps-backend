// src/modules/master/reject-master-controller.js
const service = require('./reject-master-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log(
    '🔍 Fetching MstReject (active only / master reject) | Username:',
    username
  );

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Active Reject master data (MstReject) fetched successfully',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstReject (active / master reject):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

// src/modules/master/packing-master-controller.js
const service = require('./packing-master-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log(
    '🔍 Fetching MstBarangJadi (active only / master packing) | Username:',
    username
  );

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Active Packing master data (MstBarangJadi) fetched successfully',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstBarangJadi (active / master packing):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

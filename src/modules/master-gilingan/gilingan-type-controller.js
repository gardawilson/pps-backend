// src/modules/master/gilingan-type-controller.js
const service = require('./gilingan-type-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log('🔍 Fetching MstGilingan (active only) | Username:', username);

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Active Gilingan master data fetched successfully',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstGilingan (active):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

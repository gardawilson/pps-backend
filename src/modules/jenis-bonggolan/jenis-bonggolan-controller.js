// src/modules/master/jenis-bonggolan-controller.js
const service = require('./jenis-bonggolan-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log('🔍 Fetching MstBonggolan (active only) | Username:', username);

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Data MstBonggolan (active) berhasil diambil',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstBonggolan (active):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

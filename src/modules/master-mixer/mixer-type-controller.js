// src/modules/master/mixer-type-controller.js
const service = require('./mixer-type-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log('🔍 Fetching MstMixer (active only) | Username:', username);

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Data MstMixer (active) berhasil diambil',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstMixer (active):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

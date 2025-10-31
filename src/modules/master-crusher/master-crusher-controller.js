const service = require('./master-crusher-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log('🔍 Fetching MstCrusher (active only) | Username:', username);

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Data MstCrusher (active) berhasil diambil',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstCrusher (active):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

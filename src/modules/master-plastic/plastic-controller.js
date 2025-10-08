const masterJenisPlastikService = require('./plastic-service');

async function getJenisPlastik(req, res) {
  const { username } = req;
  console.log("🔍 Fetching MstJenisPlastik data | Username:", username);

  try {
    const data = await masterJenisPlastikService.getAllJenisPlastikAktif();

    if (!data || data.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Data MstJenisPlastik tidak ditemukan',
        data: []
      });
    }

    res.json({
      success: true,
      message: 'Data MstJenisPlastik berhasil diambil',
      data,
      totalData: data.length
    });

  } catch (error) {
    console.error('Error fetching MstJenisPlastik:', error);
    res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getJenisPlastik };

const masterBlokService = require('./master-blok-service');

async function getBlok(req, res) {
  const { username } = req;
  console.log("🔍 Fetching MstBlok data | Username:", username);

  try {
    const data = await masterBlokService.getAllBlok();

    if (!data || data.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Data MstBlok tidak ditemukan',
        data: []
      });
    }

    res.json({
      success: true,
      message: 'Data MstBlok berhasil diambil',
      data,
      totalData: data.length
    });

  } catch (error) {
    console.error('Error fetching MstBlok:', error);
    res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getBlok };

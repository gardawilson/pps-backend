const masterLokasiService = require('../services/master-lokasi-service');

async function getLokasi(req, res) {
  const { username } = req;
  console.log("🔍 Fetching MstLokasi data | Username:", username);

  try {
    const data = await masterLokasiService.getAllLokasiAktif();

    if (!data || data.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Data MstLokasi tidak ditemukan',
        data: []
      });
    }

    res.json({
      success: true,
      message: 'Data MstLokasi berhasil diambil',
      data,
      totalData: data.length
    });

  } catch (error) {
    console.error('Error fetching MstLokasi:', error);
    res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getLokasi };

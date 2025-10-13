// controllers/bongkar-susun-controller.js
const bongkarSusunService = require('./bongkar-susun-service');

async function getByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah tervalidasi formatnya oleh route regex
  console.log("🔍 Fetching BongkarSusun_h | Username:", username, "| date:", date);

  try {
    const data = await bongkarSusunService.getByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data BongkarSusun_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date }
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data BongkarSusun_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date }
    });
  } catch (error) {
    console.error('Error fetching BongkarSusun_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getByDate };

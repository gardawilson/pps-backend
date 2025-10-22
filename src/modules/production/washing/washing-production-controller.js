// controllers/production-controller.js
const washingProduksiService = require('./washing-production-service');


async function getAllProduksi(req, res) {
  const { username } = req;

  // pagination (default 20)
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100); // batasi max 100

  try {
    const { data, total } = await washingProduksiService.getAllProduksi(page, pageSize);

    return res.status(200).json({
      success: true,
      message: 'WashingProduksi_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
      },
    });
  } catch (error) {
    console.error('Error fetching WashingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}



async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah match regex di route
  console.log("🔍 Fetching WashingProduksi_h | Username:", username, "| date:", date);

  try {
    const data = await washingProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data WashingProduksi_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date }
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data WashingProduksi_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date }
    });
  } catch (error) {
    console.error('Error fetching WashingProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message
    });
  }
}

module.exports = { getProduksiByDate , getAllProduksi};

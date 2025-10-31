const service = require('./crusher-production-service');

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  // Optional filters
  const idMesin = req.query.idMesin ? parseInt(req.query.idMesin, 10) : null;
  const shift   = req.query.shift ? String(req.query.shift).trim() : null;

  console.log("🔍 Fetching CrusherProduksi_h | user:", username, "| date:", date, "| idMesin:", idMesin, "| shift:", shift);

  try {
    const data = await service.getProduksiByDate({ date, idMesin, shift });

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No CrusherProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date, idMesin, shift },
      });
    }

    return res.status(200).json({
      success: true,
      message: `CrusherProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date, idMesin, shift },
    });
  } catch (error) {
    console.error('Error fetching CrusherProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getCrusherMasters(req, res) {
  try {
    const data = await service.getCrusherMasters();
    return res.status(200).json({
      success: true,
      message: 'MstCrusher retrieved successfully',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstCrusher:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getProduksiByDate, getCrusherMasters };

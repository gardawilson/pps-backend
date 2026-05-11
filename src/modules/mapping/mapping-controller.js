const mappingService = require("./mapping-service");

async function getMapping(req, res) {
  const { username } = req;
  console.log("Fetching mapping blok-warehouse | Username:", username);

  try {
    const data = await mappingService.getBlokWarehouseMapping();

    if (!data || data.length === 0) {
      return res.status(404).json({
        success: false,
        message: "Data mapping blok-warehouse tidak ditemukan",
        data: [],
      });
    }

    return res.json({
      success: true,
      message: "Data mapping blok-warehouse berhasil diambil",
      data,
      totalData: data.length,
    });
  } catch (error) {
    console.error("Error fetching mapping blok-warehouse:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function getLokasiByBlok(req, res) {
  const { username } = req;
  const { blok } = req.query;
  console.log(
    "Fetching lokasi by blok from mapping | Username:",
    username,
    "| Blok:",
    blok,
  );

  if (!blok || !String(blok).trim()) {
    return res.status(400).json({
      success: false,
      message: "Parameter query 'blok' wajib diisi",
    });
  }

  try {
    const data = await mappingService.getLokasiByBlok(String(blok).trim());

    if (!data || data.length === 0) {
      return res.status(404).json({
        success: false,
        message: "Data lokasi tidak ditemukan untuk blok tersebut",
        data: [],
      });
    }

    return res.json({
      success: true,
      message: "Data lokasi berdasarkan blok berhasil diambil",
      data,
      totalData: data.length,
    });
  } catch (error) {
    console.error("Error fetching lokasi by blok from mapping:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

module.exports = { getMapping, getLokasiByBlok };

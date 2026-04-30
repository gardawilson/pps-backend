const service = require("./master-barang-jadi-service");

async function getAllActive(req, res) {
  const { username } = req;
  const search = String(req.query.search || req.query.namaBJ || "").trim();

  console.log(
    "Fetching MstBarangJadi (active only) | Username:",
    username,
  );

  try {
    const data = await service.getAllActive({ search });
    return res.status(200).json({
      success: true,
      message: "Data MstBarangJadi (active) berhasil diambil",
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error("Error fetching MstBarangJadi (active):", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

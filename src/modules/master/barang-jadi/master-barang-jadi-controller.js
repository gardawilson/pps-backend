const service = require("./master-barang-jadi-service");

async function getAllActive(req, res) {
  const { username } = req;
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSize = Math.min(
    Math.max(parseInt(req.query.pageSize, 10) || 20, 1),
    100,
  );
  const search = String(req.query.search || req.query.namaBJ || "").trim();

  console.log(
    "Fetching MstBarangJadi (active only) | Username:",
    username,
  );

  try {
    const { data, total } = await service.getAllActive({
      page,
      pageSize,
      search,
    });
    return res.status(200).json({
      success: true,
      message: "Data MstBarangJadi (active) berhasil diambil",
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search,
      },
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

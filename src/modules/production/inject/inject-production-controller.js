// controllers/inject-production-controller.js
const injectProduksiService = require('./inject-production-service');

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log("🔍 Fetching InjectProduksi_h | Username:", username, "| date:", date);

  try {
    const data = await injectProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No InjectProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `InjectProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

// 🔹 GET FurnitureWIP info from InjectProduksi_h by NoProduksi
async function getFurnitureWipByNoProduksi(req, res) {
  const { username } = req;
  const { noProduksi } = req.params;

  console.log(
    '🔍 Fetching FurnitureWIP from InjectProduksi_h | Username:',
    username,
    '| NoProduksi:',
    noProduksi
  );

  try {
    const rows = await injectProduksiService.getFurnitureWipListByNoProduksi(noProduksi);

    // Kalau tidak ada baris sama sekali
    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No InjectProduksi_h / mapping FurnitureWIP found for NoProduksi ${noProduksi}`,
        data: {
          beratProdukHasilTimbang: null,
          items: [],
        },
        meta: { noProduksi },
      });
    }

    // Ambil berat dari header (semua baris sama)
    const beratProdukHasilTimbang = rows[0].BeratProdukHasilTimbang ?? null;

    // Map hanya IdFurnitureWIP & NamaFurnitureWIP untuk frontend
    const items = rows.map((r) => ({
      IdFurnitureWIP: r.IdFurnitureWIP,
      NamaFurnitureWIP: r.NamaFurnitureWIP,
    }));

    return res.status(200).json({
      success: true,
      message: `FurnitureWIP for NoProduksi ${noProduksi} retrieved successfully`,
      data: {
        beratProdukHasilTimbang,
        items,
      },
      meta: { noProduksi },
    });
  } catch (error) {
    console.error('Error fetching FurnitureWIP from InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

// 🔹 GET BarangJadi info (Packing) from InjectProduksi_h by NoProduksi
async function getPackingByNoProduksi(req, res) {
  const { username } = req;
  const { noProduksi } = req.params;

  console.log(
    '🔍 Fetching BarangJadi (Packing) from InjectProduksi_h | Username:',
    username,
    '| NoProduksi:',
    noProduksi
  );

  try {
    const rows = await injectProduksiService.getPackingListByNoProduksi(noProduksi);

    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No InjectProduksi_h / mapping Produk (BarangJadi) found for NoProduksi ${noProduksi}`,
        data: {
          beratProdukHasilTimbang: null,
          items: [],
        },
        meta: { noProduksi },
      });
    }

    // Ambil berat dari header (semua baris sama)
    const beratProdukHasilTimbang = rows[0].BeratProdukHasilTimbang ?? null;

    // Map hanya IdBJ & NamaBJ untuk frontend
    const items = rows.map((r) => ({
      IdBJ: r.IdBJ,
      NamaBJ: r.NamaBJ,
    }));

    return res.status(200).json({
      success: true,
      message: `BarangJadi (Packing) for NoProduksi ${noProduksi} retrieved successfully`,
      data: {
        beratProdukHasilTimbang,
        items,
      },
      meta: { noProduksi },
    });
  } catch (error) {
    console.error('Error fetching BarangJadi (Packing) from InjectProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}


module.exports = {
  getProduksiByDate,
  getFurnitureWipByNoProduksi,
  getPackingByNoProduksi
};

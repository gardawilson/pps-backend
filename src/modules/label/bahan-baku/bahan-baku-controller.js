// controllers/bahan-baku-controller.js
const bahanBakuService = require('./bahan-baku-service');

exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await bahanBakuService.getAll({ page, limit, search });
    const totalPages = Math.ceil(total / limit);

    res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Bahan Baku List Error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};


exports.getPalletByNoBahanBaku = async (req, res) => {
  const { nobahanbaku } = req.params;

  try {
    const pallets = await bahanBakuService.getPalletByNoBahanBaku(nobahanbaku);

    if (!pallets || pallets.length === 0) {
      return res.status(404).json({
        success: false,
        message: `Pallet tidak ditemukan untuk NoBahanBaku ${nobahanbaku}`,
      });
    }

    return res.status(200).json({
      success: true,
      data: { nobahanbaku, pallets },
    });
  } catch (err) {
    console.error('Get BahanBaku Pallet Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};


exports.getDetailByNoBahanBakuAndNoPallet = async (req, res) => {
  const { nobahanbaku, nopallet } = req.params;

  try {
    const details = await bahanBakuService.getDetailByNoBahanBakuAndNoPallet({
      nobahanbaku,
      nopallet,
    });

    if (!details || details.length === 0) {
      return res.status(404).json({
        success: false,
        message: `Detail tidak ditemukan untuk NoBahanBaku ${nobahanbaku} dan NoPallet ${nopallet}`,
      });
    }

    return res.status(200).json({
      success: true,
      data: { nobahanbaku, nopallet, details },
    });
  } catch (err) {
    console.error('Get BahanBaku Detail Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};


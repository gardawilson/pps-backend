// controllers/bahan-baku-controller.js
const bahanBakuService = require("./bahan-baku-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");

exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || "").trim();

    const { data, total } = await bahanBakuService.getAll({
      page,
      limit,
      search,
    });
    const totalPages = Math.ceil(total / limit);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error("Get Bahan Baku List Error:", err);
    return res
      .status(500)
      .json({ success: false, message: "Terjadi kesalahan server" });
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
    console.error("Get BahanBaku Pallet Error:", err);
    return res
      .status(500)
      .json({ success: false, message: "Terjadi kesalahan server" });
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
    console.error("Get BahanBaku Detail Error:", err);
    return res
      .status(500)
      .json({ success: false, message: "Terjadi kesalahan server" });
  }
};

exports.updateByNoBahanBakuAndNoPallet = async (req, res) => {
  const { nobahanbaku, nopallet } = req.params;

  try {
    const NoBahanBaku = String(nobahanbaku || "").trim();
    const NoPallet = String(nopallet || "").trim();

    if (!NoBahanBaku || !NoPallet) {
      return res.status(400).json({
        success: false,
        message: "NoBahanBaku dan NoPallet wajib diisi",
      });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({
        success: false,
        message: "Unauthorized (idUsername missing)",
      });
    }

    const actorUsername = getActorUsername(req) || "system";

    // ✅ pastikan body object
    const body = req.body && typeof req.body === "object" ? req.body : {};

    // ✅ jangan percaya audit fields dari client
    const {
      actorId: _clientActorId,
      requestId: _clientRequestId,
      ...safeBody
    } = body;

    const payload = {
      ...safeBody,
      NoBahanBaku,
      NoPallet,
      actorId, // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // ✅ business field (username), overwrite dari token
    payload.header =
      payload.header && typeof payload.header === "object"
        ? payload.header
        : {};
    payload.header.UpdateBy = actorUsername;

    const result =
      await bahanBakuService.updateByNoBahanBakuAndNoPallet(payload);

    return res.status(200).json({
      success: true,
      message: "Pallet bahan baku berhasil diupdate",
      data: result,
    });
  } catch (err) {
    console.error("Update BahanBaku Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

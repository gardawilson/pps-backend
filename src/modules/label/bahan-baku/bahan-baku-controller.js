// controllers/bahan-baku-controller.js
const bahanBakuService = require("./bahan-baku-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");
const { getIo } = require("../../../core/utils/socket-instance");
const { generateLabelPdf } = require("../../../core/utils/pdf/label-generator");
const { buildBahanBakuLabelHtml } = require("../../../core/utils/pdf/templates/bahan-baku-label-pdf/bahan-baku-label-pdf");

exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || "").trim();
    const includeUsed =
      String(req.query.includeUsed || "").toLowerCase() === "true";

    const { data, total } = await bahanBakuService.getAll({
      page,
      limit,
      search,
      includeUsed,
    });
    const totalPages = Math.ceil(total / limit);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages, includeUsed },
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

// GET /labels/bahan-baku/:nobahanbaku/pallet/:nopallet/pdf
exports.generatePdf = async (req, res) => {
  try {
    const NoBahanBaku = String(req.params.nobahanbaku || "").trim();
    const NoPallet = String(req.params.nopallet || "").trim();

    if (!NoBahanBaku || !NoPallet) {
      return res.status(400).json({ success: false, message: "NoBahanBaku dan NoPallet wajib diisi" });
    }

    const row = await bahanBakuService.getByPalletForPdf(NoBahanBaku, NoPallet);

    const d = new Date(row.DateCreate);
    const dd = String(d.getDate()).padStart(2, "0");
    const mmm = d.toLocaleDateString("id-ID", { month: "short" });
    const yy = String(d.getFullYear()).slice(-2);

    const data = {
      noLabel:          `${row.NoBahanBaku}-${row.NoPallet}`,
      noBahanBaku:      row.NoBahanBaku,
      noPallet:         row.NoPallet,
      namaJenisPlastik: row.NamaJenisPlastik,
      namaSupplier:     row.NamaSupplier,
      noPlat:           row.NoPlat,
      sakSisa:          row.SakSisa,
      beratSisa:        `${Number(row.BeratSisa).toFixed(2)} kg`,
      tanggal:          `${dd}-${mmm}-${yy}`,
      createBy:         row.CreateBy,
      watermarkText:    row.HasBeenPrinted > 0 ? `COPY ${row.HasBeenPrinted}` : "",
      details:          row.details,
    };

    const pdfBuffer = await generateLabelPdf(data, buildBahanBakuLabelHtml);

    res.set({
      "Content-Type": "application/pdf",
      "Content-Disposition": `inline; filename="label-${NoPallet}.pdf"`,
      "Content-Length": pdfBuffer.length,
    });

    return res.end(pdfBuffer);
  } catch (err) {
    console.error("BahanBaku PDF Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
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

    const result = await bahanBakuService.incrementHasBeenPrinted({
      NoBahanBaku,
      NoPallet,
      actorId,
      requestId: makeRequestId(req),
    });

    const io = getIo();
    if (io)
      io.emit("print_confirmed", {
        noLabel: NoBahanBaku,
        hasBeenPrinted: result.HasBeenPrinted,
      });

    return res.status(200).json({
      success: true,
      message: "HasBeenPrinted berhasil ditambah",
      data: result,
    });
  } catch (err) {
    console.error("Increment HasBeenPrinted BahanBaku Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

// controllers/bonggolan-controller.js
const service = require("./bonggolan-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");
const { getIo } = require("../../../core/utils/socket-instance");
const { generateLabelPdf } = require("../../../core/utils/pdf/label-generator");
const { buildBonggolanLabelHtml } = require("../../../core/utils/pdf/templates/bonggolan-label-pdf/bonggolan-label-pdf");

// GET all header bonggolan
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || "").trim();
    const includeUsed =
      String(req.query.includeUsed || "").toLowerCase() === "true";

    const { data, total } = await service.getAll({
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
    console.error("Get Bonggolan List Error:", err);
    return res
      .status(500)
      .json({ success: false, message: "Terjadi kesalahan server" });
  }
};

exports.create = async (req, res) => {
  try {
    // ✅ pastikan body object
    const payload = req.body && typeof req.body === "object" ? req.body : {};

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    // ✅ audit fields (ID only)
    payload.actorId = actorId;
    payload.requestId = makeRequestId(req);

    // ✅ business field CreateBy (username), overwrite supaya tidak spoof dari client
    payload.header =
      payload.header && typeof payload.header === "object"
        ? payload.header
        : {};
    payload.header.CreateBy = getActorUsername(req) || "system";

    const result = await service.createBonggolanCascade(payload);

    return res.status(201).json({
      success: true,
      message: "Bonggolan berhasil dibuat",
      data: result,
    });
  } catch (err) {
    console.error("Create Bonggolan Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

exports.update = async (req, res) => {
  const { nobonggolan, noBonggolan } = req.params;

  try {
    const NoBonggolan = String(nobonggolan || noBonggolan || "").trim();
    if (!NoBonggolan) {
      return res
        .status(400)
        .json({ success: false, message: "nobonggolan wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
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
      NoBonggolan,
      actorId, // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // =========================================
    // ✅ BACKWARD COMPATIBILITY:
    // Kalau client lama kirim field flat (Berat, IdBonggolan, dst),
    // pindahkan ke payload.header supaya cocok dengan service cascade.
    // =========================================
    payload.header =
      payload.header && typeof payload.header === "object"
        ? payload.header
        : {};

    const liftKeys = [
      "Berat",
      "IdBonggolan",
      "IdWarehouse",
      "IdStatus",
      "DateCreate",
      "DateUsage",
      "Blok",
      "IdLokasi",
    ];

    for (const k of liftKeys) {
      if (
        Object.prototype.hasOwnProperty.call(payload, k) &&
        payload.header[k] === undefined
      ) {
        payload.header[k] = payload[k];
        delete payload[k];
      }
    }

    // ✅ business field (username) — overwrite dari token
    payload.header.UpdateBy = actorUsername;

    const result = await service.updateBonggolanCascade(payload);

    return res.status(200).json({
      success: true,
      message: "Bonggolan berhasil diupdate",
      data: result,
    });
  } catch (err) {
    console.error("Update Bonggolan Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

exports.delete = async (req, res) => {
  const { nobonggolan, noBonggolan } = req.params;

  try {
    const NoBonggolan = String(nobonggolan || noBonggolan || "").trim();
    if (!NoBonggolan) {
      return res
        .status(400)
        .json({ success: false, message: "nobonggolan wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const payload = {
      NoBonggolan,
      actorId, // ✅ audit uses ID
      requestId: makeRequestId(req),
    };

    const result = await service.deleteBonggolanCascade(payload);

    return res.status(200).json({
      success: true,
      message: `Bonggolan ${NoBonggolan} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error("Delete Bonggolan Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
  const { nobonggolan, noBonggolan } = req.params;

  try {
    const NoBonggolan = String(nobonggolan || noBonggolan || "").trim();
    if (!NoBonggolan) {
      return res
        .status(400)
        .json({ success: false, message: "nobonggolan wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const result = await service.incrementHasBeenPrinted({
      NoBonggolan,
      actorId,
      requestId: makeRequestId(req),
    });

    const io = getIo();
    if (io)
      io.emit("print_confirmed", {
        noLabel: NoBonggolan,
        hasBeenPrinted: result.HasBeenPrinted,
      });

    return res.status(200).json({
      success: true,
      message: "HasBeenPrinted berhasil ditambah",
      data: result,
    });
  } catch (err) {
    console.error("Increment HasBeenPrinted Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

// GET /labels/bonggolan/:noBonggolan/pdf
exports.generatePdf = async (req, res) => {
  try {
    const NoBonggolan = String(req.params.noBonggolan || "").trim();
    if (!NoBonggolan) {
      return res.status(400).json({ success: false, message: "noBonggolan wajib diisi" });
    }

    const row = await service.getByNoBonggolan(NoBonggolan);

    const d = new Date(row.Tanggal);
    const dd = String(d.getDate()).padStart(2, "0");
    const mmm = d.toLocaleDateString("id-ID", { month: "short" });
    const yy = String(d.getFullYear()).slice(-2);

    const data = {
      noLabel:      row.NoBonggolan,
      jenisPlastik: row.Jenis,
      mesin:        row.Ket || "-",
      shift:        row.Shift ? String(row.Shift) : "-",
      berat:        row.Berat != null ? `${row.Berat} kg` : "-",
      warehouse:    row.Warehouse || "-",
      tanggal:      `${dd}-${mmm}-${yy}`,
      createBy:     row.CreateBy || "-",
      watermarkText: row.HasBeenPrinted > 0 ? `COPY ${row.HasBeenPrinted}` : "",
    };

    const pdfBuffer = await generateLabelPdf(data, buildBonggolanLabelHtml);

    res.set({
      "Content-Type": "application/pdf",
      "Content-Disposition": `inline; filename="label-${NoBonggolan}.pdf"`,
      "Content-Length": pdfBuffer.length,
    });

    return res.end(pdfBuffer);
  } catch (err) {
    console.error("Bonggolan PDF Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

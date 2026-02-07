// controllers/broker-controller.js

const brokerService = require("./broker-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");

// GET all header broker
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || "").trim();

    const { data, total } = await brokerService.getAll({ page, limit, search });
    const totalPages = Math.ceil(total / limit);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error("Get Broker List Error:", err);
    return res
      .status(500)
      .json({ success: false, message: "Terjadi kesalahan server" });
  }
};

// GET one header + details
exports.getOne = async (req, res) => {
  const { nobroker } = req.params;

  try {
    const details = await brokerService.getBrokerDetailByNoBroker(nobroker);

    if (!details || details.length === 0) {
      return res.status(404).json({
        success: false,
        message: `Data tidak ditemukan untuk NoBroker ${nobroker}`,
      });
    }

    return res.status(200).json({
      success: true,
      data: { nobroker, details },
    });
  } catch (err) {
    console.error("Get Broker_d Error:", err);
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

    // ✅ untuk audit trail (ID saja)
    payload.actorId = actorId;
    payload.requestId = makeRequestId(req);

    // ✅ business field di Broker_h (tetap string username)
    // overwrite supaya tidak spoof dari client
    payload.header = payload.header || {};
    payload.header.CreateBy = getActorUsername(req) || "system";

    const result = await brokerService.createBrokerCascade(payload);

    return res.status(201).json({
      success: true,
      message: "Broker berhasil dibuat",
      data: result,
    });
  } catch (err) {
    console.error("Create Broker Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

exports.update = async (req, res) => {
  const { nobroker } = req.params;

  try {
    const NoBroker = String(nobroker || "").trim();
    if (!NoBroker) {
      return res
        .status(400)
        .json({ success: false, message: "nobroker wajib diisi" });
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
      NoBroker,
      actorId, // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // ✅ business field (username), overwrite dari token
    payload.header =
      payload.header && typeof payload.header === "object"
        ? payload.header
        : {};
    payload.header.UpdateBy = actorUsername;

    const result = await brokerService.updateBrokerCascade(payload);

    return res.status(200).json({
      success: true,
      message: "Broker berhasil diupdate",
      data: result,
    });
  } catch (err) {
    console.error("Update Broker Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

exports.remove = async (req, res) => {
  const { nobroker } = req.params;

  try {
    const NoBroker = String(nobroker || "").trim();
    if (!NoBroker) {
      return res
        .status(400)
        .json({ success: false, message: "nobroker wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const payload = {
      NoBroker,
      actorId, // ✅ audit uses ID
      requestId: makeRequestId(req),
    };

    const result = await brokerService.deleteBrokerCascade(payload);

    return res.status(200).json({
      success: true,
      message: `Broker ${NoBroker} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error("Delete Broker Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

// GET partial info
exports.getPartialInfo = async (req, res) => {
  const { nobroker, nosak } = req.params;

  try {
    const NoBroker = String(nobroker || "").trim();
    const NoSakNum = Number(nosak);

    if (!NoBroker || !Number.isFinite(NoSakNum)) {
      return res.status(400).json({
        success: false,
        message: "nobroker dan nosak wajib diisi (nosak harus angka)",
      });
    }

    const data = await brokerService.getPartialInfoByBrokerAndSak(
      NoBroker,
      Math.trunc(NoSakNum),
    );

    if (!data.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data partial untuk NoBroker ${NoBroker} / NoSak ${Math.trunc(NoSakNum)}`,
        totalRows: 0,
        totalPartialWeight: 0,
        data: [],
        meta: { nobroker: NoBroker, nosak: Math.trunc(NoSakNum) },
      });
    }

    return res.status(200).json({
      success: true,
      message: "Partial info berhasil diambil",
      totalRows: data.rows.length,
      totalPartialWeight: data.totalPartialWeight,
      data: data.rows,
      meta: { nobroker: NoBroker, nosak: Math.trunc(NoSakNum) },
    });
  } catch (err) {
    console.error("Get Partial Info Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

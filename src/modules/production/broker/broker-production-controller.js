// controllers/broker-production-controller.js
const brokerProduksiService = require("./broker-production-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");

async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support both ?noProduksi= and ?search=
  const search =
    (typeof req.query.noProduksi === "string" && req.query.noProduksi) ||
    (typeof req.query.search === "string" && req.query.search) ||
    "";

  try {
    const { data, total } = await brokerProduksiService.getAllProduksi(
      page,
      pageSize,
      search,
    );

    return res.status(200).json({
      success: true,
      message: "BrokerProduksi_h retrieved successfully",
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search, // echo back for client state
      },
    });
  } catch (error) {
    console.error("Error fetching BrokerProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function getInputsByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }
  try {
    // make sure brokerProduksiService.fetchInputs exists
    const data = await brokerProduksiService.fetchInputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[getInputsByNoProduksi]", e);
    return res
      .status(500)
      .json({
        success: false,
        message: "Internal Server Error",
        error: e.message,
      });
  }
}

async function getOutputsByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }
  try {
    const data = await brokerProduksiService.fetchOutputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[getOutputsByNoProduksi]", e);
    return res
      .status(500)
      .json({
        success: false,
        message: "Internal Server Error",
        error: e.message,
      });
  }
}

async function getOutputsBonggolanByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }
  try {
    const data = await brokerProduksiService.fetchOutputsBonggolan(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[getOutputsBonggolanByNoProduksi]", e);
    return res
      .status(500)
      .json({
        success: false,
        message: "Internal Server Error",
        error: e.message,
      });
  }
}

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log(
    "🔍 Fetching BrokerProduksi_h | Username:",
    username,
    "| date:",
    date,
  );

  try {
    const data = await brokerProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No BrokerProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `BrokerProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching BrokerProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function createProduksi(req, res) {
  // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
  const body = req.body && typeof req.body === "object" ? req.body : {};

  // ✅ jangan percaya audit fields dari client
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...b
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized (idUsername missing)",
    });
  }

  // ✅ username untuk business fields / audit actor string
  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";

  // ✅ request id per HTTP request (kalau ada header ikut pakai)
  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId) res.setHeader("x-request-id", requestId);

  // helper kecil buat parse number
  const toInt = (v) => {
    if (v === undefined || v === null || v === "") return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : Math.trunc(n);
  };
  const toFloat = (v) => {
    if (v === undefined || v === null || v === "") return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  };

  // ✅ payload business (tanpa audit fields)
  const payload = {
    tglProduksi: b.tglProduksi, // 'YYYY-MM-DD'
    idMesin: toInt(b.idMesin), // number
    idOperator: toInt(b.idOperator), // number
    jam: b.jam, // number or 'HH:mm-HH:mm'
    shift: toInt(b.shift), // number
    createBy: actorUsername, // controller overwrite dari token

    checkBy1: b.checkBy1 ?? null,
    checkBy2: b.checkBy2 ?? null,
    approveBy: b.approveBy ?? null,
    jmlhAnggota: toInt(b.jmlhAnggota),
    hadir: toInt(b.hadir),
    hourMeter: toFloat(b.hourMeter),

    hourStart: b.hourStart || null, // ex: '08:00:00'
    hourEnd: b.hourEnd || null, // ex: '09:00:00'
  };

  // optional: validasi cepat agar error 400 rapih (service juga akan validasi)
  const must = [];
  if (!payload.tglProduksi) must.push("tglProduksi");
  if (payload.idMesin == null) must.push("idMesin");
  if (payload.idOperator == null) must.push("idOperator");
  if (payload.jam == null) must.push("jam");
  if (payload.shift == null) must.push("shift");
  if (must.length) {
    return res.status(400).json({
      success: false,
      message: `Field wajib: ${must.join(", ")}`,
      error: { fields: must },
    });
  }

  try {
    // ✅ Forward audit context ke service (trigger akan pakai SESSION_CONTEXT dari ctx ini)
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ ubah signature service jadi (payload, ctx)
    const result = await brokerProduksiService.createBrokerProduksi(
      payload,
      ctx,
    );

    // support return: { header, audit? }
    const header = result?.header ?? result;

    return res.status(201).json({
      success: true,
      message: "Created",
      data: header,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error("[createProduksi]", err);
    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message:
        status === 500 ? "Internal Server Error" : err.message || "Error",
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
      },
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  }
}

async function updateProduksi(req, res) {
  const noProduksi = req.params.noProduksi; // dari URL
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: "noProduksi is required in route param",
    });
  }

  // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
  const body = req.body && typeof req.body === "object" ? req.body : {};

  // ✅ jangan percaya audit fields dari client
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...b
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized (idUsername missing)",
    });
  }

  // ✅ username untuk business fields / audit actor string
  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";

  // ✅ request id per HTTP request (kalau ada header ikut pakai)
  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId) res.setHeader("x-request-id", requestId);

  // helper kecil buat parse number
  const toInt = (v) => {
    if (v === undefined || v === null || v === "") return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : Math.trunc(n);
  };
  const toFloat = (v) => {
    if (v === undefined || v === null || v === "") return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  };

  // ✅ payload business (tanpa audit fields)
  // body boleh partial, jadi field yang tidak dikirim biarkan undefined
  const payload = {
    // kalau dikirim, service akan resolve date-only & guard lock
    tglProduksi: b.tglProduksi, // 'YYYY-MM-DD'

    idMesin: b.idMesin !== undefined ? toInt(b.idMesin) : undefined,
    idOperator: b.idOperator !== undefined ? toInt(b.idOperator) : undefined,

    // jam boleh string 'HH:mm-HH:mm' / number / null (kalau mau set null)
    jam: b.jam !== undefined ? b.jam : undefined,

    shift: b.shift !== undefined ? toInt(b.shift) : undefined,

    // business fields lain
    checkBy1: b.checkBy1 !== undefined ? (b.checkBy1 ?? null) : undefined,
    checkBy2: b.checkBy2 !== undefined ? (b.checkBy2 ?? null) : undefined,
    approveBy: b.approveBy !== undefined ? (b.approveBy ?? null) : undefined,

    jmlhAnggota: b.jmlhAnggota !== undefined ? toInt(b.jmlhAnggota) : undefined,
    hadir: b.hadir !== undefined ? toInt(b.hadir) : undefined,

    hourMeter: b.hourMeter !== undefined ? toFloat(b.hourMeter) : undefined,

    hourStart: b.hourStart !== undefined ? b.hourStart || null : undefined,
    hourEnd: b.hourEnd !== undefined ? b.hourEnd || null : undefined,

    // NOTE:
    // - Jangan kirim updateBy kalau kolomnya tidak ada di tabel.
    // - Kalau kamu punya kolom UpdateBy/DateTimeUpdate, baru inject di sini.
    // updateBy: actorUsername,
  };

  // optional: validasi cepat agar error 400 rapih (service juga akan validasi)
  // untuk update: minimal harus ada 1 field yang dikirim
  const hasAnyField = Object.values(payload).some((v) => v !== undefined);
  if (!hasAnyField) {
    return res.status(400).json({
      success: false,
      message: "No fields to update",
      error: { fields: [] },
    });
  }

  try {
    // ✅ Forward audit context ke service (trigger akan pakai SESSION_CONTEXT dari ctx ini)
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ pastikan signature service: (noProduksi, payload, ctx)
    const result = await brokerProduksiService.updateBrokerProduksi(
      noProduksi,
      payload,
      ctx,
    );

    const header = result?.header ?? result;

    return res.status(200).json({
      success: true,
      message: "Updated",
      data: header,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error("[updateProduksi]", err);
    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message:
        status === 500 ? "Internal Server Error" : err.message || "Error",
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
      },
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  }
}

async function deleteProduksi(req, res) {
  const noProduksi = req.params.noProduksi;
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: "noProduksi is required in route param",
    });
  }

  // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
  const body = req.body && typeof req.body === "object" ? req.body : {};

  // ✅ jangan percaya audit fields dari client
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ..._b
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized (idUsername missing)",
    });
  }

  // ✅ username untuk audit actor string
  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";

  // ✅ request id per HTTP request (kalau ada header ikut pakai)
  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId) res.setHeader("x-request-id", requestId);

  try {
    // ✅ Forward audit context ke service (trigger akan pakai SESSION_CONTEXT dari ctx ini)
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ pastikan signature service: (noProduksi, ctx)
    const result = await brokerProduksiService.deleteBrokerProduksi(
      noProduksi,
      ctx,
    );

    // kalau service return header deleted, kita support juga
    return res.status(200).json({
      success: true,
      message: "Deleted",
      data: result?.header ?? undefined,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error("[deleteProduksi]", err);
    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message:
        status === 500 ? "Internal Server Error" : err.message || "Error",
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
      },
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  }
}

async function validateLabel(req, res) {
  const { labelCode } = req.params;

  // Validate input
  if (!labelCode || typeof labelCode !== "string") {
    return res.status(400).json({
      success: false,
      message: "Label number is required and must be a string",
    });
  }

  try {
    const result = await brokerProduksiService.validateLabel(labelCode);

    if (!result.found) {
      return res.status(404).json({
        success: false,
        message: `Label ${labelCode} not found or already used`,
        prefix: result.prefix,
        tableName: result.tableName,
      });
    }

    return res.status(200).json({
      success: true,
      message: "Label validated successfully",
      prefix: result.prefix,
      tableName: result.tableName,
      totalRecords: result.count,
      data: result.data, // Now returns array of all matching records
    });
  } catch (error) {
    console.error("Error validating label:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function upsertInputsAndPartials(req, res) {
  const noProduksi = String(req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: "noProduksi is required",
      error: {
        field: "noProduksi",
        message: "Parameter noProduksi tidak boleh kosong",
      },
    });
  }

  // ✅ pastikan body object
  const body = req.body && typeof req.body === "object" ? req.body : {};

  // ✅ jangan percaya audit fields dari client
  // (biar client tidak bisa spoof requestId/actorId dan biar tidak bikin null/aneh)
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...payload
  } = body;

  // ✅ actor wajib (audit)
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized (idUsername missing)",
    });
  }

  // ✅ username untuk business fields / audit actor string
  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";

  // ✅ request id per HTTP request (kalau ada header ikut pakai)
  const requestId = String(makeRequestId(req) || "").trim();

  // optional: echo header for tracing
  if (requestId) res.setHeader("x-request-id", requestId);

  // optional validate: at least one input exists
  const hasInput = [
    "broker",
    "bb",
    "washing",
    "crusher",
    "gilingan",
    "mixer",
    "reject",
    "bbPartial",
    "gilinganPartial",
    "mixerPartial",
    "rejectPartial",
  ].some((key) => Array.isArray(payload?.[key]) && payload[key].length > 0);

  // if (!hasInput) { ... } // kalau mau strict, aktifkan lagi

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await brokerProduksiService.upsertInputsAndPartials(
      noProduksi,
      payload,
      ctx,
    );

    // Support beberapa bentuk return (backward compatible)
    const success = result?.success !== undefined ? !!result.success : true;
    const hasWarnings = !!result?.hasWarnings;
    const data = result?.data ?? result;

    let statusCode = 200;
    let message = "Inputs & partials processed successfully";

    if (!success) {
      const totalInvalid = Number(data?.summary?.totalInvalid ?? 0);
      const totalInserted = Number(data?.summary?.totalInserted ?? 0);
      const totalPartialsCreated = Number(
        data?.summary?.totalPartialsCreated ?? 0,
      );

      if (totalInvalid > 0) {
        statusCode = 422;
        message = "Beberapa data tidak valid";
      } else if (totalInserted === 0 && totalPartialsCreated === 0) {
        statusCode = 400;
        message = "Tidak ada data yang berhasil diproses";
      }
    } else if (hasWarnings) {
      message = "Inputs & partials processed with warnings";
    }

    return res.status(statusCode).json({
      success,
      message,
      data,
      meta: {
        noProduksi,
        hasInput,
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (e) {
    console.error("[upsertInputsAndPartials]", e);
    const status = e.statusCode || e.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? "Internal Server Error" : e.message,
      error: {
        message: e.message,
        details: process.env.NODE_ENV === "development" ? e.stack : undefined,
      },
    });
  }
}

async function deleteInputsAndPartials(req, res) {
  const noProduksi = String(req.params.noProduksi || "").trim();

  if (!noProduksi) {
    return res.status(400).json({
      success: false,
      message: "noProduksi is required",
      error: {
        field: "noProduksi",
        message: "Parameter noProduksi tidak boleh kosong",
      },
    });
  }

  // ✅ Strip client audit fields
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...payload
  } = req.body || {};

  // ✅ Get trusted audit context
  const actorId = getActorId(req);
  if (!actorId) {
    return res.status(401).json({
      success: false,
      message: "Unauthorized (idUsername missing)",
    });
  }

  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";
  const requestId = String(makeRequestId(req) || "").trim();

  if (requestId) res.setHeader("x-request-id", requestId);

  // Validate input
  const hasInput = [
    "broker",
    "bb",
    "washing",
    "crusher",
    "gilingan",
    "mixer",
    "reject",
    "bbPartial",
    "brokerPartial",
    "gilinganPartial",
    "mixerPartial",
    "rejectPartial",
  ].some((key) => Array.isArray(payload?.[key]) && payload[key].length > 0);

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: "Tidak ada data input yang diberikan",
      error: { message: "Request body harus berisi minimal satu array input" },
    });
  }

  try {
    // ✅ Forward audit context
    const ctx = { actorId, actorUsername, requestId };

    const result = await brokerProduksiService.deleteInputsAndPartials(
      noProduksi,
      payload,
      ctx,
    );

    const { success, hasWarnings, data } = result;

    let statusCode = 200;
    let message = "Inputs & partials deleted successfully";

    if (!success) {
      statusCode = 404;
      message = "Tidak ada data yang berhasil dihapus";
    } else if (hasWarnings) {
      message = "Inputs & partials deleted with warnings";
    }

    return res.status(statusCode).json({
      success,
      message,
      data,
      meta: {
        noProduksi,
        hasInput,
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (e) {
    console.error("[deleteInputsAndPartials]", e);
    const status = e.statusCode || e.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? "Internal Server Error" : e.message,
      error: {
        message: e.message,
        details: process.env.NODE_ENV === "development" ? e.stack : undefined,
      },
    });
  }
}

module.exports = {
  getProduksiByDate,
  getInputsByNoProduksi,
  getOutputsByNoProduksi,
  getOutputsBonggolanByNoProduksi,
  getAllProduksi,
  createProduksi,
  updateProduksi,
  deleteProduksi,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

// controllers/gilingan-production-controller.js
const gilinganProduksiService = require("./gilingan-production-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");
const { toInt, toFloat, normalizeTime } = require("../../../core/utils/parse");

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log(
    "🔍 Fetching GilinganProduksi_h | Username:",
    username,
    "| date:",
    date,
  );

  try {
    const data = await gilinganProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No GilinganProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `GilinganProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching GilinganProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

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
    const { data, total } = await gilinganProduksiService.getAllProduksi(
      page,
      pageSize,
      search,
    );

    return res.status(200).json({
      success: true,
      message: "GilinganProduksi_h retrieved successfully",
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
    console.error("Error fetching GilinganProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function createProduksi(req, res) {
  try {
    // ===============================
    // Audit context
    // ===============================
    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const actorUsername =
      getActorUsername(req) || req.username || req.user?.username || "system";
    const requestId = String(makeRequestId(req) || "").trim();
    if (requestId) res.setHeader("x-request-id", requestId);

    // ===============================
    // Body tanpa audit fields dari client
    // ===============================
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const {
      actorId: _cActorId,
      actorUsername: _cActorUsername,
      actor: _cActor,
      requestId: _cRequestId,
      ...b
    } = body;

    // ===============================
    // Payload business
    // ===============================
    const payload = {
      tglProduksi: b.tglProduksi,
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      shift: toInt(b.shift),

      createBy: actorUsername,
      checkBy1: b.checkBy1 ?? null,
      checkBy2: b.checkBy2 ?? null,
      approveBy: b.approveBy ?? null,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),
      hourStart: normalizeTime(b.hourStart) ?? null,
      hourEnd: normalizeTime(b.hourEnd) ?? null,
    };

    // ===============================
    // Quick validation
    // ===============================
    const must = [];
    if (!payload.tglProduksi) must.push("tglProduksi");
    if (payload.idMesin == null) must.push("idMesin");
    if (payload.idOperator == null) must.push("idOperator");
    if (payload.shift == null) must.push("shift");

    if (must.length) {
      return res.status(400).json({
        success: false,
        message: `Field wajib: ${must.join(", ")}`,
        error: { fields: must },
      });
    }

    // ===============================
    // Call service
    // ===============================
    const result = await gilinganProduksiService.createGilinganProduksi(
      payload,
      { actorId, actorUsername, requestId },
    );
    const header = result?.header ?? result;

    return res.status(201).json({
      success: true,
      message: "Created",
      data: header,
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[Gilingan][createProduksi]", err);
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
        audit:
          err.actorId && err.actorUsername
            ? {
                actorId: err.actorId,
                actorUsername: err.actorUsername,
                requestId: err.requestId,
              }
            : undefined,
      },
    });
  }
}

async function updateProduksi(req, res) {
  try {
    // ===============================
    // Route param check
    // ===============================
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: "noProduksi is required in route param",
      });
    }

    // ===============================
    // Audit context
    // ===============================
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

    // ===============================
    // Body tanpa audit fields dari client
    // ===============================
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const {
      actorId: _cActorId,
      actorUsername: _cActorUsername,
      actor: _cActor,
      requestId: _cRequestId,
      ...b
    } = body;

    // ===============================
    // Payload business (partial update)
    // ===============================
    const payload = {
      tglProduksi: b.tglProduksi,
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      shift: toInt(b.shift),

      checkBy1: b.checkBy1 ?? undefined,
      checkBy2: b.checkBy2 ?? undefined,
      approveBy: b.approveBy ?? undefined,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),

      hourStart: normalizeTime(b.hourStart),
      hourEnd: normalizeTime(b.hourEnd),
    };

    // ===============================
    // Call service
    // ===============================
    const ctx = { actorId, actorUsername, requestId };
    const result = await gilinganProduksiService.updateGilinganProduksi(
      noProduksi,
      payload,
      ctx,
    );
    const header = result?.header ?? result;

    return res.status(200).json({
      success: true,
      message: "Updated",
      data: header,
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[Gilingan][updateProduksi]", err);
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
        audit: {
          actorId: getActorId(req),
          actorUsername: getActorUsername(req),
          requestId: makeRequestId(req),
        },
      },
    });
  }
}

async function deleteProduksi(req, res) {
  try {
    // ===============================
    // Validasi route param
    // ===============================
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: "noProduksi is required in route param",
      });
    }

    // ===============================
    // Audit context
    // ===============================
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

    const ctx = { actorId, actorUsername, requestId };

    // ===============================
    // Call service
    // ===============================
    await gilinganProduksiService.deleteGilinganProduksi(noProduksi, ctx);

    return res.status(200).json({
      success: true,
      message: "Deleted",
      meta: { audit: ctx },
    });
  } catch (err) {
    console.error("[Gilingan][deleteProduksi]", err);
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
        audit: {
          actorId: err.actorId || null,
          actorUsername: err.actorUsername || null,
          requestId: err.requestId || null,
        },
      },
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
    const data = await gilinganProduksiService.fetchInputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[gilingan.getInputsByNoProduksi]", e);
    return res.status(500).json({
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
    const data = await gilinganProduksiService.fetchOutputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[gilingan.getOutputsByNoProduksi]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
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
    const result = await gilinganProduksiService.validateLabel(labelCode);

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
    "bonggolan",
    "crusher",
    "reject",
    "brokerPartial",
    "rejectPartial",
  ].some((key) => Array.isArray(payload?.[key]) && payload[key].length > 0);

  // if (!hasInput) { ... } // kalau mau strict, aktifkan lagi

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await gilinganProduksiService.upsertInputsAndPartials(
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
    "bonggolan",
    "crusher",
    "reject",
    "brokerPartial",
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

    const result = await gilinganProduksiService.deleteInputsAndPartials(
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
  getAllProduksi,
  createProduksi,
  updateProduksi,
  deleteProduksi,
  getInputsByNoProduksi,
  getOutputsByNoProduksi,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

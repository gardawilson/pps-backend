// controllers/hotstamping-production-controller.js
const hotStampingService = require("./hot-stamp-production-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");
const {
  toInt,
  toFloat,
  normalizeTime,
  toBit,
  toIntUndef,
  toFloatUndef,
  toBitUndef,
  toStrUndef,
  toJamInt,
} = require("../../../core/utils/parse");

async function getProduksiByDate(req, res) {
  const date = req.params.date;

  try {
    const data = await hotStampingService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No HotStamping_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `HotStamping_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching HotStamping_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

// ✅ GET ALL (paged)
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
    const { data, total } = await hotStampingService.getAllProduksi(
      page,
      pageSize,
      search,
    );

    return res.status(200).json({
      success: true,
      message: "HotStamping_h retrieved successfully",
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
    console.error("Error fetching HotStamping_h:", error);
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
        .json({ success: false, message: "Unauthorized (actorId missing)" });
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
      createBy: _cCreateBy,
      checkBy1: _cCheckBy1,
      checkBy2: _cCheckBy2,
      approveBy: _cApproveBy,
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
      jamKerja: b.jamKerja ?? null,
      hourMeter: toFloat(b.hourMeter),
      hourStart: normalizeTime(b.hourStart) ?? null,
      hourEnd: normalizeTime(b.hourEnd) ?? null,
      // ✅ audit
      createBy: actorUsername,
      checkBy1: null,
      checkBy2: null,
      approveBy: null,
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
    const result = await hotStampingService.createHotStampingProduksi(payload, {
      actorId,
      actorUsername,
      requestId,
    });

    return res.status(201).json({
      success: true,
      message: "HotStamping_h created",
      data: result.header,
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[HotStamping][createHotStampingProduksi]", err);
    const status = err.statusCode || err.status || 500;
    return res.status(status).json({
      success: false,
      message:
        status === 500 ? "Internal Server Error" : err.message || "Error",
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
      },
      meta:
        err.actorId && err.actorUsername
          ? {
              actorId: err.actorId,
              actorUsername: err.actorUsername,
              requestId: err.requestId,
            }
          : undefined,
    });
  }
}

async function updateProduksi(req, res) {
  try {
    // ===============================
    // Audit context
    // ===============================
    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (actorId missing)" });
    }

    const actorUsername =
      getActorUsername(req) || req.username || req.user?.username || "system";
    const requestId = String(makeRequestId(req) || "").trim();
    if (requestId) res.setHeader("x-request-id", requestId);

    // ===============================
    // Get noProduksi
    // ===============================
    const noProduksi = String(req.params.noProduksi || "").trim();
    if (!noProduksi) {
      return res
        .status(400)
        .json({ success: false, message: "noProduksi wajib" });
    }

    // ===============================
    // Body tanpa audit fields dari client
    // ===============================
    const b = req.body && typeof req.body === "object" ? req.body : {};
    const {
      actorId: _cActorId,
      actorUsername: _cActorUsername,
      actor: _cActor,
      requestId: _cRequestId,
      createBy: _cCreateBy,
      ...body
    } = b;

    // ===============================
    // Build payload normalized
    // ===============================
    const payload = {
      tglProduksi: body.tglProduksi, // undefined | null | 'YYYY-MM-DD'

      idMesin: toIntUndef(body.idMesin),
      idOperator: toIntUndef(body.idOperator),
      shift: toIntUndef(body.shift),

      jamKerja: body.jamKerja ?? undefined, // optional parse later

      hourMeter: toFloatUndef(body.hourMeter),
      hourStart: toStrUndef(body.hourStart),
      hourEnd: toStrUndef(body.hourEnd),

      checkBy1: toStrUndef(body.checkBy1),
      checkBy2: toStrUndef(body.checkBy2),
      approveBy: toStrUndef(body.approveBy),
    };

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await hotStampingService.updateHotStampingProduksi(
      noProduksi,
      payload,
      { actorId, actorUsername, requestId },
    );

    return res.status(200).json({
      success: true,
      message: "HotStamping_h updated",
      data: result.header,
      meta: { audit: result.audit },
    });
  } catch (err) {
    console.error("[HotStamping][updateHotStampingProduksi]", err);
    const status = err.statusCode || err.status || 500;
    return res.status(status).json({
      success: false,
      message: status === 500 ? "Internal Server Error" : err.message,
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
      },
      meta:
        err.actorId && err.actorUsername
          ? {
              actorId: err.actorId,
              actorUsername: err.actorUsername,
              requestId: err.requestId,
            }
          : undefined,
    });
  }
}

async function deleteProduksi(req, res) {
  try {
    // ===============================
    // Audit context
    // ===============================
    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (actorId missing)" });
    }

    const actorUsername =
      getActorUsername(req) || req.username || req.user?.username || "system";

    const requestId = String(makeRequestId(req) || "").trim();
    if (requestId) res.setHeader("x-request-id", requestId);

    // ===============================
    // Get noProduksi
    // ===============================
    const noProduksi = String(req.params.noProduksi || "").trim();
    if (!noProduksi) {
      return res
        .status(400)
        .json({ success: false, message: "noProduksi wajib" });
    }

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await hotStampingService.deleteHotStampingProduksi(
      noProduksi,
      {
        actorId,
        actorUsername,
        requestId,
      },
    );

    return res.status(200).json({
      success: true,
      message: "PackingProduksi_h deleted",
      meta: { audit: result.audit },
    });
  } catch (err) {
    console.error("[Packing][deletePackingProduksi]", err);
    const status = err.statusCode || err.status || 500;

    return res.status(status).json({
      success: false,
      message: status === 500 ? "Internal Server Error" : err.message,
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
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
    const data = await hotStampingService.fetchInputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[hotstamp.getInputsByNoProduksi]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

async function validateFwipLabel(req, res) {
  const labelCode = String(req.params.labelCode || "").trim();

  if (!labelCode) {
    return res.status(400).json({
      success: false,
      message: "labelCode is required",
    });
  }

  try {
    const result = await hotStampingService.validateFwipLabel(labelCode);

    if (!result.found) {
      return res.status(404).json({
        success: false,
        message: `FWIP label ${labelCode} not found or already used`,
        tableName: result.tableName,
      });
    }

    return res.status(200).json({
      success: true,
      message: "FWIP label validated successfully",
      tableName: result.tableName,
      totalRecords: result.count,
      data: result.data,
    });
  } catch (e) {
    console.error("[validateFwipLabel]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
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

  // ✅ Pastikan body object
  const body = req.body && typeof req.body === "object" ? req.body : {};

  // ✅ Strip client audit fields (jangan percaya dari client)
  const {
    actorId: _clientActorId,
    actorUsername: _clientActorUsername,
    actor: _clientActor,
    requestId: _clientRequestId,
    ...payload
  } = body;

  // ✅ Get trusted audit context from token/session
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

  // Optional: echo header for tracing
  if (requestId) res.setHeader("x-request-id", requestId);

  // ✅ Validate: at least one input exists
  const hasInput = [
    "furnitureWip",
    "cabinetMaterial",
    "furnitureWipPartial",
  ].some(
    (key) =>
      payload[key] && Array.isArray(payload[key]) && payload[key].length > 0,
  );

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: "Tidak ada data input yang diberikan",
      error: {
        message:
          "Request body harus berisi minimal satu array input yang tidak kosong",
      },
    });
  }

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await hotStampingService.upsertInputsAndPartials(
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
      const totalUpdated = Number(data?.summary?.totalUpdated ?? 0); // ✅ Support UPSERT
      const totalPartialsCreated = Number(
        data?.summary?.totalPartialsCreated ?? 0,
      );

      if (totalInvalid > 0) {
        statusCode = 422;
        message = "Beberapa data tidak valid";
      } else if (
        totalInserted + totalUpdated === 0 &&
        totalPartialsCreated === 0
      ) {
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
    console.error("[inject.upsertInputsAndPartials]", e);
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

  // ✅ Validate input
  const hasInput = [
    "furnitureWip",
    "cabinetMaterial",
    "furnitureWipPartial",
  ].some(
    (key) =>
      payload[key] && Array.isArray(payload[key]) && payload[key].length > 0,
  );

  if (!hasInput) {
    return res.status(400).json({
      success: false,
      message: "Tidak ada data input yang diberikan",
      error: {
        message:
          "Request body harus berisi minimal satu array input yang tidak kosong",
      },
    });
  }

  try {
    // ✅ Forward audit context
    const ctx = { actorId, actorUsername, requestId };

    const result = await hotStampingService.deleteInputsAndPartials(
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
    console.error("[inject.deleteInputsAndPartials]", e);
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
  validateFwipLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

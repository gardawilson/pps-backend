// controllers/sortir-reject-controller.js
const sortirRejectService = require("./sortir-reject-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../../core/utils/http-context");

async function getAllSortirReject(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support ?noBJSortir= or ?search=
  const search =
    (typeof req.query.noBJSortir === "string" && req.query.noBJSortir) ||
    (typeof req.query.search === "string" && req.query.search) ||
    "";

  // OPTIONAL date range
  // Example: ?dateFrom=2025-12-01&dateTo=2025-12-31
  const dateFrom =
    (typeof req.query.dateFrom === "string" && req.query.dateFrom) || null;
  const dateTo =
    (typeof req.query.dateTo === "string" && req.query.dateTo) || null;

  try {
    const { data, total } = await sortirRejectService.getAllSortirReject(
      page,
      pageSize,
      search,
      dateFrom,
      dateTo,
    );

    return res.status(200).json({
      success: true,
      message: "BJSortirReject_h retrieved successfully",
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search,
        dateFrom,
        dateTo,
      },
    });
  } catch (error) {
    console.error("Error fetching BJSortirReject_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function getSortirRejectByDate(req, res) {
  const { username } = req; // dari verifyToken
  const date = req.params.date;

  console.log(
    "🔍 Fetching BJSortirReject_h | Username:",
    username,
    "| date:",
    date,
  );

  try {
    const data = await sortirRejectService.getSortirRejectByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No BJSortirReject_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `BJSortirReject_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching BJSortirReject_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function createSortirReject(req, res) {
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
    // Body
    // ===============================
    const body = req.body && typeof req.body === "object" ? req.body : {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === "") return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : Math.trunc(n);
    };

    // 🔑 idUsername DIAMBIL DARI TOKEN
    const idUsername = toInt(req.idUsername ?? req.user?.idUsername) ?? null;

    // ===============================
    // Payload business
    // ===============================
    const payload = {
      tglBJSortir: body.tglBJSortir,
      idWarehouse: toInt(body.idWarehouse),
      idUsername, // 🔥 injected, bukan dari client
    };

    // ===============================
    // Validation
    // ===============================
    const must = [];
    if (!payload.tglBJSortir) must.push("tglBJSortir");
    if (payload.idWarehouse == null) must.push("idWarehouse");
    if (payload.idUsername == null) must.push("idUsername");

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
    const result = await sortirRejectService.createSortirReject(payload, {
      actorId,
      actorUsername,
      requestId,
    });

    return res.status(201).json({
      success: true,
      message: "BJSortirReject_h created",
      data: result.header,
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[SortirReject][create]", err);
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

async function updateSortirReject(req, res) {
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
    // Get noBJSortir
    // ===============================
    const noBJSortir = String(req.params.noBJSortir || "").trim();
    if (!noBJSortir) {
      return res
        .status(400)
        .json({ success: false, message: "noBJSortir wajib" });
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
      updateBy: _cUpdateBy,
      ...body
    } = b;

    // ===============================
    // Helper normalizer
    // ===============================
    const toIntUndef = (v) => {
      if (v === undefined) return undefined;
      if (v === null || v === "") return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : Math.trunc(n);
    };

    // ===============================
    // Build payload normalized
    // ===============================
    const payload = {
      // header
      tglBJSortir: body.tglBJSortir ?? undefined, // distinguish not-sent vs null
      idWarehouse: toIntUndef(body.idWarehouse),

      // jika nantinya service mendukung update mapping
      barangJadi: Array.isArray(body.barangJadi) ? body.barangJadi : undefined,

      furnitureWip: Array.isArray(body.furnitureWip)
        ? body.furnitureWip
        : undefined,
    };

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await sortirRejectService.updateSortirReject(
      noBJSortir,
      payload,
      { actorId, actorUsername, requestId },
    );

    return res.status(200).json({
      success: true,
      message: "BJSortirReject_h updated",
      data: result.header,
      meta: { audit: result.audit },
    });
  } catch (err) {
    console.error("[SortirReject][updateSortirReject]", err);
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

async function deleteSortirReject(req, res) {
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
    // Get noBJSortir
    // ===============================
    const noBJSortir = String(req.params.noBJSortir || "").trim();
    if (!noBJSortir) {
      return res
        .status(400)
        .json({ success: false, message: "noBJSortir wajib" });
    }

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await sortirRejectService.deleteSortirReject(noBJSortir, {
      actorId,
      actorUsername,
      requestId,
    });

    return res.status(200).json({
      success: true,
      message: "BJSortirReject_h deleted",
      meta: { audit: result.audit },
    });
  } catch (err) {
    console.error("[SortirReject][deleteSortirReject]", err);
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

async function getInputsByNoBJSortir(req, res) {
  const noBJSortir = String(req.params.noBJSortir || "").trim();
  if (!noBJSortir) {
    return res
      .status(400)
      .json({ success: false, message: "noBJSortir is required" });
  }

  try {
    const data = await sortirRejectService.fetchInputs(noBJSortir);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[sortirReject.getInputsByNoBJSortir]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

async function getOutputsRejectByNoBJSortir(req, res) {
  const noBJSortir = String(req.params.noBJSortir || "").trim();
  if (!noBJSortir) {
    return res
      .status(400)
      .json({ success: false, message: "noBJSortir is required" });
  }

  try {
    const data = await sortirRejectService.fetchOutputsReject(noBJSortir);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[sortirReject.getOutputsRejectByNoBJSortir]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

async function upsertInputs(req, res) {
  const noProduksi = String(req.params.noBJSortir || "").trim();

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
  const hasInput = ["furnitureWip", "cabinetMaterial", "barangJadi"].some(
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

    const result = await sortirRejectService.upsertInputs(
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

async function deleteInputs(req, res) {
  const noProduksi = String(req.params.noBJSortir || "").trim();

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
  const hasInput = ["furnitureWip", "cabinetMaterial", "barangJadi"].some(
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

    const result = await sortirRejectService.deleteInputs(
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
  getAllSortirReject,
  getSortirRejectByDate,
  createSortirReject,
  updateSortirReject,
  deleteSortirReject,
  getInputsByNoBJSortir,
  getOutputsRejectByNoBJSortir,
  upsertInputs,
  deleteInputs,
};

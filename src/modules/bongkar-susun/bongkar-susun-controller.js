// controllers/bongkar-susun-controller.js
const bongkarSusunService = require("./bongkar-susun-service");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../core/utils/http-context");
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
} = require("../../core/utils/parse");

async function getByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah tervalidasi formatnya oleh route regex
  console.log(
    "🔍 Fetching BongkarSusun_h | Username:",
    username,
    "| date:",
    date,
  );

  try {
    const data = await bongkarSusunService.getByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data BongkarSusun_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data BongkarSusun_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching BongkarSusun_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function getAllBongkarSusun(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support ?noBongkarSusun= dan ?search=
  const search =
    (typeof req.query.noBongkarSusun === "string" &&
      req.query.noBongkarSusun) ||
    (typeof req.query.search === "string" && req.query.search) ||
    "";

  try {
    const { data, total } = await bongkarSusunService.getAllBongkarSusun(
      page,
      pageSize,
      search,
    );

    return res.status(200).json({
      success: true,
      message: "BongkarSusun_h retrieved successfully",
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
    console.error("Error fetching BongkarSusun_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function createBongkarSusun(req, res) {
  try {
    // ===============================
    // Audit context
    // ===============================
    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({
        success: false,
        message: "Unauthorized (actorId missing)",
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
    const { createBy: _cCreateBy, username: _usernameFromClient, ...b } = body;

    // ===============================
    // Payload business
    // ===============================
    const payload = {
      tanggal: b.tanggal || b.tgl || null, // required
      username: req.username || req.user?.username || null, // di-resolve ke IdUsername di service
      note: b.note ?? null,
    };

    // ===============================
    // Quick validation
    // ===============================
    const must = [];
    if (!payload.tanggal) must.push("tanggal");
    if (!payload.username) must.push("username");

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
    const result = await bongkarSusunService.createBongkarSusun(payload, {
      actorId,
      actorUsername,
      requestId,
    });

    return res.status(201).json({
      success: true,
      message: "BongkarSusun_h created",
      data: result.header,
      meta: {
        audit: {
          actorId,
          actorUsername,
          requestId,
        },
      },
    });
  } catch (err) {
    console.error("[BongkarSusun][createBongkarSusun]", err);

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

async function updateBongkarSusun(req, res) {
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
    // Get noBongkarSusun
    // ===============================
    const noBongkarSusun = String(req.params.noBongkarSusun || "").trim();
    if (!noBongkarSusun) {
      return res
        .status(400)
        .json({ success: false, message: "noBongkarSusun wajib" });
    }

    // ===============================
    // Raw body
    // ===============================
    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === "") return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    // ===============================
    // Build header payload
    //   — explicit if-check per field supaya key yang tidak
    //     dikirim client TIDAK masuk object sama sekali.
    //     Ini penting: _updateHeaderWithTx pakai hasOwnProperty
    //     untuk putus apakah field diupdate atau tidak.
    // ===============================
    const headerPayload = {};

    if (b.tanggal !== undefined) {
      headerPayload.tanggal = b.tanggal;
    }

    if (b.idUsername !== undefined) {
      headerPayload.idUsername = toInt(b.idUsername);
    }

    if (b.note !== undefined) {
      headerPayload.note = b.note === "" ? null : b.note;
    }

    // ===============================
    // Inputs payload (terpisah dari header)
    // ===============================
    const inputsPayloadOrNull = b.inputs ?? null;

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await bongkarSusunService.updateBongkarSusunCascade(
      noBongkarSusun,
      headerPayload,
      inputsPayloadOrNull,
      {
        actorId,
        actorUsername,
        requestId,
      },
    );

    return res.status(200).json({
      success: true,
      message: "BongkarSusun updated",
      data: result.header,
      meta: {
        audit: result.audit,
        inputs: result.inputs,
      },
    });
  } catch (err) {
    console.error("[BongkarSusun][updateBongkarSusun]", err);
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

async function deleteBongkarSusun(req, res) {
  try {
    // ===============================
    // Audit context
    // ===============================
    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({
        success: false,
        message: "Unauthorized (actorId missing)",
      });
    }

    const actorUsername =
      getActorUsername(req) || req.username || req.user?.username || "system";

    const requestId = String(makeRequestId(req) || "").trim();
    if (requestId) res.setHeader("x-request-id", requestId);

    // ===============================
    // Get noBongkarSusun
    // ===============================
    const noBongkarSusun = String(req.params.noBongkarSusun || "").trim();
    if (!noBongkarSusun) {
      return res.status(400).json({
        success: false,
        message: "noBongkarSusun wajib",
      });
    }

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await bongkarSusunService.deleteBongkarSusun(
      noBongkarSusun,
      {
        actorId,
        actorUsername,
        requestId,
      },
    );

    return res.status(200).json({
      success: true,
      message: "BongkarSusun_h deleted",
      meta: { audit: result.audit },
    });
  } catch (err) {
    console.error("[BongkarSusun][deleteBongkarSusun]", err);
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

// controllers/bongkarSusunController.js
async function getInputsByNoBongkarSusun(req, res) {
  const noBongkarSusun = (req.params.noBongkarSusun || "").trim();
  if (!noBongkarSusun) {
    return res
      .status(400)
      .json({ success: false, message: "noBongkarSusun is required" });
  }

  try {
    const data = await bongkarSusunService.fetchInputs(noBongkarSusun);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[getInputsByNoBongkarSusun]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

async function getOutputsByNoBongkarSusun(req, res) {
  const noBongkarSusun = (req.params.noBongkarSusun || "").trim();
  if (!noBongkarSusun) {
    return res
      .status(400)
      .json({ success: false, message: "noBongkarSusun is required" });
  }

  try {
    const data = await bongkarSusunService.fetchOutputs(noBongkarSusun);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[getOutputsByNoBongkarSusun]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

async function getOutputList(req, res, fetchFn, errorTag) {
  const noBongkarSusun = (req.params.noBongkarSusun || "").trim();
  if (!noBongkarSusun) {
    return res
      .status(400)
      .json({ success: false, message: "noBongkarSusun is required" });
  }

  try {
    const data = await fetchFn(noBongkarSusun);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error(errorTag, e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

async function getOutputsBbByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsBb,
    "[getOutputsBbByNoBongkarSusun]",
  );
}

async function getOutputsBarangJadiByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsBarangJadi,
    "[getOutputsBarangJadiByNoBongkarSusun]",
  );
}

async function getOutputsBonggolanByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsBonggolan,
    "[getOutputsBonggolanByNoBongkarSusun]",
  );
}

async function getOutputsBrokerByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsBroker,
    "[getOutputsBrokerByNoBongkarSusun]",
  );
}

async function getOutputsCrusherByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsCrusher,
    "[getOutputsCrusherByNoBongkarSusun]",
  );
}

async function getOutputsFurnitureWipByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsFurnitureWip,
    "[getOutputsFurnitureWipByNoBongkarSusun]",
  );
}

async function getOutputsGilinganByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsGilingan,
    "[getOutputsGilinganByNoBongkarSusun]",
  );
}

async function getOutputsMixerByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsMixer,
    "[getOutputsMixerByNoBongkarSusun]",
  );
}

async function getOutputsWashingByNoBongkarSusun(req, res) {
  return getOutputList(
    req,
    res,
    bongkarSusunService.fetchOutputsWashing,
    "[getOutputsWashingByNoBongkarSusun]",
  );
}

async function validateLabel(req, res) {
  const { labelCode } = req.params;

  if (!labelCode || typeof labelCode !== "string") {
    return res.status(400).json({
      success: false,
      message: "Label number is required and must be a string",
    });
  }

  try {
    const result =
      await bongkarSusunService.validateLabelBongkarSusun(labelCode);

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
      data: result.data,
    });
  } catch (error) {
    console.error("Error validating label (BongkarSusun):", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function upsertInputs(req, res) {
  const noProduksi = String(req.params.noBongkarSusun || "").trim();

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
    "broker",
    "bb",
    "washing",
    "crusher",
    "gilingan",
    "mixer",
    "bonggolan",
    "furnitureWip",
    "barangJadi",
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

    const result = await bongkarSusunService.upsertInputs(
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
  const noProduksi = String(req.params.noBongkarSusun || "").trim();

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
    "broker",
    "bb",
    "washing",
    "crusher",
    "gilingan",
    "mixer",
    "bonggolan",
    "furnitureWip",
    "barangJadi",
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

    const result = await bongkarSusunService.deleteInputs(
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
  getByDate,
  getAllBongkarSusun,
  createBongkarSusun,
  updateBongkarSusun,
  deleteBongkarSusun,
  getInputsByNoBongkarSusun,
  getOutputsByNoBongkarSusun,
  getOutputsBbByNoBongkarSusun,
  getOutputsBarangJadiByNoBongkarSusun,
  getOutputsBonggolanByNoBongkarSusun,
  getOutputsBrokerByNoBongkarSusun,
  getOutputsCrusherByNoBongkarSusun,
  getOutputsFurnitureWipByNoBongkarSusun,
  getOutputsGilinganByNoBongkarSusun,
  getOutputsMixerByNoBongkarSusun,
  getOutputsWashingByNoBongkarSusun,
  validateLabel,
  upsertInputs,
  deleteInputs,
};

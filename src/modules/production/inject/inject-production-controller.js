// controllers/inject-production-controller.js
const injectProduksiService = require("./inject-production-service");
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

// ------------------------------------------------------------
// ✅ GET ALL (paged)
// ------------------------------------------------------------
async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  const search =
    (typeof req.query.noProduksi === "string" && req.query.noProduksi) ||
    (typeof req.query.search === "string" && req.query.search) ||
    "";

  try {
    const { data, total } = await injectProduksiService.getAllProduksi(
      page,
      pageSize,
      search,
    );

    return res.status(200).json({
      success: true,
      message: "InjectProduksi_h retrieved successfully",
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
    console.error("Error fetching InjectProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// 🔹 GET InjectProduksi_h by date (YYYY-MM-DD)
// ------------------------------------------------------------
async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log(
    "🔍 Fetching InjectProduksi_h | Username:",
    username,
    "| date:",
    date,
  );

  try {
    const data = await injectProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No InjectProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `InjectProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching InjectProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// 🔹 GET FurnitureWIP info from InjectProduksi_h by NoProduksi
// ------------------------------------------------------------
async function getFurnitureWipByNoProduksi(req, res) {
  const { username } = req;
  const { noProduksi } = req.params;

  console.log(
    "🔍 Fetching FurnitureWIP from InjectProduksi_h | Username:",
    username,
    "| NoProduksi:",
    noProduksi,
  );

  try {
    const rows =
      await injectProduksiService.getFurnitureWipListByNoProduksi(noProduksi);

    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No InjectProduksi_h / mapping FurnitureWIP found for NoProduksi ${noProduksi}`,
        data: { beratProdukHasilTimbang: null, items: [] },
        meta: { noProduksi },
      });
    }

    const beratProdukHasilTimbang = rows[0].BeratProdukHasilTimbang ?? null;

    const items = rows.map((r) => ({
      IdFurnitureWIP: r.IdFurnitureWIP,
      NamaFurnitureWIP: r.NamaFurnitureWIP,
    }));

    return res.status(200).json({
      success: true,
      message: `FurnitureWIP for NoProduksi ${noProduksi} retrieved successfully`,
      data: { beratProdukHasilTimbang, items },
      meta: { noProduksi },
    });
  } catch (error) {
    console.error("Error fetching FurnitureWIP from InjectProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// 🔹 GET BarangJadi info (Packing) from InjectProduksi_h by NoProduksi
// ------------------------------------------------------------
async function getPackingByNoProduksi(req, res) {
  const { username } = req;
  const { noProduksi } = req.params;

  console.log(
    "🔍 Fetching BarangJadi (Packing) from InjectProduksi_h | Username:",
    username,
    "| NoProduksi:",
    noProduksi,
  );

  try {
    const rows =
      await injectProduksiService.getPackingListByNoProduksi(noProduksi);

    if (!rows || rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `No InjectProduksi_h / mapping Produk (BarangJadi) found for NoProduksi ${noProduksi}`,
        data: { beratProdukHasilTimbang: null, items: [] },
        meta: { noProduksi },
      });
    }

    const beratProdukHasilTimbang = rows[0].BeratProdukHasilTimbang ?? null;

    const items = rows.map((r) => ({
      IdBJ: r.IdBJ,
      NamaBJ: r.NamaBJ,
    }));

    return res.status(200).json({
      success: true,
      message: `BarangJadi (Packing) for NoProduksi ${noProduksi} retrieved successfully`,
      data: { beratProdukHasilTimbang, items },
      meta: { noProduksi },
    });
  } catch (error) {
    console.error(
      "Error fetching BarangJadi (Packing) from InjectProduksi_h:",
      error,
    );
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

// ------------------------------------------------------------
// ✅ POST Create InjectProduksi_h
// ------------------------------------------------------------
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
      actorId: _cActorId,
      actorUsername: _cActorUsername,
      actor: _cActor,
      requestId: _cRequestId,
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
      jam: toInt(b.jam),

      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),

      idCetakan: toInt(b.idCetakan),
      idWarna: toInt(b.idWarna),

      enableOffset: toBit(b.enableOffset) ?? 0,
      offsetCurrent: toInt(b.offsetCurrent),
      offsetNext: toInt(b.offsetNext),

      idFurnitureMaterial: toInt(b.idFurnitureMaterial),

      hourMeter: toFloat(b.hourMeter),
      beratProdukHasilTimbang: toFloat(b.beratProdukHasilTimbang),

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
    const result = await injectProduksiService.createInjectProduksi(payload, {
      actorId,
      actorUsername,
      requestId,
    });
    const header = result?.header ?? result;

    return res.status(201).json({
      success: true,
      message: "InjectProduksi_h created",
      data: header,
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[Inject][createProduksi]", err);
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

// ------------------------------------------------------------
// ✅ PUT Update InjectProduksi_h (dynamic fields)
// ------------------------------------------------------------
async function updateProduksi(req, res) {
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
    const b = req.body || {};
    const {
      actorId: _cActorId,
      actorUsername: _cActorUsername,
      actor: _cActor,
      requestId: _cRequestId,
      ...body
    } = b;

    // ===============================
    // Build payload normalized
    // ===============================
    const payload = {
      tglProduksi: body.tglProduksi, // undefined | 'YYYY-MM-DD' | null

      idMesin: toIntUndef(body.idMesin),
      idOperator: toIntUndef(body.idOperator),
      shift: toIntUndef(body.shift),

      jam: toJamInt(body.jam),

      jmlhAnggota: toIntUndef(body.jmlhAnggota),
      hadir: toIntUndef(body.hadir),

      idCetakan: toIntUndef(body.idCetakan),
      idWarna: toIntUndef(body.idWarna),

      enableOffset: toBitUndef(body.enableOffset),
      offsetCurrent: toIntUndef(body.offsetCurrent),
      offsetNext: toIntUndef(body.offsetNext),

      idFurnitureMaterial: toIntUndef(body.idFurnitureMaterial),

      hourMeter: toFloatUndef(body.hourMeter),
      beratProdukHasilTimbang: toFloatUndef(body.beratProdukHasilTimbang),

      hourStart: toStrUndef(body.hourStart),
      hourEnd: toStrUndef(body.hourEnd),

      checkBy1: toStrUndef(body.checkBy1),
      checkBy2: toStrUndef(body.checkBy2),
      approveBy: toStrUndef(body.approveBy),
    };

    // ===============================
    // Call service with audit context
    // ===============================
    const result = await injectProduksiService.updateInjectProduksi(
      noProduksi,
      payload,
      {
        actorId,
        actorUsername,
        requestId,
      },
    );

    return res.status(200).json({
      success: true,
      message: "InjectProduksi_h updated",
      data: result.header,
      meta: { audit: result.audit },
    });
  } catch (err) {
    console.error("[Inject][updateProduksi]", err);
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

// ------------------------------------------------------------
// ✅ DELETE InjectProduksi_h
// ------------------------------------------------------------
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
    await injectProduksiService.deleteInjectProduksi(noProduksi, ctx);

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
    const data = await injectProduksiService.fetchInputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[inject.getInputsByNoProduksi]", e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || "Internal Server Error",
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
    const data = await injectProduksiService.fetchOutputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[inject.getOutputsByNoProduksi]", e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || "Internal Server Error",
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
    const data = await injectProduksiService.fetchOutputsBonggolan(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[inject.getOutputsBonggolanByNoProduksi]", e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || "Internal Server Error",
    });
  }
}

async function getOutputsFurnitureWipByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }

  try {
    const data =
      await injectProduksiService.fetchOutputsFurnitureWip(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[inject.getOutputsFurnitureWipByNoProduksi]", e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || "Internal Server Error",
    });
  }
}

async function getOutputsPackingByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }

  try {
    const data = await injectProduksiService.fetchOutputsPacking(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[inject.getOutputsPackingByNoProduksi]", e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || "Internal Server Error",
    });
  }
}

async function getOutputsRejectByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();
  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }

  try {
    const data = await injectProduksiService.fetchOutputsReject(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[inject.getOutputsRejectByNoProduksi]", e);
    return res.status(e.statusCode || 500).json({
      success: false,
      message: e.message || "Internal Server Error",
    });
  }
}

async function validateLabel(req, res) {
  const labelCode = String(req.params.labelCode || "").trim();

  if (!labelCode) {
    return res.status(400).json({
      success: false,
      message: "labelCode is required",
    });
  }

  try {
    const result = await injectProduksiService.validateLabel(labelCode);

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
  } catch (e) {
    console.error("[inject.validateLabel]", e);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: e.message,
    });
  }
}

/**
 * ✅ UPSERT Inputs & Partials untuk Inject Production
 * Support: broker, mixer, gilingan, furnitureWip, cabinetMaterial (UPSERT)
 * Support: partials (existing + new)
 */
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
    "broker",
    "mixer",
    "gilingan",
    "furnitureWip",
    "cabinetMaterial",
    "brokerPartial",
    "mixerPartial",
    "gilinganPartial",
    "furnitureWipPartial",
  ].some((k) => Array.isArray(payload?.[k]) && payload[k].length > 0);

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

    const result = await injectProduksiService.upsertInputsAndPartials(
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
    // Full inputs
    "broker",
    "mixer",
    "gilingan",
    "furnitureWip",
    "cabinetMaterial",
    // Existing partial labels
    "brokerPartial",
    "mixerPartial",
    "gilinganPartial",
    "furnitureWipPartial",
  ].some((k) => Array.isArray(payload?.[k]) && payload[k].length > 0);

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

    const result = await injectProduksiService.deleteInputsAndPartials(
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
  getAllProduksi,
  getProduksiByDate,
  getFurnitureWipByNoProduksi,
  getPackingByNoProduksi,
  createProduksi,
  updateProduksi,
  deleteProduksi,
  getInputsByNoProduksi,
  getOutputsByNoProduksi,
  getOutputsBonggolanByNoProduksi,
  getOutputsFurnitureWipByNoProduksi,
  getOutputsPackingByNoProduksi,
  getOutputsRejectByNoProduksi,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

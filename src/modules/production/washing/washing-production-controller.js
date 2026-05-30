// controllers/production-controller.js
const washingProduksiService = require("./washing-production-service");
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

// controller/washingProduksiController.js (misal)
async function getAllProduksi(req, res) {
  // pagination (default 20)
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100); // batasi max 100

  // support both ?noProduksi= and ?search=
  const search =
    (typeof req.query.noProduksi === "string" && req.query.noProduksi) ||
    (typeof req.query.search === "string" && req.query.search) ||
    "";

  const idMesinRaw =
    typeof req.query.idMesin === "string" ? req.query.idMesin.trim() : "";
  const tanggalRaw =
    typeof req.query.tanggal === "string" ? req.query.tanggal.trim() : "";
  const shiftRaw =
    typeof req.query.shift === "string" ? req.query.shift.trim() : "";

  let idMesin = null;
  if (idMesinRaw) {
    const parsedIdMesin = Number(idMesinRaw);
    if (!Number.isInteger(parsedIdMesin) || parsedIdMesin <= 0) {
      return res.status(400).json({
        success: false,
        message: "Query param idMesin harus integer positif",
      });
    }
    idMesin = parsedIdMesin;
  }

  const tanggalRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (tanggalRaw && !tanggalRegex.test(tanggalRaw)) {
    return res.status(400).json({
      success: false,
      message: "Query param tanggal harus format YYYY-MM-DD",
    });
  }

  let shift = null;
  if (shiftRaw) {
    const parsedShift = Number(shiftRaw);
    if (!Number.isInteger(parsedShift) || parsedShift <= 0) {
      return res.status(400).json({
        success: false,
        message: "Query param shift harus integer positif",
      });
    }
    shift = parsedShift;
  }

  try {
    const { data, total } = await washingProduksiService.getAllProduksi(
      page,
      pageSize,
      search,
      idMesin,
      tanggalRaw || null,
      shift,
    );

    return res.status(200).json({
      success: true,
      message: "WashingProduksi_h retrieved successfully",
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search, // echo back untuk state di client (sama seperti broker)
        idMesin,
        tanggal: tanggalRaw || null,
        shift,
      },
    });
  } catch (error) {
    console.error("Error fetching WashingProduksi_h:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah match regex di route
  console.log(
    "🔍 Fetching WashingProduksi_h | Username:",
    username,
    "| date:",
    date,
  );

  try {
    const data = await washingProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data WashingProduksi_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data WashingProduksi_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error("Error fetching WashingProduksi_h:", error);
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

  const toIntArray = (v) => {
    if (v === undefined || v === null || v === "") return [];
    const raw = Array.isArray(v) ? v : String(v).split(",");
    return [
      ...new Set(
        raw
          .map((x) => Number(String(x).trim()))
          .filter((n) => Number.isFinite(n))
          .map((n) => Math.trunc(n))
          .filter((n) => n > 0),
      ),
    ];
  };
  const operatorIds = toIntArray(
    b.idOperators !== undefined ? b.idOperators : b.idOperator,
  );

  // ✅ payload business (tanpa audit fields)
  const payload = {
    tglProduksi: b.tglProduksi, // 'YYYY-MM-DD'
    idMesin: toInt(b.idMesin), // number
    idOperators: operatorIds, // multi operator for detail table
    idOperator: operatorIds[0] ?? null, // backward compat: header operator utama
    outputJenisId: toInt(b.outputJenisId), // WashingProduksi_h.OutputJenisId
    idRegu: toInt(b.idRegu), // WashingProduksi_h.IdRegu

    // washing pakai jamKerja
    jamKerja: b.jamKerja, // number atau 'HH:mm-HH:mm'
    shift: toInt(b.shift), // number

    createBy: actorUsername, // controller overwrite dari token

    checkBy1: b.checkBy1 ?? null,
    checkBy2: b.checkBy2 ?? null,
    approveBy: b.approveBy ?? null,
    jmlhAnggota: toInt(b.jmlhAnggota),
    hadir: toInt(b.hadir),
    hourMeter: toFloat(b.hourMeter),
    isBlower: toBit(b.isBlower),

    hourStart: normalizeTime(b.hourStart) ?? null,
    hourEnd: normalizeTime(b.hourEnd) ?? null,
  };

  // optional: validasi cepat agar error 400 rapih (service juga akan validasi)
  const must = [];
  if (!payload.tglProduksi) must.push("tglProduksi");
  if (payload.idMesin == null) must.push("idMesin");
  if (!Array.isArray(payload.idOperators) || payload.idOperators.length === 0) {
    must.push("idOperator");
  }
  if (payload.outputJenisId == null) must.push("outputJenisId");
  if (payload.jamKerja == null) must.push("jamKerja");
  if (payload.shift == null) must.push("shift");
  if (must.length) {
    return res.status(400).json({
      success: false,
      message: `Field wajib: ${must.join(", ")}`,
      error: { fields: must },
    });
  }

  if (b.isBlower !== undefined && payload.isBlower == null) {
    return res.status(400).json({
      success: false,
      message: "Field isBlower harus bernilai 1 atau 0",
      error: { fields: ["isBlower"] },
    });
  }

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ pastikan signature service: (payload, ctx)
    const result = await washingProduksiService.createWashingProduksi(
      payload,
      ctx,
    );
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
    console.error("[Washing][createProduksi]", err);
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

  // ✅ request id per HTTP request
  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId) res.setHeader("x-request-id", requestId);
  // ===============================
  // payload business (PARTIAL OK)
  // ===============================
  const payload = {
    tglProduksi: b.tglProduksi,

    idMesin: b.idMesin !== undefined ? toInt(b.idMesin) : undefined,
    idOperator: b.idOperator !== undefined ? toInt(b.idOperator) : undefined,

    jamKerja:
      b.jam !== undefined
        ? b.jam
        : b.jamKerja !== undefined
          ? b.jamKerja
          : undefined,

    shift: b.shift !== undefined ? toInt(b.shift) : undefined,

    checkBy1: b.checkBy1 !== undefined ? (b.checkBy1 ?? null) : undefined,
    checkBy2: b.checkBy2 !== undefined ? (b.checkBy2 ?? null) : undefined,
    approveBy: b.approveBy !== undefined ? (b.approveBy ?? null) : undefined,

    jmlhAnggota: b.jmlhAnggota !== undefined ? toInt(b.jmlhAnggota) : undefined,
    hadir: b.hadir !== undefined ? toInt(b.hadir) : undefined,

    hourMeter: b.hourMeter !== undefined ? toFloat(b.hourMeter) : undefined,
    isBlower: b.isBlower !== undefined ? toBitUndef(b.isBlower) : undefined,

    hourStart:
      b.hourStart !== undefined ? normalizeTime(b.hourStart) : undefined,
    hourEnd: b.hourEnd !== undefined ? normalizeTime(b.hourEnd) : undefined,
  };

  // optional: validasi cepat agar error 400 rapih
  const hasAnyField = Object.values(payload).some((v) => v !== undefined);
  if (!hasAnyField) {
    return res.status(400).json({
      success: false,
      message: "No fields to update",
      error: { fields: [] },
    });
  }

  if (b.isBlower !== undefined && payload.isBlower == null) {
    return res.status(400).json({
      success: false,
      message: "Field isBlower harus bernilai 1 atau 0",
      error: { fields: ["isBlower"] },
    });
  }

  try {
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ signature service: (noProduksi, payload, ctx)
    const result = await washingProduksiService.updateWashingProduksi(
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
    console.error("[Crusher][updateProduksi]", err);
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

  const actorUsername =
    getActorUsername(req) || req.username || req.user?.username || "system";

  const requestId = String(makeRequestId(req) || "").trim();
  if (requestId) res.setHeader("x-request-id", requestId);

  try {
    const ctx = { actorId, actorUsername, requestId };

    // ⚠️ pastikan signature service: (noProduksi, ctx)
    const result = await washingProduksiService.deleteWashingProduksi(
      noProduksi,
      ctx,
    );

    return res.status(200).json({
      success: true,
      message: "Deleted",
      data: result?.header ?? undefined,
      meta: {
        audit: { actorId, actorUsername, requestId },
      },
    });
  } catch (err) {
    console.error("[Washing][deleteProduksi]", err);
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

async function getInputsByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || "").trim();

  if (!noProduksi) {
    return res
      .status(400)
      .json({ success: false, message: "noProduksi is required" });
  }

  try {
    const data = await washingProduksiService.fetchInputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Inputs retrieved", data });
  } catch (e) {
    console.error("[washing.getInputsByNoProduksi]", e);
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
    const data = await washingProduksiService.fetchOutputs(noProduksi);
    return res
      .status(200)
      .json({ success: true, message: "Outputs retrieved", data });
  } catch (e) {
    console.error("[washing.getOutputsByNoProduksi]", e);
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
    const result = await washingProduksiService.validateLabel(labelCode);

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
      data: result.data, // Returns array of all matching records
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

  const forbiddenNewKeys = Object.keys(payload).filter((k) =>
    /new$/i.test(String(k)),
  );
  if (forbiddenNewKeys.length > 0) {
    return res.status(400).json({
      success: false,
      message: `Key payload tidak diizinkan: ${forbiddenNewKeys.join(", ")}. Gunakan format standar tanpa suffix 'New' (contoh: gilinganPartial).`,
    });
  }

  // optional validate: at least one input exists
  const hasInput = [
    "bb",
    "bbPartial",
    "washing",
    "gilingan",
    "gilinganPartial",
  ].some((key) => Array.isArray(payload?.[key]) && payload[key].length > 0);

  // if (!hasInput) { ... } // kalau mau strict, aktifkan lagi

  try {
    // ✅ Forward audit context ke service
    const ctx = { actorId, actorUsername, requestId };

    const result = await washingProduksiService.upsertInputsAndPartials(
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
    "bb",
    "washing",
    "gilingan",
    "bbPartial",
    "gilinganPartial",
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

    const result = await washingProduksiService.deleteInputsAndPartials(
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

async function splitProduksiTime(req, res) {
  const normalizeSqlTimeToHms = (value) => {
    if (value == null) return value;
    if (value instanceof Date) {
      const hh = String(value.getUTCHours()).padStart(2, "0");
      const mm = String(value.getUTCMinutes()).padStart(2, "0");
      const ss = String(value.getUTCSeconds()).padStart(2, "0");
      return `${hh}:${mm}:${ss}`;
    }
    const raw = String(value).trim();
    const m = /(\d{2}):(\d{2}):(\d{2})/.exec(raw);
    return m ? `${m[1]}:${m[2]}:${m[3]}` : raw;
  };

  const idMesinRaw = String(req.params.idMesin || "").trim();
  const tanggal = String(req.params.tanggal || "").trim();

  const idMesin = Number(idMesinRaw);
  if (!Number.isInteger(idMesin) || idMesin <= 0) {
    return res
      .status(400)
      .json({ success: false, message: "idMesin harus integer positif" });
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(tanggal)) {
    return res.status(400).json({
      success: false,
      message: "tanggal harus format YYYY-MM-DD",
    });
  }

  const body = req.body && typeof req.body === "object" ? req.body : {};
  const hourStart = String(body.hourStart || "").trim();
  const outputJenisId = Number(body.outputJenisId);

  if (!hourStart) {
    return res.status(400).json({
      success: false,
      message: "hourStart wajib diisi",
    });
  }
  const timeRegex = /^\d{2}:\d{2}(:\d{2})?$/;
  if (!timeRegex.test(hourStart)) {
    return res.status(400).json({
      success: false,
      message: "Format hourStart harus HH:mm atau HH:mm:ss",
    });
  }
  if (!Number.isInteger(outputJenisId) || outputJenisId <= 0) {
    return res.status(400).json({
      success: false,
      message: "outputJenisId wajib integer positif",
    });
  }

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

  try {
    const ctx = { actorId, actorUsername, requestId };
    const result = await washingProduksiService.splitProduksiTime(
      { idMesin, tanggal },
      { hourStart, outputJenisId },
      ctx,
    );

    const header = result?.header
      ? {
          ...result.header,
          HourStart: normalizeSqlTimeToHms(result.header.HourStart),
          HourEnd: normalizeSqlTimeToHms(result.header.HourEnd),
        }
      : result?.header;

    return res.status(201).json({
      success: true,
      message: "Produksi berhasil di-split",
      data: {
        ...result,
        header,
      },
      meta: { audit: { actorId, actorUsername, requestId } },
    });
  } catch (err) {
    console.error("[washing.splitProduksiTime]", err);
    const status = err.statusCode || err.status || 500;
    return res.status(status).json({
      success: false,
      message:
        status === 500 ? "Internal Server Error" : err.message || "Error",
      error: {
        message: err.message,
        details: process.env.NODE_ENV === "development" ? err.stack : undefined,
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
  splitProduksiTime,
};

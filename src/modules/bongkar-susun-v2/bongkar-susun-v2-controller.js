// modules/bongkar-susun-v2/bongkar-susun-v2-controller.js
const service = require("./bongkar-susun-v2-service");
const { detectCategory } = require("./bongkar-susun-v2-category-registry");
const {
  getActorId,
  getActorUsername,
  makeRequestId,
} = require("../../core/utils/http-context");

function makeCtx(req) {
  return {
    actorId: getActorId(req),
    actorUsername: getActorUsername(req) || "system",
    requestId: makeRequestId(req),
  };
}

// GET /bongkar-susun-v2/label/:labelCode
async function getLabelInfo(req, res) {
  try {
    const data = await service.getLabelInfo(req.params.labelCode);
    return res.status(200).json({ success: true, data });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

// GET /bongkar-susun-v2
async function getAll(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSize = Math.min(
    Math.max(parseInt(req.query.pageSize, 10) || 20, 1),
    100,
  );
  const search = (req.query.search || "").trim();

  try {
    const result = await service.getAll(page, pageSize, search);
    return res.status(200).json({
      success: true,
      data: result.data,
      total: result.total,
      page,
      pageSize,
    });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

// GET /bongkar-susun-v2/:noBongkarSusun
async function getDetail(req, res) {
  try {
    const data = await service.getDetail(req.params.noBongkarSusun);
    return res.status(200).json({ success: true, data });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

// POST /bongkar-susun-v2
async function create(req, res) {
  const { note, inputs, outputs } = req.body || {};
  const ctx = makeCtx(req);

  if (!ctx.actorId) {
    return res
      .status(401)
      .json({ success: false, message: "actorId tidak ditemukan dari token" });
  }

  // Deteksi kategori dari input pertama
  const firstInput = Array.isArray(inputs) && inputs[0];
  if (!firstInput) {
    return res
      .status(400)
      .json({ success: false, message: "inputs wajib diisi" });
  }

  const code = String(firstInput).trim();
  const category = detectCategory(code);

  if (!category) {
    return res.status(400).json({
      success: false,
      message: `Label ${code} tidak dikenali kategorinya`,
    });
  }

  try {
    const result = await service.createBongkarSusunByCategory(
      category,
      { note, inputs, outputs },
      ctx,
    );

    return res.status(201).json({ success: true, data: result });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

// DELETE /bongkar-susun-v2/:noBongkarSusun
async function deleteBongkarSusun(req, res) {
  const ctx = makeCtx(req);

  if (!ctx.actorId) {
    return res
      .status(401)
      .json({ success: false, message: "actorId tidak ditemukan dari token" });
  }

  try {
    const result = await service.deleteBongkarSusun(
      req.params.noBongkarSusun,
      ctx,
    );
    return res.status(200).json({ success: true, data: result });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

module.exports = {
  getLabelInfo,
  getAll,
  getDetail,
  create,
  deleteBongkarSusun,
};

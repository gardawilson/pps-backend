const service = require("./sortir-reject-v2-service");
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

async function getAll(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSize = Math.min(
    Math.max(parseInt(req.query.pageSize, 10) || 20, 1),
    100,
  );
  const search = (req.query.search || req.query.noBJSortir || "").trim();

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

async function getDetail(req, res) {
  try {
    const data = await service.getDetail(req.params.noBJSortir);
    return res.status(200).json({ success: true, data });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

async function create(req, res) {
  const { idWarehouse, inputs, outputs } = req.body || {};
  const ctx = makeCtx(req);

  if (!ctx.actorId) {
    return res
      .status(401)
      .json({ success: false, message: "actorId tidak ditemukan dari token" });
  }

  try {
    const result = await service.create(
      { idWarehouse, inputs, outputs },
      ctx,
    );

    return res.status(201).json({ success: true, data: result });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

async function createReject(req, res) {
  const ctx = makeCtx(req);

  if (!ctx.actorId) {
    return res
      .status(401)
      .json({ success: false, message: "actorId tidak ditemukan dari token" });
  }

  try {
    const result = await service.createReject(
      req.params.noBJSortir,
      req.body || {},
      ctx,
    );

    return res.status(201).json({ success: true, data: result });
  } catch (e) {
    return res
      .status(e.statusCode || 500)
      .json({ success: false, message: e.message });
  }
}

async function deleteSortirReject(req, res) {
  const ctx = makeCtx(req);

  if (!ctx.actorId) {
    return res
      .status(401)
      .json({ success: false, message: "actorId tidak ditemukan dari token" });
  }

  try {
    const result = await service.deleteSortirReject(
      req.params.noBJSortir,
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
  createReject,
  deleteSortirReject,
};

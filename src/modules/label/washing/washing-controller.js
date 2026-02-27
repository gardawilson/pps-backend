const labelWashingService = require('./washing-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context'); 

// GET all header washing
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await labelWashingService.getAll({ page, limit, search });
    const totalPages = Math.ceil(total / limit);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages }
    });
  } catch (err) {
    console.error('Get Washing List Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

// GET one header + details
exports.getOne = async (req, res) => {
  const { nowashing } = req.params;

  try {
    const details = await labelWashingService.getWashingDetailByNoWashing(nowashing);

    if (!details || details.length === 0) {
      return res.status(404).json({
        success: false,
        message: `Data tidak ditemukan untuk NoWashing ${nowashing}`
      });
    }

    return res.status(200).json({ success: true, data: { nowashing, details } });
  } catch (err) {
    console.error('Get Washing_d Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

exports.create = async (req, res) => {
  try {
    const payload = req.body || {};

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    // ✅ untuk audit trail (ID saja)
    payload.actorId = actorId;
    payload.requestId = makeRequestId(req);

    // ✅ business field di Washing_h (tetap string username)
    // (overwrite supaya tidak spoof dari client)
    payload.header = payload.header || {};
    payload.header.CreateBy = getActorUsername(req) || 'system';

    const result = await labelWashingService.createWashingCascade(payload);

    return res.status(201).json({
      success: true,
      message: 'Washing berhasil dibuat',
      data: result,
    });
  } catch (err) {
    console.error('Create Washing Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};


exports.update = async (req, res) => {
  const { nowashing } = req.params;

  try {
    const NoWashing = String(nowashing || '').trim();
    if (!NoWashing) {
      return res.status(400).json({ success: false, message: 'nowashing wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const actorUsername = getActorUsername(req) || 'system';

    // ✅ pastikan body object
    const body = req.body && typeof req.body === 'object' ? req.body : {};

    // ✅ jangan percaya audit fields dari client
    const { actorId: _clientActorId, requestId: _clientRequestId, ...safeBody } = body;

    const payload = {
      ...safeBody,
      NoWashing,
      actorId,                  // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // ✅ business field (username), overwrite dari token
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};
    payload.header.UpdateBy = actorUsername;

    const result = await labelWashingService.updateWashingCascade(payload);

    return res.status(200).json({
      success: true,
      message: 'Washing berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Washing Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.remove = async (req, res) => {
  const { nowashing } = req.params;

  try {
    const NoWashing = String(nowashing || '').trim();
    if (!NoWashing) {
      return res.status(400).json({ success: false, message: 'nowashing wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const payload = {
      NoWashing,
      actorId,                 // ✅ audit uses ID
      requestId: makeRequestId(req),
    };

    const result = await labelWashingService.deleteWashingCascade(payload);

    return res.status(200).json({
      success: true,
      message: `Washing ${NoWashing} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Washing Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
  const { nowashing } = req.params;

  try {
    const NoWashing = String(nowashing || "").trim();
    if (!NoWashing) {
      return res
        .status(400)
        .json({ success: false, message: "nowashing wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const result = await labelWashingService.incrementHasBeenPrinted({
      NoWashing,
      actorId,
      requestId: makeRequestId(req),
    });

    return res.status(200).json({
      success: true,
      message: "HasBeenPrinted berhasil ditambah",
      data: result,
    });
  } catch (err) {
    console.error("Increment HasBeenPrinted Error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

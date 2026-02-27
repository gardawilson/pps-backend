// controllers/gilingan-controller.js  (atau sesuai struktur kamu)
const service = require('./gilingan-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');

// GET /labels/gilingan?page=&limit=&search=
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await service.getAll({ page, limit, search });
    const totalPages = Math.ceil(total / limit);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Gilingan List Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

exports.create = async (req, res) => {
  try {
    // ✅ pastikan body object
    const payload = req.body && typeof req.body === 'object' ? req.body : {};

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    // ✅ audit fields (ID only)
    payload.actorId = actorId;
    payload.requestId = makeRequestId(req);

    // ✅ business field CreateBy (username), overwrite supaya tidak spoof dari client
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};
    payload.header.CreateBy = getActorUsername(req) || 'system';

    const result = await service.createGilinganCascade(payload);

    return res.status(201).json({
      success: true,
      message: 'Gilingan berhasil dibuat',
      data: result,
    });
  } catch (err) {
    console.error('Create Gilingan Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.update = async (req, res) => {
  const { nogilingan, noGilingan } = req.params;

  try {
    const NoGilingan = String(nogilingan || noGilingan || '').trim();
    if (!NoGilingan) {
      return res.status(400).json({ success: false, message: 'nogilingan wajib diisi' });
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
      NoGilingan,
      actorId, // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // =========================================
    // ✅ BACKWARD COMPATIBILITY:
    // kalau client lama kirim field flat,
    // pindahkan ke payload.header supaya cocok dengan service cascade
    // =========================================
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};

    const liftKeys = [
      'Berat',
      'IdGilingan',
      'IsPartial',
      'IdStatus',
      'DateCreate',
      'DateUsage',
      'Blok',
      'IdLokasi',
    ];

    for (const k of liftKeys) {
      if (Object.prototype.hasOwnProperty.call(payload, k) && payload.header[k] === undefined) {
        payload.header[k] = payload[k];
        delete payload[k];
      }
    }

    // ✅ business field (username) — overwrite dari token
    payload.header.UpdateBy = actorUsername;

    const result = await service.updateGilinganCascade(payload);

    return res.status(200).json({
      success: true,
      message: 'Gilingan berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Gilingan Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.delete = async (req, res) => {
  const { nogilingan, noGilingan } = req.params;

  try {
    const NoGilingan = String(nogilingan || noGilingan || '').trim();
    if (!NoGilingan) {
      return res.status(400).json({ success: false, message: 'nogilingan wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const payload = {
      NoGilingan,
      actorId,
      requestId: makeRequestId(req),
    };

    const result = await service.deleteGilinganCascade(payload);

    return res.status(200).json({
      success: true,
      message: `Gilingan ${NoGilingan} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Gilingan Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.getGilinganPartialInfo = async (req, res) => {
  const { nogilingan, noGilingan } = req.params;

  try {
    const NoGilingan = String(nogilingan || noGilingan || '').trim();
    if (!NoGilingan) {
      return res.status(400).json({
        success: false,
        message: 'NoGilingan is required.',
      });
    }

    const data = await service.getPartialInfoByGilingan(NoGilingan);

    if (!data.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No partial data for NoGilingan ${NoGilingan}`,
        totalRows: 0,
        totalPartialWeight: 0,
        data: [],
        meta: { NoGilingan },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Gilingan partial info retrieved successfully',
      totalRows: data.rows.length,
      totalPartialWeight: data.totalPartialWeight,
      data: data.rows,
      meta: { NoGilingan },
    });
  } catch (err) {
    console.error('Get Gilingan Partial Info Error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
  const { nogilingan, noGilingan } = req.params;

  try {
    const NoGilingan = String(nogilingan || noGilingan || '').trim();
    if (!NoGilingan) {
      return res.status(400).json({ success: false, message: 'nogilingan wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const result = await service.incrementHasBeenPrinted({
      NoGilingan,
      actorId,
      requestId: makeRequestId(req),
    });

    return res.status(200).json({
      success: true,
      message: 'HasBeenPrinted berhasil ditambah',
      data: result,
    });
  } catch (err) {
    console.error('Increment HasBeenPrinted Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

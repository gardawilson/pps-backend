// controllers/crusher-controller.js
const service = require('./crusher-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');

// GET all header crusher
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
    console.error('Get Crusher List Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

// (Optional) GET one header (kalau kamu punya service-nya)
// exports.getOne = async (req, res) => {
//   const { nocrusher } = req.params;
//   try {
//     const NoCrusher = String(nocrusher || '').trim();
//     if (!NoCrusher) {
//       return res.status(400).json({ success: false, message: 'nocrusher wajib diisi' });
//     }
//
//     const data = await service.getOne(NoCrusher); // sesuaikan nama service
//     if (!data) {
//       return res.status(404).json({ success: false, message: `Data tidak ditemukan untuk NoCrusher ${NoCrusher}` });
//     }
//
//     return res.status(200).json({ success: true, data });
//   } catch (err) {
//     console.error('Get Crusher Error:', err);
//     return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
//   }
// };

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

    const result = await service.createCrusherCascade(payload);

    return res.status(201).json({
      success: true,
      message: 'Crusher berhasil dibuat',
      data: result,
    });
  } catch (err) {
    console.error('Create Crusher Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.update = async (req, res) => {
  const { nocrusher, noCrusher } = req.params;

  try {
    const NoCrusher = String(nocrusher || noCrusher || '').trim();
    if (!NoCrusher) {
      return res.status(400).json({ success: false, message: 'nocrusher wajib diisi' });
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
      NoCrusher,
      actorId, // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // =========================================
    // ✅ BACKWARD COMPATIBILITY:
    // Kalau client lama kirim field flat (Berat, IdCrusher, dst),
    // pindahkan ke payload.header supaya cocok dengan service cascade.
    // =========================================
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};

    const liftKeys = [
      'Berat',
      'IdCrusher',
      'IdWarehouse',
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

    // ✅ pakai service cascade
    const result = await service.updateCrusherCascade(payload);

    return res.status(200).json({
      success: true,
      message: 'Crusher berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Crusher Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};


exports.delete = async (req, res) => {
  const { nocrusher, noCrusher } = req.params;

  try {
    const NoCrusher = String(nocrusher || noCrusher || '').trim();
    if (!NoCrusher) {
      return res.status(400).json({ success: false, message: 'nocrusher wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const payload = {
      NoCrusher,
      actorId, // ✅ audit uses ID
      requestId: makeRequestId(req),
    };

    const result = await service.deleteCrusherCascade(payload);

    return res.status(200).json({
      success: true,
      message: `Crusher ${NoCrusher} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Crusher Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
  const { nocrusher, noCrusher } = req.params;

  try {
    const NoCrusher = String(nocrusher || noCrusher || '').trim();
    if (!NoCrusher) {
      return res.status(400).json({ success: false, message: 'nocrusher wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const result = await service.incrementHasBeenPrinted({
      NoCrusher,
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

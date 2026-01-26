// controllers/packing-controller.js
const service = require('./packing-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');

// GET /labels/packing?page=&limit=&search=
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await service.getAll({ page, limit, search });
    const totalPages = Math.max(Math.ceil(total / limit), 1);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Packing List Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

exports.create = async (req, res) => {
  try {
    const payload = req.body && typeof req.body === 'object' ? req.body : {};

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    // ✅ audit fields
    payload.actorId = actorId;
    payload.requestId = makeRequestId(req);

    // ✅ business field CreateBy — overwrite dari token
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};
    payload.header.CreateBy = getActorUsername(req) || 'system';

    const result = await service.createPacking(payload);

    const headers = Array.isArray(result?.headers) ? result.headers : [];
    const count =
      typeof result?.output?.count === 'number'
        ? result.output.count
        : (headers.length || 1);

    const msg =
      count > 1
        ? `${count} Packing labels created successfully`
        : 'Packing created successfully';

    return res.status(201).json({
      success: true,
      message: msg,
      data: result,
    });
  } catch (err) {
    console.error('Create Packing Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.update = async (req, res) => {
  const { noBJ, nobj } = req.params;

  try {
    const NoBJ = String(noBJ || nobj || '').trim();
    if (!NoBJ) {
      return res.status(400).json({ success: false, message: 'noBJ wajib diisi' });
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
      actorId, // ✅ audit pakai ID
      requestId: makeRequestId(req),
    };

    // backward compatibility:
    // kalau client lama kirim field flat (Pcs, Berat, IdBJ, dll),
    // angkat ke payload.header
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};

    const liftKeys = [
      'IdBJ',
      'IdWarehouse',
      'Jam',
      'Pcs',
      'Berat',
      'IsPartial',
      'Blok',
      'IdLokasi',
      'DateCreate',
      'CreateBy',
    ];

    for (const k of liftKeys) {
      if (Object.prototype.hasOwnProperty.call(payload, k) && payload.header[k] === undefined) {
        payload.header[k] = payload[k];
        delete payload[k];
      }
    }

    // ✅ business field CreateBy — overwrite dari token
    payload.header.CreateBy = actorUsername;

    const result = await service.updatePacking(NoBJ, payload);

    return res.status(200).json({
      success: true,
      message: 'Packing berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Packing Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.delete = async (req, res) => {
  const { noBJ, nobj } = req.params;

  try {
    const NoBJ = String(noBJ || nobj || '').trim();
    if (!NoBJ) {
      return res.status(400).json({ success: false, message: 'noBJ wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    // ✅ audit payload untuk delete (karena service delete butuh actorId/requestId)
    const payload = {
      actorId,
      requestId: makeRequestId(req),
    };

    const result = await service.deletePacking(NoBJ, payload);

    return res.status(200).json({
      success: true,
      message: `Packing ${NoBJ} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Packing Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

// GET /labels/packing/partials/:nobj
exports.getPackingPartialInfo = async (req, res) => {
  const { nobj, noBJ } = req.params;

  try {
    const NoBJ = String(nobj || noBJ || '').trim();
    if (!NoBJ) {
      return res.status(400).json({ success: false, message: 'NoBJ is required.' });
    }

    const data = await service.getPartialInfoByBJ(NoBJ);

    if (!data?.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No partial data for NoBJ ${NoBJ}`,
        totalRows: 0,
        totalPartialPcs: 0,
        data: [],
        meta: { NoBJ: NoBJ },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Packing partial info retrieved successfully',
      totalRows: data.rows.length,
      totalPartialPcs: data.totalPartialPcs,
      data: data.rows,
      meta: { NoBJ: NoBJ },
    });
  } catch (err) {
    console.error('Get Packing Partial Info Error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
};
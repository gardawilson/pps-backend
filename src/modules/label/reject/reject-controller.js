// routes/labels/reject-controller.js
const service = require('./reject-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');

// GET /labels/reject?page=&limit=&search=
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
    console.error('Get Reject List Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

/**
 * Expected body:
 * {
 *   "header": {
 *     "IdReject": 1,               // required
 *     "Berat": 25.5,               // optional
 *     "DateCreate": "2025-10-28",  // optional
 *     "Jam": "08:00",              // optional
 *     "IsPartial": 0,              // optional
 *     "IdWarehouse": 3,            // optional/required by business rule (service validates)
 *     "Blok": "A",                 // optional
 *     "IdLokasi": 1                // optional (INT)
 *   },
 *   "outputCode": "S.0000001234"   // required: S.=Inject, BH.=HotStamping, BI.=PasangKunci, BJ.=Spanner, J.=BJSortir
 * }
 */
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

    const result = await service.createReject(payload);

    const headers = Array.isArray(result?.headers) ? result.headers : [];
    const count =
      typeof result?.output?.count === 'number'
        ? result.output.count
        : (headers.length || 1);

    const msg =
      count > 1 ? `${count} Reject labels created successfully` : 'Reject created successfully';

    return res.status(201).json({
      success: true,
      message: msg,
      data: result,
    });
  } catch (err) {
    console.error('Create Reject Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

/**
 * PUT /labels/reject/:noReject
 *
 * Body:
 * {
 *   "header": { ... partial fields ... },
 *   "outputCode": "BH.0000001234"   // optional: kalau dikirim (bahkan null/"") -> service akan update mapping sesuai pattern
 * }
 */
exports.update = async (req, res) => {
  const { noReject, noreject } = req.params;

  try {
    const NoReject = String(noReject || noreject || '').trim();
    if (!NoReject) {
      return res.status(400).json({ success: false, message: 'noReject wajib diisi' });
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
      actorId,
      requestId: makeRequestId(req),
    };

    // backward compatibility:
    // kalau client lama kirim field flat (IdReject, Berat, DateCreate, dll), angkat ke payload.header
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};

    const liftKeys = [
      'IdReject',
      'IdWarehouse',
      'Berat',
      'Jam',
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

    const result = await service.updateReject(NoReject, payload);

    return res.status(200).json({
      success: true,
      message: 'Reject berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Reject Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

// DELETE /labels/reject/:noReject
exports.delete = async (req, res) => {
  const { noReject, noreject } = req.params;

  try {
    const NoReject = String(noReject || noreject || '').trim();
    if (!NoReject) {
      return res.status(400).json({ success: false, message: 'noReject wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    // ✅ audit payload untuk delete (service delete butuh actorId/requestId)
    const payload = {
      actorId,
      requestId: makeRequestId(req),
    };

    const result = await service.deleteReject(NoReject, payload);

    return res.status(200).json({
      success: true,
      message: `Reject ${NoReject} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Reject Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

// GET /labels/reject/partials/:noreject
exports.getRejectPartialInfo = async (req, res) => {
  const { noreject, noReject } = req.params;

  try {
    const NoReject = String(noreject || noReject || '').trim();
    if (!NoReject) {
      return res.status(400).json({
        success: false,
        message: 'NoReject is required.',
      });
    }

    const data = await service.getPartialInfoByReject(NoReject);

    if (!data?.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No partial data for NoReject ${NoReject}`,
        totalRows: 0,
        totalPartialBerat: 0,
        data: [],
        meta: { NoReject },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Reject partial info retrieved successfully',
      totalRows: data.rows.length,
      totalPartialBerat: data.totalPartialBerat,
      data: data.rows,
      meta: { NoReject },
    });
  } catch (err) {
    console.error('Get Reject Partial Info Error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
};

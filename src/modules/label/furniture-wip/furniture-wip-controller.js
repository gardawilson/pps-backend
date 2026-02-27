// controllers/furniture-wip-controller.js
const service = require('./furniture-wip-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');

// GET /labels/furniture-wip?page=&limit=&search=
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
    console.error('Get Furniture WIP List Error:', err);
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

    const result = await service.createFurnitureWip(payload);

    const headers = Array.isArray(result?.headers) ? result.headers : [];
    const count =
      typeof result?.output?.count === 'number'
        ? result.output.count
        : (headers.length || 1);

    const msg =
      count > 1
        ? `${count} Furniture WIP labels created successfully`
        : 'Furniture WIP created successfully';

    return res.status(201).json({
      success: true,
      message: msg,
      data: result,
    });
  } catch (err) {
    console.error('Create Furniture WIP Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.update = async (req, res) => {
  const { noFurnitureWip, nofurniturewip } = req.params;

  try {
    const NoFurnitureWIP = String(noFurnitureWip || nofurniturewip || '').trim();
    if (!NoFurnitureWIP) {
      return res.status(400).json({ success: false, message: 'noFurnitureWip wajib diisi' });
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
    // kalau client lama kirim field flat (Pcs, Berat, IdFurnitureWIP, dll),
    // angkat ke payload.header
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};

    const liftKeys = [
      'IDFurnitureWIP', 'IdFurnitureWIP',
      'IdWarehouse',
      'Jam',
      'Pcs',
      'Berat',
      'IsPartial',
      'IdWarna',
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
    // (untuk update, kamu sebelumnya pakai CreateBy; kalau kamu punya kolom UpdateBy di tabel, ganti ke UpdateBy)
    payload.header.CreateBy = actorUsername;

    const result = await service.updateFurnitureWip(NoFurnitureWIP, payload);

    return res.status(200).json({
      success: true,
      message: 'Furniture WIP berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Furniture WIP Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.delete = async (req, res) => {
  const { noFurnitureWip, nofurniturewip } = req.params;

  try {
    const NoFurnitureWIP = String(noFurnitureWip || nofurniturewip || '').trim();
    if (!NoFurnitureWIP) {
      return res.status(400).json({ success: false, message: 'noFurnitureWip wajib diisi' });
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

    const result = await service.deleteFurnitureWip(NoFurnitureWIP, payload);

    return res.status(200).json({
      success: true,
      message: `Furniture WIP ${NoFurnitureWIP} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Furniture WIP Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

// GET /labels/furniture-wip/partials/:nofurniturewip
exports.getFurnitureWipPartialInfo = async (req, res) => {
  const { nofurniturewip, noFurnitureWip } = req.params;

  try {
    const NoFurnitureWIP = String(nofurniturewip || noFurnitureWip || '').trim();
    if (!NoFurnitureWIP) {
      return res.status(400).json({ success: false, message: 'NoFurnitureWIP is required.' });
    }

    const data = await service.getPartialInfoByFurnitureWip(NoFurnitureWIP);

    if (!data?.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No partial data for NoFurnitureWIP ${NoFurnitureWIP}`,
        totalRows: 0,
        totalPartialPcs: 0,
        data: [],
        meta: { noFurnitureWIP: NoFurnitureWIP },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'FurnitureWIP partial info retrieved successfully',
      totalRows: data.rows.length,
      totalPartialPcs: data.totalPartialPcs,
      data: data.rows,
      meta: { noFurnitureWIP: NoFurnitureWIP },
    });
  } catch (err) {
    console.error('Get FurnitureWIP Partial Info Error:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
  const { noFurnitureWip, nofurniturewip } = req.params;

  try {
    const NoFurnitureWIP = String(noFurnitureWip || nofurniturewip || "").trim();
    if (!NoFurnitureWIP) {
      return res
        .status(400)
        .json({ success: false, message: "noFurnitureWip wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const result = await service.incrementHasBeenPrinted({
      NoFurnitureWIP,
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

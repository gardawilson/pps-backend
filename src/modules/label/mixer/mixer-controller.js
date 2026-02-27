// controllers/mixer-controller.js
const mixerService = require('./mixer-service');
const { getActorId, getActorUsername, makeRequestId } = require('../../../core/utils/http-context');

// GET all header mixer
exports.getAll = async (req, res) => {
  try {
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 20;
    const search = (req.query.search || '').trim();

    const { data, total } = await mixerService.getAll({ page, limit, search });
    const totalPages = Math.ceil(total / limit);

    return res.status(200).json({
      success: true,
      data,
      meta: { page, limit, total, totalPages },
    });
  } catch (err) {
    console.error('Get Mixer List Error:', err);
    return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
  }
};

// GET one header + details (mirror broker getOne)
exports.getOne = async (req, res) => {
  const { nomixer } = req.params;

  try {
    const details = await mixerService.getMixerDetailByNoMixer(nomixer);

    if (!details || details.length === 0) {
      return res.status(404).json({
        success: false,
        message: `Data tidak ditemukan untuk NoMixer ${nomixer}`,
      });
    }

    return res.status(200).json({
      success: true,
      data: { nomixer, details },
    });
  } catch (err) {
    console.error('Get Mixer_d Error:', err);
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

    // ✅ audit (ID only)
    payload.actorId = actorId;
    payload.requestId = makeRequestId(req);

    // ✅ overwrite business field CreateBy dari token (anti spoof)
    payload.header = payload.header || {};
    payload.header.CreateBy = getActorUsername(req) || 'system';

    // (optional) kalau service createMixer butuh IdWarehouse dll, biarkan validasi di service
    const result = await mixerService.createMixerCascade(payload);

    return res.status(201).json({
      success: true,
      message: 'Mixer berhasil dibuat',
      data: result,
    });
  } catch (err) {
    console.error('Create Mixer Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.update = async (req, res) => {
  const { nomixer } = req.params;

  try {
    const NoMixer = String(nomixer || '').trim();
    if (!NoMixer) {
      return res.status(400).json({ success: false, message: 'nomixer wajib diisi' });
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
      NoMixer,
      actorId,
      requestId: makeRequestId(req),
    };

    // ✅ business field (username)
    payload.header = payload.header && typeof payload.header === 'object' ? payload.header : {};
    payload.header.UpdateBy = actorUsername;

    // NOTE: outputCode pattern tetap sama, karena payload.outputCode diteruskan apa adanya
    const result = await mixerService.updateMixerCascade(payload);

    return res.status(200).json({
      success: true,
      message: 'Mixer berhasil diupdate',
      data: result,
    });
  } catch (err) {
    console.error('Update Mixer Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.remove = async (req, res) => {
  const { nomixer } = req.params;

  try {
    const NoMixer = String(nomixer || '').trim();
    if (!NoMixer) {
      return res.status(400).json({ success: false, message: 'nomixer wajib diisi' });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res.status(401).json({ success: false, message: 'Unauthorized (idUsername missing)' });
    }

    const payload = {
      NoMixer,
      actorId,
      requestId: makeRequestId(req),
    };

    const result = await mixerService.deleteMixerCascade(payload);

    return res.status(200).json({
      success: true,
      message: `Mixer ${NoMixer} berhasil dihapus`,
      data: result,
    });
  } catch (err) {
    console.error('Delete Mixer Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

// GET partial info for one Mixer + NoSak
exports.getPartialInfo = async (req, res) => {
  const { nomixer, nosak } = req.params;

  try {
    const NoMixer = String(nomixer || '').trim();
    const NoSakNum = Number(nosak);

    if (!NoMixer || !Number.isFinite(NoSakNum)) {
      return res.status(400).json({
        success: false,
        message: 'nomixer dan nosak wajib diisi (nosak harus angka)',
      });
    }

    const data = await mixerService.getPartialInfoByMixerAndSak(NoMixer, Math.trunc(NoSakNum));

    if (!data.rows || data.rows.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data partial untuk NoMixer ${NoMixer} / NoSak ${Math.trunc(NoSakNum)}`,
        totalRows: 0,
        totalPartialWeight: 0,
        data: [],
        meta: { nomixer: NoMixer, nosak: Math.trunc(NoSakNum) },
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Partial info berhasil diambil',
      totalRows: data.rows.length,
      totalPartialWeight: data.totalPartialWeight,
      data: data.rows,
      meta: { nomixer: NoMixer, nosak: Math.trunc(NoSakNum) },
    });
  } catch (err) {
    console.error('Get Mixer Partial Info Error:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

exports.incrementHasBeenPrinted = async (req, res) => {
  const { nomixer } = req.params;

  try {
    const NoMixer = String(nomixer || "").trim();
    if (!NoMixer) {
      return res
        .status(400)
        .json({ success: false, message: "nomixer wajib diisi" });
    }

    const actorId = getActorId(req);
    if (!actorId) {
      return res
        .status(401)
        .json({ success: false, message: "Unauthorized (idUsername missing)" });
    }

    const result = await mixerService.incrementHasBeenPrinted({
      NoMixer,
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

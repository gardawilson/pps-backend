// controllers/bongkar-susun-controller.js
const bongkarSusunService = require('./bongkar-susun-service');

async function getByDate(req, res) {
  const { username } = req;
  const date = req.params.date; // sudah tervalidasi formatnya oleh route regex
  console.log(
    '🔍 Fetching BongkarSusun_h | Username:',
    username,
    '| date:',
    date
  );

  try {
    const data = await bongkarSusunService.getByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `Tidak ada data BongkarSusun_h untuk tanggal ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `Data BongkarSusun_h untuk tanggal ${date} berhasil diambil`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching BongkarSusun_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getAllBongkarSusun(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support ?noBongkarSusun= dan ?search=
  const search =
    (typeof req.query.noBongkarSusun === 'string' &&
      req.query.noBongkarSusun) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  try {
    const { data, total } = await bongkarSusunService.getAllBongkarSusun(
      page,
      pageSize,
      search
    );

    return res.status(200).json({
      success: true,
      message: 'BongkarSusun_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search,
      },
    });
  } catch (error) {
    console.error('Error fetching BongkarSusun_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function createBongkarSusun(req, res) {
  try {
    // ⬇️ Ambil username dari JWT (verifyToken)
    const username = req.username || req.user?.username || null;

    if (!username) {
      return res.status(401).json({
        success: false,
        message: 'Username tidak ditemukan di token (unauthorized)',
      });
    }

    const b = req.body || {};

    // ⬇️ Tidak pakai idUsername dari body lagi
    const payload = {
      tanggal: b.tanggal, // 'YYYY-MM-DD'
      username,           // akan di-resolve ke IdUsername di service
      note: b.note ?? null,
    };

    const result = await bongkarSusunService.createBongkarSusun(payload);

    return res
      .status(201)
      .json({ success: true, message: 'Created', data: result.header });
  } catch (err) {
    console.error('Error create BongkarSusun_h:', err);
    const status = err.statusCode || 500;
    return res
      .status(status)
      .json({ success: false, message: err.message || 'Internal Error' });
  }
}

async function updateBongkarSusun(req, res) {
  try {
    const noBongkarSusun = req.params.noBongkarSusun;
    if (!noBongkarSusun) {
      return res.status(400).json({
        success: false,
        message: 'noBongkarSusun wajib diisi di URL',
      });
    }

    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    // Karena UPDATE, semua field opsional.
    const payload = {};

    if (b.tanggal !== undefined) {
      payload.tanggal = b.tanggal; // 'YYYY-MM-DD'
    }

    // ⬇️ opsional: kalau suatu saat mau pakai, bisa kirim idUsername dari body.
    if (b.idUsername !== undefined) {
      payload.idUsername = toInt(b.idUsername);
    }

    if (b.note !== undefined) {
      payload.note = b.note === '' ? null : b.note;
    }

    const result = await bongkarSusunService.updateBongkarSusun(
      noBongkarSusun,
      payload
    );

    return res.status(200).json({
      success: true,
      message: 'Updated',
      data: result.header,
    });
  } catch (err) {
    console.error('Error update BongkarSusun_h:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}

async function deleteBongkarSusun(req, res) {
  try {
    const noBongkarSusun = req.params.noBongkarSusun;
    if (!noBongkarSusun) {
      return res.status(400).json({
        success: false,
        message: 'noBongkarSusun wajib diisi di URL',
      });
    }

    await bongkarSusunService.deleteBongkarSusun(noBongkarSusun);

    return res.status(200).json({
      success: true,
      message: 'Deleted',
    });
  } catch (err) {
    console.error('Error delete BongkarSusun_h:', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}

module.exports = {
  getByDate,
  getAllBongkarSusun,
  createBongkarSusun,
  updateBongkarSusun,
  deleteBongkarSusun,
};

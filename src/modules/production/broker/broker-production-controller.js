// controllers/broker-production-controller.js
const brokerProduksiService = require('./broker-production-service');


async function getAllProduksi(req, res) {
  const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
  const pageSizeRaw = parseInt(req.query.pageSize, 10) || 20;
  const pageSize = Math.min(Math.max(pageSizeRaw, 1), 100);

  // support both ?noProduksi= and ?search=
  const search =
    (typeof req.query.noProduksi === 'string' && req.query.noProduksi) ||
    (typeof req.query.search === 'string' && req.query.search) ||
    '';

  try {
    const { data, total } = await brokerProduksiService.getAllProduksi(
      page,
      pageSize,
      search
    );

    return res.status(200).json({
      success: true,
      message: 'BrokerProduksi_h retrieved successfully',
      totalData: total,
      data,
      meta: {
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        hasNextPage: page * pageSize < total,
        hasPrevPage: page > 1,
        search, // echo back for client state
      },
    });
  } catch (error) {
    console.error('Error fetching BrokerProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}


async function getInputsByNoProduksi(req, res) {
  const noProduksi = (req.params.noProduksi || '').trim();
  if (!noProduksi) {
    return res.status(400).json({ success:false, message:'noProduksi is required' });
  }
  try {
    // make sure brokerProduksiService.fetchInputs exists
    const data = await brokerProduksiService.fetchInputs(noProduksi);
    return res.status(200).json({ success:true, message:'Inputs retrieved', data });
  } catch (e) {
    console.error('[getInputsByNoProduksi]', e);
    return res.status(500).json({ success:false, message:'Internal Server Error', error:e.message });
  }
}


async function getProduksiByDate(req, res) {
  const { username } = req;
  const date = req.params.date;
  console.log("🔍 Fetching BrokerProduksi_h | Username:", username, "| date:", date);

  try {
    const data = await brokerProduksiService.getProduksiByDate(date);

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(200).json({
        success: true,
        message: `No BrokerProduksi_h data found for date ${date}`,
        totalData: 0,
        data: [],
        meta: { date },
      });
    }

    return res.status(200).json({
      success: true,
      message: `BrokerProduksi_h data for ${date} retrieved successfully`,
      totalData: data.length,
      data,
      meta: { date },
    });
  } catch (error) {
    console.error('Error fetching BrokerProduksi_h:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}


async function createProduksi(req, res) {
  try {
    // dari verifyToken middleware
    const username = req.username || req.user?.username || 'system';

    // body bisa datang sebagai string (x-www-form-urlencoded) atau JSON
    const b = req.body || {};

    // helper kecil buat parse number
    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };
    const toFloat = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    const payload = {
      tglProduksi: b.tglProduksi,                 // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),                  // number
      idOperator: toInt(b.idOperator),            // number
      jam: b.jam,                                 // number or 'HH:mm-HH:mm'
      shift: toInt(b.shift),                      // number
      createBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),

      // ⬇️ tambahin ini
      hourStart: b.hourStart || null,             // ex: '08:00:00'
      hourEnd: b.hourEnd || null,                 // ex: '09:00:00'
    };

    const result = await brokerProduksiService.createBrokerProduksi(payload);

    return res
      .status(201)
      .json({ success: true, message: 'Created', data: result.header });
  } catch (err) {
    const status = err.statusCode || 500;
    return res
      .status(status)
      .json({ success: false, message: err.message || 'Internal Error' });
  }
}


async function updateProduksi(req, res) {
  try {
    const noProduksi = req.params.noProduksi;        // dari URL
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

    // dari verifyToken middleware
    const username = req.username || req.user?.username || 'system';

    const b = req.body || {};

    const toInt = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };
    const toFloat = (v) => {
      if (v === undefined || v === null || v === '') return null;
      const n = Number(v);
      return Number.isNaN(n) ? null : n;
    };

    // body boleh partial, jadi jangan wajibkan semua
    const payload = {
      tglProduksi: b.tglProduksi,               // 'YYYY-MM-DD'
      idMesin: toInt(b.idMesin),
      idOperator: toInt(b.idOperator),
      jam: b.jam,                               // kalau dikirim, kita parse lagi
      shift: toInt(b.shift),
      updateBy: username,
      checkBy1: b.checkBy1,
      checkBy2: b.checkBy2,
      approveBy: b.approveBy,
      jmlhAnggota: toInt(b.jmlhAnggota),
      hadir: toInt(b.hadir),
      hourMeter: toFloat(b.hourMeter),
      hourStart: b.hourStart || null,
      hourEnd: b.hourEnd || null,
    };

    const result = await brokerProduksiService.updateBrokerProduksi(noProduksi, payload);

    return res.status(200).json({
      success: true,
      message: 'Updated',
      data: result.header,
    });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}


async function deleteProduksi(req, res) {
  try {
    const noProduksi = req.params.noProduksi;
    if (!noProduksi) {
      return res.status(400).json({
        success: false,
        message: 'noProduksi is required in route param',
      });
    }

    await brokerProduksiService.deleteBrokerProduksi(noProduksi);

    return res.status(200).json({
      success: true,
      message: 'Deleted',
    });
  } catch (err) {
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Internal Error',
    });
  }
}

module.exports = { getProduksiByDate, getInputsByNoProduksi, getAllProduksi, createProduksi, updateProduksi, deleteProduksi };

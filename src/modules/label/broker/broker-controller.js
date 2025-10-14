// controllers/broker-controller.js

const brokerService = require('./broker-service');

exports.getAll = async (req, res) => {
    try {
      const page = parseInt(req.query.page, 10) || 1;
      const limit = parseInt(req.query.limit, 10) || 20;
      const search = (req.query.search || '').trim();
  
      const { data, total } = await brokerService.getAll({ page, limit, search });
      const totalPages = Math.ceil(total / limit);
  
      res.status(200).json({
        success: true,
        data,
        meta: { page, limit, total, totalPages },
      });
    } catch (err) {
      console.error('Get Broker List Error:', err);
      res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
    }
  };
  
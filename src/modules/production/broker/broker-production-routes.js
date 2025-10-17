// routes/broker-production-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const brokerProduksiController = require('./broker-production-controller');

// GET BrokerProduksi_h by date (YYYY-MM-DD)
router.get(
  '/broker/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  brokerProduksiController.getProduksiByDate
);

module.exports = router;

// routes/inject-production-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const injectProduksiController = require('./inject-production-controller');

// GET InjectProduksi_h by date (YYYY-MM-DD)
router.get(
  '/inject/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  injectProduksiController.getProduksiByDate
);

module.exports = router;

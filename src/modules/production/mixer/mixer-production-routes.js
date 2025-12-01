// routes/mixer-production-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const mixerProduksiController = require('./mixer-production-controller');

// GET MixerProduksi_h by date (YYYY-MM-DD)
router.get(
  '/mixer/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  mixerProduksiController.getProduksiByDate
);

module.exports = router;

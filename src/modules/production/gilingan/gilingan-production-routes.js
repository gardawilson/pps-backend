// routes/gilingan-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const gilinganProduksiController = require('./gilingan-production-controller');

// GET GilinganProduksi_h by date (YYYY-MM-DD)
router.get(
  '/gilingan/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  gilinganProduksiController.getProduksiByDate
);

module.exports = router;

// routes/production-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const washingProduksiController = require('./production-controller');

// Validasi pola tanggal langsung di route (YYYY-MM-DD)
router.get(
  '/washing/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  washingProduksiController.getProduksiByDate
);

module.exports = router;

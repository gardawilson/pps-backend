// routes/packing-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const packingController = require('./packing-production-controller');

// GET PackingProduksi_h by date (YYYY-MM-DD)
// Example: GET /api/packing/2025-12-02
router.get(
  '/packing/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  packingController.getProduksiByDate
);

module.exports = router;

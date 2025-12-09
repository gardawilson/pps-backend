// routes/hotstamping-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const hotStampingController = require('./hot-stamp-production-controller');

// GET HotStamping_h by date (YYYY-MM-DD)
// Example: GET /api/hotstamping/2025-12-02
router.get(
  '/hot-stamp/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  hotStampingController.getProduksiByDate
);

module.exports = router;

// routes/key-fitting-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const keyFittingController = require('./key-fitting-production-controller');

// GET Key Fitting (PasangKunci_h) by date (YYYY-MM-DD)
// Example: GET /api/key-fitting/2025-12-02
router.get(
  '/key-fitting/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  keyFittingController.getProductionByDate
);

module.exports = router;

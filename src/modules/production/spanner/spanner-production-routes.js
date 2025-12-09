// routes/spanner-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const spannerController = require('./spanner-production-controller');

// GET Spanner_h by date (YYYY-MM-DD)
// Example: GET /api/spanner/2025-12-02
router.get(
  '/spanner/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  spannerController.getProductionByDate
);

module.exports = router;

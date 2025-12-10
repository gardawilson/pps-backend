// routes/sortir-reject-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const sortirRejectController = require('./sortir-reject-controller');

// GET BJSortirReject_h by date (YYYY-MM-DD)
// Example: GET /api/sortir-reject/2025-12-02
router.get(
  '/sortir-reject/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  sortirRejectController.getSortirRejectByDate
);

module.exports = router;

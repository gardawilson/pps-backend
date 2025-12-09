// routes/return-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const returnController = require('./return-production-controller');

// GET BJRetur_h by date (YYYY-MM-DD)
// Example: GET /api/returns/2025-12-02
router.get(
  '/return/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  returnController.getReturnsByDate
);

module.exports = router;

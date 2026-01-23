// routes/production-overlap-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const overlapController = require('./production-overlap-controller');

// Contoh:
// GET /production/broker/overlap?date=2025-11-03&idMesin=12&start=22:00&end=02:00&exclude=BP-0000123
router.get(
  '/:kind(broker|crusher|washing|gilingan|mixer|inject)/overlap',
  verifyToken,
  overlapController.checkOverlapGeneric
);

module.exports = router;

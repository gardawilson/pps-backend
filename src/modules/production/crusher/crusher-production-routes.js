const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const ctrl = require('./crusher-production-controller');

// GET CrusherProduksi_h by date (YYYY-MM-DD)
router.get(
  '/crusher/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  ctrl.getProduksiByDate
);

// GET master crushers (enabled only, for dropdowns)
router.get(
  '/crusher/masters',
  verifyToken,
  ctrl.getCrusherMasters
);

module.exports = router;

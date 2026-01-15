// master-warehouse-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const ctrl = require('./master-warehouse-controller');

// List warehouse (active only by default)
// Query: ?includeDisabled=1&q=inje&orderBy=NamaWarehouse&orderDir=ASC
router.get('/warehouse', verifyToken, ctrl.list);

module.exports = router;

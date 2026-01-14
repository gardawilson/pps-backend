// master-pembeli-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const ctrl = require('./master-pembeli-controller');

// List pembeli (active only by default)
// Query: ?includeDisabled=1&q=ana&orderBy=NamaPembeli&orderDir=ASC
router.get('/pembeli', verifyToken, ctrl.list);

module.exports = router;

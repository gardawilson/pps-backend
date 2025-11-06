const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./master-operator-controller');

// List operators (active only by default)
// Query: ?includeDisabled=1&q=ana&orderBy=NamaOperator&orderDir=ASC
router.get('/', verifyToken, ctrl.list);

module.exports = router;

const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./master-mesin-controller');

// GET by idbagian (only active by default)
// The regex enforces numeric-only for :idbagian
router.get('/:idbagian(\\d+)', verifyToken, ctrl.getByIdBagian);

module.exports = router;

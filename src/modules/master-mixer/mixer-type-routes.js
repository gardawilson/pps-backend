// src/modules/master/mixer-type-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./mixer-type-controller');

// GET only active (Enable = 1)
router.get('/', verifyToken, ctrl.getAllActive);

module.exports = router;

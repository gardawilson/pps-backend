// src/modules/master/furniture-wip-type-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./furniture-wip-type-controller');

// GET only active furniture WIP (Enable = 1)
router.get('/', verifyToken, ctrl.getAllActive);

module.exports = router;

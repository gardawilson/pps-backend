// src/modules/master/packing-master-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./packing-master-controller');

// GET only active Packing master (MstBarangJadi where Enable = 1)
router.get('/', verifyToken, ctrl.getAllActive);

module.exports = router;

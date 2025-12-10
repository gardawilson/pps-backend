// src/modules/master/reject-master-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./reject-master-controller');

// GET only active Reject master (MstReject where Enable = 1)
router.get('/', verifyToken, ctrl.getAllActive);

module.exports = router;

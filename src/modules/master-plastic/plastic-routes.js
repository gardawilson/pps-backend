const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const masterJenisPlastikController = require('./plastic-controller');

// GET semua JenisPlastik yang aktif
router.get('/', verifyToken, masterJenisPlastikController.getJenisPlastik);

module.exports = router;

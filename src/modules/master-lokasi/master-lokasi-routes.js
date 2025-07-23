const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const masterLokasiController = require('./master-lokasi-controller');

router.get('/mst-lokasi', verifyToken, masterLokasiController.getLokasi);

module.exports = router;
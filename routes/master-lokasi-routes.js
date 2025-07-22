const express = require('express');
const router = express.Router();
const verifyToken = require('../middleware/verify-token');
const masterLokasiController = require('../controllers/master-lokasi-controller');

router.get('/mst-lokasi', verifyToken, masterLokasiController.getLokasi);

module.exports = router;
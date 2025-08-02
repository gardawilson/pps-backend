// routes/label-detail-routes.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const labelDetailController = require('./label-detail-controller');

// Tidak perlu pakai NoSO, cukup nomorLabel saja
router.get('/label/detail/:nomorLabel', verifyToken, labelDetailController.getLabelDetail);

module.exports = router;
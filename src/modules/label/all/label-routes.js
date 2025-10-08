const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const labelController = require('./label-controller');

// Semua label
router.get('/label/all', verifyToken, labelController.getAllLabelsHandler);

// 🔥 Update lokasi berdasarkan NomorLabel
router.post('/label/update-lokasi', verifyToken, labelController.updateLabelLocationHandler);

module.exports = router;

const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const washingProduksiController = require('./washing-produksi-controller');

// GET semua header
router.get('/', verifyToken, washingProduksiController.getAll);

module.exports = router;

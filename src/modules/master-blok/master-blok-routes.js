const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const masterBlokController = require('./master-blok-controller');

// GET semua blok aktif
router.get('/', verifyToken, masterBlokController.getBlok);

module.exports = router;

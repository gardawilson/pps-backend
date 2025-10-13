// routes/bongkar-susun-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const bongkarSusunController = require('./bongkar-susun-controller');

// GET data BongkarSusun_h untuk tanggal tertentu
router.get(
  '/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  bongkarSusunController.getByDate
);

module.exports = router;

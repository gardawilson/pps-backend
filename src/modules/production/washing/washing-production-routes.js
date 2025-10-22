// routes/production-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const washingProduksiController = require('./washing-production-controller');


// GET /washing?page=1&pageSize=20
router.get('/washing', verifyToken, washingProduksiController.getAllProduksi);


// Validasi pola tanggal langsung di route (YYYY-MM-DD)
router.get(
  '/washing/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  washingProduksiController.getProduksiByDate
);



module.exports = router;

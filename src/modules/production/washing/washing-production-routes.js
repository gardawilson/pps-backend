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


// ✅ Create WashingProduksi_h
router.post('/washing', verifyToken, washingProduksiController.createProduksi);

router.put('/washing/:noProduksi', verifyToken, washingProduksiController.updateProduksi);

router.delete('/washing/:noProduksi', verifyToken, washingProduksiController.deleteProduksi);



// GET /api/production/washing/:noProduksi/inputs
router.get(
  '/washing/:noProduksi/inputs',
  verifyToken,
  washingProduksiController.getInputsByNoProduksi
);


router.get('/washing/validate-label/:labelCode', verifyToken, washingProduksiController.validateLabel);

router.post('/washing/:noProduksi/inputs', verifyToken, washingProduksiController.upsertInputsAndPartials);

router.delete('/washing/:noProduksi/inputs', verifyToken, washingProduksiController.deleteInputsAndPartials);

module.exports = router;

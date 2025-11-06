// routes/broker-production-route.js
const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const brokerProduksiController = require('./broker-production-controller');


// GET /broker?page=1&pageSize=20
router.get('/broker', verifyToken, brokerProduksiController.getAllProduksi);

router.get('/broker/:noProduksi/inputs', verifyToken, brokerProduksiController.getInputsByNoProduksi);

// GET BrokerProduksi_h by date (YYYY-MM-DD)
router.get(
  '/broker/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  brokerProduksiController.getProduksiByDate
);

// ✅ Create
router.post('/broker', verifyToken, brokerProduksiController.createProduksi);

// ✅ Update by NoProduksi
router.put('/broker/:noProduksi', verifyToken, brokerProduksiController.updateProduksi);

// DELETE /api/production/broker/:noProduksi
router.delete('/broker/:noProduksi', verifyToken, brokerProduksiController.deleteProduksi);


module.exports = router;

// routes/hotstamping-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const hotStampingController = require('./hot-stamp-production-controller');

// GET HotStamping_h by date (YYYY-MM-DD)
// Example: GET /api/hotstamping/2025-12-02
router.get(
  '/hot-stamp/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  hotStampingController.getProduksiByDate
);

router.get(
  '/hot-stamp',
  verifyToken,
  hotStampingController.getAllProduksi
);

router.post(
  '/hot-stamp',
  verifyToken,
  hotStampingController.createProduksi
);


router.put(
  '/hot-stamp/:noProduksi',
  verifyToken,
  hotStampingController.updateProduksi
);


router.delete(
  '/hot-stamp/:noProduksi',
  verifyToken,
  hotStampingController.deleteProduksi
);


router.get(
  '/hot-stamp/:noProduksi/inputs',
  verifyToken,
  hotStampingController.getInputsByNoProduksi
);


router.get(
  '/hot-stamp/validate-fwip/:labelCode',
  verifyToken,
  hotStampingController.validateFwipLabel
);


router.post('/hot-stamp/:noProduksi/inputs', verifyToken, hotStampingController.upsertInputs);

// DELETE inputs & partials
router.delete(
  '/hot-stamp/:noProduksi/inputs',
  verifyToken,
  hotStampingController.deleteInputsAndPartials
);

module.exports = router;

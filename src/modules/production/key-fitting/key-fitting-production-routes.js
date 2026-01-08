// routes/key-fitting-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const keyFittingController = require('./key-fitting-production-controller');

// Example: GET /api/key-fitting/key-fitting?page=1&pageSize=20&search=PK.
router.get(
  '/key-fitting',
  verifyToken,
  keyFittingController.getAllProduksi
);


// GET Key Fitting (PasangKunci_h) by date (YYYY-MM-DD)
// Example: GET /api/key-fitting/2025-12-02
router.get(
  '/key-fitting/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  keyFittingController.getProductionByDate
);


// ✅ POST CREATE (baru)
router.post('/key-fitting', verifyToken, keyFittingController.createProduksi);

// routes/key-fitting-production-route.js
router.put(
  '/key-fitting/:noProduksi',
  verifyToken,
  keyFittingController.updateProduksi
);

router.delete('/key-fitting/:noProduksi', verifyToken, keyFittingController.deleteProduksi);



router.get(
  '/key-fitting/:noProduksi/inputs',
  verifyToken,
  keyFittingController.getInputsByNoProduksi
);

router.get(
  '/key-fitting/validate-fwip/:labelCode',
  verifyToken,
  keyFittingController.validateLabel
);


router.post(
  '/key-fitting/:noProduksi/inputs',
  verifyToken,
  keyFittingController.upsertInputs
);


router.delete(
  '/key-fitting/:noProduksi/inputs',
  verifyToken,
  keyFittingController.deleteInputsAndPartials
);


module.exports = router;

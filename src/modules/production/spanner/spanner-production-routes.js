// routes/spanner-production-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const spannerController = require('./spanner-production-controller');

// ✅ GET ALL Spanner (pagination + search)
// Example:
//   GET /api/spanner/spanner?page=1&pageSize=20&search=SP.0000
//   GET /api/spanner/spanner?noProduksi=SP.0000
router.get('/spanner', verifyToken, spannerController.getAllProduksi);

// ✅ existing: GET Spanner_h by date (YYYY-MM-DD)
// Example: GET /api/spanner/spanner/2025-12-02
router.get(
  '/spanner/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  spannerController.getProductionByDate
);


router.post('/spanner', verifyToken, spannerController.createProduksi);


router.put(
  '/spanner/:noProduksi',
  verifyToken,
  spannerController.updateProduksi
);


router.delete(
  '/spanner/:noProduksi',
  verifyToken,
  spannerController.deleteProduksi
);


router.get(
  '/spanner/:noProduksi/inputs',
  verifyToken,
  spannerController.getInputsByNoProduksi
);

router.post(
  '/spanner/:noProduksi/inputs',
  verifyToken,
  spannerController.upsertInputs
);

router.delete(
  '/spanner/:noProduksi/inputs',
  verifyToken,
  spannerController.deleteInputsAndPartials
);

module.exports = router;

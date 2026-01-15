// routes/sortir-reject-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const sortirRejectController = require('./sortir-reject-controller');


// ✅ GET ALL (paging + search + optional date range)
// Example: GET /api/sortir-reject?page=1&pageSize=20&search=SR.0001&dateFrom=2025-12-01&dateTo=2025-12-31
router.get('/sortir-reject', verifyToken, sortirRejectController.getAllSortirReject);


// GET BJSortirReject_h by date (YYYY-MM-DD)
// Example: GET /api/sortir-reject/2025-12-02
router.get(
  '/sortir-reject/:date(\\d{4}-\\d{2}-\\d{2})',
  verifyToken,
  sortirRejectController.getSortirRejectByDate
);

router.post('/sortir-reject', verifyToken, sortirRejectController.createSortirReject);

router.put('/sortir-reject/:noBJSortir', verifyToken, sortirRejectController.updateSortirReject);

router.delete(
  '/sortir-reject/:noBJSortir',
  verifyToken,
  sortirRejectController.deleteSortirReject
);



router.get(
  '/sortir-reject/:noBJSortir/inputs',
  verifyToken,
  sortirRejectController.getInputsByNoBJSortir
);

router.post(
  '/sortir-reject/:noBJSortir/inputs',
  verifyToken,
  sortirRejectController.upsertInputs
);

router.delete(
  '/sortir-reject/:noBJSortir/inputs',
  verifyToken,
  sortirRejectController.deleteInputs
);


module.exports = router;

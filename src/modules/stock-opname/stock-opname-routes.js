const express = require('express');
const verifyToken = require('../../core/middleware/verify-token');
const {
  noStockOpnameHandler,
  stockOpnameAcuanHandler,
  stockOpnameHasilHandler,
  deleteStockOpnameHasilHandler,
  validateStockOpnameLabelHandler,
  insertStockOpnameLabelHandler,
  stockOpnameFamiliesHandler,
  stockOpnameAscendDataHandler,
  saveStockOpnameAscendHasilHandler,
  fetchQtyUsageHandler,
  deleteStockOpnameHasilAscendHandler
} = require('./stock-opname-controller');

const router = express.Router();

router.get('/no-stock-opname', verifyToken, noStockOpnameHandler);
router.get('/no-stock-opname/:noso/acuan', verifyToken, stockOpnameAcuanHandler);
router.get('/no-stock-opname/:noso/hasil', verifyToken, stockOpnameHasilHandler);
router.delete('/no-stock-opname/:noso/hasil', verifyToken, deleteStockOpnameHasilHandler);
router.post('/no-stock-opname/:noso/validate-label', verifyToken, validateStockOpnameLabelHandler); 
router.post('/no-stock-opname/:noso/insert-label', verifyToken, insertStockOpnameLabelHandler);
router.get('/no-stock-opname/:noso/families', verifyToken, stockOpnameFamiliesHandler);


// Ambil data Ascend berdasarkan FamilyID
router.get(
  '/no-stock-opname/:noso/families/:familyid/ascend',
  verifyToken,
  stockOpnameAscendDataHandler
);
// Simpan hasil Stock Opname Ascend (upsert MERGE)
router.post(
  '/no-stock-opname/:noso/ascend/hasil',
  verifyToken,
  saveStockOpnameAscendHasilHandler
);
// Ambil total QtyUsage untuk item tertentu sejak tanggal SO
router.get(
  '/no-stock-opname/:itemId/usage',
  verifyToken,
  fetchQtyUsageHandler
);

router.delete(
  '/no-stock-opname/:noso/ascend/hasil/:itemId',
  verifyToken,
  deleteStockOpnameHasilAscendHandler
);




module.exports = router;
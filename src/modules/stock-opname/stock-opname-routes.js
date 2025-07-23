const express = require('express');
const verifyToken = require('../../core/middleware/verify-token');
const {
  noStockOpnameHandler,
  stockOpnameAcuanHandler,
  stockOpnameHasilHandler,
  deleteStockOpnameHasilHandler,
  validateStockOpnameLabelHandler,
  insertStockOpnameLabelHandler
} = require('./stock-opname-controller');

const router = express.Router();

router.get('/no-stock-opname', verifyToken, noStockOpnameHandler);
router.get('/no-stock-opname/:noso/acuan', verifyToken, stockOpnameAcuanHandler);
router.get('/no-stock-opname/:noso/hasil', verifyToken, stockOpnameHasilHandler);
router.delete('/no-stock-opname/:noso/hasil', verifyToken, deleteStockOpnameHasilHandler);
router.post('/no-stock-opname/:noso/validate-label', verifyToken, validateStockOpnameLabelHandler); 
router.post('/no-stock-opname/:noso/insert-label', verifyToken, insertStockOpnameLabelHandler);


module.exports = router;
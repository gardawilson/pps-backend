// routes/bj-jual-route.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const bjJualController = require('./bj-jual-controller');

router.get('/', verifyToken, bjJualController.getAllBJJual);
router.post('/', verifyToken, bjJualController.createBJJual);
router.put(
  '/:noBJJual',
  verifyToken,
  bjJualController.updateBJJual
);

router.delete(
  '/:noBJJual',
  verifyToken,
  bjJualController.deleteBJJual
);

router.get(
  '/:noBJJual/inputs',
  verifyToken,
  bjJualController.getInputsByNoBJJual
);

router.post(
  '/:noBJJual/inputs',
  verifyToken,
  bjJualController.upsertInputs
);


router.delete(
  '/:noBJJual/inputs',
  verifyToken,
  bjJualController.deleteInputsAndPartials
);



module.exports = router;

const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const sharedController = require('./production-shared-controller');

router.get(
  '/lookup-label/:labelCode',
  verifyToken,
  sharedController.lookupLabel
);

module.exports = router;

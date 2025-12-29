const express = require('express');
const router = express.Router();

const updateController = require('./update-controller');

// ✅ PUBLIC
router.get('/:appId/version', updateController.getVersion);
router.get('/:appId/download/:file', updateController.downloadApk);

// ✅ ADMIN ONLY
router.post('/:appId/publish', express.json(), updateController.publishVersion);

module.exports = router;

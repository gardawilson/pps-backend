const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const attachPermissions = require('../../core/middleware/attach-permissions');
const requirePermission = require('../../core/middleware/require-permission');

const ctrl = require('./audit-controller');

router.use(verifyToken, attachPermissions);

/**
 * 🎯 Generic history endpoint
 * GET /api/audit/:module/:documentNo/history
 * 
 * Examples:
 * - /api/audit/washing/B.0000013196/history
 * - /api/audit/broker/D.0000016388/history
 * - /api/audit/crusher/CR.0000001234/history
 */
router.get(
  '/:module/:documentNo/history',
  requirePermission('label_washing:read'), // generic permission
  ctrl.getDocumentHistory
);

/**
 * 🔹 Get available modules (utility)
 * GET /api/audit/modules
 */
router.get(
  '/modules',
  requirePermission('label_washing:read'),
  ctrl.getAvailableModules
);

module.exports = router;
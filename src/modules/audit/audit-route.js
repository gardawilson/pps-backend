// routes/audit/audit-routes.js

const express = require("express");
const router = express.Router();
const verifyToken = require("../../core/middleware/verify-token");
const attachPermissions = require("../../core/middleware/attach-permissions");
const requirePermission = require("../../core/middleware/require-permission");
const ctrl = require("./audit-controller");

router.use(verifyToken, attachPermissions);

/**
 * 🔹 Get available modules with prefixes (utility)
 * GET /api/audit/modules
 *
 * ⚠️ IMPORTANT: This route MUST be defined BEFORE the /:documentNo/history route
 * to prevent "modules" from being treated as a document number
 */
router.get(
  "/modules",
  requirePermission("label_washing:read"),
  ctrl.getAvailableModules,
);

/**
 * 🎯 Auto-detect module from document number prefix
 * GET /api/audit/:documentNo/history
 *
 * Examples:
 * - /api/audit/S.0000029967/history  → Auto-detects inject_produksi (prefix: S)
 * - /api/audit/B.0000013196/history  → Auto-detects washing (prefix: B)
 * - /api/audit/D.0000016388/history  → Auto-detects broker (prefix: D)
 *
 * This is the ONLY endpoint for getting audit history.
 * Module is automatically detected from the document number prefix.
 */
router.get(
  "/:documentNo/history",
  requirePermission("label_washing:read"),
  ctrl.getDocumentHistory,
);

module.exports = router;

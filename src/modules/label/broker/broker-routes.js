// routes/labels/broker-routes.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./broker-controller"); // pastikan path sesuai strukturmu

// Urutan penting: verify → attach → require → controller
router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/broker",
  requirePermission("label_broker:read"),
  ctrl.getAll,
);

// GET one header's details by NoBroker
router.get(
  "/labels/broker/:nobroker",
  requirePermission("label_broker:read"),
  ctrl.getOne,
);

// CREATE broker header + details (+ optional outputs)
router.post(
  "/labels/broker",
  requirePermission("label_broker:create"),
  ctrl.create,
);

// UPDATE broker header + details (+ optional outputs)
router.put(
  "/labels/broker/:nobroker",
  requirePermission("label_broker:update"),
  ctrl.update,
);

router.patch(
  "/labels/broker/:nobroker/print",
  requirePermission("label_broker:update"),
  ctrl.incrementHasBeenPrinted,
);

router.delete(
  "/labels/broker/:nobroker",
  requirePermission("label_broker:delete"),
  ctrl.remove,
);

// Example: GET /api/labels/broker/partials/D.0000000123/45
router.get(
  "/labels/broker/partials/:nobroker/:nosak",
  requirePermission("label_broker:read"),
  ctrl.getPartialInfo,
);

module.exports = router;

// routes/labels/gilingan-routes.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./gilingan-controller");

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/gilingan",
  requirePermission("label_crusher:read"),
  ctrl.getAll,
);

// CREATE Gilingan
router.post(
  "/labels/gilingan",
  requirePermission("label_crusher:create"),
  ctrl.create,
);

// UPDATE Gilingan
router.put(
  "/labels/gilingan/:noGilingan",
  requirePermission("label_crusher:update"),
  ctrl.update,
);

router.patch(
  "/labels/gilingan/:noGilingan/print",
  requirePermission("label_crusher:update"),
  ctrl.incrementHasBeenPrinted,
);

// DELETE Gilingan
router.delete(
  "/labels/gilingan/:noGilingan",
  requirePermission("label_crusher:delete"),
  ctrl.delete,
);

// Example: GET /api/labels/gilingan/partials/V.0000003626
router.get(
  "/labels/gilingan/partials/:nogilingan",
  requirePermission("label_crusher:read"), // atau label_gilingan:read kalau kamu punya
  ctrl.getGilinganPartialInfo,
);

module.exports = router;

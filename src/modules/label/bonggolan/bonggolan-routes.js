// routes/labels/bonggolan-routes.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./bonggolan-controller");

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/bonggolan",
  requirePermission("label_bonggolan:read"),
  ctrl.getAll,
);

// CREATE Bonggolan
router.post(
  "/labels/bonggolan",
  requirePermission("label_bonggolan:create"),
  ctrl.create,
);

// routes/labels/bonggolan.js
router.put(
  "/labels/bonggolan/:noBonggolan",
  requirePermission("label_bonggolan:update"),
  ctrl.update,
);

router.patch(
  "/labels/bonggolan/:noBonggolan/print",
  requirePermission("label_bonggolan:update"),
  ctrl.incrementHasBeenPrinted,
);

// DELETE Bonggolan
router.delete(
  "/labels/bonggolan/:noBonggolan",
  requirePermission("label_bonggolan:delete"),
  ctrl.delete,
);

module.exports = router;

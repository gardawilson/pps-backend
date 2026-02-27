const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./crusher-controller");

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/crusher",
  requirePermission("label_crusher:read"),
  ctrl.getAll,
);

// CREATE Crusher
router.post(
  "/labels/crusher",
  requirePermission("label_crusher:create"),
  ctrl.create,
);

router.put(
  "/labels/crusher/:noCrusher",
  requirePermission("label_crusher:update"),
  ctrl.update,
);

router.patch(
  "/labels/crusher/:noCrusher/print",
  requirePermission("label_crusher:update"),
  ctrl.incrementHasBeenPrinted,
);

// DELETE Crusher
router.delete(
  "/labels/crusher/:noCrusher",
  requirePermission("label_crusher:delete"),
  ctrl.delete,
);

module.exports = router;

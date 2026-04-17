const express = require("express");
const router = express.Router();
const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./washing-controller");

// urutan penting: verify → attach → require → controller
router.use(verifyToken, attachPermissions);

router.get(
  "/labels/washing",
  requirePermission("label_washing:read"),
  ctrl.getAll,
);

router.get(
  "/labels/washing/:nowashing",
  requirePermission("label_washing:read"),
  ctrl.getOne,
);

router.post(
  "/labels/washing",
  requirePermission("label_washing:create"),
  ctrl.create,
);

router.put(
  "/labels/washing/:nowashing",
  (req, res, next) => {
    const perms = req.userPermissions;

    if (!perms) {
      return res
        .status(500)
        .json({ success: false, message: "Permissions not attached" });
    }

    if (
      perms.has("*") ||
      perms.has("qc_label:update") ||
      perms.has("label_washing:update")
    ) {
      return next();
    }

    return res.status(403).json({
      success: false,
      message: "Forbidden: insufficient permission",
      requiredAnyOf: ["qc_label:update", "label_washing:update"],
    });
  },
  ctrl.update,
);

router.patch(
  "/labels/washing/:nowashing/print",
  requirePermission("label_washing:update"),
  ctrl.incrementHasBeenPrinted,
);

router.delete(
  "/labels/washing/:nowashing",
  requirePermission("label_washing:delete"),
  ctrl.remove,
);

// GET /labels/washing/:nowashing/pdf
router.get(
  "/labels/washing/:nowashing/pdf",
  requirePermission("label_washing:read"),
  ctrl.generatePdf,
);

module.exports = router;

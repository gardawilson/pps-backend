// routes/labels/mixer-routes.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./mixer-controller"); // pastikan path sesuai

// Urutan penting: verify → attach → require → controller
router.use(verifyToken, attachPermissions);

// GET all Mixer (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/mixer",
  requirePermission("label_mixer:read"), // sesuaikan dengan permission-mu
  ctrl.getAll,
);

// GET one header's details by NoMixer
router.get(
  "/labels/mixer/:nomixer",
  requirePermission("label_mixer:read"),
  ctrl.getOne,
);

// ⬇️ NEW: CREATE Mixer header + details + optional outputs (outputCode)
router.post(
  "/labels/mixer",
  requirePermission("label_mixer:create"),
  ctrl.create,
);

// UPDATE mixer header + details (+ optional outputs)
router.put(
  "/labels/mixer/:nomixer",
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
      perms.has("label_mixer:update")
    ) {
      return next();
    }

    return res.status(403).json({
      success: false,
      message: "Forbidden: insufficient permission",
      requiredAnyOf: ["qc_label:update", "label_mixer:update"],
    });
  },
  ctrl.update,
);

router.patch(
  "/labels/mixer/:nomixer/print",
  requirePermission("label_mixer:update"),
  ctrl.incrementHasBeenPrinted,
);

// DELETE mixer
router.delete(
  "/labels/mixer/:nomixer",
  requirePermission("label_mixer:delete"),
  ctrl.remove,
);

// Example: GET /api/labels/mixer/partials/H.0000022318/1
router.get(
  "/labels/mixer/partials/:nomixer/:nosak",
  requirePermission("label_mixer:read"),
  ctrl.getPartialInfo,
);

// GET /labels/mixer/:nomixer/pdf
router.get(
  "/labels/mixer/:nomixer/pdf",
  requirePermission("label_mixer:read"),
  ctrl.generatePdf,
);

module.exports = router;

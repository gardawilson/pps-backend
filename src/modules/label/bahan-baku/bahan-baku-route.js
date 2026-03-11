// routes/master/bahan-baku-routes.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");

const ctrl = require("./bahan-baku-controller");

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/bahan-baku",
  requirePermission("penerimaanbahanbaku:read"), // sesuaikan permission-mu
  ctrl.getAll,
);

// GET pallet list by NoBahanBaku
router.get(
  "/labels/bahan-baku/:nobahanbaku/pallet",
  requirePermission("penerimaanbahanbaku:read"),
  ctrl.getPalletByNoBahanBaku,
);

router.get(
  "/labels/bahan-baku/:nobahanbaku/pallet/:nopallet",
  requirePermission("penerimaanbahanbaku:read"),
  ctrl.getDetailByNoBahanBakuAndNoPallet,
);

// PUT update pallet header by NoBahanBaku and NoPallet
router.put(
  "/labels/bahan-baku/:nobahanbaku/pallet/:nopallet",
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
      perms.has("penerimaanbahanbaku:update")
    ) {
      return next();
    }

    return res.status(403).json({
      success: false,
      message: "Forbidden: insufficient permission",
      requiredAnyOf: ["qc_label:update", "penerimaanbahanbaku:update"],
    });
  },
  ctrl.updateByNoBahanBakuAndNoPallet,
);

router.patch(
  "/labels/bahan-baku/:nobahanbaku/pallet/:nopallet/print",
  requirePermission("penerimaanbahanbaku:update"),
  ctrl.incrementHasBeenPrinted,
);

module.exports = router;

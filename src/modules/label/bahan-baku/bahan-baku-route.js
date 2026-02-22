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
  requirePermission("label_crusher:read"), // sesuaikan permission-mu
  ctrl.getAll,
);

// GET pallet list by NoBahanBaku
router.get(
  "/labels/bahan-baku/:nobahanbaku/pallet",
  requirePermission("label_crusher:read"),
  ctrl.getPalletByNoBahanBaku,
);

router.get(
  "/labels/bahan-baku/:nobahanbaku/pallet/:nopallet",
  requirePermission("label_crusher:read"),
  ctrl.getDetailByNoBahanBakuAndNoPallet,
);

// PUT update pallet header by NoBahanBaku and NoPallet
router.put(
  "/labels/bahan-baku/:nobahanbaku/pallet/:nopallet",
  requirePermission("label_crusher:read"),
  ctrl.updateByNoBahanBakuAndNoPallet,
);

module.exports = router;

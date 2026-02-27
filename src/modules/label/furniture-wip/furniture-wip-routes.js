// routes/labels/furniture-wip-routes.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const attachPermissions = require("../../../core/middleware/attach-permissions");
const requirePermission = require("../../../core/middleware/require-permission");
const ctrl = require("./furniture-wip-controller");

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  "/labels/furniture-wip",
  requirePermission("label_furniturewip:read"), // 🔁 sesuaikan dengan permission yang kamu pakai
  ctrl.getAll,
);

// CREATE Furniture WIP
router.post(
  "/labels/furniture-wip",
  requirePermission("label_furniturewip:create"), // ganti sesuai permission-mu
  ctrl.create,
);

// UPDATE Furniture WIP
router.put(
  "/labels/furniture-wip/:noFurnitureWip",
  requirePermission("label_furniturewip:update"),
  ctrl.update,
);

router.patch(
  "/labels/furniture-wip/:noFurnitureWip/print",
  requirePermission("label_furniturewip:update"),
  ctrl.incrementHasBeenPrinted,
);

// DELETE Furniture WIP
router.delete(
  "/labels/furniture-wip/:noFurnitureWip",
  requirePermission("label_furniturewip:delete"), // atau pakai permission lain yg kamu pakai
  ctrl.delete,
);

// Example: GET /api/labels/furniture-wip/partials/BB.0000000123
router.get(
  "/labels/furniture-wip/partials/:nofurniturewip",
  requirePermission("label_furniturewip:read"), // atau permission yang kamu pakai
  ctrl.getFurnitureWipPartialInfo,
);

module.exports = router;

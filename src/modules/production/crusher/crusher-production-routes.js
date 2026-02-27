const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const ctrl = require("./crusher-production-controller");

router.get("/crusher", verifyToken, ctrl.getAllProduksi);

// GET CrusherProduksi_h by date (YYYY-MM-DD)
router.get(
  "/crusher/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  ctrl.getProduksiByDate,
);

// GET master crushers (enabled only, for dropdowns)
router.get("/crusher/masters", verifyToken, ctrl.getCrusherMasters);

router.post("/crusher", verifyToken, ctrl.createProduksi);

router.put("/crusher/:noCrusherProduksi", verifyToken, ctrl.updateProduksi); // ⬅️ NEW

router.delete("/crusher/:noCrusherProduksi", verifyToken, ctrl.deleteProduksi); // ⬅️ NEW

router.get(
  "/crusher/:noCrusherProduksi/inputs",
  verifyToken,
  ctrl.getInputsByNoCrusherProduksi,
);

router.get(
  "/crusher/:noCrusherProduksi/outputs",
  verifyToken,
  ctrl.getOutputsByNoCrusherProduksi,
);

router.get(
  "/crusher/validate-label/:labelCode",
  verifyToken,
  ctrl.validateLabel,
); // ⬅️ NEW

router.post(
  "/crusher/:noCrusherProduksi/inputs",
  verifyToken,
  ctrl.upsertInputsAndPartials,
); // ⬅️ NEW

router.delete(
  "/crusher/:noCrusherProduksi/inputs",
  verifyToken,
  ctrl.deleteInputsAndPartials,
); // ⬅️ NEW

module.exports = router;

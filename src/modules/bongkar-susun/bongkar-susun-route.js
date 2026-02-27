// routes/bongkar-susun-route.js
const express = require("express");
const router = express.Router();
const verifyToken = require("../../core/middleware/verify-token");
const bongkarSusunController = require("./bongkar-susun-controller");

// GET data BongkarSusun_h untuk tanggal tertentu
router.get(
  "/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  bongkarSusunController.getByDate,
);

// GET /bongkar-susun?page=1&pageSize=20&search=...
router.get("/", verifyToken, bongkarSusunController.getAllBongkarSusun);

// ✅ NEW: CREATE
router.post("/", verifyToken, bongkarSusunController.createBongkarSusun);

// ✅ UPDATE
router.put(
  "/:noBongkarSusun",
  verifyToken,
  bongkarSusunController.updateBongkarSusun,
);

// ✅ DELETE
router.delete(
  "/:noBongkarSusun",
  verifyToken,
  bongkarSusunController.deleteBongkarSusun,
);

// routes/bongkar-susun-routes.js
router.get(
  "/:noBongkarSusun/inputs",
  verifyToken,
  bongkarSusunController.getInputsByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs",
  verifyToken,
  bongkarSusunController.getOutputsByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/bb",
  verifyToken,
  bongkarSusunController.getOutputsBbByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/barang-jadi",
  verifyToken,
  bongkarSusunController.getOutputsBarangJadiByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/bonggolan",
  verifyToken,
  bongkarSusunController.getOutputsBonggolanByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/broker",
  verifyToken,
  bongkarSusunController.getOutputsBrokerByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/crusher",
  verifyToken,
  bongkarSusunController.getOutputsCrusherByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/furniture-wip",
  verifyToken,
  bongkarSusunController.getOutputsFurnitureWipByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/gilingan",
  verifyToken,
  bongkarSusunController.getOutputsGilinganByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/mixer",
  verifyToken,
  bongkarSusunController.getOutputsMixerByNoBongkarSusun,
);

router.get(
  "/:noBongkarSusun/outputs/washing",
  verifyToken,
  bongkarSusunController.getOutputsWashingByNoBongkarSusun,
);

router.get(
  "/validate-label/:labelCode",
  verifyToken,
  bongkarSusunController.validateLabel,
);

// routes/bongkar-susun-routes.js
router.post(
  "/:noBongkarSusun/inputs",
  verifyToken,
  bongkarSusunController.upsertInputs,
);

// DELETE: delete inputs dari NoBongkarSusun
router.delete(
  "/:noBongkarSusun/inputs",
  verifyToken,
  bongkarSusunController.deleteInputs,
);

module.exports = router;

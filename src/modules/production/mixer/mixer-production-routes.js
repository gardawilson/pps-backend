// routes/mixer-production-route.js
const express = require("express");
const router = express.Router();
const verifyToken = require("../../../core/middleware/verify-token");
const mixerProduksiController = require("./mixer-production-controller");

// GET MixerProduksi_h by date (YYYY-MM-DD)
router.get(
  "/mixer/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  mixerProduksiController.getProduksiByDate,
);

// GET /mixer?page=1&pageSize=20&search=M.0000...
router.get("/mixer", verifyToken, mixerProduksiController.getAllProduksi);

router.post("/mixer", verifyToken, mixerProduksiController.createProduksi);

router.put(
  "/mixer/:noProduksi",
  verifyToken,
  mixerProduksiController.updateProduksi,
);

router.delete(
  "/mixer/:noProduksi",
  verifyToken,
  mixerProduksiController.deleteProduksi,
);

router.get(
  "/mixer/:noProduksi/inputs",
  verifyToken,
  mixerProduksiController.getInputsByNoProduksi,
);

router.get(
  "/mixer/:noProduksi/outputs",
  verifyToken,
  mixerProduksiController.getOutputsByNoProduksi,
);

router.get(
  "/mixer/validate-label/:labelCode",
  verifyToken,
  mixerProduksiController.validateLabel,
);

router.post(
  "/mixer/:noProduksi/inputs",
  verifyToken,
  mixerProduksiController.upsertInputsAndPartials,
);

router.delete(
  "/mixer/:noProduksi/inputs",
  verifyToken,
  mixerProduksiController.deleteInputsAndPartials,
);

module.exports = router;

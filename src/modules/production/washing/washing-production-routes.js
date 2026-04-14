// routes/production-route.js
const express = require("express");
const router = express.Router();
const verifyToken = require("../../../core/middleware/verify-token");
const washingProduksiController = require("./washing-production-controller");
const reguCtrl = require("../../master/regu/master-regu-controller");

// GET /washing?page=1&pageSize=20
router.get("/washing", verifyToken, washingProduksiController.getAllProduksi);

// Validasi pola tanggal langsung di route (YYYY-MM-DD)
router.get(
  "/washing/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  washingProduksiController.getProduksiByDate,
);

// ✅ Create WashingProduksi_h
// req.body support: { ..., isBlower: 1 | 0 }
router.post("/washing", verifyToken, washingProduksiController.createProduksi);

// req.body support: { ..., isBlower: 1 | 0 }
router.put(
  "/washing/:noProduksi",
  verifyToken,
  washingProduksiController.updateProduksi,
);

router.delete(
  "/washing/:noProduksi",
  verifyToken,
  washingProduksiController.deleteProduksi,
);

// GET /api/production/washing/:noProduksi/inputs
router.get(
  "/washing/:noProduksi/inputs",
  verifyToken,
  washingProduksiController.getInputsByNoProduksi,
);

// GET /api/production/washing/:noProduksi/outputs
router.get(
  "/washing/:noProduksi/outputs",
  verifyToken,
  washingProduksiController.getOutputsByNoProduksi,
);

router.get(
  "/washing/validate-label/:labelCode",
  verifyToken,
  washingProduksiController.validateLabel,
);

router.post(
  "/washing/:noProduksi/inputs",
  verifyToken,
  washingProduksiController.upsertInputsAndPartials,
);

router.delete(
  "/washing/:noProduksi/inputs",
  verifyToken,
  washingProduksiController.deleteInputsAndPartials,
);

// List regu
// Query: ?q=nama&idBagian=1&orderBy=NamaRegu&orderDir=ASC
router.get("/washing/regu", verifyToken, reguCtrl.list);

module.exports = router;

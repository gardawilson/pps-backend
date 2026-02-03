// routes/inject-production-route.js
const express = require("express");
const router = express.Router();
const verifyToken = require("../../../core/middleware/verify-token");
const injectProduksiController = require("./inject-production-controller");

// ✅ GET ALL InjectProduksi_h (paged)
router.get("/inject", verifyToken, injectProduksiController.getAllProduksi);

// 🔹 GET Furniture WIP from InjectProduksi_h by NoProduksi
router.get(
  "/inject/furniture-wip/:noProduksi",
  verifyToken,
  injectProduksiController.getFurnitureWipByNoProduksi,
);

// 🔹 GET Barang Jadi (Packing) from InjectProduksi_h by NoProduksi
router.get(
  "/inject/packing/:noProduksi",
  verifyToken,
  injectProduksiController.getPackingByNoProduksi,
);

// 🔹 GET InjectProduksi_h by date (YYYY-MM-DD)
// ⚠️ keep this LAST so it doesn't conflict with /inject (list)
router.get(
  "/inject/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  injectProduksiController.getProduksiByDate,
);

router.post("/inject", verifyToken, injectProduksiController.createProduksi);

router.put(
  "/inject/:noProduksi",
  verifyToken,
  injectProduksiController.updateProduksi,
);

router.delete(
  "/inject/:noProduksi",
  verifyToken,
  injectProduksiController.deleteProduksi,
);

router.get(
  "/inject/:noProduksi/inputs",
  verifyToken,
  injectProduksiController.getInputsByNoProduksi,
);

router.get(
  "/inject/validate-label/:labelCode",
  verifyToken,
  injectProduksiController.validateLabel,
);

router.post(
  "/inject/:noProduksi/inputs",
  verifyToken,
  injectProduksiController.upsertInputsAndPartials,
);

router.delete(
  "/inject/:noProduksi/inputs",
  verifyToken,
  injectProduksiController.deleteInputsAndPartials,
);

module.exports = router;

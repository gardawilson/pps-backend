// routes/gilingan-production-route.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const gilinganProduksiController = require("./gilingan-production-controller");

// GET GilinganProduksi_h by date (YYYY-MM-DD)
router.get(
  "/gilingan/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  gilinganProduksiController.getProduksiByDate,
);

// GET /gilingan/produksi?page=1&pageSize=20&search=G.00001
router.get("/gilingan", verifyToken, gilinganProduksiController.getAllProduksi);

router.post(
  "/gilingan",
  verifyToken,
  gilinganProduksiController.createProduksi,
);

// UPDATE
router.put(
  "/gilingan/:noProduksi",
  verifyToken,
  gilinganProduksiController.updateProduksi,
);

// DELETE
router.delete(
  "/gilingan/:noProduksi",
  verifyToken,
  gilinganProduksiController.deleteProduksi,
);

// routes/gilingan-produksi.js
router.get(
  "/gilingan/:noProduksi/inputs",
  verifyToken,
  gilinganProduksiController.getInputsByNoProduksi,
);

// Add this route after your existing routes
router.get(
  "/gilingan/validate-label/:labelCode",
  verifyToken,
  gilinganProduksiController.validateLabel,
);

router.post(
  "/gilingan/:noProduksi/inputs",
  verifyToken,
  gilinganProduksiController.upsertInputsAndPartials,
);

router.delete(
  "/gilingan/:noProduksi/inputs",
  verifyToken,
  gilinganProduksiController.deleteInputsAndPartials,
);

module.exports = router;

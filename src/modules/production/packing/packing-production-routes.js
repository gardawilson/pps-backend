// routes/packing-production-route.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const packingController = require("./packing-production-controller");

// ✅ GET ALL (paging + search)
// Example: GET /api/packing/packing?page=1&pageSize=20&search=PK.00001
router.get("/packing", verifyToken, packingController.getAllProduksi);

// ✅ GET by date (YYYY-MM-DD)
// Example: GET /api/packing/packing/2025-12-02
router.get(
  "/packing/:date(\\d{4}-\\d{2}-\\d{2})",
  verifyToken,
  packingController.getProduksiByDate,
);

router.post("/packing", verifyToken, packingController.createProduksi);

router.put(
  "/packing/:noPacking",
  verifyToken,
  packingController.updateProduksi,
);

router.delete(
  "/packing/:noPacking",
  verifyToken,
  packingController.deleteProduksi,
);

router.get(
  "/packing/:noPacking/inputs",
  verifyToken,
  packingController.getInputsByNoPacking,
);

router.get(
  "/packing/:noPacking/outputs",
  verifyToken,
  packingController.getOutputsByNoPacking,
);

router.post(
  "/packing/:noPacking/inputs",
  verifyToken,
  packingController.upsertInputsAndPartials,
);

router.delete(
  "/packing/:noPacking/inputs",
  verifyToken,
  packingController.deleteInputsAndPartials,
);

module.exports = router;

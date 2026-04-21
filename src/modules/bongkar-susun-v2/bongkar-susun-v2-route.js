// modules/bongkar-susun-v2/bongkar-susun-v2-route.js
const express = require("express");
const router = express.Router();
const verifyToken = require("../../core/middleware/verify-token");
const ctrl = require("./bongkar-susun-v2-controller");

// Cek info label sebelum di-input (universal, routing by prefix)
router.get("/label/:labelCode", verifyToken, ctrl.getLabelInfo);

// List semua transaksi bongkar susun v2
router.get("/", verifyToken, ctrl.getAll);

// Detail satu transaksi
router.get("/:noBongkarSusun", verifyToken, ctrl.getDetail);

// Buat transaksi bongkar susun baru
router.post("/", verifyToken, ctrl.create);

// Hapus transaksi
router.delete("/:noBongkarSusun", verifyToken, ctrl.deleteBongkarSusun);

module.exports = router;

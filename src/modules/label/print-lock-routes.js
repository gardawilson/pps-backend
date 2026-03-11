const express = require("express");
const router = express.Router();
const verifyToken = require("../../core/middleware/verify-token");
const attachPermissions = require("../../core/middleware/attach-permissions");
const ctrl = require("./print-lock-controller");

router.use(verifyToken, attachPermissions);

// List semua active print locks
router.get("/labels/print-locks", ctrl.getAllLocks);

// Acquire lock — dipanggil saat user buka print preview
// contoh: POST /api/labels/B.0000000003/print-lock
router.post("/labels/:noLabel/print-lock", ctrl.acquire);

// Release lock — dipanggil jika user close preview tanpa cetak
router.delete("/labels/:noLabel/print-lock", ctrl.release);

module.exports = router;

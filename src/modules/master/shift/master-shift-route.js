const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const ctrl = require("./master-shift-controller");

// GET shift hour by tanggal & shift
// Query: ?tanggal=2026-05-09&shift=1
router.get("/shift/hour", verifyToken, ctrl.getShiftHours);

module.exports = router;

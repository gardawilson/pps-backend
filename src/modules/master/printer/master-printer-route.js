const express = require("express");
const verifyToken = require("../../../core/middleware/verify-token");
const ctrl = require("./master-printer-controller");

const router = express.Router();

router.get("/mst-printer", verifyToken, ctrl.list);
router.post("/mst-printer", verifyToken, ctrl.upsert);
router.delete("/mst-printer/:mac", verifyToken, ctrl.remove);

module.exports = router;

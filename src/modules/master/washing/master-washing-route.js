const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const ctrl = require("./master-washing-controller");

// GET only active (IsEnable = 1)
router.get("/", verifyToken, ctrl.getAllActive);

module.exports = router;

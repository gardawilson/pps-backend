const express = require("express");
const router = express.Router();

const verifyToken = require("../../core/middleware/verify-token");
const ctrl = require("./master-mesin-controller");

// GET mesin broker (IdBagianMesin = 2)
router.get("/broker", verifyToken, ctrl.getBroker);
// GET mesin washing (IdBagianMesin = 7)
router.get("/washing", verifyToken, ctrl.getWashing);
// GET mesin crusher (IdBagianMesin = 3)
router.get("/crusher", verifyToken, ctrl.getCrusher);

// GET by idbagian (only active by default)
// The regex enforces numeric-only for :idbagian
router.get("/:idbagian(\\d+)", verifyToken, ctrl.getByIdBagian);

module.exports = router;

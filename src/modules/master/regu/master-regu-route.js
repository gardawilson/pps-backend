// master-regu-route.js
const express = require("express");
const router = express.Router();

const verifyToken = require("../../../core/middleware/verify-token");
const ctrl = require("./master-regu-controller");

// List regu
// Query: ?q=nama&idBagian=1&orderBy=NamaRegu&orderDir=ASC
router.get("/regu", verifyToken, ctrl.list);

module.exports = router;

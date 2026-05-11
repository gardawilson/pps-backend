const express = require("express");
const router = express.Router();
const verifyToken = require("../../core/middleware/verify-token");
const mappingController = require("./mapping-controller");

router.get("/blok", verifyToken, mappingController.getMapping);
router.get("/lokasi", verifyToken, mappingController.getLokasiByBlok);

module.exports = router;

const express = require('express');
const router = express.Router();

const verifyToken = require('../../core/middleware/verify-token');
const ctrl = require('./master-furniture-material-controller');


router.get('/cabinet-materials', ctrl.getMasterCabinetMaterials);

// Lookup by IdCetakan + IdWarna
router.get('/by-cetakan-warna', verifyToken, ctrl.getByCetakanWarna);

module.exports = router;

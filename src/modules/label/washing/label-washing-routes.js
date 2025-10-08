const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const labelWashingController = require('./label-washing-controller');

// GET all header washing (pagination + search)
router.get('/labels/washing', verifyToken, labelWashingController.getAll);

// GET one header + detail by NoWashing
router.get('/labels/washing/:nowashing', verifyToken, labelWashingController.getOne);

// POST create header washing
router.post('/labels/washing', verifyToken, labelWashingController.createHeader);

// POST create details for a washing header
router.post('/labels/washing/:nowashing/details', verifyToken, labelWashingController.createDetails);

module.exports = router;

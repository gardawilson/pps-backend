const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const labelWashingController = require('./label-washing-controller');

// GET semua data header Washing_h
router.get('/label/washing', verifyToken, labelWashingController.getLabelList);

// GET detail Washing_d berdasarkan NoWashing
router.get('/label/washing/detail/:nowashing', verifyToken, labelWashingController.getDetailLabel);

// POST header Washing_h
router.post('/label/washing', verifyToken, labelWashingController.createWashingData);

// ✅ Tambahkan route POST untuk simpan data detail Washing_d
router.post('/label/washing/detail', verifyToken, labelWashingController.createWashingDetail);

module.exports = router;

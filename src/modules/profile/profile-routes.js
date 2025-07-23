const express = require('express');
const { getProfile, changePassword } = require('./profile-controller');
const verifyToken = require('../../core/middleware/verify-token');

const router = express.Router();

router.get('/profile', verifyToken, getProfile);
router.post('/change-password', verifyToken, changePassword);

module.exports = router;

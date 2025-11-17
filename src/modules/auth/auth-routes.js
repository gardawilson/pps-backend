const express = require('express');
const router = express.Router();
const authController = require('./auth-controller');

router.use(express.json()); // Middleware parsing JSON

router.post('/login1', authController.login);

module.exports = router;

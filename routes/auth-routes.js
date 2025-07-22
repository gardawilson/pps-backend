const express = require('express');
const router = express.Router();
const authController = require('../controllers/auth-controller');

router.use(express.json()); // Middleware parsing JSON

router.post('/login', authController.login);

module.exports = router;

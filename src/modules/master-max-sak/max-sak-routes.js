const express = require('express');
const router = express.Router();
const verifyToken = require('../../core/middleware/verify-token');
const maxSakController = require('./max-sak-controller');

// GET list (dengan pagination & optional filter by IdBagian)
router.get('/master/max-sak', verifyToken, maxSakController.getAll);

// GET single by IdBagian
router.get('/:id', verifyToken, maxSakController.getOne);

// CREATE
router.post('/master/max-sak', verifyToken, maxSakController.create);

// UPDATE
router.put('/master/max-sak/:id', verifyToken, maxSakController.update);

// DELETE
router.delete('/master/max-sak/:id', verifyToken, maxSakController.remove);

module.exports = router;

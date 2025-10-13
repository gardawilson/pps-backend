const express = require('express');
const router = express.Router();
const verifyToken = require('../../../core/middleware/verify-token');
const attachPermissions = require('../../../core/middleware/attach-permissions');
const requirePermission = require('../../../core/middleware/require-permission');
const ctrl = require('./label-washing-controller');

// urutan penting: verify → attach → require → controller
router.use(verifyToken, attachPermissions);

router.get('/labels/washing',
  requirePermission('label_washing:read'),
  ctrl.getAll);

router.get('/labels/washing/:nowashing',
  requirePermission('label_washing:read'),
  ctrl.getOne);

router.post('/labels/washing',
  requirePermission('label_washing:create'),
  ctrl.create);

router.put('/labels/washing/:nowashing',
  requirePermission('label_washing:update'),
  ctrl.update);

router.delete('/labels/washing/:nowashing',
  requirePermission('label_washing:delete'),
  ctrl.remove);

module.exports = router;

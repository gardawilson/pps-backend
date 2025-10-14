// routes/labels/broker-routes.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const attachPermissions = require('../../../core/middleware/attach-permissions');
const requirePermission = require('../../../core/middleware/require-permission');
const ctrl = require('./broker-controller'); // pastikan path sesuai strukturmu

// Urutan penting: verify → attach → require → controller
router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  '/labels/broker',
  requirePermission('label_broker:read'),
  ctrl.getAll
);

// // GET one
// router.get(
//   '/labels/broker/:nobroker',
//   requirePermission('label_broker:read'),
//   ctrl.getOne
// );

// // CREATE
// router.post(
//   '/labels/broker',
//   requirePermission('label_broker:create'),
//   ctrl.create
// );

// // UPDATE
// router.put(
//   '/labels/broker/:nobroker',
//   requirePermission('label_broker:update'),
//   ctrl.update
// );

// // DELETE
// router.delete(
//   '/labels/broker/:nobroker',
//   requirePermission('label_broker:delete'),
//   ctrl.remove
// );

module.exports = router;

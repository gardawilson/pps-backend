// routes/labels/packing-routes.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const attachPermissions = require('../../../core/middleware/attach-permissions');
const requirePermission = require('../../../core/middleware/require-permission');
const ctrl = require('./packing-controller');

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  '/labels/packing',
  requirePermission('label_crusher:read'), // 🔁 sesuaikan dengan permission yang kamu pakai
  ctrl.getAll
);

// CREATE Packing / BarangJadi
router.post(
  '/labels/packing',
  requirePermission('label_crusher:create'), // ganti sesuai permission-mu
  ctrl.create
);

// UPDATE (EDIT)
router.put(
  '/labels/packing/:noBJ',
  requirePermission('label_crusher:update'), // sesuaikan permission
  ctrl.update
);


// DELETE
router.delete(
  '/labels/packing/:noBJ',
  requirePermission('label_crusher:delete'),
  ctrl.remove
);


router.get(
  '/labels/packing/partials/:nobj',
  requirePermission('label_crusher:read'),
  ctrl.getPackingPartialInfo
);


module.exports = router;

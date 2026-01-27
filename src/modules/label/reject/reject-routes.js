// routes/labels/reject-routes.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const attachPermissions = require('../../../core/middleware/attach-permissions');
const requirePermission = require('../../../core/middleware/require-permission');
const ctrl = require('./reject-controller');

router.use(verifyToken, attachPermissions);

// GET all (pagination + search ?page=&limit=&search=)
router.get(
  '/labels/reject',
  requirePermission('label_reject:read'), // 🔁 sesuaikan dengan permission yang kamu pakai di sistem
  ctrl.getAll
);


// CREATE Reject
router.post(
    '/labels/reject',
    requirePermission('label_reject:create'), // sesuaikan dengan permission-mu
    ctrl.create
  );


  // UPDATE satu Reject
router.put(
    '/labels/reject/:noReject',
    requirePermission('label_reject:update'), // sesuaikan permission-mu
    ctrl.update
  );


  // DELETE
router.delete(
    '/labels/reject/:noReject',
    requirePermission('label_reject:delete'), // sesuaikan dengan permission-mu
    ctrl.delete
  );
  
  
  router.get(
    '/labels/reject/partials/:noreject',
    requirePermission('label_reject:read'),
    ctrl.getRejectPartialInfo
  );


module.exports = router;

// routes/labels/mixer-routes.js
const express = require('express');
const router = express.Router();

const verifyToken = require('../../../core/middleware/verify-token');
const attachPermissions = require('../../../core/middleware/attach-permissions');
const requirePermission = require('../../../core/middleware/require-permission');
const ctrl = require('./mixer-controller'); // pastikan path sesuai

// Urutan penting: verify → attach → require → controller
router.use(verifyToken, attachPermissions);

// GET all Mixer (pagination + search ?page=&limit=&search=)
router.get(
  '/labels/mixer',
  requirePermission('label_crusher:read'), // sesuaikan dengan permission-mu
  ctrl.getAll
);


// GET one header's details by NoMixer
router.get(
    '/labels/mixer/:nomixer',
    requirePermission('label_crusher:read'),
    ctrl.getOne
  );
  

  // ⬇️ NEW: CREATE Mixer header + details + optional outputs (outputCode)
router.post(
    '/labels/mixer',
    requirePermission('label_crusher:create'),
    ctrl.create
  );


  // UPDATE mixer header + details (+ optional outputs)
router.put(
    '/labels/mixer/:nomixer',
    requirePermission('label_crusher:update'),
    ctrl.update
  );
  


  // DELETE mixer
router.delete(
    '/labels/mixer/:nomixer',
    requirePermission('label_crusher:delete'),
    ctrl.remove
  );
  

  // Example: GET /api/labels/mixer/partials/H.0000022318/1
router.get(
    '/labels/mixer/partials/:nomixer/:nosak',
    requirePermission('label_crusher:read'),
    ctrl.getPartialInfo
  );

// (Nanti kalau mau GET one / create / update / delete tinggal tambah di sini)
module.exports = router;

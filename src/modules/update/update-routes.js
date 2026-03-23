const express = require("express");
const router = express.Router();
const multer = require("multer");

const updateController = require("./update-controller");

const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (_req, file, cb) => {
    if (file.originalname.toLowerCase().endsWith(".apk")) {
      cb(null, true);
    } else {
      const e = new Error("Only .apk files are allowed");
      e.statusCode = 400;
      cb(e, false);
    }
  },
});

// ✅ PUBLIC
router.get("/:appId/version", updateController.getVersion);
router.get("/:appId/download/:file", updateController.downloadApk);

// ✅ ADMIN ONLY
router.post(
  "/:appId/publish",
  upload.single("apk"),
  updateController.publishVersion,
);

module.exports = router;

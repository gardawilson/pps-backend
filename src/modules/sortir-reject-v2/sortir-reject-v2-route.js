const express = require("express");
const router = express.Router();
const verifyToken = require("../../core/middleware/verify-token");
const ctrl = require("./sortir-reject-v2-controller");

router.get("/label/:labelCode", verifyToken, ctrl.getLabelInfo);
router.get("/", verifyToken, ctrl.getAll);
router.get("/:noBJSortir", verifyToken, ctrl.getDetail);
router.post("/", verifyToken, ctrl.create);
router.post("/:noBJSortir/reject", verifyToken, ctrl.createReject);
router.delete("/:noBJSortir", verifyToken, ctrl.deleteSortirReject);

module.exports = router;

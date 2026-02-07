// controllers/audit/audit-controller.js

const auditService = require("./audit-service");

/**
 * 🎯 Auto-detect module from document number prefix
 * Route: GET /api/audit/:documentNo/history
 */
exports.getDocumentHistory = async (req, res) => {
  const documentNo = String(req.params.documentNo || "").trim();

  if (!documentNo) {
    return res.status(400).json({
      success: false,
      message: "Document number is required",
    });
  }

  try {
    const actor = req.username || req.user?.username || "system";

    // Call service WITHOUT module parameter - let it auto-detect
    const result = await auditService.getDocumentHistory({
      documentNo,
      actor,
    });

    return res.status(200).json({
      success: true,
      message: `Audit history for ${result.module.toUpperCase()}: ${documentNo}`,
      data: result,
    });
  } catch (err) {
    console.error("[audit.getDocumentHistory]", err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

/**
 * 🔹 Get available modules with prefixes (utility endpoint)
 */
exports.getAvailableModules = async (req, res) => {
  try {
    const modules = Object.keys(auditService.MODULE_CONFIG).map((key) => ({
      module: key,
      prefix: auditService.MODULE_CONFIG[key].prefix || null,
      pkField: auditService.MODULE_CONFIG[key].pkField,
      tables: [
        auditService.MODULE_CONFIG[key].headerTable,
        auditService.MODULE_CONFIG[key].detailTable,
        ...(auditService.MODULE_CONFIG[key].outputTables || []),
        ...(auditService.MODULE_CONFIG[key].inputTables || []),
      ].filter(Boolean),
    }));

    return res.status(200).json({
      success: true,
      data: {
        modules,
        prefixMap: modules
          .filter((m) => m.prefix)
          .reduce((acc, m) => {
            acc[m.prefix] = m.module;
            return acc;
          }, {}),
      },
    });
  } catch (err) {
    console.error("[audit.getAvailableModules]", err);
    return res.status(500).json({
      success: false,
      message: err.message || "Terjadi kesalahan server",
    });
  }
};

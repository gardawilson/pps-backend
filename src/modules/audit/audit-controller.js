const auditService = require('./audit-service');

/**
 * 🎯 Generic handler - module + documentNo dari params/query
 * Route: GET /api/audit/:module/:documentNo/history
 */
exports.getDocumentHistory = async (req, res) => {
  const module = String(req.params.module || '').trim();
  const documentNo = String(req.params.documentNo || '').trim();

  if (!module) {
    return res.status(400).json({ 
      success: false, 
      message: 'Module is required (washing, broker, crusher, etc)' 
    });
  }

  if (!documentNo) {
    return res.status(400).json({ 
      success: false, 
      message: 'Document number is required' 
    });
  }

  try {
    const actor = req.username || req.user?.username || 'system';

    const result = await auditService.getDocumentHistory({
      module,
      documentNo,
      actor, // optional untuk logging
    });

    return res.status(200).json({
      success: true,
      message: `Audit history for ${result.module.toUpperCase()}: ${documentNo}`,
      data: result,
    });
  } catch (err) {
    console.error('[audit.getDocumentHistory]', err);
    const status = err.statusCode || 500;
    return res.status(status).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};

/**
 * 🔹 Get available modules (utility endpoint)
 */
exports.getAvailableModules = async (req, res) => {
  try {
    const modules = Object.keys(auditService.MODULE_CONFIG).map(key => ({
      module: key,
      pkField: auditService.MODULE_CONFIG[key].pkField,
      tables: [
        auditService.MODULE_CONFIG[key].headerTable,
        auditService.MODULE_CONFIG[key].detailTable,
        ...auditService.MODULE_CONFIG[key].outputTables
      ]
    }));

    return res.status(200).json({
      success: true,
      data: { modules }
    });
  } catch (err) {
    console.error('[audit.getAvailableModules]', err);
    return res.status(500).json({
      success: false,
      message: err.message || 'Terjadi kesalahan server',
    });
  }
};
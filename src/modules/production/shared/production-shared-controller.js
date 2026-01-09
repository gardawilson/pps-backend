// production-shared-controller.js
const sharedService = require('./production-shared-service');

async function lookupLabel(req, res) {
  const labelCode = String(req.params.labelCode || '').trim();

  if (!labelCode) {
    return res.status(400).json({
      success: false,
      message: 'labelCode is required',
    });
  }

  try {
    const result = await sharedService.lookupLabel(labelCode);

    if (!result.found) {
      return res.status(404).json({
        success: false,
        message: `Label ${labelCode} not found or already used`,
        tableName: result.tableName,
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Label lookup success',
      tableName: result.tableName,
      totalRecords: result.count,
      data: result.data,
    });
  } catch (e) {
    console.error('[shared.lookupLabel]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
  }
}

module.exports = {
  lookupLabel,
};

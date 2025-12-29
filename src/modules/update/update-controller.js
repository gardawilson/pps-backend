const updateService = require('./update-service');

async function getVersion(req, res) {
  try {
    const appId = String(req.params.appId || '').trim();
    const data = await updateService.readVersionJson(appId);

    return res.status(200).json({
      success: true,
      message: 'Update version info retrieved',
      data,
    });
  } catch (err) {
    console.error('[update.getVersion]', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
}

async function downloadApk(req, res) {
  try {
    const appId = String(req.params.appId || '').trim();
    const fileName = String(req.params.file || '').trim();

    await updateService.streamApk(appId, fileName, req, res);
  } catch (err) {
    console.error('[update.downloadApk]', err);
    if (res.headersSent) return;
    return res.status(err.statusCode || 500).send(err.message || 'Internal Server Error');
  }
}

async function publishVersion(req, res) {
  try {
    const appId = String(req.params.appId || '').trim();

    const token = String(req.headers['x-update-token'] || '');
    await updateService.assertPublishToken(token);

    const body = req.body || {};
    const result = await updateService.publishVersionJson(appId, body);

    return res.status(200).json({
      success: true,
      message: 'version.json updated',
      data: result,
    });
  } catch (err) {
    console.error('[update.publishVersion]', err);
    return res.status(err.statusCode || 500).json({
      success: false,
      message: err.message || 'Internal Server Error',
    });
  }
}

module.exports = { getVersion, downloadApk, publishVersion };

const service = require('./master-mesin-service');

async function getByIdBagian(req, res) {
  const { username } = req;

  // Enforced numeric by route, but still parse & guard
  const idStr = req.params.idbagian;
  const idBagianMesin = Number.parseInt(idStr, 10);

  // Optional toggle: include disabled via query ?includeDisabled=1
  const includeDisabled = String(req.query.includeDisabled || '0') === '1';

  if (!Number.isInteger(idBagianMesin)) {
    return res.status(400).json({
      success: false,
      message: 'idbagian must be an integer',
    });
  }

  console.log(
    '🔍 Fetching MstMesin by IdBagianMesin | Username:',
    username,
    '| IdBagianMesin:',
    idBagianMesin,
    '| includeDisabled:',
    includeDisabled
  );

  try {
    const data = await service.getByIdBagian({ idBagianMesin, includeDisabled });
    return res.status(200).json({
      success: true,
      message: 'Data MstMesin by IdBagianMesin berhasil diambil',
      idBagianMesin,
      includeDisabled,
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstMesin by IdBagianMesin:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

async function getBroker(req, res) {
  const { username } = req;
  const idBagianMesin = 2;
  const includeDisabled = String(req.query.includeDisabled || '1') === '1';

  console.log(
    '🔍 Fetching MstMesin broker | Username:',
    username,
    '| IdBagianMesin:',
    idBagianMesin,
    '| includeDisabled:',
    includeDisabled
  );

  try {
    const data = await service.getByIdBagian({ idBagianMesin, includeDisabled });
    return res.status(200).json({
      success: true,
      message: 'Data MstMesin broker berhasil diambil',
      idBagianMesin,
      includeDisabled,
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstMesin broker:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getByIdBagian, getBroker };

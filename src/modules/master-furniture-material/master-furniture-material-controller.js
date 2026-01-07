const service = require('./master-furniture-material-service');


async function getMasterCabinetMaterials(req, res) {
  const idWarehouseRaw = req.query.idWarehouse;

  const idWarehouse = (idWarehouseRaw === undefined || idWarehouseRaw === null || idWarehouseRaw === '')
    ? null
    : Number(idWarehouseRaw);

  if (idWarehouse === null || Number.isNaN(idWarehouse)) {
    return res.status(400).json({
      success: false,
      message: 'Query parameter idWarehouse is required',
      example: '/api/production/hot-stamp/cabinet-materials?idWarehouse=5',
    });
  }

  try {
    const result = await service.getMasterCabinetMaterials({
      idWarehouse,
    });

    return res.status(200).json({
      success: true,
      message: `Found ${result.count} cabinet materials`,
      totalRecords: result.count,
      data: result.data,
    });
  } catch (e) {
    console.error('[getMasterCabinetMaterials]', e);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: e.message,
    });
  }
}


async function getByCetakanWarna(req, res) {
  const { username } = req;

  const idCetakanRaw = req.query.idCetakan;
  const idWarnaRaw = req.query.idWarna;

  const idCetakan = idCetakanRaw === undefined || idCetakanRaw === null || idCetakanRaw === ''
    ? null
    : Number(idCetakanRaw);

  const idWarna = idWarnaRaw === undefined || idWarnaRaw === null || idWarnaRaw === ''
    ? null
    : Number(idWarnaRaw);

  if (idCetakan === null || Number.isNaN(idCetakan) || idWarna === null || Number.isNaN(idWarna)) {
    return res.status(400).json({
      success: false,
      message: 'Provide ?idCetakan=<int> AND ?idWarna=<int>',
      error: {
        fields: ['idCetakan', 'idWarna'],
      },
    });
  }

  console.log('🔍 Lookup FurnitureMaterial by Cetakan+Warna |', { username, idCetakan, idWarna });

  try {
    const row = await service.getByCetakanWarna({ idCetakan, idWarna });

    if (!row) {
      return res.status(404).json({
        success: false,
        message: 'Mapping Cetakan + Warna tidak ditemukan',
        data: null,
      });
    }

    return res.status(200).json({
      success: true,
      message: 'Furniture material berhasil ditemukan',
      data: row,
    });
  } catch (error) {
    console.error('Error lookup furniture material:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getMasterCabinetMaterials, getByCetakanWarna };

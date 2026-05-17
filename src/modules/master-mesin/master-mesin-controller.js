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
    const rows = await service.getBrokerByNoProduksi({ idBagianMesin, includeDisabled });
    const activeShiftMeta = rows[0]
      ? {
          currentDate: rows[0].CurrentDate ?? null,
          currentTime: rows[0].CurrentTime ?? null,
          shift: rows[0].ActiveShift ?? null,
          hourStart: rows[0].ActiveShiftHourStart ?? null,
          hourEnd: rows[0].ActiveShiftHourEnd ?? null,
          validFrmDate: rows[0].ActiveShiftValidFrmDate ?? null,
        }
      : {
          currentDate: null,
          currentTime: null,
          shift: null,
          hourStart: null,
          hourEnd: null,
          validFrmDate: null,
        };
    const data = rows.map((row) => ({
      IdMesin: row.IdMesin,
      NamaMesin: row.NamaMesin,
      Bagian: row.Bagian,
      Target: row.Target,
      NoProduksi: row.NoProduksi ?? null,
      TglProduksi: row.TglProduksi ?? null,
      OutputJenisId: row.OutputJenisId ?? null,
      OutputJenisNama: row.OutputJenisNama ?? null,
      OutputJenisItemCode: row.OutputJenisItemCode ?? null,
      IdOperator: row.IdOperator ?? null,
      Operator: row.Operator ?? null,
      Shift: row.Shift ?? null,
      HourStart: row.HourStart ?? null,
      HourEnd: row.HourEnd ?? null,
    }));

    return res.status(200).json({
      success: true,
      message: 'Data broker per NoProduksi hari ini berhasil diambil',
      idBagianMesin,
      includeDisabled,
      activeShift: activeShiftMeta,
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

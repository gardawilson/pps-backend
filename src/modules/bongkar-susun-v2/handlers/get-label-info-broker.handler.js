const { sql, poolPromise } = require("../../../core/config/db");

exports.getLabelInfoBroker = async (labelCode) => {
  const pool = await poolPromise;
  const result = await pool.request().input("NoBroker", sql.VarChar(50), labelCode)
    .query(`
      SELECT
        h.NoBroker        AS labelCode,
        h.IdJenisPlastik  AS idJenis,
        h.IdWarehouse,
        h.IdStatus,
        h.Density,
        h.Moisture,
        h.MaxMeltTemp,
        h.MinMeltTemp,
        h.MFI,
        h.VisualNote,
        h.Density2,
        h.Density3,
        h.Moisture2,
        h.Moisture3,
        h.Blok,
        h.IdLokasi,
        h.HasBeenPrinted
      FROM dbo.Broker_h h
      WHERE h.NoBroker = @NoBroker
    `);

  if (!result.recordset.length) {
    const e = new Error(`Label ${labelCode} tidak ditemukan atau sudah terpakai`);
    e.statusCode = 404;
    throw e;
  }

  const saksRes = await pool
    .request()
    .input("NoBroker", sql.VarChar(50), labelCode).query(`
      SELECT NoSak, Berat
      FROM dbo.Broker_d
      WHERE NoBroker = @NoBroker AND DateUsage IS NULL
      ORDER BY NoSak
    `);

  if (!saksRes.recordset.length) {
    const e = new Error(`Label ${labelCode} tidak ditemukan atau sudah terpakai`);
    e.statusCode = 404;
    throw e;
  }

  const row = result.recordset[0];
  const saks = saksRes.recordset.map((s) => ({
    noSak: s.NoSak,
    berat: s.Berat,
  }));
  return {
    labelCode: row.labelCode,
    category: "broker",
    idJenis: row.idJenis,
    idWarehouse: row.IdWarehouse,
    idStatus: row.IdStatus,
    density: row.Density,
    moisture: row.Moisture,
    maxMeltTemp: row.MaxMeltTemp,
    minMeltTemp: row.MinMeltTemp,
    mfi: row.MFI,
    visualNote: row.VisualNote,
    density2: row.Density2,
    density3: row.Density3,
    moisture2: row.Moisture2,
    moisture3: row.Moisture3,
    blok: row.Blok,
    idLokasi: row.IdLokasi,
    hasBeenPrinted: row.HasBeenPrinted,
    jumlahSak: saks.length,
    totalBerat: saks.reduce((sum, s) => sum + s.berat, 0),
    saks,
  };
};

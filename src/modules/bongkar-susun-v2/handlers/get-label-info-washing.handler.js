const { sql, poolPromise } = require("../../../core/config/db");

exports.getLabelInfoWashing = async (labelCode) => {
  const pool = await poolPromise;
  const result = await pool
    .request()
    .input("NoWashing", sql.VarChar(50), labelCode).query(`
      SELECT
        h.NoWashing        AS labelCode,
        h.IdJenisPlastik   AS idJenis,
        mw.Nama            AS namaJenis,
        h.IdWarehouse,
        h.IdStatus,
        h.Density,
        h.Moisture,
        h.Density2,
        h.Moisture2,
        h.Density3,
        h.Moisture3
      FROM Washing_h h
      INNER JOIN MstWashing mw ON mw.IdWashing = h.IdJenisPlastik
      WHERE h.NoWashing = @NoWashing
    `);

  if (!result.recordset.length) {
    const e = new Error(`Label ${labelCode} tidak ditemukan atau sudah terpakai`);
    e.statusCode = 404;
    throw e;
  }

  const saksRes = await pool
    .request()
    .input("NoWashing", sql.VarChar(50), labelCode).query(`
      SELECT NoSak, Berat
      FROM Washing_d
      WHERE NoWashing = @NoWashing AND DateUsage IS NULL
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
    category: "washing",
    idJenis: row.idJenis,
    namaJenis: row.namaJenis,
    idWarehouse: row.IdWarehouse,
    idStatus: row.IdStatus,
    density: row.Density,
    moisture: row.Moisture,
    density2: row.Density2,
    moisture2: row.Moisture2,
    density3: row.Density3,
    moisture3: row.Moisture3,
    jumlahSak: saks.length,
    totalBerat: saks.reduce((sum, s) => sum + s.berat, 0),
    saks,
  };
};

const { sql, poolPromise } = require("../../../core/config/db");
const { getByNoMixer } = require("../../label/mixer/mixer-service");
const { conflict } = require("../../../core/utils/http-error");

exports.getLabelInfoMixer = async (labelCode) => {
  const code = String(labelCode || "").trim();
  const row = await getByNoMixer(code);

  if (row.IsPartial === true || row.IsPartial === 1) {
    throw conflict("Tidak dapat bongkar susun label yang sudah di partial");
  }

  const pool = await poolPromise;
  const saksRes = await pool
    .request()
    .input("NoMixer", sql.VarChar(50), code).query(`
      SELECT NoSak, Berat
      FROM dbo.Mixer_d
      WHERE NoMixer = @NoMixer
        AND DateUsage IS NULL
      ORDER BY NoSak
    `);

  const saks = (saksRes.recordset || []).map((s) => ({
    noSak: s.NoSak,
    berat: s.Berat,
  }));

  return {
    labelCode: row.NoMixer,
    category: "mixer",
    dateCreate: row.DateCreate,
    idJenis: row.IdMixer,
    namaJenis: row.NamaMixer ?? row.Jenis,
    jumlahSak: row.JumlahSak,
    berat: row.SisaBerat,
    saks,
    createBy: row.CreateBy,
    mesin: row.Mesin,
    shift: row.Shift,
    hasBeenPrinted: row.HasBeenPrinted ?? 0,
  };
};

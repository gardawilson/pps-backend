const { conflict } = require("../../../core/utils/http-error");
const { getByNoBJ } = require("../../label/packing/packing-service");

exports.getLabelInfoBarangJadi = async (labelCode) => {
  const row = await getByNoBJ(labelCode);

  if (row.DateUsage) {
    throw conflict(`Label ${row.NoBJ} sudah terpakai`);
  }

  if (row.IsPartial === true || row.IsPartial === 1) {
    throw conflict("Tidak dapat sortir reject label yang sudah di partial");
  }

  return {
    labelCode: row.NoBJ,
    category: "barangJadi",
    dateCreate: row.DateCreate,
    idJenis: row.IdBJ,
    namaJenis: row.NamaBJ,
    pcs: row.Pcs,
    hasBeenPrinted: row.HasBeenPrinted ?? 0,
    createBy: row.CreateBy,
    mesin: row.Mesin,
    shift: row.Shift,
  };
};

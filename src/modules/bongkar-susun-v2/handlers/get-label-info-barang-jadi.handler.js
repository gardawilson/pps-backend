const { conflict } = require("../../../core/utils/http-error");
const { getByNoBJ } = require("../../label/packing/packing-service");

exports.getLabelInfoBarangJadi = async (labelCode) => {
  const row = await getByNoBJ(labelCode);

  if (row.DateUsage) {
    throw conflict(`Label ${row.NoBJ} sudah terpakai`);
  }

  return {
    labelCode: row.NoBJ,
    category: "barangJadi",
    dateCreate: row.DateCreate,
    idBJ: row.IdBJ,
    namaBJ: row.NamaBJ,
    pcs: row.Pcs,
    hasBeenPrinted: row.HasBeenPrinted ?? 0,
    createBy: row.CreateBy,
    mesin: row.Mesin,
    shift: row.Shift,
  };
};

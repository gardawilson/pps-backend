const {
  getByNoFurnitureWip,
} = require("../../label/furniture-wip/furniture-wip-service");
const { conflict } = require("../../../core/utils/http-error");

exports.getLabelInfoFurnitureWip = async (labelCode) => {
  const row = await getByNoFurnitureWip(labelCode);

  return {
    labelCode: row.NoFurnitureWIP,
    category: "furnitureWip",
    dateCreate: row.DateCreate,
    idJenis: row.IdFurnitureWIP,
    namaJenis: row.NamaFurnitureWIP,
    pcs: row.Pcs,
    hasBeenPrinted: row.HasBeenPrinted ?? 0,
    createBy: row.CreateBy,
    mesin: row.Mesin,
    shift: row.Shift,
  };
};

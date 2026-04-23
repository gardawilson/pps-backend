const { getByNoFurnitureWip } = require("../../label/furniture-wip/furniture-wip-service");

exports.getLabelInfoFurnitureWip = async (labelCode) => {
  const row = await getByNoFurnitureWip(labelCode);

  return {
    labelCode: row.NoFurnitureWIP,
    category: "furnitureWip",
    dateCreate: row.DateCreate,
    idFurnitureWIP: row.IdFurnitureWIP,
    namaFurnitureWIP: row.NamaFurnitureWIP,
    pcs: row.Pcs,
    hasBeenPrinted: row.HasBeenPrinted ?? 0,
    createBy: row.CreateBy,
    mesin: row.Mesin,
    shift: row.Shift,
  };
};

function normalizeLabelCode(labelCode) {
  return String(labelCode || "").trim();
}

function detectCategory(labelCode) {
  const code = normalizeLabelCode(labelCode);
  if (code.startsWith("BA.")) return "barangJadi";
  if (code.startsWith("B.")) return "washing";
  if (code.startsWith("D.")) return "broker";
  if (code.startsWith("F.")) return "crusher";
  if (code.startsWith("V.")) return "gilingan";
  if (code.startsWith("BB.")) return "furnitureWip";
  if (code.startsWith("M.")) return "bonggolan";
  return null;
}

const CREATE_METHOD_BY_CATEGORY = {
  washing: "createBongkarSusunWashing",
  broker: "createBongkarSusunBroker",
  crusher: "createBongkarSusunCrusher",
  gilingan: "createBongkarSusunGilingan",
  furnitureWip: "createBongkarSusunFurnitureWip",
  barangJadi: "createBongkarSusunBarangJadi",
  bonggolan: "createBongkarSusunBonggolan",
};

const LABEL_INFO_METHOD_BY_CATEGORY = {
  washing: "getLabelInfoWashing",
  broker: "getLabelInfoBroker",
  crusher: "getLabelInfoCrusher",
  gilingan: "getLabelInfoGilingan",
  furnitureWip: "getLabelInfoFurnitureWip",
  barangJadi: "getLabelInfoBarangJadi",
  bonggolan: "getLabelInfoBonggolan",
};

module.exports = {
  detectCategory,
  normalizeLabelCode,
  CREATE_METHOD_BY_CATEGORY,
  LABEL_INFO_METHOD_BY_CATEGORY,
};

function normalizeLabelCode(labelCode) {
  return String(labelCode || "").trim();
}

function detectCategory(labelCode) {
  const code = normalizeLabelCode(labelCode);
  if (code.startsWith("B.")) return "washing";
  if (code.startsWith("D.")) return "broker";
  if (code.startsWith("M.")) return "bonggolan";
  return null;
}

const CREATE_METHOD_BY_CATEGORY = {
  washing: "createBongkarSusunWashing",
  broker: "createBongkarSusunBroker",
  bonggolan: "createBongkarSusunBonggolan",
};

const LABEL_INFO_METHOD_BY_CATEGORY = {
  washing: "getLabelInfoWashing",
  broker: "getLabelInfoBroker",
  bonggolan: "getLabelInfoBonggolan",
};

module.exports = {
  detectCategory,
  normalizeLabelCode,
  CREATE_METHOD_BY_CATEGORY,
  LABEL_INFO_METHOD_BY_CATEGORY,
};

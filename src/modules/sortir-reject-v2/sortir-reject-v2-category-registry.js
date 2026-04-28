function normalizeLabelCode(labelCode) {
  return String(labelCode || "").trim();
}

function detectCategory(labelCode) {
  const code = normalizeLabelCode(labelCode);
  if (code.startsWith("BA.")) return "barangJadi";
  if (code.startsWith("BB.")) return "furnitureWip";
  return null;
}

module.exports = {
  detectCategory,
  normalizeLabelCode,
};

const { getLabelInfoByNoMixer } = require("../../label/mixer/mixer-service");

exports.getLabelInfoMixer = async (labelCode) => {
  const code = String(labelCode || "").trim();
  return getLabelInfoByNoMixer(code);
};

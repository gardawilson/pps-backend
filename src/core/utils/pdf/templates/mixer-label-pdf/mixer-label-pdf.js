const fs = require("fs");
const path = require("path");

const templatePath = path.join(__dirname, "mixer-label-pdf.html");

function buildMixerLabelHtml(data) {
  const templateHtml = fs.readFileSync(templatePath, "utf8");
  return templateHtml
    .replace(/{{noLabel}}/g, data.noLabel || "-")
    .replace("{{jenisPlastik}}", data.jenisPlastik || "-")
    .replace("{{mesin}}", data.mesin || "-")
    .replace("{{jumlahSak}}", data.jumlahSak || "-")
    .replace("{{totalBerat}}", data.totalBerat || "-")
    .replace("{{shift}}", data.shift || "-")
    .replace("{{tanggal}}", data.tanggal || "-")
    .replace("{{createBy}}", data.createBy || "-")
    .replace("{{qrBase64}}", data.qrBase64 || "")
    .replace("{{watermarkText}}", data.watermarkText || "");
}

module.exports = { buildMixerLabelHtml };

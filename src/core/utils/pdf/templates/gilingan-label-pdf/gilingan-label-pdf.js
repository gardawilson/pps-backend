const fs = require("fs");
const path = require("path");

const templatePath = path.join(__dirname, "gilingan-label-pdf.html");

function buildGilinganLabelHtml(data) {
  const templateHtml = fs.readFileSync(templatePath, "utf8");
  return templateHtml
    .replace(/{{noLabel}}/g, data.noLabel || "-")
    .replace("{{namaGilingan}}", data.namaGilingan || "-")
    .replace("{{mesin}}", data.mesin || "-")
    .replace("{{berat}}", data.berat || "-")
    .replace("{{tanggal}}", data.tanggal || "-")
    .replace("{{createBy}}", data.createBy || "-")
    .replace("{{qrBase64}}", data.qrBase64 || "")
    .replace("{{watermarkText}}", data.watermarkText || "");
}

module.exports = { buildGilinganLabelHtml };

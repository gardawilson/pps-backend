const fs = require("fs");
const path = require("path");

const templatePath = path.join(__dirname, "packing-label-pdf.html");

function buildPackingLabelHtml(data) {
  const templateHtml = fs.readFileSync(templatePath, "utf8");
  return templateHtml
    .replace(/{{noLabel}}/g, data.noLabel || "-")
    .replace("{{namaProduk}}", data.namaProduk || "-")
    .replace("{{kode}}", data.kode || "-")
    .replace("{{berat}}", data.berat || "-")
    .replace("{{pcs}}", data.pcs || "-")
    .replace("{{tanggal}}", data.tanggal || "-")
    .replace("{{createBy}}", data.createBy || "-")
    .replace("{{qrBase64}}", data.qrBase64 || "")
    .replace("{{watermarkText}}", data.watermarkText || "");
}

module.exports = { buildPackingLabelHtml };

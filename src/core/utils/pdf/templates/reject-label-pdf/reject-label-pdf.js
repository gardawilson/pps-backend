const fs = require("fs");
const path = require("path");

const templatePath = path.join(__dirname, "reject-label-pdf.html");

function buildRejectLabelHtml(data) {
  const templateHtml = fs.readFileSync(templatePath, "utf8");
  return templateHtml
    .replace(/{{noLabel}}/g, data.noLabel || "-")
    .replace("{{namaReject}}", data.namaReject || "-")
    .replace("{{mesin}}", data.mesin || "-")
    .replace("{{shift}}", data.shift || "-")
    .replace("{{berat}}", data.berat || "-")
    .replace("{{tanggal}}", data.tanggal || "-")
    .replace("{{createBy}}", data.createBy || "-")
    .replace("{{qrBase64}}", data.qrBase64 || "")
    .replace("{{watermarkText}}", data.watermarkText || "");
}

module.exports = { buildRejectLabelHtml };

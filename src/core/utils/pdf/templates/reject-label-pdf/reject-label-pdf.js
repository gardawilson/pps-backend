const fs = require("fs");
const path = require("path");

const templatePath = path.join(__dirname, "reject-label-pdf.html");

function buildRejectLabelHtml(data) {
  const templateHtml = fs.readFileSync(templatePath, "utf8");
  return templateHtml
    .replace(/{{noLabel}}/g, data.noLabel || "-")
    .replace("{{namaReject}}", data.namaReject || "-")
    .replace("{{mesinLabel}}", data.mesinLabel || "Mesin &nbsp;")
    .replace("{{mesin}}", data.mesin || "-")
    .replace("{{shiftRow}}", (data.shift && data.shift !== "-") ? `<div class="row">Shift &nbsp;: ${data.shift}</div>` : "")
    .replace("{{berat}}", data.berat || "-")
    .replace("{{tanggal}}", data.tanggal || "-")
    .replace("{{createBy}}", data.createBy || "-")
    .replace("{{qrBase64}}", data.qrBase64 || "")
    .replace("{{watermarkText}}", data.watermarkText || "");
}

module.exports = { buildRejectLabelHtml };

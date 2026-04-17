const fs = require("fs");
const path = require("path");

const templatePath = path.join(__dirname, "bahan-baku-label-pdf.html");

function buildDetailTable(rows) {
  if (!rows || rows.length === 0) return "";
  const rowsHtml = rows
    .map(
      (r) =>
        `<tr><td>${r.NoSak}</td><td class="right">${Number(r.Berat).toFixed(2)}</td></tr>`,
    )
    .join("");
  return `<div class="detail-table-wrap"><table class="detail-table">
      <thead><tr><th>Sak</th><th class="right">Berat</th></tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table></div>`;
}

function buildBahanBakuLabelHtml(data) {
  const details = data.details || [];
  const chunkSize = Math.ceil(details.length / 4) || 1;
  const col1 = details.slice(0, chunkSize);
  const col2 = details.slice(chunkSize, chunkSize * 2);
  const col3 = details.slice(chunkSize * 2, chunkSize * 3);
  const col4 = details.slice(chunkSize * 3);

  const detailTables = [col1, col2, col3, col4]
    .filter((col) => col.length > 0)
    .map(buildDetailTable)
    .join("");

  const templateHtml = fs.readFileSync(templatePath, "utf8");
  return templateHtml
    .replace(/{{noLabel}}/g, data.noLabel || "-")
    .replace(/{{noPallet}}/g, data.noPallet || "-")
    .replace("{{namaJenisPlastik}}", data.namaJenisPlastik || "-")
    .replace("{{namaSupplier}}", data.namaSupplier || "-")
    .replace("{{noPlat}}", data.noPlat || "-")
    .replace(/{{sakSisa}}/g, data.sakSisa != null ? String(data.sakSisa) : "-")
    .replace(/{{beratSisa}}/g, data.beratSisa || "-")
    .replace("{{tanggal}}", data.tanggal || "-")
    .replace("{{createBy}}", data.createBy || "-")
    .replace("{{qrBase64}}", data.qrBase64 || "")
    .replace("{{watermarkText}}", data.watermarkText || "")
    .replace("{{detailTables}}", detailTables);
}

module.exports = { buildBahanBakuLabelHtml };

const moment = require("moment");

// Untuk tampilan (UI)
function formatDate(date) {
  return moment(date).format("DD MMM YYYY"); // contoh: 13 Nov 2025
}

// Untuk API / SQL: paksa ke 'YYYY-MM-DD'
function toApiDate(value) {
  if (!value) return null;

  const m = moment(
    value,
    [
      "DD MMM YYYY", // dari Flutter: "13 Nov 2025"
      "YYYY-MM-DD", // kalau suatu saat sudah kirim raw
      moment.ISO_8601, // jaga-jaga kalau format ISO
    ],
    true, // strict parse
  );

  if (!m.isValid()) return null;

  return m.format("YYYY-MM-DD"); // contoh: "2025-11-13"
}

module.exports = { formatDate, toApiDate };

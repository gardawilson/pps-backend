// src/core/config/produksi-mesin-config.js

const PRODUKSI_MESIN_SOURCES = [
  // =========================
  // STATIC (tanpa DB lookup)
  // =========================

  // BONGKAR SUSUN (contoh kode: "BG.0000000001" / "BG....")
  // NOTE: tidak punya IdMesin, lokasi ditentukan static
  { prefix: 'BG.', staticBlok: 'BSS', staticIdLokasi: 1 },
  { prefix: 'L.', staticBlok: 'WA', staticIdLokasi: 1 },

  // =========================
  // FROM TABLE (pakai DB lookup)
  // =========================

  // HOT STAMPING: "BH.0000000001"
  { prefix: 'BH.', table: 'HotStamping_h',      codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // KEY FITTING / PASANG KUNCI: "BI.0000000001"
  { prefix: 'BI.', table: 'PasangKunci_h',      codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // SPANNER: "BJ.0000000108"
  { prefix: 'BJ.', table: 'Spanner_h',          codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // PACKING: "BD.0000000001"
  { prefix: 'BD.', table: 'PackingProduksi_h',  codeColumn: 'NoPacking',         idMesinColumn: 'IdMesin' },

  // WASHING: "C.0000000077"
  { prefix: 'C.',  table: 'WashingProduksi_h',  codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // BROKER: "E.0000000001"
  { prefix: 'E.',  table: 'BrokerProduksi_h',   codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // CRUSHER: "G.0000000006"
  { prefix: 'G.',  table: 'CrusherProduksi_h',  codeColumn: 'NoCrusherProduksi', idMesinColumn: 'IdMesin' },

  // GILINGAN: "W.0000004148"
  { prefix: 'W.',  table: 'GilinganProduksi_h', codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // MIXER: "I.0000004787"
  { prefix: 'I.',  table: 'MixerProduksi_h',    codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },

  // INJECT: "S.0000029953"
  { prefix: 'S.',  table: 'InjectProduksi_h',   codeColumn: 'NoProduksi',        idMesinColumn: 'IdMesin' },
];

module.exports = {
  PRODUKSI_MESIN_SOURCES,
};

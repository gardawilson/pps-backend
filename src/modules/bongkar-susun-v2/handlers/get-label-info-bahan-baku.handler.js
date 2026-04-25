const {
  getPalletByNoBahanBaku,
  getDetailByNoBahanBakuAndNoPallet,
} = require("../../label/bahan-baku/bahan-baku-service");
const { conflict } = require("../../../core/utils/http-error");

function normalizeNoBahanBaku(labelCode) {
  const code = String(labelCode || "").trim();
  if (code.startsWith("A.")) {
    return code.split("-")[0];
  }
  return code.split("-")[0];
}

function parseNoPallet(labelCode) {
  const code = String(labelCode || "").trim();
  const raw = code.startsWith("A.") ? code.slice(2) : code;
  const parts = raw.split("-");
  return parts.length > 1 ? parts[1] : null;
}

exports.getLabelInfoBahanBaku = async (labelCode) => {
  const noBahanBaku = normalizeNoBahanBaku(labelCode);
  const noPallet = parseNoPallet(labelCode);
  if (!noPallet) {
    const e = new Error(
      `Label ${labelCode} harus memakai format A.<NoBahanBaku>-<NoPallet>`,
    );
    e.statusCode = 400;
    throw e;
  }

  const pallets = await getPalletByNoBahanBaku(noBahanBaku);

  if (!Array.isArray(pallets) || pallets.length === 0) {
    const e = new Error(`Label ${labelCode} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  const selectedPallets = pallets.filter(
    (p) => String(p.NoPallet) === String(noPallet),
  );

  if (!selectedPallets.length) {
    const e = new Error(`Label ${labelCode} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  const details = noPallet
    ? await getDetailByNoBahanBakuAndNoPallet({
        nobahanbaku: noBahanBaku,
        nopallet: noPallet,
      })
    : [];

  if (details.some((item) => item.IsPartial === true || item.IsPartial === 1)) {
    throw conflict("Tidak dapat bongkar susun label yang sudah di partial");
  }

  if (details.length > 0 && details.every((item) => item.DateUsage != null)) {
    throw conflict(`Label ${labelCode} sudah terpakai`);
  }

  return {
    labelCode: `${noBahanBaku}-${selectedPallets[0].NoPallet}`,
    category: "bahanBaku",
    noPallet: selectedPallets[0].NoPallet,
    idJenis: selectedPallets[0].IdJenisPlastik,
    namaJenis: selectedPallets[0].NamaJenisPlastik,
    idWarehouse: selectedPallets[0].IdWarehouse,
    namaWarehouse: selectedPallets[0].NamaWarehouse,
    keterangan: selectedPallets[0].Keterangan,
    idStatus: selectedPallets[0].IdStatus,
    statusText: selectedPallets[0].StatusText,
    moisture: selectedPallets[0].Moisture,
    meltingIndex: selectedPallets[0].MeltingIndex,
    elasticity: selectedPallets[0].Elasticity,
    tenggelam: selectedPallets[0].Tenggelam,
    density: selectedPallets[0].Density,
    density2: selectedPallets[0].Density2,
    density3: selectedPallets[0].Density3,
    hasBeenPrinted: selectedPallets[0].HasBeenPrinted ?? 0,
    blok: "BSS",
    idLokasi: 1,
    sakActual: selectedPallets[0].SakActual,
    beratActual: selectedPallets[0].BeratActual,
    sakSisa: selectedPallets[0].SakSisa,
    beratSisa: selectedPallets[0].BeratSisa,
    isEmpty: Boolean(selectedPallets[0].IsEmpty),
    details,
  };
};

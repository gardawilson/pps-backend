const TUTUP_TRANSAKSI_SOURCES = {
bongkarSusun: {
    table: 'dbo.BongkarSusun_h',
    codeColumn: 'NoBongkarSusun',
    dateColumn: 'Tanggal',
  },
washingProduksi: {
    table: 'dbo.WashingProduksi_h',
    codeColumn: 'NoProduksi',
    dateColumn: 'TglProduksi',
  },
  brokerProduksi: {
    table: 'dbo.BrokerProduksi_h',
    codeColumn: 'NoProduksi',
    dateColumn: 'TglProduksi',
  },
  crusherProduksi: {
    table: 'dbo.CrusherProduksi_h',
    codeColumn: 'NoCrusherProduksi',
    dateColumn: 'Tanggal',
  },
  gilinganProduksi: {
    table: 'dbo.GilinganProduksi_h',
    codeColumn: 'NoProduksi',
    dateColumn: 'Tanggal',
  },
  mixerProduksi: {
    table: 'dbo.MixerProduksi_h',
    codeColumn: 'NoProduksi',
    dateColumn: 'TglProduksi',
  },
    hotStamping: {
    table: 'dbo.HotStamping_h',
    codeColumn: 'NoProduksi',
    dateColumn: 'Tanggal',
  },
  // tambah lainnya...
};

module.exports = { TUTUP_TRANSAKSI_SOURCES };

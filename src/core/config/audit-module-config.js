// src/services/audit/audit-module-config.js

const MODULE_CONFIG = {
  washing: {
    pkField: "NoWashing",
    headerTable: "Washing_h",
    detailTable: "Washing_d",
    outputTables: ["WashingProduksiOutput", "BongkarSusunOutputWashing"],

    inputTables: [
      "WashingProduksiInputWashing",
      "BrokerProduksiInputWashing",
      "BongkarSusunInputWashing",
    ],

    // ✅ NEW: Output display config
    outputDisplayConfig: {
      WashingProduksiOutput: {
        displayField: "NoProduksi",
        label: "Washing Produksi",
      },
      BongkarSusunOutputWashing: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdJenisPlastik",
        joinTable: "MstJenisPlastik",
        joinKey: "IdJenisPlastik",
        displayField: "Jenis",
        alias: "NamaJenisPlastik",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", true: "PASS", 0: "HOLD", false: "HOLD" },
  },

  broker: {
    pkField: "NoBroker",
    headerTable: "Broker_h",
    detailTable: "Broker_d",
    outputTables: ["BrokerProduksiOutput", "BongkarSusunOutputBroker"],

    inputTables: [
      "BrokerProduksiInputBroker",
      "BrokerProduksiInputBrokerPartial",
      "GilinganProduksiInputBroker",
      "GilinganProduksiInputBrokerPartial",
      "MixerProduksiInputBroker",
      "MixerProduksiInputBrokerPartial",
      "InjectProduksiInputBroker",
      "InjectProduksiInputFurnitureWIPPartial",
      "BongkarSusunInputBroker",
    ],

    // ✅ NEW
    outputDisplayConfig: {
      BrokerProduksiOutput: {
        displayField: "NoProduksi",
        label: "Broker Produksi",
      },
      BongkarSusunOutputBroker: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdJenisPlastik",
        joinTable: "MstJenisPlastik",
        joinKey: "IdJenisPlastik",
        displayField: "Jenis",
        alias: "NamaJenisPlastik",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", true: "PASS", 0: "HOLD", false: "HOLD" },
  },

  crusher: {
    pkField: "NoCrusher",
    headerTable: "Crusher",
    detailTable: null,
    outputTables: ["CrusherProduksiOutput", "BongkarSusunOutputCrusher"],

    inputTables: [
      "BrokerProduksiInputCrusher",
      "GilinganProduksiInputCrusher",
      "BongkarSusunInputCrusher",
    ],

    // ✅ NEW
    outputDisplayConfig: {
      BongkarSusunOutputCrusher: {
        displayField: "NoCrusherProduksi",
        label: "Crusher Produksi",
      },
      BongkarSusunOutputCrusher: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdCrusher",
        joinTable: "MstCrusher",
        joinKey: "IdCrusher",
        displayField: "NamaCrusher",
        alias: "NamaCrusher",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Berat", "DateCreate", "Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", 0: "HOLD" },
  },

  gilingan: {
    pkField: "NoGilingan",
    headerTable: "Gilingan",
    detailTable: null,
    outputTables: ["GilinganProduksiOutput", "BongkarSusunOutputGilingan"],

    inputTables: [
      "WashingProduksiInputGilingan",
      "WashingProduksiInputGilinganPartial",
      "BrokerProduksiInputGilingan",
      "BrokerProduksiInputGilinganPartial",
      "MixerProduksiInputGilingan",
      "MixerProduksiInputGilinganPartial",
      "InjectProduksiInputGilingan",
      "InjectProduksiInputGilinganPartial",
      "BongkarSusunInputGilingan",
    ],

    // ✅ NEW
    outputDisplayConfig: {
      GilinganProduksiOutput: {
        displayField: "NoProduksi",
        label: "Gilingan Produksi",
      },
      BongkarSusunOutputGilingan: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdGilingan",
        joinTable: "MstGilingan",
        joinKey: "IdGilingan",
        displayField: "NamaGilingan",
        alias: "NamaGilingan",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Berat", "DateCreate", "Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", 0: "HOLD" },
  },

  bonggolan: {
    pkField: "NoBonggolan",
    headerTable: "Bonggolan",
    detailTable: null,
    outputTables: [
      "BrokerProduksiOutputBonggolan",
      "InjectProduksiOutputBonggolan",
      "BongkarSusunOutputBonggolan",
    ],

    inputTables: [
      "CrusherProduksiInputBonggolan",
      "GilinganProduksiInputBonggolan",
      "BongkarSusunInputBonggolan",
    ],

    // ✅ NEW
    outputDisplayConfig: {
      BrokerProduksiOutputBonggolan: {
        displayField: "NoProduksi",
        label: "Broker Produksi",
      },
      InjectProduksiOutputBonggolan: {
        displayField: "NoProduksi",
        label: "Inject Produksi",
      },
      BongkarSusunOutputBonggolan: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdBonggolan",
        joinTable: "MstBonggolan",
        joinKey: "IdBonggolan",
        displayField: "NamaBonggolan",
        alias: "NamaBonggolan",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Berat", "DateCreate", "Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", 0: "HOLD" },
  },

  mixer: {
    pkField: "NoMixer",
    headerTable: "Mixer_h",
    detailTable: "Mixer_d",
    outputTables: [
      "MixerProduksiOutput",
      "InjectProduksiOutputMixer",
      "BongkarSusunOutputMixer",
    ],

    inputTables: [
      "BrokerProduksiInputMixer",
      "BrokerProduksiInputMixerPartial",
      "MixerProduksiInputMixer",
      "MixerProduksiInputMixerPartial",
      "InjectProduksiInputMixer",
      "InjectProduksiInputMixerPartial",
      "BongkarSusunInputMixer",
    ],

    // ✅ NEW
    outputDisplayConfig: {
      MixerProduksiOutput: {
        displayField: "NoProduksi",
        label: "Mixer Produksi",
      },
      InjectProduksiOutputMixer: {
        displayField: "NoProduksi",
        label: "Inject Produksi",
      },
      BongkarSusunOutputMixer: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdMixer",
        joinTable: "MstMixer",
        joinKey: "IdMixer",
        displayField: "Jenis",
        alias: "NamaMixer",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", true: "PASS", 0: "HOLD", false: "HOLD" },
  },

  furniturewip: {
    pkField: "NoFurnitureWIP",
    headerTable: "FurnitureWIP",
    detailTable: null,
    outputTables: [
      "InjectProduksiOutputFurnitureWIP",
      "HotStampingOutputLabelFWIP",
      "PasangKunciOutputLabelFWIP",
      "SpannerOutputLabelFWIP",
      "BJReturFurnitureWIP_d",
      "BongkarSusunOutputFurnitureWIP",
    ],

    inputTables: [
      "InjectProduksiInputFurnitureWIP",
      "InjectProduksiInputFurnitureWIPPartial",
      "HotStampingInputLabelFWIP",
      "HotStampingInputLabelFWIPPartial",
      "PasangKunciInputLabelFWIP",
      "PasangKunciInputLabelFWIPPartial",
      "SpannerInputLabelFWIP",
      "SpannerInputLabelFWIPPartial",
      "PackingProduksiInputLabelFWIP",
      "PackingProduksiInputLabelFWIPPartial",
      "BJSortirRejectInputLabelFurnitureWIP",
      "BongkarSusunInputFurnitureWIP",
    ],

    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      InjectProduksiOutputFurnitureWIP: {
        displayField: "NoProduksi",
        label: "Produksi Inject",
      },
      HotStampingOutputLabelFWIP: {
        displayField: "NoProduksi",
        label: "Hot Stamping",
      },
      PasangKunciOutputLabelFWIP: {
        displayField: "NoProduksi",
        label: "Pasang Kunci",
      },
      SpannerOutputLabelFWIP: {
        displayField: "NoProduksi",
        label: "Produksi Spanner",
      },
      BJReturFurnitureWIP_d: {
        displayField: "NoRetur",
        label: "Retur BJ",
      },
      BongkarSusunOutputFurnitureWIP: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
    },

    headerParseFields: [
      {
        jsonField: "IDFurnitureWIP",
        joinTable: "MstCabinetWIP",
        joinKey: "IdCabinetWIP",
        displayField: "Nama",
        alias: "NamaFurnitureWIP",
      },
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
    scalarFields: ["Pcs", "Berat", "DateCreate", "Blok", "IdLokasi"],
    statusField: "IdStatus",
    statusMapping: { 1: "PASS", 0: "HOLD" },
  },

  barangjadi: {
    pkField: "NoBJ",
    headerTable: "BarangJadi",
    detailTable: null,
    outputTables: [
      "InjectProduksiOutputBarangJadi",
      "PackingProduksiOutputLabelBJ",
      "BongkarSusunOutputBarangjadi",
      "BJReturBarangJadi_d",
    ],

    inputTables: ["BJSortirRejectInputLabelBarangJadi"],

    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      InjectProduksiOutputBarangJadi: {
        displayField: "NoProduksi",
        label: "Produksi Inject",
      },
      PackingProduksiOutputLabelBJ: {
        displayField: "NoPacking",
        label: "Produksi Packing",
      },
      BongkarSusunOutputBarangjadi: {
        displayField: "NoBongkarSusun",
        label: "Bongkar Susun",
      },
      BJReturBarangJadi_d: {
        displayField: "NoRetur",
        label: "Retur",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdBJ",
        joinTable: "MstBarangJadi",
        joinKey: "IdBJ",
        displayField: "NamaBJ",
        alias: "NamaBJ",
      },
    ],
    scalarFields: ["Pcs", "Berat", "DateCreate", "Blok", "IdLokasi"],
    // statusField: 'IdStatus',
    // statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

  reject: {
    pkField: "NoReject",
    headerTable: "RejectV2",
    detailTable: null,
    outputTables: [
      "InjectProduksiOutputRejectV2",
      "HotStampingOutputRejectV2",
      "PasangKunciOutputRejectV2",
      "SpannerOutputRejectV2",
      "BJSortirRejectOutputLabelReject",
    ],

    inputTables: [
      "BrokerProduksiInputReject",
      "BrokerProduksiInputRejectPartial",
      "GilinganProduksiInputRejectV2",
      "GilinganProduksiInputRejectV2Partial",
    ],

    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      InjectProduksiOutputRejectV2: {
        displayField: "NoProduksi",
        label: "Produksi Inject",
      },
      HotStampingOutputRejectV2: {
        displayField: "NoProduksi",
        label: "Hot Stamping",
      },
      PasangKunciOutputRejectV2: {
        displayField: "NoProduksi",
        label: "Pasang Kunci",
      },
      SpannerOutputRejectV2: {
        displayField: "NoProduksi",
        label: "Spanner",
      },
      BJSortirRejectOutputLabelReject: {
        displayField: "NoBJSortir",
        label: "Sortir Reject",
      },
    },

    headerParseFields: [
      {
        jsonField: "IdReject",
        joinTable: "MstReject",
        joinKey: "IdReject",
        displayField: "NamaReject",
        alias: "NamaReject",
      },
    ],
    scalarFields: ["Berat", "DateCreate", "Blok", "IdLokasi"],
    // statusField: 'IdStatus',
    // statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

  washing_produksi: {
    pkField: "NoProduksi",
    headerTable: "WashingProduksi_h",
    detailTable: null,

    outputTables: [],
    inputTables: [
      "WashingProduksiInput",
      "WashingProduksiInputBBPartial",
      "WashingProduksiInputWashing",
      "WashingProduksiInputGilingan",
      "WashingProduksiInputGilinganPartial",
    ],

    // ✅ scalar = pure scalar (TIDAK join, TIDAK reference)
    scalarFields: [
      "TglProduksi",
      "JamKerja",
      "Shift",
      "CreateBy",
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    // ✅ relational / reference fields
    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  broker_produksi: {
    pkField: "NoProduksi",
    headerTable: "BrokerProduksi_h",
    detailTable: null,

    outputTables: [],
    inputTables: [
      "BrokerProduksiInputBB",
      "BrokerProduksiInputBBPartial",
      "BrokerProduksiInputWashing",
      "BrokerProduksiInputBroker",
      "BrokerProduksiInputBrokerPartial",
      "BrokerProduksiInputCrusher",
      "BrokerProduksiInputGilingan",
      "BrokerProduksiInputGilinganPartial",
      "BrokerProduksiInputMixer",
      "BrokerProduksiInputMixerPartial",
      "BrokerProduksiInputReject",
      "BrokerProduksiInputRejectPartial",
    ],

    scalarFields: [
      "TglProduksi",
      "Shift",
      "Jam", // ✅ brokerproduksi pakai "Jam" (bukan JamKerja)
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
      "CreateBy",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  crusher_produksi: {
    pkField: "NoCrusherProduksi",
    headerTable: "CrusherProduksi_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "CrusherProduksiInputBB",
      "CrusherProduksiInputBBPartial",
      "CrusherProduksiInputBonggolan",
    ],

    scalarFields: [
      "Tanggal",
      "Jam",
      "Shift",
      "CreateBy",
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  gilingan_produksi: {
    pkField: "NoProduksi",
    headerTable: "GilinganProduksi_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "GilinganProduksiInputBroker",
      "GilinganProduksiInputBrokerPartial",
      "GilinganProduksiInputCrusher",
      "GilinganProduksiInputBonggolan",
      "GilinganProduksiInputBJ",
      "GilinganProduksiInputRejectV2",
      "GilinganProduksiInputRejectV2Partial",
    ],

    scalarFields: [
      "Tanggal",
      "Jam",
      "Shift",
      "CreateBy",
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  mixer_produksi: {
    pkField: "NoProduksi",
    headerTable: "MixerProduksi_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "MixerProduksiInputBB",
      "MixerProduksiInputBBPartial",
      "MixerProduksiInputBroker",
      "MixerProduksiInputBrokerPartial",
      "MixerProduksiInputGilingan",
      "MixerProduksiInputGilinganPartial",
      "MixerProduksiInputMixer",
      "MixerProduksiInputMixerPartial",
    ],

    scalarFields: [
      "TglProduksi",
      "Jam",
      "Shift",
      "CreateBy",
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  inject_produksi: {
    pkField: "NoProduksi",
    headerTable: "InjectProduksi_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "InjectProduksiInputBroker",
      "InjectProduksiInputBrokerPartial",
      "InjectProduksiInputFurnitureWIP",
      "InjectProduksiInputFurnitureWIPPartial",
      "InjectProduksiInputGilingan",
      "InjectProduksiInputGilinganPartial",
      "InjectProduksiInputMixer",
      "InjectProduksiInputMixerPartial",
    ],

    scalarFields: [
      "TglProduksi",
      "Jam",
      "Shift",
      "CreateBy",
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
      "IdCetakan",
      "IdWarna",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  hot_stamping: {
    pkField: "NoProduksi",
    headerTable: "HotStamping_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "HotStampingInputLabelFWIP",
      "HotStampingInputLabelFWIPPartial",
    ],

    scalarFields: [
      "TglProduksi",
      "Jam",
      "Shift",
      "CreateBy",
      "JmlhAnggota",
      "Hadir",
      "HourMeter",
      "HourStart",
      "HourEnd",
      "IdCetakan",
      "IdWarna",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  pasang_kunci: {
    pkField: "NoProduksi",
    headerTable: "PasangKunci_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "PasangKunciInputLabelFWIP",
      "PasangKunciInputLabelFWIPPartial",
    ],

    scalarFields: [
      "Tanggal",
      "Shift",
      "CreateBy",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  spanner: {
    pkField: "NoProduksi",
    headerTable: "Spanner_h",
    detailTable: null,

    outputTables: [],

    inputTables: ["SpannerInputLabelFWIP", "SpannerInputLabelFWIPPartial"],

    scalarFields: [
      "Tanggal",
      "Shift",
      "CreateBy",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  packing: {
    pkField: "NoPacking",
    headerTable: "PackingProduksi_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "PackingProduksiInputLabelFWIP",
      "PackingProduksiInputLabelFWIPPartial",
    ],

    scalarFields: [
      "Tanggal",
      "Shift",
      "CreateBy",
      "HourMeter",
      "HourStart",
      "HourEnd",
    ],

    headerParseFields: [
      {
        jsonField: "IdOperator",
        joinTable: "MstOperator",
        joinKey: "IdOperator",
        displayField: "NamaOperator",
        alias: "NamaOperator",
      },
      {
        jsonField: "IdMesin",
        joinTable: "MstMesin",
        joinKey: "IdMesin",
        displayField: "NamaMesin",
        alias: "NamaMesin",
      },
    ],
  },

  sortir_reject: {
    pkField: "NoBJSortir",
    headerTable: "BJSortirReject_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "BJSortirRejectInputLabelBarangJadi",
      "BJSortirRejectInputLabelFurnitureWIP",
    ],

    scalarFields: ["TglBJSortir"],

    headerParseFields: [
      {
        jsonField: "IdWarehouse",
        joinTable: "MstWarehouse",
        joinKey: "IdWarehouse",
        displayField: "NamaWarehouse",
        alias: "NamaWarehouse",
      },
    ],
  },

  bj_jual: {
    pkField: "NoBJJual",
    headerTable: "BJJual_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "BJJual_dLabelBarangJadi",
      "BJJual_dLabelBarangJadiPartial",
      "BJJual_dLabelFurnitureWIP",
      "BJJual_dLabelFurnitureWIPPartial",
    ],

    scalarFields: ["Tanggal", "Remark"],

    headerParseFields: [
      {
        jsonField: "IdPembeli",
        joinTable: "MstPembeli",
        joinKey: "IdPembeli",
        displayField: "NamaPembeli",
        alias: "NamaPembeli",
      },
    ],
  },

  bongkar_susun: {
    pkField: "NoBongkarSusun",
    headerTable: "BongkarSusun_h",
    detailTable: null,

    outputTables: [],

    inputTables: [
      "BongkarSusunInputBahanBaku",
      "BongkarSusunInputWashing",
      "BongkarSusunInputBroker",
      "BongkarSusunInputCrusher",
      "BongkarSusunInputGilingan",
      "BongkarSusunInputMixer",
      "BongkarSusunInputBonggolan",
      "BongkarSusunInputFurnitureWIP",
      "BongkarSusunInputBarangjadi",
    ],

    scalarFields: ["Tanggal", "Note"],

    headerParseFields: [],
  },
};

module.exports = { MODULE_CONFIG };

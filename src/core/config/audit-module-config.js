// src/services/audit/audit-module-config.js

const MODULE_CONFIG = {
  washing: {
    prefix: "B",
    pkField: "NoWashing",
    headerTable: "Washing_h",
    detailTable: "Washing_d",
    outputTables: ["WashingProduksiOutput", "BongkarSusunOutputWashing"],
    inputTables: [
      "WashingProduksiInputWashing",
      "BrokerProduksiInputWashing",
      "BongkarSusunInputWashing",
    ],
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
    prefix: "D",
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
    prefix: "F",
    pkField: "NoCrusher",
    headerTable: "Crusher",
    detailTable: null,
    outputTables: ["CrusherProduksiOutput", "BongkarSusunOutputCrusher"],
    inputTables: [
      "BrokerProduksiInputCrusher",
      "GilinganProduksiInputCrusher",
      "BongkarSusunInputCrusher",
    ],
    outputDisplayConfig: {
      CrusherProduksiOutput: {
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
    prefix: "V",
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
    prefix: "M",
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
    prefix: "H",
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
    prefix: "BB",
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
    prefix: "BA",
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
  },

  reject: {
    prefix: "BF",
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
  },

  washing_produksi: {
    prefix: "C",
    pkField: "NoProduksi",
    headerTable: "WashingProduksi_h",

    supportsOutputMutation: true,

    detailTable: null,
    outputTables: ["WashingProduksiOutput"],
    inputTables: [
      "WashingProduksiInput",
      "WashingProduksiInputBBPartial",
      "WashingProduksiInputWashing",
      "WashingProduksiInputGilingan",
      "WashingProduksiInputGilinganPartial",
    ],
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
    prefix: "E",
    pkField: "NoProduksi",
    headerTable: "BrokerProduksi_h",
    supportsOutputMutation: true,
    detailTable: null,
    outputTables: ["BrokerProduksiOutput", "BrokerProduksiOutputBonggolan"],
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
      "Jam",
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
    prefix: "G",
    pkField: "NoCrusherProduksi",
    headerTable: "CrusherProduksi_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["CrusherProduksiOutput"],
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
    prefix: "W",
    pkField: "NoProduksi",
    headerTable: "GilinganProduksi_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["GilinganProduksiOutput"],
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
    prefix: "I",
    pkField: "NoProduksi",
    headerTable: "MixerProduksi_h",
    detailTable: null,
    outputTables: ["MixerProduksiOutput"],
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
    prefix: "S",
    pkField: "NoProduksi",
    headerTable: "InjectProduksi_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: [
      "InjectProduksiOutputBonggolan",
      "InjectProduksiOutputMixer",
      "InjectProduksiOutputFurnitureWIP",
    ],
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
    prefix: "BH",
    pkField: "NoProduksi",
    headerTable: "HotStamping_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["HotStampingOutputLabelFWIP"],
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
    prefix: "BI",
    pkField: "NoProduksi",
    headerTable: "PasangKunci_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["PasangKunciOutputLabelFWIP"],
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
    prefix: "BJ",
    pkField: "NoProduksi",
    headerTable: "Spanner_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["SpannerOutputLabelFWIP"],
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
    prefix: "BD",
    pkField: "NoPacking",
    headerTable: "PackingProduksi_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["PackingProduksiOutputLabelBJ"],
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
    prefix: "J",
    pkField: "NoBJSortir",
    headerTable: "BJSortirReject_h",
    detailTable: null,
    supportsOutputMutation: true,
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
    prefix: "K",
    pkField: "NoBJJual",
    headerTable: "BJJual_h",
    detailTable: null,
    supportsOutputMutation: true,
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

  return: {
    prefix: "L",
    pkField: "NoRetur",
    headerTable: "BJRetur_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: ["BJReturBarangJadi_d", "BJReturFurnitureWIP_d"],
    inputTables: [],
    scalarFields: ["Tanggal", "Invoice"],
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
    prefix: "BG",
    pkField: "NoBongkarSusun",
    headerTable: "BongkarSusun_h",
    detailTable: null,
    supportsOutputMutation: true,
    outputTables: [
      "BongkarSusunOutputBahanBaku",
      "BongkarSusunOutputBarangjadi",
      "BongkarSusunOutputBonggolan",
      "BongkarSusunOutputGilingan",
      "BongkarSusunOutputWashing",
      "BongkarSusunOutputCrusher",
      "BongkarSusunOutputBroker",
      "BongkarSusunOutputMixer",
      "BongkarSusunOutputFurnitureWIP",
    ],

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

/**
 * 🎯 Helper function to detect module from document number prefix
 * @param {string} documentNo - Document number (e.g., "S.0000029967", "B.0000013196")
 * @returns {string|null} - Module key or null if not found
 */
function detectModuleFromPrefix(documentNo) {
  if (!documentNo || typeof documentNo !== "string") {
    return null;
  }

  // Extract prefix before the dot
  const match = documentNo.match(/^([A-Z]+)\./i);
  if (!match) {
    return null;
  }

  const prefix = match[1].toUpperCase();

  // Find module with matching prefix
  for (const [moduleKey, config] of Object.entries(MODULE_CONFIG)) {
    if (config.prefix && config.prefix.toUpperCase() === prefix) {
      return moduleKey;
    }
  }

  return null;
}

/**
 * 🎯 Get module config by prefix or module key
 * @param {string} identifier - Either prefix (e.g., "S") or module key (e.g., "inject_produksi")
 * @returns {object|null} - Module config or null if not found
 */
function getModuleConfig(identifier) {
  if (!identifier || typeof identifier !== "string") {
    return null;
  }

  const normalized = identifier.toLowerCase().trim();

  // First try direct module key lookup
  if (MODULE_CONFIG[normalized]) {
    return { key: normalized, config: MODULE_CONFIG[normalized] };
  }

  // Then try prefix lookup
  const upperIdentifier = identifier.toUpperCase();
  for (const [moduleKey, config] of Object.entries(MODULE_CONFIG)) {
    if (config.prefix && config.prefix.toUpperCase() === upperIdentifier) {
      return { key: moduleKey, config };
    }
  }

  return null;
}

module.exports = {
  MODULE_CONFIG,
  detectModuleFromPrefix,
  getModuleConfig,
};

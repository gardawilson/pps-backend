// src/services/audit/audit-module-config.js

const MODULE_CONFIG = {
  washing: {
    pkField: 'NoWashing',
    headerTable: 'Washing_h',
    detailTable: 'Washing_d',
    outputTables: ['WashingProduksiOutput', 'BongkarSusunOutputWashing'],
    
    inputTables: ['WashingProduksiInputWashing', 'BrokerProduksiInputWashing'],

    // ✅ NEW: Output display config
    outputDisplayConfig: {
      'WashingProduksiOutput': { 
        displayField: 'NoProduksi',
        label: 'Washing Produksi'
      },
      'BongkarSusunOutputWashing': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    headerParseFields: [
      {
        jsonField: 'IdJenisPlastik',
        joinTable: 'MstJenisPlastik',
        joinKey: 'IdJenisPlastik',
        displayField: 'Jenis',
        alias: 'NamaJenisPlastik',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', 'true': 'PASS', '0': 'HOLD', 'false': 'HOLD' },
  },

  broker: {
    pkField: 'NoBroker',
    headerTable: 'Broker_h',
    detailTable: 'Broker_d',
    outputTables: ['BrokerProduksiOutput', 'BongkarSusunOutputBroker'],

    inputTables: ['BrokerProduksiInputBroker', 'BrokerProduksiInputBrokerPartial', 'GilinganProduksiInputBroker', 'GilinganProduksiInputBrokerPartial', 'MixerProduksiInputBroker', 'MixerProduksiInputBrokerPartial', 'InjectProduksiInputBroker', 'InjectProduksiInputFurnitureWIPPartial'],
    
    // ✅ NEW
    outputDisplayConfig: {
      'BrokerProduksiOutput': { 
        displayField: 'NoProduksi',
        label: 'Broker Produksi'
      },
      'BongkarSusunOutputBroker': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    
    headerParseFields: [
      {
        jsonField: 'IdJenisPlastik',
        joinTable: 'MstJenisPlastik',
        joinKey: 'IdJenisPlastik',
        displayField: 'Jenis',
        alias: 'NamaJenisPlastik',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', 'true': 'PASS', '0': 'HOLD', 'false': 'HOLD' },
  },

  crusher: {
    pkField: 'NoCrusher',
    headerTable: 'Crusher',
    detailTable: null,
    outputTables: ['CrusherProduksiOutput', 'BongkarSusunOutputCrusher'],

    inputTables: ['BrokerProduksiInputCrusher', 'GilinganProduksiInputCrusher'],
    
    // ✅ NEW
    outputDisplayConfig: {
      'BongkarSusunOutputCrusher': { 
        displayField: 'NoCrusherProduksi',
        label: 'Crusher Produksi'
      },
      'BongkarSusunOutputCrusher': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    headerParseFields: [
      {
        jsonField: 'IdCrusher',
        joinTable: 'MstCrusher',
        joinKey: 'IdCrusher',
        displayField: 'NamaCrusher',
        alias: 'NamaCrusher',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Berat', 'DateCreate', 'Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

  gilingan: {
    pkField: 'NoGilingan',
    headerTable: 'Gilingan',
    detailTable: null,
    outputTables: ['GilinganProduksiOutput', 'BongkarSusunOutputGilingan'],
    
    inputTables: ['WashingProduksiInputGilingan', 'WashingProduksiInputGilinganPartial', 'BrokerProduksiInputGilingan', 'BrokerProduksiInputGilinganPartial', 'MixerProduksiInputGilingan', 'MixerProduksiInputGilinganPartial', 'InjectProduksiInputGilingan', 'InjectProduksiInputGilinganPartial'],

    // ✅ NEW
    outputDisplayConfig: {
      'GilinganProduksiOutput': { 
        displayField: 'NoProduksi',
        label: 'Gilingan Produksi'
      },
      'BongkarSusunOutputGilingan': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    headerParseFields: [
      {
        jsonField: 'IdGilingan',
        joinTable: 'MstGilingan',
        joinKey: 'IdGilingan',
        displayField: 'NamaGilingan',
        alias: 'NamaGilingan',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Berat', 'DateCreate', 'Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

  bonggolan: {
    pkField: 'NoBonggolan',
    headerTable: 'Bonggolan',
    detailTable: null,
    outputTables: ['BrokerProduksiOutputBonggolan','InjectProduksiOutputBonggolan', 'BongkarSusunOutputBonggolan'],

    inputTables: ['CrusherProduksiInputBonggolan', 'GilinganProduksiInputBonggolan'],

    
    // ✅ NEW
    outputDisplayConfig: {
      'BrokerProduksiOutputBonggolan': { 
        displayField: 'NoProduksi',
        label: 'Broker Produksi'
      },
      'InjectProduksiOutputBonggolan': { 
        displayField: 'NoProduksi',
        label: 'Inject Produksi'
      },
            'BongkarSusunOutputBonggolan': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    headerParseFields: [
      {
        jsonField: 'IdBonggolan',
        joinTable: 'MstBonggolan',
        joinKey: 'IdBonggolan',
        displayField: 'NamaBonggolan',
        alias: 'NamaBonggolan',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Berat', 'DateCreate', 'Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

  mixer: {
    pkField: 'NoMixer',
    headerTable: 'Mixer_h',
    detailTable: 'Mixer_d',
    outputTables: ['MixerProduksiOutput', 'InjectProduksiOutputMixer', 'BongkarSusunOutputMixer'],

    inputTables: ['BrokerProduksiInputMixer', 'BrokerProduksiInputMixerPartial', 'MixerProduksiInputMixer', 'MixerProduksiInputMixerPartial', 'InjectProduksiInputMixer', 'InjectProduksiInputMixerPartial'],
    
    // ✅ NEW
    outputDisplayConfig: {
      'MixerProduksiOutput': { 
        displayField: 'NoProduksi',
        label: 'Mixer Produksi'
      },
      'InjectProduksiOutputMixer': { 
        displayField: 'NoProduksi',
        label: 'Inject Produksi'
      },
      'BongkarSusunOutputMixer': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    headerParseFields: [
      {
        jsonField: 'IdMixer',
        joinTable: 'MstMixer',
        joinKey: 'IdMixer',
        displayField: 'Jenis',
        alias: 'NamaMixer',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', 'true': 'PASS', '0': 'HOLD', 'false': 'HOLD' },
  },

  furniturewip: {
    pkField: 'NoFurnitureWIP',
    headerTable: 'FurnitureWIP',
    detailTable: null,
    outputTables: ['InjectProduksiOutputFurnitureWIP', 'HotStampingOutputLabelFWIP', 'PasangKunciOutputLabelFWIP', 'SpannerOutputLabelFWIP', 'BJReturFurnitureWIP_d', 'BongkarSusunOutputFurnitureWIP'],
    
    inputTables: ['InjectProduksiInputFurnitureWIP', 'InjectProduksiInputFurnitureWIPPartial', 'HotStampingInputLabelFWIP', 'HotStampingInputLabelFWIPPartial', 'PasangKunciInputLabelFWIP', 'PasangKunciInputLabelFWIPPartial', 'SpannerInputLabelFWIP', 'SpannerInputLabelFWIPPartial', 'PackingProduksiInputLabelFWIP', 'PackingProduksiInputLabelFWIPPartial', 'BJSortirRejectInputLabelFurnitureWIP'],

    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      'InjectProduksiOutputFurnitureWIP': { 
        displayField: 'NoProduksi',
        label: 'Produksi Inject'
      },
      'HotStampingOutputLabelFWIP': { 
        displayField: 'NoProduksi',
        label: 'Hot Stamping'
      },
      'PasangKunciOutputLabelFWIP': { 
        displayField: 'NoProduksi',
        label: 'Pasang Kunci'
      },
      'SpannerOutputLabelFWIP': { 
        displayField: 'NoProduksi',
        label: 'Produksi Spanner'
      },
      'BJReturFurnitureWIP_d': { 
        displayField: 'NoRetur',
        label: 'Retur BJ'
      },
      'BongkarSusunOutputFurnitureWIP': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      }
    },
    
    headerParseFields: [
      {
        jsonField: 'IDFurnitureWIP',
        joinTable: 'MstCabinetWIP',
        joinKey: 'IdCabinetWIP',
        displayField: 'Nama',
        alias: 'NamaFurnitureWIP',
      },
      {
        jsonField: 'IdWarehouse',
        joinTable: 'MstWarehouse',
        joinKey: 'IdWarehouse',
        displayField: 'NamaWarehouse',
        alias: 'NamaWarehouse',
      },
    ],
    scalarFields: ['Pcs', 'Berat', 'DateCreate', 'Blok', 'IdLokasi'],
    statusField: 'IdStatus',
    statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

  barangjadi: {
    pkField: 'NoBJ',
    headerTable: 'BarangJadi',
    detailTable: null,
    outputTables: ['InjectProduksiOutputBarangJadi', 'PackingProduksiOutputLabelBJ', 'BongkarSusunOutputBarangjadi', 'BJReturBarangJadi_d'],

    inputTables: ['BJSortirRejectInputLabelBarangJadi'],
    
    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      'InjectProduksiOutputBarangJadi': { 
        displayField: 'NoProduksi',
        label: 'Produksi Inject'
      },
      'PackingProduksiOutputLabelBJ': { 
        displayField: 'NoPacking',
        label: 'Produksi Packing'
      },
      'BongkarSusunOutputBarangjadi': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      },
      'BJReturBarangJadi_d': { 
        displayField: 'NoRetur',
        label: 'Retur'
      },
    },
    
    headerParseFields: [
      {
        jsonField: 'IdBJ',
        joinTable: 'MstBarangJadi',
        joinKey: 'IdBJ',
        displayField: 'NamaBJ',
        alias: 'NamaBJ',
      },
    ],
    scalarFields: ['Pcs', 'Berat', 'DateCreate', 'Blok', 'IdLokasi'],
    // statusField: 'IdStatus',
    // statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },

   reject: {
    pkField: 'NoReject',
    headerTable: 'RejectV2',
    detailTable: null,
    outputTables: ['InjectProduksiOutputRejectV2', 'HotStampingOutputRejectV2', 'PasangKunciOutputRejectV2', 'SpannerOutputRejectV2', 'BJSortirRejectOutputLabelReject'],
    
    inputTables: ['BrokerProduksiInputReject', 'BrokerProduksiInputRejectPartial', 'GilinganProduksiInputRejectV2', 'GilinganProduksiInputRejectV2Partial'],

    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      'InjectProduksiOutputRejectV2': { 
        displayField: 'NoProduksi',
        label: 'Produksi Inject'
      },
      'HotStampingOutputRejectV2': { 
        displayField: 'NoProduksi',
        label: 'Hot Stamping'
      },
      'PasangKunciOutputRejectV2': { 
        displayField: 'NoProduksi',
        label: 'Pasang Kunci'
      },
      'SpannerOutputRejectV2': { 
        displayField: 'NoProduksi',
        label: 'Spanner'
      },
      'BJSortirRejectOutputLabelReject': { 
        displayField: 'NoBJSortir',
        label: 'Sortir Reject'
      },
    },
    
    headerParseFields: [
      {
        jsonField: 'IdReject',
        joinTable: 'MstReject',
        joinKey: 'IdReject',
        displayField: 'NamaReject',
        alias: 'NamaReject',
      },
    ],
    scalarFields: ['Berat', 'DateCreate', 'Blok', 'IdLokasi'],
    // statusField: 'IdStatus',
    // statusMapping: { '1': 'PASS', '0': 'HOLD' },
  },
};

module.exports = { MODULE_CONFIG };
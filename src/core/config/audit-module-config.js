// src/services/audit/audit-module-config.js

const MODULE_CONFIG = {
  washing: {
    pkField: 'NoWashing',
    headerTable: 'Washing_h',
    detailTable: 'Washing_d',
    outputTables: ['BongkarSusunOutputWashing', 'WashingProduksiOutput'],
    
    // ✅ NEW: Output display config
    outputDisplayConfig: {
      'BongkarSusunOutputWashing': { 
        displayField: 'NoBongkarSusun',
        label: 'Bongkar Susun'
      },
      'WashingProduksiOutput': { 
        displayField: 'NoProduksi',
        label: 'Washing Produksi'
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
    outputTables: ['BongkarSusunOutputBroker'],
    
    // ✅ NEW
    outputDisplayConfig: {
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
    outputTables: ['BongkarSusunOutputCrusher'],
    
    // ✅ NEW
    outputDisplayConfig: {
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
    outputTables: ['BongkarSusunOutputGilingan'],
    
    // ✅ NEW
    outputDisplayConfig: {
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
    outputTables: ['BongkarSusunOutputBonggolan'],
    
    // ✅ NEW
    outputDisplayConfig: {
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
    outputTables: ['BongkarSusunOutputMixer'],
    
    // ✅ NEW
    outputDisplayConfig: {
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
    outputTables: ['InjectProduksiOutputFurnitureWIP', 'BJReturFurnitureWIP_d', 'BongkarSusunOutputFurnitureWIP'],
    
    // ✅ NEW: Multiple output tables with different display fields
    outputDisplayConfig: {
      'InjectProduksiOutputFurnitureWIP': { 
        displayField: 'NoProduksi',
        label: 'Produksi Inject'
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
};

module.exports = { MODULE_CONFIG };
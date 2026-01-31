// src/core/config/produksi-input-mapping.config.js

/**
 * Configuration untuk mapping input dan partial di semua modul produksi.
 * File ini menjadi single source of truth untuk struktur tabel dan relasi.
 * 
 * @module produksi-input-mapping.config
 */

/**
 * Configuration untuk setiap jenis partial yang ada di sistem
 */
const PARTIAL_CONFIGS = {
  bb: {
    tableName: 'BahanBakuPartial',
    sourceTable: 'BahanBaku_d',
    partialColumn: 'NoBBPartial',
    prefix: 'P.',
    keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
    weightColumn: 'Berat',
    // Special case: BB bisa pakai BeratAct atau Berat
    weightSourceColumn: 'ISNULL(NULLIF(BeratAct, 0), Berat)',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputBBPartial',
      washingProduksi: 'WashingProduksiInputBBPartial',
      crusherProduksi: 'CrusherProduksiInputBBPartial',
      gilinganProduksi: 'GilinganProduksiInputBBPartial',
      mixerProduksi: 'MixerProduksiInputBBPartial',
    }
  },
  
  broker: {
    tableName: 'BrokerPartial',
    sourceTable: 'Broker_d',
    partialColumn: 'NoBrokerPartial',
    prefix: 'Q.',
    keys: ['NoBroker', 'NoSak'],
    weightColumn: 'Berat',
    weightSourceColumn: 'Berat',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputBrokerPartial',
      gilinganProduksi: 'GilinganProduksiInputBrokerPartial',
      mixerProduksi: 'MixerProduksiInputBrokerPartial',
      injectProduksi: 'InjectProduksiInputBrokerPartial',

    }
  },
  
  // washing: {
  //   tableName: 'WashingPartial',
  //   sourceTable: 'Washing_d',
  //   partialColumn: 'NoWashingPartial',
  //   prefix: 'W.',
  //   keys: ['NoWashing', 'NoSak'],
  //   weightColumn: 'Berat',
  //   weightSourceColumn: 'Berat',
  //   mappingTables: {
  //     brokerProduksi: 'BrokerProduksiInputWashingPartial',
  //     crusherProduksi: 'CrusherProduksiInputWashingPartial',
  //   }
  // },
  
  gilingan: {
    tableName: 'GilinganPartial',
    sourceTable: 'Gilingan',
    partialColumn: 'NoGilinganPartial',
    prefix: 'Y.',
    keys: ['NoGilingan'],
    weightColumn: 'Berat',
    weightSourceColumn: 'Berat',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputGilinganPartial',
      washingProduksi: 'WashingProduksiInputGilinganPartial',
      crusherProduksi: 'CrusherProduksiInputGilinganPartial',
      mixerProduksi: 'MixerProduksiInputGilinganPartial',
      injectProduksi: 'InjectProduksiInputGilinganPartial',
    }
  },
  
  mixer: {
    tableName: 'MixerPartial',
    sourceTable: 'Mixer_d',
    partialColumn: 'NoMixerPartial',
    prefix: 'T.',
    keys: ['NoMixer', 'NoSak'],
    weightColumn: 'Berat',
    weightSourceColumn: 'Berat',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputMixerPartial',
      crusherProduksi: 'CrusherProduksiInputMixerPartial',
      mixerProduksi: 'MixerProduksiInputMixerPartial',
      injectProduksi: 'InjectProduksiInputMixerPartial',

    }
  },
  
  reject: {
    tableName: 'RejectV2Partial',
    sourceTable: 'RejectV2',
    partialColumn: 'NoRejectPartial',
    prefix: 'BK.',
    keys: ['NoReject'],
    weightColumn: 'Berat',
    weightSourceColumn: 'Berat',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputRejectPartial',
      crusherProduksi: 'CrusherProduksiInputRejectPartial',
      gilinganProduksi: 'GilinganProduksiInputRejectV2Partial',
    }
  },
  
  // crusher: {
  //   tableName: 'CrusherPartial',
  //   sourceTable: 'Crusher',
  //   partialColumn: 'NoCrusherPartial',
  //   prefix: 'CR.',
  //   keys: ['NoCrusher'],
  //   weightColumn: 'Berat',
  //   weightSourceColumn: 'Berat',
  //   mappingTables: {
  //     brokerProduksi: 'BrokerProduksiInputCrusherPartial',
  //     gilinganProduksi: 'GilinganProduksiInputCrusherPartial',
  //   }
  // },

  furnitureWip: {
    tableName: 'FurnitureWIPPartial',
    sourceTable: 'FurnitureWIP',
    partialColumn: 'NoFurnitureWIPPartial',
    prefix: 'BC.',
    keys: ['NoFurnitureWIP'],
    weightColumn: 'Pcs',
    weightSourceColumn: 'Pcs',
    mappingTables: {
      injectProduksi: 'InjectProduksiInputFurnitureWIPPartial',
      hotStamping: 'HotStampingInputLabelFWIPPartial',
      keyFitting: 'PasangKunciInputLabelFWIPPartial',
      spanner: 'SpannerInputLabelFWIPPartial',
      packingProduksi: 'PackingProduksiInputLabelFWIPPartial',
      bjJual: 'BJJual_dLabelFurnitureWIPPartial',
    }
  },

  barangJadi: {
    tableName: 'BarangJadiPartial',
    sourceTable: 'BarangJadi',
    partialColumn: 'NoBJPartial',
    prefix: 'BL.',
    keys: ['NoBJ'],
    weightColumn: 'Pcs',
    weightSourceColumn: 'Pcs',
    mappingTables: {
      bjJual: 'BJJual_dLabelBarangJadiPartial',
    }
  },
};

/**
 * Configuration untuk input mapping (full/non-partial)
 */
const INPUT_CONFIGS = {
  brokerProduksi: {
    broker: {
      sourceTable: 'Broker_d',
      keys: ['NoBroker', 'NoSak'],
      mappingTable: 'BrokerProduksiInputBroker',
      dateUsageColumn: 'DateUsage',
    },
    bb: {
      sourceTable: 'BahanBaku_d',
      keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
      mappingTable: 'BrokerProduksiInputBB',
      dateUsageColumn: 'DateUsage',
    },
    washing: {
      sourceTable: 'Washing_d',
      keys: ['NoWashing', 'NoSak'],
      mappingTable: 'BrokerProduksiInputWashing',
      dateUsageColumn: 'DateUsage',
    },
    crusher: {
      sourceTable: 'Crusher',
      keys: ['NoCrusher'],
      mappingTable: 'BrokerProduksiInputCrusher',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'BrokerProduksiInputGilingan',
      dateUsageColumn: 'DateUsage',
    },
    mixer: {
      sourceTable: 'Mixer_d',
      keys: ['NoMixer', 'NoSak'],
      mappingTable: 'BrokerProduksiInputMixer',
      dateUsageColumn: 'DateUsage',
    },
    reject: {
      sourceTable: 'RejectV2',
      keys: ['NoReject'],
      mappingTable: 'BrokerProduksiInputReject',
      dateUsageColumn: 'DateUsage',
    },
  },
  
  washingProduksi: {
    bb: {
      sourceTable: 'BahanBaku_d',
      keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
      mappingTable: 'WashingProduksiInput',
      dateUsageColumn: 'DateUsage',
    },
    washing: {
      sourceTable: 'Washing_d',
      keys: ['NoWashing', 'NoSak'],
      mappingTable: 'WashingProduksiInputWashing',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'WashingProduksiInputGilingan',
      dateUsageColumn: 'DateUsage',
    },
  },
  
  crusherProduksi: {
    bb: {
      sourceTable: 'BahanBaku_d',
      keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
      mappingTable: 'CrusherProduksiInputBB',
      dateUsageColumn: 'DateUsage',
    },
    bonggolan: {
      sourceTable: 'Bonggolan',
      keys: ['NoBonggolan'],
      mappingTable: 'CrusherProduksiInputBonggolan',
      dateUsageColumn: 'DateUsage',
    },
  },
  
  gilinganProduksi: {
    broker: {
      sourceTable: 'Broker_d',
      keys: ['NoBroker', 'NoSak'],
      mappingTable: 'GilinganProduksiInputBroker',
      dateUsageColumn: 'DateUsage',
    },
    bonggolan: {
      sourceTable: 'Bonggolan',
      keys: ['NoBonggolan'],
      mappingTable: 'GilinganProduksiInputBonggolan',
      dateUsageColumn: 'DateUsage',
    },
    crusher: {
      sourceTable: 'Crusher',
      keys: ['NoCrusher'],
      mappingTable: 'GilinganProduksiInputCrusher',
      dateUsageColumn: 'DateUsage',
    },
    reject: {
      sourceTable: 'RejectV2',
      keys: ['NoReject'],
      mappingTable: 'GilinganProduksiInputRejectV2',
      dateUsageColumn: 'DateUsage',
    },
  },
  
  mixerProduksi: {
    bb: {
      sourceTable: 'BahanBaku_d',
      keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
      mappingTable: 'MixerProduksiInputBB',
      dateUsageColumn: 'DateUsage',
    },
    broker: {
      sourceTable: 'Broker_d',
      keys: ['NoBroker', 'NoSak'],
      mappingTable: 'MixerProduksiInputBroker',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'MixerProduksiInputGilingan',
      dateUsageColumn: 'DateUsage',
    },
    mixer: {
      sourceTable: 'Mixer_d',
      keys: ['NoMixer', 'NoSak'],
      mappingTable: 'MixerProduksiInputMixer',
      dateUsageColumn: 'DateUsage',
    },
  },

  injectProduksi: {
    broker: {
      sourceTable: 'Broker_d',
      keys: ['NoBroker', 'NoSak'],
      mappingTable: 'InjectProduksiInputBroker',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'InjectProduksiInputGilingan',
      dateUsageColumn: 'DateUsage',
    },
    mixer: {
      sourceTable: 'Mixer_d',
      keys: ['NoMixer', 'NoSak'],
      mappingTable: 'InjectProduksiInputMixer',
      dateUsageColumn: 'DateUsage',
    },
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'InjectProduksiInputFurnitureWIP',
      dateUsageColumn: 'DateUsage',
    },
  },

  hotStamping: {
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'HotStampingInputLabelFWIP',
      dateUsageColumn: 'DateUsage',
    },
  },

  keyFitting: {
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'PasangKunciInputLabelFWIP',
      dateUsageColumn: 'DateUsage',
    },
  },

   spanner: {
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'SpannerInputLabelFWIP',
      dateUsageColumn: 'DateUsage',
    },
  },

  packingProduksi: {
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'PackingProduksiInputLabelFWIP',
      dateUsageColumn: 'DateUsage',
    },
  },

   sortirReject: {
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'BJSortirRejectInputLabelFurnitureWIP',
      dateUsageColumn: 'DateUsage',
    },
    barangJadi: {
      sourceTable: 'BarangJadi',
      keys: ['NoBJ'],
      mappingTable: 'BJSortirRejectInputLabelBarangJadi',
      dateUsageColumn: 'DateUsage',
    },
  },

  bongkarSusun: {
    bb: {
      sourceTable: 'BahanBaku_d',
      keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
      mappingTable: 'BongkarSusunInputBahanBaku',
      dateUsageColumn: 'DateUsage',
    },
    washing: {
      sourceTable: 'Washing_d',
      keys: ['NoWashing', 'NoSak'],
      mappingTable: 'BongkarSusunInputWashing',
      dateUsageColumn: 'DateUsage',
    },
    broker: {
      sourceTable: 'Broker_d',
      keys: ['NoBroker', 'NoSak'],
      mappingTable: 'BongkarSusunInputBroker',
      dateUsageColumn: 'DateUsage',
    },
    crusher: {
      sourceTable: 'Crusher',
      keys: ['NoCrusher'],
      mappingTable: 'BongkarSusunInputCrusher',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'BongkarSusunInputGilingan',
      dateUsageColumn: 'DateUsage',
    },
    mixer: {
      sourceTable: 'Mixer_d',
      keys: ['NoMixer', 'NoSak'],
      mappingTable: 'BongkarSusunInputMixer',
      dateUsageColumn: 'DateUsage',
    },
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'BongkarSusunInputFurnitureWIP',
      dateUsageColumn: 'DateUsage',
    },
    bonggolan: {
      sourceTable: 'Bonggolan',
      keys: ['NoBonggolan'],
      mappingTable: 'BongkarSusunInputBonggolan',
      dateUsageColumn: 'DateUsage',
    },
    barangJadi: {
      sourceTable: 'BarangJadi',
      keys: ['NoBJ'],
      mappingTable: 'BongkarSusunInputBarangJadi',
      dateUsageColumn: 'DateUsage',
    },
  },

  bjJual: {
    furnitureWip: {
      sourceTable: 'FurnitureWIP',
      keys: ['NoFurnitureWIP'],
      mappingTable: 'BJSortirRejectInputLabelFurnitureWIP',
      dateUsageColumn: 'DateUsage',
    },
    barangJadi: {
      sourceTable: 'BarangJadi',
      keys: ['NoBJ'],
      mappingTable: 'BJJual_dLabelBarangJadi',
      dateUsageColumn: 'DateUsage',
    },
  },
};

/**
 * Metadata untuk setiap produksi type
 */
const PRODUKSI_CONFIGS = {
  brokerProduksi: {
    headerTable: 'BrokerProduksi_h',
    entityKey: 'brokerProduksi',
    lockResource: 'SEQ_PARTIALS',
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },
  
  washingProduksi: {
    headerTable: 'WashingProduksi_h',
    entityKey: 'washingProduksi',
    lockResource: 'SEQ_WASHING_PARTIALS',
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },
  
  crusherProduksi: {
    headerTable: 'CrusherProduksi_h',
    entityKey: 'crusherProduksi',
    lockResource: 'SEQ_CRUSHER_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoCrusherProduksi',
  },
  
  gilinganProduksi: {
    headerTable: 'GilinganProduksi_h',
    entityKey: 'gilinganProduksi',
    lockResource: 'SEQ_GILINGAN_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoProduksi',
  },
  
  mixerProduksi: {
    headerTable: 'MixerProduksi_h',
    entityKey: 'mixerProduksi',
    lockResource: 'SEQ_MIXER_PARTIALS',
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },

  injectProduksi: {
    headerTable: 'InjectProduksi_h',
    entityKey: 'injectProduksi',
    lockResource: 'SEQ_INJECT_PARTIALS',
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },

  hotStamping: {
    headerTable: 'HotStamping_h',
    entityKey: 'hotStamping',
    lockResource: 'SEQ_HOTSTAMPING_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoProduksi',
  },

  keyFitting: {
    headerTable: 'PasangKunci_h',
    entityKey: 'keyFitting',
    lockResource: 'SEQ_KEYFITTING_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoProduksi',
  },

  spanner: {
    headerTable: 'Spanner_h',
    entityKey: 'spanner',
    lockResource: 'SEQ_SPANNER_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoProduksi',
  },

  packingProduksi: {
    headerTable: 'PackingProduksi_h',
    entityKey: 'packingProduksi',
    lockResource: 'SEQ_PACKING_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoPacking',
  },

  sortirReject: {
    headerTable: 'BJSortirReject_h',
    entityKey: 'sortirReject',
    lockResource: 'SEQ_SORTIR_PARTIALS',
    dateColumn: 'TglBJSortir',
    codeColumn: 'NoBJSortir',
  },

  bongkarSusun: {
    headerTable: 'BongkarSusun_h',
    entityKey: 'bongkarSusun',
    lockResource: 'SEQ_BS_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoBongkarSusun',
  },

  bjJual: {
    headerTable: 'BJJual_h',
    entityKey: 'bjJual',
    lockResource: 'SEQ_BJJUAL_PARTIALS',
    dateColumn: 'Tanggal',
    codeColumn: 'NoBJJual',
  },
};


const UPSERT_INPUT_CONFIGS = {
  injectProduksi: {
    cabinetMaterial: {
      mappingTable: 'InjectProduksiInputCabinetMaterial',
      sourceTable: 'MstCabinetMaterial',
      keyColumn: 'IdCabinetMaterial',
      quantityColumn: 'Pcs',
      validateColumn: 'Enable',
      validateValue: 1,
      aggregateByKey: true, // SUM quantities by key sebelum insert/update
    }
  },
  hotStamping: {
    cabinetMaterial: {
      mappingTable: 'HotStampingInputMaterial',
      sourceTable: 'MstCabinetMaterial',
      keyColumn: 'IdCabinetMaterial',
      quantityColumn: 'Jumlah',
      validateColumn: 'Enable',
      validateValue: 1,
      aggregateByKey: true, // SUM quantities by key sebelum insert/update
    }
  },
  keyFitting: {
    cabinetMaterial: {
      mappingTable: 'PasangKunciInputMaterial',
      sourceTable: 'MstCabinetMaterial',
      keyColumn: 'IdCabinetMaterial',
      quantityColumn: 'Jumlah',
      validateColumn: 'Enable',
      validateValue: 1,
      aggregateByKey: true, // SUM quantities by key sebelum insert/update
    }
  },
  spanner: {
    cabinetMaterial: {
      mappingTable: 'SpannerInputMaterial',
      sourceTable: 'MstCabinetMaterial',
      keyColumn: 'IdCabinetMaterial',
      quantityColumn: 'Jumlah',
      validateColumn: 'Enable',
      validateValue: 1,
      aggregateByKey: true, // SUM quantities by key sebelum insert/update
    }
  },
  packingProduksi: {
    cabinetMaterial: {
      mappingTable: 'PackingProduksiInputMaterial',
      sourceTable: 'MstCabinetMaterial',
      keyColumn: 'IdCabinetMaterial',
      quantityColumn: 'Jumlah',
      validateColumn: 'Enable',
      validateValue: 1,
      aggregateByKey: true, // SUM quantities by key sebelum insert/update
    }
  },
  bjJual: {
    cabinetMaterial: {
      mappingTable: 'PackingProduksiInputMaterial',
      sourceTable: 'MstCabinetMaterial',
      keyColumn: 'IdCabinetMaterial',
      quantityColumn: 'Pcs',
      validateColumn: 'Enable',
      validateValue: 1,
      aggregateByKey: true, // SUM quantities by key sebelum insert/update
    }
  }
};

/**
 * Mapping label untuk UI responses
 */
const INPUT_LABELS = {
  broker: 'Broker',
  bb: 'Bahan Baku',
  washing: 'Washing',
  crusher: 'Crusher',
  gilingan: 'Gilingan',
  mixer: 'Mixer',
  reject: 'Reject',
  furnitureWip: 'Furniture WIP',
  barangJadi: 'Barang Jadi',
  cabinetMaterial: 'Cabinet Material',
};

/**
 * Tolerance untuk floating point comparison (1 gram = 0.001 kg)
 */
const WEIGHT_TOLERANCE = 0.001;

module.exports = {
  PARTIAL_CONFIGS,
  INPUT_CONFIGS,
  PRODUKSI_CONFIGS,
  INPUT_LABELS,
  WEIGHT_TOLERANCE,
  UPSERT_INPUT_CONFIGS
};
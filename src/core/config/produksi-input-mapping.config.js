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
    }
  },
  
  washing: {
    tableName: 'WashingPartial',
    sourceTable: 'Washing_d',
    partialColumn: 'NoWashingPartial',
    prefix: 'W.',
    keys: ['NoWashing', 'NoSak'],
    weightColumn: 'Berat',
    weightSourceColumn: 'Berat',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputWashingPartial',
      crusherProduksi: 'CrusherProduksiInputWashingPartial',
    }
  },
  
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
    }
  },
  
  crusher: {
    tableName: 'CrusherPartial',
    sourceTable: 'Crusher',
    partialColumn: 'NoCrusherPartial',
    prefix: 'CR.',
    keys: ['NoCrusher'],
    weightColumn: 'Berat',
    weightSourceColumn: 'Berat',
    mappingTables: {
      brokerProduksi: 'BrokerProduksiInputCrusherPartial',
      gilinganProduksi: 'GilinganProduksiInputCrusherPartial',
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
    washing: {
      sourceTable: 'Washing_d',
      keys: ['NoWashing', 'NoSak'],
      mappingTable: 'CrusherProduksiInputWashing',
      dateUsageColumn: 'DateUsage',
    },
    crusher: {
      sourceTable: 'Crusher',
      keys: ['NoCrusher'],
      mappingTable: 'CrusherProduksiInputCrusher',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'CrusherProduksiInputGilingan',
      dateUsageColumn: 'DateUsage',
    },
    mixer: {
      sourceTable: 'Mixer_d',
      keys: ['NoMixer', 'NoSak'],
      mappingTable: 'CrusherProduksiInputMixer',
      dateUsageColumn: 'DateUsage',
    },
    reject: {
      sourceTable: 'RejectV2',
      keys: ['NoReject'],
      mappingTable: 'CrusherProduksiInputReject',
      dateUsageColumn: 'DateUsage',
    },
  },
  
  gilinganProduksi: {
    bb: {
      sourceTable: 'BahanBaku_d',
      keys: ['NoBahanBaku', 'NoPallet', 'NoSak'],
      mappingTable: 'GilinganProduksiInputBB',
      dateUsageColumn: 'DateUsage',
    },
    crusher: {
      sourceTable: 'Crusher',
      keys: ['NoCrusher'],
      mappingTable: 'GilinganProduksiInputCrusher',
      dateUsageColumn: 'DateUsage',
    },
    gilingan: {
      sourceTable: 'Gilingan',
      keys: ['NoGilingan'],
      mappingTable: 'GilinganProduksiInputGilingan',
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
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },
  
  gilinganProduksi: {
    headerTable: 'GilinganProduksi_h',
    entityKey: 'gilinganProduksi',
    lockResource: 'SEQ_GILINGAN_PARTIALS',
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },
  
  mixerProduksi: {
    headerTable: 'MixerProduksi_h',
    entityKey: 'mixerProduksi',
    lockResource: 'SEQ_MIXER_PARTIALS',
    dateColumn: 'TglProduksi',
    codeColumn: 'NoProduksi',
  },
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
};
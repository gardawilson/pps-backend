// src/core/utils/sql-generator/produksi-upsert-delete-sql.generator.js

/**
 * SQL Generator untuk DELETE UPSERT inputs
 * Simple DELETE tanpa perlu update parent (karena tidak ada DateUsage tracking)
 */

const {
  UPSERT_INPUT_CONFIGS,
  PRODUKSI_CONFIGS,
} = require("../config/produksi-input-mapping.config");

/** convert "IdCabinetMaterial" -> "idCabinetMaterial" */
function toCamelField(dbKey) {
  if (!dbKey) return dbKey;
  return dbKey.charAt(0).toLowerCase() + dbKey.slice(1);
}

/** json path for OPENJSON: IdCabinetMaterial -> $.idCabinetMaterial */
function jsonPath(dbKey) {
  return `$.${toCamelField(dbKey)}`;
}

/**
 * Generate complete SQL untuk DELETE UPSERT inputs
 * @param {string} produksiType - e.g., 'injectProduksi'
 * @param {string[]} requestedTypes - e.g., ['cabinetMaterial']
 */
function generateUpsertInputsDeleteSQL(produksiType, requestedTypes) {
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];
  const upsertConfigs = UPSERT_INPUT_CONFIGS[produksiType];

  if (!produksiConfig)
    throw new Error(`Unknown produksi type: ${produksiType}`);
  if (!upsertConfigs) return _generateEmptySQL();

  const activeConfigs = requestedTypes.reduce((acc, type) => {
    const config = upsertConfigs[type];
    if (config) acc[type] = config;
    return acc;
  }, {});

  if (Object.keys(activeConfigs).length === 0) return _generateEmptySQL();

  const sections = Object.entries(activeConfigs)
    .map(([type, config]) =>
      _generateSingleDeleteSection(type, config, produksiConfig),
    )
    .join("\n\n");

  const summaryInserts = Object.keys(activeConfigs)
    .map(
      (type) =>
        `  INSERT INTO @out SELECT '${type}', @${type}Deleted, @${type}NotFound;`,
    )
    .join("\n");

  return `
SET NOCOUNT ON;

DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

${sections}

${summaryInserts}

SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
`.trim();
}

/**
 * Generate SQL section untuk delete satu jenis UPSERT input
 */
function _generateSingleDeleteSection(type, config, produksiConfig) {
  const { mappingTable, keyColumn } = config;
  const keyCamel = toCamelField(keyColumn);

  return `
-- ============================================
-- ${type.toUpperCase()}
-- ============================================
DECLARE @${type}Deleted int = 0;
DECLARE @${type}NotFound int = 0;

-- Count records yang akan dihapus
SELECT @${type}Deleted = COUNT(*)
FROM dbo.${mappingTable} map
INNER JOIN OPENJSON(@jsInputs, '$.${type}')
  WITH (${keyCamel} int '${jsonPath(keyColumn)}') j
  ON map.${keyColumn} = j.${keyCamel}
WHERE map.${produksiConfig.codeColumn} = @no;

-- Delete records
DELETE map
FROM dbo.${mappingTable} map
INNER JOIN OPENJSON(@jsInputs, '$.${type}')
  WITH (${keyCamel} int '${jsonPath(keyColumn)}') j
  ON map.${keyColumn} = j.${keyCamel}
WHERE map.${produksiConfig.codeColumn} = @no;

-- Calculate not found
DECLARE @${type}Requested int;
SELECT @${type}Requested = COUNT(*) FROM OPENJSON(@jsInputs,'$.${type}');
SET @${type}NotFound = @${type}Requested - @${type}Deleted;
`.trim();
}

function _generateEmptySQL() {
  return `
SET NOCOUNT ON;
DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);
SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
`.trim();
}

module.exports = {
  generateUpsertInputsDeleteSQL,
};

// src/core/utils/sql-generator/produksi-upsert-sql.generator.js

/**
 * SQL Generator untuk UPSERT inputs (INSERT new + UPDATE existing)
 * Pattern ini digunakan untuk material yang bisa diakumulasi per key tanpa per-sak/batch
 * Contoh: Cabinet Material yang aggregate per IdCabinetMaterial
 */

const {
  UPSERT_INPUT_CONFIGS,
  PRODUKSI_CONFIGS
} = require('../config/produksi-input-mapping.config');

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
 * Generate complete SQL untuk UPSERT inputs
 * @param {string} produksiType - e.g., 'injectProduksi'
 * @param {string[]} requestedTypes - e.g., ['cabinetMaterial']
 */
function generateUpsertInputsSQL(produksiType, requestedTypes) {
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];
  const upsertConfigs = UPSERT_INPUT_CONFIGS[produksiType];

  if (!produksiConfig) throw new Error(`Unknown produksi type: ${produksiType}`);
  if (!upsertConfigs) return _generateEmptySQL();

  const activeConfigs = requestedTypes.reduce((acc, type) => {
    const config = upsertConfigs[type];
    if (config) acc[type] = config;
    return acc;
  }, {});

  if (Object.keys(activeConfigs).length === 0) return _generateEmptySQL();

  const sections = Object.entries(activeConfigs)
    .map(([type, config]) => _generateSingleUpsertSection(type, config, produksiConfig))
    .join('\n\n');

  const summaryInserts = Object.keys(activeConfigs)
    .map(type => `  INSERT INTO @out SELECT '${type}', @${type}Inserted, @${type}Updated, 0, @${type}Invalid;`)
    .join('\n');

  return `
SET NOCOUNT ON;

DECLARE @out TABLE(Section sysname, Inserted int, Updated int, Skipped int, Invalid int);

${sections}

${summaryInserts}

SELECT Section, Inserted, Updated, Skipped, Invalid FROM @out ORDER BY Section;
`.trim();
}

/**
 * Generate SQL section untuk satu jenis UPSERT input
 */
function _generateSingleUpsertSection(type, config, produksiConfig) {
  const {
    mappingTable,
    sourceTable,
    keyColumn,
    quantityColumn,
    validateColumn,
    validateValue,
  } = config;

  const keyCamel = toCamelField(keyColumn);
  const qtyCamel = toCamelField(quantityColumn);

  return `
-- ============================================
-- ${type.toUpperCase()} (UPSERT)
-- ============================================
DECLARE @${type}Inserted int = 0;
DECLARE @${type}Updated int = 0;
DECLARE @${type}Invalid int = 0;

-- Temp table untuk aggregated data (SUM by key)
DECLARE @${type}Src TABLE(${keyColumn} int, ${quantityColumn} int);

INSERT INTO @${type}Src(${keyColumn}, ${quantityColumn})
SELECT ${keyColumn}, SUM(ISNULL(${quantityColumn}, 0)) AS ${quantityColumn}
FROM OPENJSON(@jsInputs, '$.${type}')
WITH (
  ${keyColumn} int '${jsonPath(keyColumn)}',
  ${quantityColumn} int '${jsonPath(quantityColumn)}'
)
WHERE ${keyColumn} IS NOT NULL
GROUP BY ${keyColumn};

-- Count invalid: quantity <= 0 OR material not exists/disabled
SELECT @${type}Invalid = COUNT(*)
FROM @${type}Src s
WHERE s.${quantityColumn} <= 0
   OR NOT EXISTS (
     SELECT 1 FROM dbo.${sourceTable} m WITH (NOLOCK)
     WHERE m.${keyColumn} = s.${keyColumn}
       AND m.${validateColumn} = ${validateValue}
   );

-- UPDATE existing records
UPDATE tgt
SET tgt.${quantityColumn} = src.${quantityColumn}
FROM dbo.${mappingTable} tgt
INNER JOIN @${type}Src src ON src.${keyColumn} = tgt.${keyColumn}
WHERE tgt.${produksiConfig.codeColumn} = @no
  AND src.${quantityColumn} > 0
  AND EXISTS (
    SELECT 1 FROM dbo.${sourceTable} m WITH (NOLOCK)
    WHERE m.${keyColumn} = src.${keyColumn}
      AND m.${validateColumn} = ${validateValue}
  );

SET @${type}Updated = @@ROWCOUNT;

-- INSERT new records
INSERT INTO dbo.${mappingTable}(${produksiConfig.codeColumn}, ${keyColumn}, ${quantityColumn})
SELECT @no, src.${keyColumn}, src.${quantityColumn}
FROM @${type}Src src
WHERE src.${quantityColumn} > 0
  AND EXISTS (
    SELECT 1 FROM dbo.${sourceTable} m WITH (NOLOCK)
    WHERE m.${keyColumn} = src.${keyColumn}
      AND m.${validateColumn} = ${validateValue}
  )
  AND NOT EXISTS (
    SELECT 1 FROM dbo.${mappingTable} x WITH (NOLOCK)
    WHERE x.${produksiConfig.codeColumn} = @no
      AND x.${keyColumn} = src.${keyColumn}
  );

SET @${type}Inserted = @@ROWCOUNT;
`.trim();
}

function _generateEmptySQL() {
  return `
SET NOCOUNT ON;
DECLARE @out TABLE(Section sysname, Inserted int, Updated int, Skipped int, Invalid int);
SELECT Section, Inserted, Updated, Skipped, Invalid FROM @out ORDER BY Section;
`.trim();
}

module.exports = {
  generateUpsertInputsSQL,
};
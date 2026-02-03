// src/core/utils/sql-generator/produksi-delete-sql.generator.js

/**
 * SQL Generator untuk delete inputs & partials secara dinamis.
 * Menghasilkan SQL berdasarkan config yang ada di produksi-input-mapping.config.js
 */

const {
  INPUT_CONFIGS,
  PARTIAL_CONFIGS,
  PRODUKSI_CONFIGS,
} = require("../config/produksi-input-mapping.config");

/** convert "NoSak" -> "noSak", "NoBahanBaku" -> "noBahanBaku" */
function toCamelField(dbKey) {
  if (!dbKey) return dbKey;
  return dbKey.charAt(0).toLowerCase() + dbKey.slice(1);
}

/** json path for OPENJSON: NoSak -> $.noSak */
function jsonPath(dbKey) {
  return `$.${toCamelField(dbKey)}`;
}

/** choose sql type based on key name */
function sqlTypeForKey(dbKey) {
  const k = String(dbKey || "").toLowerCase();
  if (k === "nosak" || k === "nopallet") return "int";
  return "varchar(50)";
}

/**
 * Generate SQL untuk delete inputs (full materials)
 * @param {string} produksiType
 * @param {string[]} requestedTypes
 */
function generateInputsDeleteSQL(produksiType, requestedTypes) {
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];
  const inputConfigs = INPUT_CONFIGS[produksiType];

  if (!produksiConfig)
    throw new Error(`Unknown produksi type: ${produksiType}`);
  if (!inputConfigs)
    throw new Error(`No input configs found for ${produksiType}`);

  const activeConfigs = requestedTypes.reduce((acc, type) => {
    const config = inputConfigs[type];
    if (config) acc[type] = config;
    return acc;
  }, {});

  if (Object.keys(activeConfigs).length === 0) return _generateEmptySQL();

  const sections = Object.entries(activeConfigs)
    .map(([type, config]) =>
      _generateSingleInputDeleteSection(type, config, produksiConfig),
    )
    .join("\n\n");

  return `
SET NOCOUNT ON;

DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

${sections}

SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
`.trim();
}

/**
 * Generate SQL section untuk delete satu jenis input
 */
function _generateSingleInputDeleteSection(type, config, produksiConfig) {
  const varPrefix = type.toUpperCase();

  const keys = config.keys || [];
  const keyFieldsLower = keys.map(toCamelField).join(", ");

  // OPENJSON WITH clause
  const withClause = keys
    .map((k) => `${toCamelField(k)} ${sqlTypeForKey(k)} '${jsonPath(k)}'`)
    .join(", ");

  // JOIN conditions untuk mapping table
  const joinConditions = keys
    .map((k) => `map.${k} = j.${toCamelField(k)}`)
    .join(" AND ");

  // JOIN conditions untuk source table DateUsage reset
  const sourceJoinConditions = keys
    .map((k) => `d.${k} = map.${k}`)
    .join(" AND ");

  const dateUsageColumn = config.dateUsageColumn || "DateUsage";

  return `
-- ${varPrefix}
DECLARE @${type}Deleted int = 0, @${type}NotFound int = 0;

SELECT @${type}Deleted = COUNT(*)
FROM dbo.${config.mappingTable} map
INNER JOIN OPENJSON(@jsInputs, '$.${type}')
WITH (${withClause}) j
ON ${joinConditions}
WHERE map.${produksiConfig.codeColumn} = @no;

-- Reset ${dateUsageColumn} sebelum DELETE
IF @${type}Deleted > 0 AND '${dateUsageColumn}' <> ''
BEGIN
  UPDATE d
  SET d.${dateUsageColumn} = NULL
  FROM dbo.${config.sourceTable} d
  INNER JOIN dbo.${config.mappingTable} map
    ON ${sourceJoinConditions}
  INNER JOIN OPENJSON(@jsInputs, '$.${type}')
  WITH (${withClause}) j
  ON ${joinConditions}
  WHERE map.${produksiConfig.codeColumn} = @no;
END;

-- DELETE dari mapping table
DELETE map
FROM dbo.${config.mappingTable} map
INNER JOIN OPENJSON(@jsInputs, '$.${type}')
WITH (${withClause}) j
ON ${joinConditions}
WHERE map.${produksiConfig.codeColumn} = @no;

-- Calculate notFound
DECLARE @${type}Requested int;
SELECT @${type}Requested = COUNT(*)
FROM OPENJSON(@jsInputs, '$.${type}');

SET @${type}NotFound = @${type}Requested - @${type}Deleted;

INSERT INTO @out SELECT '${type}', @${type}Deleted, @${type}NotFound;
`.trim();
}

/**
 * Generate SQL untuk delete partials
 * @param {string} produksiType
 * @param {string[]} requestedTypes
 */
function generatePartialsDeleteSQL(produksiType, requestedTypes) {
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];
  if (!produksiConfig)
    throw new Error(`Unknown produksi type: ${produksiType}`);

  // Filter hanya partial types yang valid untuk produksiType ini
  const activeTypes = (requestedTypes || []).filter((type) => {
    const cfg = PARTIAL_CONFIGS[type];
    return !!(cfg && cfg.mappingTables && cfg.mappingTables[produksiType]);
  });

  if (activeTypes.length === 0) {
    return _generateEmptySQL();
  }

  const sections = activeTypes
    .map((type) =>
      _generateSinglePartialDeleteSection(
        type,
        PARTIAL_CONFIGS[type],
        produksiType,
      ),
    )
    .join("\n\n");

  return `
SET NOCOUNT ON;

DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

${sections}

SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
`.trim();
}

/**
 * Generate SQL section untuk delete satu jenis partial
 */
function _generateSinglePartialDeleteSection(type, config, produksiType) {
  const varName = type.charAt(0).toUpperCase() + type.slice(1);
  const mappingTable = config.mappingTables[produksiType];
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];

  const keys = config.keys || [];
  const keyFieldsDb = keys.join(", ");

  // WITH clause untuk partial column
  const partialWithClause = `${toCamelField(config.partialColumn)} varchar(50) '$.${toCamelField(config.partialColumn)}'`;

  // Temp table untuk track deleted source keys
  const tempTableDef = keys.map((k) => `${k} ${sqlTypeForKey(k)}`).join(", ");

  // JOIN conditions untuk deleted partials temp table
  const deletedJoinConditions = keys
    .map((k) => `bp.${k} = del.${k}`)
    .join(" AND ");

  // Source table UPDATE conditions
  const sourceUpdateConditions = keys
    .map((k) => `d.${k} = del.${k}`)
    .join(" AND ");

  // Partial EXISTS check
  const partialExistsConditions = keys
    .map((k) => `bp.${k} = d.${k}`)
    .join(" AND ");

  return `
-- ${type.toUpperCase()} PARTIAL
DECLARE @${type}Deleted int = 0, @${type}NotFound int = 0;

SELECT @${type}Deleted = COUNT(*)
FROM dbo.${mappingTable} map
INNER JOIN OPENJSON(@jsPartials, '$.${type}Partial')
WITH (${partialWithClause}) j
ON map.${config.partialColumn} = j.${toCamelField(config.partialColumn)}
WHERE map.${produksiConfig.codeColumn} = @no;

-- Track source keys dari partials yang akan dihapus
DECLARE @deleted${varName}Partials TABLE (${tempTableDef});

INSERT INTO @deleted${varName}Partials (${keyFieldsDb})
SELECT DISTINCT ${keyFieldsDb}
FROM dbo.${config.tableName} bp
INNER JOIN dbo.${mappingTable} map ON bp.${config.partialColumn} = map.${config.partialColumn}
INNER JOIN OPENJSON(@jsPartials, '$.${type}Partial')
WITH (${partialWithClause}) j
ON map.${config.partialColumn} = j.${toCamelField(config.partialColumn)}
WHERE map.${produksiConfig.codeColumn} = @no;

-- DELETE dari mapping table
DELETE map
FROM dbo.${mappingTable} map
INNER JOIN OPENJSON(@jsPartials, '$.${type}Partial')
WITH (${partialWithClause}) j
ON map.${config.partialColumn} = j.${toCamelField(config.partialColumn)}
WHERE map.${produksiConfig.codeColumn} = @no;

-- DELETE dari partial table
DELETE bp
FROM dbo.${config.tableName} bp
INNER JOIN OPENJSON(@jsPartials, '$.${type}Partial')
WITH (${partialWithClause}) j
ON bp.${config.partialColumn} = j.${toCamelField(config.partialColumn)};

-- Update source table IsPartial & DateUsage
IF @${type}Deleted > 0
BEGIN
  -- Masih ada partial lainnya
  UPDATE d
  SET 
    d.DateUsage = NULL,
    d.IsPartial = 1
  FROM dbo.${config.sourceTable} d
  INNER JOIN @deleted${varName}Partials del
    ON ${sourceUpdateConditions}
  WHERE EXISTS (
    SELECT 1
    FROM dbo.${config.tableName} bp
    WHERE ${partialExistsConditions}
  );
  
  -- Tidak ada partial lagi
  UPDATE d
  SET 
    d.DateUsage = NULL,
    d.IsPartial = 0
  FROM dbo.${config.sourceTable} d
  INNER JOIN @deleted${varName}Partials del
    ON ${sourceUpdateConditions}
  WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.${config.tableName} bp
    WHERE ${partialExistsConditions}
  );
END;

-- Calculate notFound
DECLARE @${type}Requested int;
SELECT @${type}Requested = COUNT(*)
FROM OPENJSON(@jsPartials, '$.${type}Partial');

SET @${type}NotFound = @${type}Requested - @${type}Deleted;

INSERT INTO @out SELECT '${type}Partial', @${type}Deleted, @${type}NotFound;
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
  generateInputsDeleteSQL,
  generatePartialsDeleteSQL,
};

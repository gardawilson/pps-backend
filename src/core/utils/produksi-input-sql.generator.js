// src/core/utils/sql-generator/produksi-input-sql.generator.js

/**
 * SQL Generator untuk attach inputs (full materials) secara dinamis.
 * Menghasilkan SQL berdasarkan config yang ada di produksi-input-mapping.config.js
 */

const {
  INPUT_CONFIGS,
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
 * Generate complete SQL untuk attach existing inputs
 * @param {string} produksiType
 * @param {string[]} requestedTypes
 */
function generateInputsAttachSQL(produksiType, requestedTypes) {
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
      _generateSingleInputSection(type, config, produksiConfig),
    )
    .join("\n\n");

  const summaryInserts = Object.keys(activeConfigs)
    .map(
      (type) =>
        `  INSERT INTO @out SELECT '${type}', @${type}Inserted, @${type}Skipped, @${type}Invalid;`,
    )
    .join("\n");

  return `
SET NOCOUNT ON;

-- Get ${produksiConfig.dateColumn} from header
DECLARE @tglProduksi datetime;
SELECT @tglProduksi = ${produksiConfig.dateColumn}
FROM dbo.${produksiConfig.headerTable} WITH (NOLOCK)
WHERE ${produksiConfig.codeColumn} = @no;

DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);

${sections}

${summaryInserts}

SELECT Section, Inserted, Skipped, Invalid FROM @out ORDER BY Section;
`.trim();
}

/**
 * Generate SQL section untuk satu jenis input
 */
function _generateSingleInputSection(type, config, produksiConfig) {
  const varPrefix = type.toUpperCase();

  const keys = config.keys || [];
  const keyFields = keys.join(", "); // DB columns
  const keyFieldsLower = keys.map(toCamelField).join(", "); // JSON alias fields (camelCase)

  // OPENJSON WITH clause: noBroker varchar(50) '$.noBroker', noSak int '$.noSak'
  const withClause = keys
    .map((k) => `${toCamelField(k)} ${sqlTypeForKey(k)} '${jsonPath(k)}'`)
    .join(", ");

  // EXISTS check to source table: b.NoBroker=j.noBroker AND b.NoSak=j.noSak
  const whereConditions = keys
    .map((k) => `d.${k} = j.${toCamelField(k)}`)
    .join(" AND ");

  // mapping check for NOT EXISTS
  const mappingConditions = keys
    .map((k) => `x.${k} = v.${toCamelField(k)}`)
    .join(" AND ");

  // skipped calculation
  const sourceComparison = keys
    .map((k) => `b.${k} = j.${toCamelField(k)}`)
    .join(" AND ");

  const mappingComparison = keys
    .map((k) => `x.${k} = j.${toCamelField(k)}`)
    .join(" AND ");

  // Update DateUsage WHERE
  const updateWhereComparison = keys
    .map((k) => `d.${k} = src.${toCamelField(k)}`)
    .join(" AND ");

  const dateUsageColumn = config.dateUsageColumn;

  return `
-- ${varPrefix}
DECLARE @${type}Inserted int = 0;
DECLARE @${type}Skipped int = 0;
DECLARE @${type}Invalid int = 0;

;WITH j AS (
  SELECT ${keyFieldsLower}
  FROM OPENJSON(@jsInputs, '$.${type}')
  WITH ( ${withClause} )
),
v AS (
  SELECT j.* FROM j
  WHERE EXISTS (SELECT 1 FROM dbo.${config.sourceTable} d WITH (NOLOCK) WHERE ${whereConditions})
)
INSERT INTO dbo.${config.mappingTable} (${produksiConfig.codeColumn}, ${keyFields})
SELECT @no, ${keyFieldsLower}
FROM v
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.${config.mappingTable} x
  WHERE x.${produksiConfig.codeColumn} = @no
    AND ${mappingConditions}
);

SET @${type}Inserted = @@ROWCOUNT;

-- Update DateUsage for ${config.sourceTable}
IF @${type}Inserted > 0 AND '${dateUsageColumn || ""}' <> ''
BEGIN
  UPDATE d
  SET d.${dateUsageColumn} = @tglProduksi
  FROM dbo.${config.sourceTable} d
  WHERE EXISTS (
    SELECT 1
    FROM OPENJSON(@jsInputs, '$.${type}')
    WITH ( ${withClause} ) src
    WHERE ${updateWhereComparison}
  );
END;

SELECT @${type}Skipped = COUNT(*)
FROM (
  SELECT ${keyFieldsLower}
  FROM OPENJSON(@jsInputs, '$.${type}')
  WITH ( ${withClause} )
) j
WHERE EXISTS (SELECT 1 FROM dbo.${config.sourceTable} b WITH (NOLOCK) WHERE ${sourceComparison})
  AND EXISTS (SELECT 1 FROM dbo.${config.mappingTable} x WHERE x.${produksiConfig.codeColumn}=@no AND ${mappingComparison});

SELECT @${type}Invalid = COUNT(*)
FROM (
  SELECT ${keyFieldsLower}
  FROM OPENJSON(@jsInputs, '$.${type}')
  WITH ( ${withClause} )
) j
WHERE NOT EXISTS (SELECT 1 FROM dbo.${config.sourceTable} b WITH (NOLOCK) WHERE ${sourceComparison});
`.trim();
}

function _generateEmptySQL() {
  return `
SET NOCOUNT ON;
DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);
SELECT Section, Inserted, Skipped, Invalid FROM @out ORDER BY Section;
`.trim();
}

module.exports = {
  generateInputsAttachSQL,
};

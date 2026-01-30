// src/core/utils/sql-generator/produksi-partial-sql.generator.js

/**
 * SQL Generator untuk insert partials secara dinamis.
 * Menghasilkan SQL berdasarkan config produksi-input-mapping.config.js
 */

const {
  PARTIAL_CONFIGS,
  PRODUKSI_CONFIGS,
  WEIGHT_TOLERANCE,
} = require('../config/produksi-input-mapping.config'); // ✅ perhatikan path

/** =========================
 *  Helpers
 *  ========================= */

/** convert "NoSak" -> "noSak", "NoBahanBaku" -> "noBahanBaku" */
function toCamelField(dbKey) {
  if (!dbKey) return dbKey;
  return dbKey.charAt(0).toLowerCase() + dbKey.slice(1);
}

function jsonPath(dbKey) {
  return `$.${toCamelField(dbKey)}`;
}

function sqlTypeForKey(dbKey) {
  const k = String(dbKey || '').toLowerCase();
  if (k === 'nosak' || k === 'nopallet') return 'int';
  return 'varchar(50)';
}

/** weight column => json field (Berat -> berat, Pcs -> pcs) */
function jsonFieldForWeight(config) {
  return toCamelField(config.weightColumn || 'Berat');
}

/** weight type: pcs -> int, lainnya -> decimal */
function sqlTypeForWeight(config) {
  const w = String(config.weightColumn || '').toLowerCase();
  if (w === 'pcs') return 'int';
  return 'decimal(18,3)';
}

/**
 * Mode input key partial:
 * - Kalau standar kamu: client kirim "xxxPartial" (TANPA PartialNew)
 * - Kalau legacy: "xxxPartialNew"
 *
 * ✅ Karena kamu bilang "tidak boleh ada PartialNew", default = false.
 */
const USE_PARTIAL_NEW_SUFFIX = false;

function requestKeyForPartial(type) {
  return USE_PARTIAL_NEW_SUFFIX ? `${type}PartialNew` : `${type}Partial`;
}

/**
 * Generate SQL insert partials untuk produksiType & requestedTypes.
 * Output recordsets:
 *  1) summary: Section, Created
 *  2) list kode per type: SELECT partialColumn FROM @<type>New
 */
function generatePartialsInsertSQL(produksiType, requestedTypes) {
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];
  if (!produksiConfig) throw new Error(`Unknown produksi type: ${produksiType}`);

  // aktifkan hanya partial type yang valid untuk produksiType ini
  const activeTypes = (requestedTypes || []).filter((type) => {
    const cfg = PARTIAL_CONFIGS[type];
    return !!(cfg && cfg.mappingTables && cfg.mappingTables[produksiType]);
  });

  if (activeTypes.length === 0) {
    return _generateEmptySQL();
  }

  const tempDecl = activeTypes
    .map((type) => {
      const cfg = PARTIAL_CONFIGS[type];
      return `DECLARE @${type}New TABLE(${cfg.partialColumn} varchar(50));`;
    })
    .join('\n');

  const sections = activeTypes
    .map((type) => _generateSinglePartialSection(type, PARTIAL_CONFIGS[type], produksiType))
    .join('\n\n');

  const summaryUnion = activeTypes
    .map((type) => {
      const sectionKey = requestKeyForPartial(type);
      return `SELECT '${sectionKey}' AS Section, COUNT(*) AS Created FROM @${type}New`;
    })
    .join('\nUNION ALL\n');

  const returnCodeSets = activeTypes
    .map((type) => {
      const cfg = PARTIAL_CONFIGS[type];
      return `SELECT ${cfg.partialColumn} FROM @${type}New;`;
    })
    .join('\n');

  return `
SET NOCOUNT ON;

-- Get ${produksiConfig.dateColumn} from header
DECLARE @tglProduksi datetime;
SELECT @tglProduksi = ${produksiConfig.dateColumn}
FROM dbo.${produksiConfig.headerTable} WITH (NOLOCK)
WHERE ${produksiConfig.codeColumn} = @no;

-- Global lock for sequence generation (10s timeout)
DECLARE @lockResult int;
EXEC @lockResult = sp_getapplock
  @Resource = '${produksiConfig.lockResource}',
  @LockMode = 'Exclusive',
  @LockTimeout = 10000,
  @DbPrincipal = 'public';

IF (@lockResult < 0)
BEGIN
  RAISERROR('Failed to acquire ${produksiConfig.lockResource} lock', 16, 1);
END;

${tempDecl}

${sections}

-- Release applock
EXEC sp_releaseapplock @Resource = '${produksiConfig.lockResource}', @DbPrincipal = 'public';

-- Summary (recordset #1)
${summaryUnion};

-- Codes per type (recordset #2..n)
${returnCodeSets}
`.trim();
}

/**
 * Generate SQL per partial type
 * - OPENJSON path sesuai standar (Partial atau PartialNew)
 * - field qty dinamis (berat / pcs) dari config.weightColumn
 */
function _generateSinglePartialSection(type, config, produksiType) {
  const varName = type.charAt(0).toUpperCase() + type.slice(1);
  const mappingTable = config.mappingTables[produksiType];
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];

  const requestKey = requestKeyForPartial(type); // ✅ "$.furnitureWipPartial" atau "$.furnitureWipPartialNew"

  const keys = config.keys || [];
  const keyFieldsDb = keys.join(', ');
  const keyFieldsJson = keys.map(toCamelField).join(', ');

  const withClause = keys
    .map((k) => `${toCamelField(k)} ${sqlTypeForKey(k)} '${jsonPath(k)}'`)
    .join(', ');

  const existingJoin = keys.map((k) => `ep.${k} = d.${k}`).join(' AND ');
  const newJoin = keys.map((k) => `np.${toCamelField(k)} = d.${k}`).join(' AND ');

  // ✅ qty mapping (Berat->$.berat decimal, Pcs->$.pcs int)
  const weightJsonField = jsonFieldForWeight(config);
  const weightSqlType = sqlTypeForWeight(config);

  return `
/* ===========================
   ${type.toUpperCase()} PARTIAL (${config.prefix}##########)
   =========================== */

IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.${requestKey}'))
BEGIN
  -- ambil nomor terakhir prefix ini (lock sudah dipegang global)
  DECLARE @next${varName} int = ISNULL((
    SELECT MAX(TRY_CAST(RIGHT(${config.partialColumn},10) AS int))
    FROM dbo.${config.tableName} WITH (UPDLOCK, HOLDLOCK)
    WHERE ${config.partialColumn} LIKE '${config.prefix}%'
  ), 0);

  ;WITH src AS (
    SELECT
      ${keyFieldsJson},
      qty,
      ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
    FROM OPENJSON(@jsPartials, '$.${requestKey}')
    WITH (
      ${withClause},
      qty ${weightSqlType} '$.${weightJsonField}'
    )
  ),
  numbered AS (
    SELECT
      NewNo = CONCAT('${config.prefix}', RIGHT(REPLICATE('0',10) + CAST(@next${varName} + rn AS varchar(10)), 10)),
      ${keyFieldsJson},
      qty
    FROM src
  )
  INSERT INTO dbo.${config.tableName} (${config.partialColumn}, ${keyFieldsDb}, ${config.weightColumn})
  OUTPUT INSERTED.${config.partialColumn} INTO @${type}New(${config.partialColumn})
  SELECT NewNo, ${keyFieldsJson}, qty
  FROM numbered;

  -- Map to produksi
  INSERT INTO dbo.${mappingTable} (${produksiConfig.codeColumn}, ${config.partialColumn})
  SELECT @no, n.${config.partialColumn}
  FROM @${type}New n;

  -- Update IsPartial & DateUsage for source table
  ;WITH existingPartials AS (
    SELECT
      ${keyFieldsDb},
      SUM(ISNULL(p.${config.weightColumn}, 0)) AS TotalQtyPartialExisting
    FROM dbo.${config.tableName} p WITH (NOLOCK)
    WHERE p.${config.partialColumn} NOT IN (SELECT ${config.partialColumn} FROM @${type}New)
    GROUP BY ${keyFieldsDb}
  ),
  newPartials AS (
    SELECT
      ${keyFieldsJson},
      SUM(qty) AS TotalQtyPartialNew
    FROM OPENJSON(@jsPartials, '$.${requestKey}')
    WITH (
      ${withClause},
      qty ${weightSqlType} '$.${weightJsonField}'
    )
    GROUP BY ${keyFieldsJson}
  )
  UPDATE d
  SET
    d.IsPartial = 1,
    d.DateUsage =
      CASE
        WHEN (${config.weightSourceColumn} - ISNULL(ep.TotalQtyPartialExisting, 0) - ISNULL(np.TotalQtyPartialNew, 0)) <= ${WEIGHT_TOLERANCE}
        THEN @tglProduksi
        ELSE d.DateUsage
      END
  FROM dbo.${config.sourceTable} d
  LEFT JOIN existingPartials ep
    ON ${existingJoin}
  INNER JOIN newPartials np
    ON ${newJoin};
END;
`.trim();
}

function _generateEmptySQL() {
  return `
SET NOCOUNT ON;

-- recordset summary kosong
SELECT CAST(NULL AS sysname) AS Section, CAST(0 AS int) AS Created WHERE 1=0;
`.trim();
}

module.exports = {
  generatePartialsInsertSQL,
};

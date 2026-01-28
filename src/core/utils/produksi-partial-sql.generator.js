// src/core/utils/sql-generator/produksi-partial-sql.generator.js

/**
 * SQL Generator untuk insert partials secara dinamis.
 * Menghasilkan SQL berdasarkan config produksi-input-mapping.config.js
 */

const {
  PARTIAL_CONFIGS,
  PRODUKSI_CONFIGS,
  WEIGHT_TOLERANCE,
} = require('../config/produksi-input-mapping.config');

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
    // tidak throw biar service bisa handle "no partial requested"
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
    .map((type) => `SELECT '${type}PartialNew' AS Section, COUNT(*) AS Created FROM @${type}New`)
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
 * Generate SQL per partial type (mirip format manual).
 * Penting: OPENJSON path pakai camelCase, bukan lowercase.
 */
function _generateSinglePartialSection(type, config, produksiType) {
  const varName = type.charAt(0).toUpperCase() + type.slice(1);
  const mappingTable = config.mappingTables[produksiType];
  const produksiConfig = PRODUKSI_CONFIGS[produksiType];

  const keys = config.keys || [];
  const keyFieldsDb = keys.join(', '); // DB columns e.g. NoBroker, NoSak
  const keyFieldsJson = keys.map(toCamelField).join(', '); // json alias e.g. noBroker, noSak

  // WITH clause OPENJSON
  // ex: noBroker varchar(50) '$.noBroker', noSak int '$.noSak'
  const withClause = keys
    .map((k) => `${toCamelField(k)} ${sqlTypeForKey(k)} '${jsonPath(k)}'`)
    .join(', ');

  // join existing partial sum to source table d.<key> = ep.<key>
  const existingJoin = keys.map((k) => `ep.${k} = d.${k}`).join(' AND ');

  // join new partial sum to source table d.<key> = np.<camelKey>
  const newJoin = keys.map((k) => `np.${toCamelField(k)} = d.${k}`).join(' AND ');

  return `
/* ===========================
   ${type.toUpperCase()} PARTIAL (${config.prefix}##########)
   =========================== */

IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.${type}PartialNew'))
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
      berat,
      ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
    FROM OPENJSON(@jsPartials, '$.${type}PartialNew')
    WITH (
      ${withClause},
      berat decimal(18,3) '$.berat'
    )
  ),
  numbered AS (
    SELECT
      NewNo = CONCAT('${config.prefix}', RIGHT(REPLICATE('0',10) + CAST(@next${varName} + rn AS varchar(10)), 10)),
      ${keyFieldsJson},
      berat
    FROM src
  )
  INSERT INTO dbo.${config.tableName} (${config.partialColumn}, ${keyFieldsDb}, ${config.weightColumn})
  OUTPUT INSERTED.${config.partialColumn} INTO @${type}New(${config.partialColumn})
  SELECT NewNo, ${keyFieldsJson}, berat
  FROM numbered;

  -- Map to produksi
  INSERT INTO dbo.${mappingTable} (${produksiConfig.codeColumn}, ${config.partialColumn})
  SELECT @no, n.${config.partialColumn}
  FROM @${type}New n;

  -- Update IsPartial & DateUsage for source table
  ;WITH existingPartials AS (
    SELECT
      ${keyFieldsDb},
      SUM(ISNULL(p.${config.weightColumn}, 0)) AS TotalBeratPartialExisting
    FROM dbo.${config.tableName} p WITH (NOLOCK)
    WHERE p.${config.partialColumn} NOT IN (SELECT ${config.partialColumn} FROM @${type}New)
    GROUP BY ${keyFieldsDb}
  ),
  newPartials AS (
    SELECT
      ${keyFieldsJson},
      SUM(berat) AS TotalBeratPartialNew
    FROM OPENJSON(@jsPartials, '$.${type}PartialNew')
    WITH (
      ${withClause},
      berat decimal(18,3) '$.berat'
    )
    GROUP BY ${keyFieldsJson}
  )
  UPDATE d
  SET
    d.IsPartial = 1,
    d.DateUsage =
      CASE
        WHEN (${config.weightSourceColumn} - ISNULL(ep.TotalBeratPartialExisting, 0) - ISNULL(np.TotalBeratPartialNew, 0)) <= ${WEIGHT_TOLERANCE}
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

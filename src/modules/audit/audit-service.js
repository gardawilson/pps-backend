// lib/services/audit/audit-service.js

const { sql, poolPromise } = require("../../core/config/db");
const { badReq } = require("../../core/utils/http-error");
const {
  MODULE_CONFIG,
  detectModuleFromPrefix,
  getModuleConfig,
} = require("../../core/config/audit-module-config");

function escapeSqlString(value) {
  return String(value ?? "").replace(/'/g, "''");
}

function logAuditQuery({ moduleKey, documentNo, query }) {
  const executableSql = [
    `DECLARE @DocumentNo VARCHAR(30) = '${escapeSqlString(documentNo)}';`,
    query,
  ].join("\n");

  console.log("[audit.getDocumentHistory] SQL START", {
    module: moduleKey,
    documentNo,
    params: { DocumentNo: documentNo },
  });
  console.log(executableSql);
}

/**
 * Get document history with auto-detection from prefix
 * PRODUCE/UNPRODUCE/ADJUST only enabled for modules that explicitly opt-in via config.supportsOutputMutation === true
 */
async function getDocumentHistory({ module, documentNo }) {
  const docNo = String(documentNo || "").trim();

  if (!docNo) {
    throw badReq("Document number wajib diisi");
  }

  let moduleKey = module ? String(module).toLowerCase().trim() : null;

  if (!moduleKey) {
    moduleKey = detectModuleFromPrefix(docNo);
    if (!moduleKey) {
      throw badReq(
        `Tidak dapat mendeteksi module dari prefix document number '${docNo}'. ` +
          `Format yang didukung: PREFIX.NOMOR (contoh: S.0000029967, B.0000013196)`,
      );
    }
  }

  const config = MODULE_CONFIG[moduleKey];
  if (!config) {
    throw badReq(
      `Module '${moduleKey}' tidak didukung. Available: ${Object.keys(MODULE_CONFIG).join(", ")}`,
    );
  }

  const supportsOutputMutation = config.supportsOutputMutation === true;
  const pool = await poolPromise;
  const rq = pool.request();
  rq.input("DocumentNo", sql.VarChar(30), docNo);

  const allTables = [
    config.headerTable,
    ...(config.detailTable ? [config.detailTable] : []),
    ...(config.outputTables || []),
    ...(config.inputTables || []),
  ].filter(Boolean);

  if (allTables.length === 0) {
    throw badReq(`Module '${moduleKey}' tidak punya table config.`);
  }

  const tableListSQL = allTables.map((t) => `'${t}'`).join(",");
  const hasInputs = config.inputTables && config.inputTables.length > 0;
  const hasOutputs = config.outputTables && config.outputTables.length > 0;
  const hasOutputDisplay =
    hasOutputs &&
    config.outputDisplayConfig &&
    Object.keys(config.outputDisplayConfig).length > 0;

  const inputTablesList = hasInputs
    ? config.inputTables.map((t) => `'${t}'`).join(",")
    : "";
  const outputTablesList = hasOutputs
    ? config.outputTables.map((t) => `'${t}'`).join(",")
    : "";

  const parsedHeaderColumns =
    config.headerParseFields && config.headerParseFields.length > 0
      ? config.headerParseFields
          .map(
            (f) => `,
    TRY_CONVERT(int, JSON_VALUE(sb.HeaderOld, '$.${f.jsonField}')) AS Old${f.jsonField},
    TRY_CONVERT(int, JSON_VALUE(COALESCE(sb.HeaderNew, sb.HeaderInserted), '$.${f.jsonField}')) AS New${f.jsonField}`,
          )
          .join("")
      : "";

  const selectParsedFields =
    config.headerParseFields && config.headerParseFields.length > 0
      ? config.headerParseFields
          .map(
            (f) => `,
  s.Old${f.jsonField},
  jpOld${f.jsonField}.${f.displayField} AS Old${f.alias},
  s.New${f.jsonField},
  jpNew${f.jsonField}.${f.displayField} AS New${f.alias}`,
          )
          .join("")
      : "";

  let joinParsedTables = "";
  if (config.headerParseFields && config.headerParseFields.length > 0) {
    config.headerParseFields.forEach((f) => {
      joinParsedTables += `
LEFT JOIN dbo.${f.joinTable} jpOld${f.jsonField} ON jpOld${f.jsonField}.${f.joinKey} = s.Old${f.jsonField}
LEFT JOIN dbo.${f.joinTable} jpNew${f.jsonField} ON jpNew${f.jsonField}.${f.joinKey} = s.New${f.jsonField}`;
    });
  }

  const scalarHeaderColumns =
    config.scalarFields && config.scalarFields.length > 0
      ? config.scalarFields
          .map((field) => {
            const isNumeric =
              /^Id/.test(field) ||
              ["Jam", "Shift", "JmlhAnggota", "Hadir", "HourMeter"].includes(
                field,
              );

            if (isNumeric) {
              return `,
    TRY_CONVERT(float, JSON_VALUE(sb.HeaderOld, '$.${field}')) AS Old${field},
    TRY_CONVERT(float, JSON_VALUE(COALESCE(sb.HeaderNew, sb.HeaderInserted), '$.${field}')) AS New${field}`;
            }

            return `,
    JSON_VALUE(sb.HeaderOld, '$.${field}') AS Old${field},
    JSON_VALUE(COALESCE(sb.HeaderNew, sb.HeaderInserted), '$.${field}') AS New${field}`;
          })
          .join("")
      : "";

  const selectScalarFields =
    config.scalarFields && config.scalarFields.length > 0
      ? config.scalarFields
          .map(
            (field) => `,
  s.Old${field},
  s.New${field}`,
          )
          .join("")
      : "";

  let statusHeaderColumns = "";
  let selectStatus = "";
  if (config.statusField && config.statusMapping) {
    const mapCasesOld = Object.entries(config.statusMapping)
      .map(
        ([key, val]) =>
          `WHEN JSON_VALUE(sb.HeaderOld, '$.${config.statusField}') = '${key}' THEN '${val}'`,
      )
      .join("\n      ");

    const mapCasesNew = Object.entries(config.statusMapping)
      .map(
        ([key, val]) =>
          `WHEN JSON_VALUE(COALESCE(sb.HeaderNew, sb.HeaderInserted), '$.${config.statusField}') = '${key}' THEN '${val}'`,
      )
      .join("\n      ");

    statusHeaderColumns = `,
    CASE
      ${mapCasesOld}
      ELSE ''
    END AS OldStatusText,
    CASE
      ${mapCasesNew}
      ELSE ''
    END AS NewStatusText`;

    selectStatus = `,
  s.OldStatusText,
  s.NewStatusText`;
  }

  const outputActionCase = supportsOutputMutation
    ? `
    WHEN s.HasProduce = 1 AND s.HasUnproduce = 1 THEN 'ADJUST'
    WHEN s.HasProduce = 1 THEN 'PRODUCE'
    WHEN s.HasUnproduce = 1 THEN 'UNPRODUCE'`
    : "";

  const detailTableName = config.detailTable
    ? `'${config.detailTable}'`
    : "NULL";

  const detailSessionColumns = config.detailTable
    ? `,
    MAX(CASE WHEN d.TableName='${config.detailTable}' AND d.Action IN ('DELETE','UPDATE') THEN d.OldData END) AS DetailsOldJson,
    MAX(CASE WHEN d.TableName='${config.detailTable}' AND d.Action IN ('INSERT','UPDATE') THEN d.NewData END) AS DetailsNewJson`
    : `,
    CAST(NULL AS nvarchar(max)) AS DetailsOldJson,
    CAST(NULL AS nvarchar(max)) AS DetailsNewJson`;

  const consumeApply = hasInputs
    ? `OUTER APPLY (
  SELECT
    CASE
      WHEN s.HasConsume = 1 THEN (
        SELECT d.TableName, d.Action, d.NewData
        FROM #Doc d
        WHERE d.SessionKey = s.SessionKey
          AND d.TableName IN (${inputTablesList})
          AND d.Action IN ('CONSUME_FULL','CONSUME_PARTIAL')
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS ConsumeJson,
    CASE
      WHEN s.HasUnconsume = 1 THEN (
        SELECT d.TableName, d.Action, d.OldData
        FROM #Doc d
        WHERE d.SessionKey = s.SessionKey
          AND d.TableName IN (${inputTablesList})
          AND d.Action IN ('UNCONSUME_FULL','UNCONSUME_PARTIAL')
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS UnconsumeJson
) c`
    : "";

  const selectConsume = hasInputs
    ? "c.ConsumeJson, c.UnconsumeJson"
    : "NULL AS ConsumeJson, NULL AS UnconsumeJson";

  const produceApply = hasOutputs
    ? `OUTER APPLY (
  SELECT
    CASE
      WHEN s.HasProduce = 1 THEN (
        SELECT d.TableName, d.Action, d.NewData
        FROM #Doc d
        WHERE d.SessionKey = s.SessionKey
          AND d.TableName IN (${outputTablesList})
          AND d.Action = 'PRODUCE'
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS ProduceJson,
    CASE
      WHEN s.HasUnproduce = 1 THEN (
        SELECT d.TableName, d.Action, d.OldData
        FROM #Doc d
        WHERE d.SessionKey = s.SessionKey
          AND d.TableName IN (${outputTablesList})
          AND d.Action = 'UNPRODUCE'
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS UnproduceJson
) p`
    : "";

  const selectProduce = hasOutputs
    ? "p.ProduceJson, p.UnproduceJson"
    : "NULL AS ProduceJson, NULL AS UnproduceJson";

  const outputApply = hasOutputDisplay
    ? `OUTER APPLY (
  SELECT (
    SELECT TOP 1
      d.TableName,
      CASE d.TableName
        ${Object.entries(config.outputDisplayConfig)
          .map(([tableName, cfg]) => `WHEN '${tableName}' THEN '${cfg.label}'`)
          .join("\n        ")}
        ELSE d.TableName
      END AS DisplayLabel,
      CASE d.TableName
        ${Object.entries(config.outputDisplayConfig)
          .map(
            ([tableName, cfg]) => `WHEN '${tableName}' THEN COALESCE(
          JSON_VALUE(d.NewData, '$.${cfg.displayField}'),
          JSON_VALUE(d.OldData, '$.${cfg.displayField}')
        )`,
          )
          .join("\n        ")}
        ELSE NULL
      END AS DisplayValue,
      d.Action
    FROM #Doc d
    WHERE d.SessionKey = s.SessionKey
      AND d.TableName IN (${outputTablesList})
      AND (d.OldData IS NOT NULL OR d.NewData IS NOT NULL)
    ORDER BY d.EventTime DESC
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
  ) AS OutputChanges
) o`
    : "";

  const selectOutput = hasOutputDisplay
    ? "o.OutputChanges"
    : "NULL AS OutputChanges";

  const query = `
IF OBJECT_ID('tempdb..#Doc') IS NOT NULL DROP TABLE #Doc;

SELECT
  a.AuditId,
  a.EventTime,
  a.Actor,
  a.RequestId,
  COALESCE(a.RequestId, CONCAT('AUDIT-', a.AuditId)) AS SessionKey,
  a.Action,
  a.TableName,
  JSON_VALUE(a.PK, '$.${config.pkField}') AS DocumentNo,
  a.OldData,
  a.NewData
INTO #Doc
FROM dbo.AuditTrail a
WHERE a.TableName IN (${tableListSQL})
  AND JSON_VALUE(a.PK, '$.${config.pkField}') = @DocumentNo;

CREATE CLUSTERED INDEX IX_Doc_SessionKey_EventTime
ON #Doc (SessionKey, EventTime);

CREATE NONCLUSTERED INDEX IX_Doc_SessionKey_Table_Action
ON #Doc (SessionKey, TableName, Action)
INCLUDE (EventTime, RequestId, Actor, OldData, NewData);

;WITH sessionBase AS (
  SELECT
    d.SessionKey,
    MAX(d.RequestId) AS RequestId,
    MIN(d.EventTime) AS StartTime,
    MAX(d.EventTime) AS EndTime,
    MAX(d.Actor) AS Actor,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='INSERT' THEN 1 ELSE 0 END) AS HasCreate,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='DELETE' THEN 1 ELSE 0 END) AS HasDelete,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='UPDATE' THEN 1 ELSE 0 END) AS HasHeaderUpdate,
    MAX(CASE WHEN ${detailTableName} IS NOT NULL AND d.TableName='${config.detailTable || ""}' AND d.Action IN ('INSERT','DELETE','UPDATE') THEN 1 ELSE 0 END) AS HasDetailChange,
    MAX(CASE WHEN d.Action IN ('CONSUME_FULL','CONSUME_PARTIAL') THEN 1 ELSE 0 END) AS HasConsume,
    MAX(CASE WHEN d.Action IN ('UNCONSUME_FULL','UNCONSUME_PARTIAL') THEN 1 ELSE 0 END) AS HasUnconsume,
    MAX(CASE WHEN d.Action = 'PRODUCE' THEN 1 ELSE 0 END) AS HasProduce,
    MAX(CASE WHEN d.Action = 'UNPRODUCE' THEN 1 ELSE 0 END) AS HasUnproduce,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='INSERT' THEN d.NewData END) AS HeaderInserted,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='UPDATE' THEN d.OldData END) AS HeaderOld,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='UPDATE' THEN d.NewData END) AS HeaderNew,
    MAX(CASE WHEN d.TableName='${config.headerTable}' AND d.Action='DELETE' THEN d.OldData END) AS HeaderDeleted
    ${detailSessionColumns}
  FROM #Doc d
  GROUP BY d.SessionKey
),
headerParsed AS (
  SELECT
    sb.*
    ${parsedHeaderColumns}
    ${scalarHeaderColumns}
    ${statusHeaderColumns}
  FROM sessionBase sb
)
SELECT
  s.StartTime,
  s.EndTime,
  u.Username AS Actor,
  s.RequestId,
  s.SessionKey,
  @DocumentNo AS DocumentNo,
  CASE
    WHEN s.HasCreate = 1 THEN 'CREATE'
    WHEN s.HasDelete = 1 THEN 'DELETE'
    ${outputActionCase}
    WHEN s.HasHeaderUpdate = 1 OR s.HasDetailChange = 1 THEN 'UPDATE'
    WHEN s.HasConsume = 1 THEN 'CONSUME'
    WHEN s.HasUnconsume = 1 THEN 'UNCONSUME'
    ELSE 'UPDATE'
  END AS SessionAction
  ${selectParsedFields}
  ${selectScalarFields}
  ${selectStatus}
  ,
  s.HeaderInserted,
  s.HeaderOld,
  s.HeaderNew,
  s.HeaderDeleted,
  s.DetailsOldJson,
  s.DetailsNewJson,
  ${selectConsume},
  ${selectProduce},
  ${selectOutput}
FROM headerParsed s
LEFT JOIN dbo.MstUsername u ON u.IdUsername = TRY_CONVERT(int, s.Actor)
${joinParsedTables}
${consumeApply}
${produceApply}
${outputApply}
ORDER BY s.StartTime;

DROP TABLE #Doc;`;

  logAuditQuery({
    moduleKey,
    documentNo: docNo,
    query,
  });

  const startedAt = Date.now();
  const rs = await rq.query(query);
  console.log("[audit.getDocumentHistory] SQL END", {
    module: moduleKey,
    documentNo: docNo,
    durationMs: Date.now() - startedAt,
    rows: rs.recordset?.length || 0,
  });

  return {
    module: moduleKey,
    documentNo: docNo,
    prefix: config.prefix || null,
    pkField: config.pkField,
    sessions: rs.recordset || [],
  };
}

module.exports = {
  getDocumentHistory,
  MODULE_CONFIG,
  detectModuleFromPrefix,
  getModuleConfig,
};

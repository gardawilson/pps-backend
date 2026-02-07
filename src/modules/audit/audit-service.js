// lib/services/audit/audit-service.js

const { sql, poolPromise } = require("../../core/config/db");
const { badReq } = require("../../core/utils/http-error");
const {
  MODULE_CONFIG,
  detectModuleFromPrefix,
  getModuleConfig,
} = require("../../core/config/audit-module-config");

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

  let cteParsing = "";
  let selectParsedFields = "";
  let joinParsedTables = "";

  if (config.headerParseFields && config.headerParseFields.length > 0) {
    cteParsing += `
,hdrParsedOld AS (
  SELECT
    h.SessionKey,
    ${config.headerParseFields
      .map(
        (f) =>
          `TRY_CONVERT(int, JSON_VALUE(h.HeaderOld, '$.${f.jsonField}')) AS Old${f.jsonField}`,
      )
      .join(",\n    ")}
  FROM hdr h
  WHERE h.HeaderOld IS NOT NULL
)`;

    cteParsing += `
,hdrParsedNew AS (
  SELECT
    h.SessionKey,
    ${config.headerParseFields
      .map(
        (f) =>
          `TRY_CONVERT(int, JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${f.jsonField}')) AS New${f.jsonField}`,
      )
      .join(",\n    ")}
  FROM hdr h
  WHERE COALESCE(h.HeaderNew, h.HeaderInserted) IS NOT NULL
)`;

    selectParsedFields = config.headerParseFields
      .map(
        (f) => `,
  ho.Old${f.jsonField},
  jpOld${f.jsonField}.${f.displayField} AS Old${f.alias},
  hn.New${f.jsonField},
  jpNew${f.jsonField}.${f.displayField} AS New${f.alias}`,
      )
      .join("");

    joinParsedTables = `
LEFT JOIN hdrParsedOld ho ON ho.SessionKey = s.SessionKey
LEFT JOIN hdrParsedNew hn ON hn.SessionKey = s.SessionKey`;

    config.headerParseFields.forEach((f) => {
      joinParsedTables += `
LEFT JOIN dbo.${f.joinTable} jpOld${f.jsonField} ON jpOld${f.jsonField}.${f.joinKey} = ho.Old${f.jsonField}
LEFT JOIN dbo.${f.joinTable} jpNew${f.jsonField} ON jpNew${f.jsonField}.${f.joinKey} = hn.New${f.jsonField}`;
    });
  }

  let selectScalarFields = "";
  if (config.scalarFields && config.scalarFields.length > 0) {
    selectScalarFields = config.scalarFields
      .map((field) => {
        const isNumeric =
          /^Id/.test(field) ||
          ["Jam", "Shift", "JmlhAnggota", "Hadir", "HourMeter"].includes(field);

        if (isNumeric) {
          return `,
TRY_CONVERT(float, JSON_VALUE(h.HeaderOld, '$.${field}')) AS Old${field},
TRY_CONVERT(float, JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${field}')) AS New${field}`;
        }

        return `,
JSON_VALUE(h.HeaderOld, '$.${field}') AS Old${field},
JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${field}') AS New${field}`;
      })
      .join("");
  }

  let selectStatus = "";
  if (config.statusField && config.statusMapping) {
    const mapCasesOld = Object.entries(config.statusMapping)
      .map(
        ([key, val]) =>
          `WHEN JSON_VALUE(h.HeaderOld, '$.${config.statusField}') = '${key}' THEN '${val}'`,
      )
      .join("\n      ");

    const mapCasesNew = Object.entries(config.statusMapping)
      .map(
        ([key, val]) =>
          `WHEN JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${config.statusField}') = '${key}' THEN '${val}'`,
      )
      .join("\n      ");

    selectStatus = `,
  CASE
      ${mapCasesOld}
      ELSE ''
  END AS OldStatusText,
  CASE
      ${mapCasesNew}
      ELSE ''
  END AS NewStatusText`;
  }

  let detailCTE = "";
  if (config.detailTable) {
    detailCTE = `
,det AS (
  SELECT
    SessionKey,
    MAX(CASE WHEN TableName='${config.detailTable}' AND Action IN ('DELETE','UPDATE') THEN OldData END) AS DetailsOldJson,
    MAX(CASE WHEN TableName='${config.detailTable}' AND Action IN ('INSERT','UPDATE') THEN NewData END) AS DetailsNewJson
  FROM doc
  GROUP BY SessionKey
)`;
  }

  let consumeCTE = "";
  const hasInputs = config.inputTables && config.inputTables.length > 0;
  if (hasInputs) {
    const inputTablesList = config.inputTables.map((t) => `'${t}'`).join(",");

    consumeCTE = `
,cons AS (
  SELECT
    SessionKey,
    CASE
      WHEN COUNT(CASE WHEN Action IN ('CONSUME_FULL','CONSUME_PARTIAL') THEN 1 END) > 0
      THEN (
        SELECT d.TableName, d.Action, d.NewData
        FROM doc d
        WHERE d.SessionKey = doc.SessionKey
          AND d.TableName IN (${inputTablesList})
          AND d.Action IN ('CONSUME_FULL','CONSUME_PARTIAL')
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS ConsumeJson,
    CASE
      WHEN COUNT(CASE WHEN Action IN ('UNCONSUME_FULL','UNCONSUME_PARTIAL') THEN 1 END) > 0
      THEN (
        SELECT d.TableName, d.Action, d.OldData
        FROM doc d
        WHERE d.SessionKey = doc.SessionKey
          AND d.TableName IN (${inputTablesList})
          AND d.Action IN ('UNCONSUME_FULL','UNCONSUME_PARTIAL')
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS UnconsumeJson
  FROM doc
  GROUP BY SessionKey
)`;
  }

  let produceCTE = "";
  const hasOutputs = config.outputTables && config.outputTables.length > 0;

  if (hasOutputs) {
    const outputTablesList = config.outputTables.map((t) => `'${t}'`).join(",");

    produceCTE = `
,prod AS (
  SELECT
    SessionKey,
    CASE
      WHEN COUNT(CASE WHEN Action = 'PRODUCE' THEN 1 END) > 0
      THEN (
        SELECT d.TableName, d.Action, d.NewData
        FROM doc d
        WHERE d.SessionKey = doc.SessionKey
          AND d.TableName IN (${outputTablesList})
          AND d.Action = 'PRODUCE'
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS ProduceJson,
    CASE
      WHEN COUNT(CASE WHEN Action = 'UNPRODUCE' THEN 1 END) > 0
      THEN (
        SELECT d.TableName, d.Action, d.OldData
        FROM doc d
        WHERE d.SessionKey = doc.SessionKey
          AND d.TableName IN (${outputTablesList})
          AND d.Action = 'UNPRODUCE'
        ORDER BY d.EventTime
        FOR JSON PATH
      )
      ELSE NULL
    END AS UnproduceJson
  FROM doc
  GROUP BY SessionKey
)`;
  }

  let outputCTE = "";
  const hasOutputDisplay =
    hasOutputs &&
    config.outputDisplayConfig &&
    Object.keys(config.outputDisplayConfig).length > 0;

  if (hasOutputDisplay) {
    const outputTablesList = config.outputTables.map((t) => `'${t}'`).join(",");

    outputCTE = `
,outputs AS (
  SELECT
    SessionKey,
    (
      SELECT TOP 1
        d.TableName,
        CASE d.TableName
          ${Object.entries(config.outputDisplayConfig)
            .map(
              ([tableName, cfg]) => `WHEN '${tableName}' THEN '${cfg.label}'`,
            )
            .join("\n          ")}
          ELSE d.TableName
        END AS DisplayLabel,
        CASE d.TableName
          ${Object.entries(config.outputDisplayConfig)
            .map(
              ([tableName, cfg]) => `WHEN '${tableName}' THEN
              COALESCE(
                JSON_VALUE(d.NewData, '$.${cfg.displayField}'),
                JSON_VALUE(d.OldData, '$.${cfg.displayField}')
              )`,
            )
            .join("\n          ")}
          ELSE NULL
        END AS DisplayValue,
        d.Action
      FROM doc d
      WHERE d.SessionKey = doc.SessionKey
        AND d.TableName IN (${outputTablesList})
        AND (d.OldData IS NOT NULL OR d.NewData IS NOT NULL)
      ORDER BY d.EventTime DESC
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS OutputChanges
  FROM doc
  GROUP BY SessionKey
)`;
  }

  const selectOutput = hasOutputDisplay
    ? "o.OutputChanges"
    : "NULL AS OutputChanges";
  const joinOutput = hasOutputDisplay
    ? "LEFT JOIN outputs o ON o.SessionKey = s.SessionKey"
    : "";

  const outputActionCase = supportsOutputMutation
    ? `
  WHEN s.HasProduce = 1 AND s.HasUnproduce = 1 THEN 'ADJUST'
  WHEN s.HasProduce = 1 THEN 'PRODUCE'
  WHEN s.HasUnproduce = 1 THEN 'UNPRODUCE'`
    : "";

  const detailTableName = config.detailTable
    ? `'${config.detailTable}'`
    : "NULL";

  const query = `
;WITH doc AS (
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
  FROM dbo.AuditTrail a
  WHERE a.TableName IN (${tableListSQL})
    AND EXISTS (
      SELECT 1
      FROM OPENJSON(a.PK)
      WHERE value = @DocumentNo
    )
)
,sessionAgg AS (
  SELECT
    SessionKey,
    MAX(RequestId) AS RequestId,
    MIN(EventTime) AS StartTime,
    MAX(EventTime) AS EndTime,
    MAX(Actor) AS Actor,

    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='INSERT' THEN 1 ELSE 0 END) AS HasCreate,
    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='DELETE' THEN 1 ELSE 0 END) AS HasDelete,

    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='UPDATE' THEN 1 ELSE 0 END) AS HasHeaderUpdate,
    MAX(CASE WHEN ${detailTableName} IS NOT NULL AND TableName='${config.detailTable || ""}' AND Action IN ('INSERT','DELETE','UPDATE') THEN 1 ELSE 0 END) AS HasDetailChange,

    MAX(CASE WHEN Action IN ('CONSUME_FULL','CONSUME_PARTIAL') THEN 1 ELSE 0 END) AS HasConsume,
    MAX(CASE WHEN Action IN ('UNCONSUME_FULL','UNCONSUME_PARTIAL') THEN 1 ELSE 0 END) AS HasUnconsume,

    MAX(CASE WHEN Action = 'PRODUCE' THEN 1 ELSE 0 END) AS HasProduce,
    MAX(CASE WHEN Action = 'UNPRODUCE' THEN 1 ELSE 0 END) AS HasUnproduce
  FROM doc
  GROUP BY SessionKey
)
,hdr AS (
  SELECT
    SessionKey,
    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='INSERT' THEN NewData END) AS HeaderInserted,
    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='UPDATE' THEN OldData END) AS HeaderOld,
    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='UPDATE' THEN NewData END) AS HeaderNew,
    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='DELETE' THEN OldData END) AS HeaderDeleted
  FROM doc
  GROUP BY SessionKey
)
${cteParsing}
${detailCTE}
${consumeCTE}
${produceCTE}
${outputCTE}

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
  ${selectStatus},

  h.HeaderInserted,
  h.HeaderOld,
  h.HeaderNew,
  h.HeaderDeleted,

  ${config.detailTable ? "d.DetailsOldJson, d.DetailsNewJson" : "NULL AS DetailsOldJson, NULL AS DetailsNewJson"},

  ${hasInputs ? "c.ConsumeJson, c.UnconsumeJson" : "NULL AS ConsumeJson, NULL AS UnconsumeJson"},

  ${hasOutputs ? "p.ProduceJson, p.UnproduceJson" : "NULL AS ProduceJson, NULL AS UnproduceJson"},

  ${selectOutput}

FROM sessionAgg s
LEFT JOIN hdr h ON h.SessionKey = s.SessionKey
LEFT JOIN dbo.MstUsername u ON u.IdUsername = TRY_CONVERT(int, s.Actor)
${joinParsedTables}
${config.detailTable ? "LEFT JOIN det d ON d.SessionKey = s.SessionKey" : ""}
${hasInputs ? "LEFT JOIN cons c ON c.SessionKey = s.SessionKey" : ""}
${hasOutputs ? "LEFT JOIN prod p ON p.SessionKey = s.SessionKey" : ""}
${joinOutput}
ORDER BY s.StartTime;`;

  const rs = await rq.query(query);

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

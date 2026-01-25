// lib/services/audit/audit-service.js

const { sql, poolPromise } = require('../../core/config/db');
const { badReq } = require('../../core/utils/http-error'); 
const { MODULE_CONFIG } = require('../../core/config/audit-module-config');

async function getDocumentHistory({ module, documentNo }) {
  const moduleKey = String(module || '').toLowerCase().trim();
  const docNo = String(documentNo || '').trim();

  const config = MODULE_CONFIG[moduleKey];
  if (!config) {
    throw badReq(`Module '${module}' tidak didukung. Available: ${Object.keys(MODULE_CONFIG).join(', ')}`);
  }

  if (!docNo) {
    throw badReq(`${config.pkField} wajib diisi`);
  }

  const pool = await poolPromise;
  const rq = pool.request();
  rq.input('DocumentNo', sql.VarChar(30), docNo);

  // Build table list (skip null detailTable)
  const allTables = [
    config.headerTable,
    ...(config.detailTable ? [config.detailTable] : []),
    ...config.outputTables
  ];
  const tableListSQL = allTables.map(t => `'${t}'`).join(',');

  // =============================
  // Build Relational Fields Parsing
  // =============================
  let cteParsing = '';
  let selectParsedFields = '';
  let joinParsedTables = '';

  if (config.headerParseFields && config.headerParseFields.length > 0) {
    cteParsing += `
,hdrParsedOld AS (
  SELECT
    h.SessionKey,
    ${config.headerParseFields.map(f => 
      `TRY_CONVERT(int, JSON_VALUE(h.HeaderOld, '$.${f.jsonField}')) AS Old${f.jsonField}`
    ).join(',\n    ')}
  FROM hdr h
  WHERE h.HeaderOld IS NOT NULL
)`;

    cteParsing += `
,hdrParsedNew AS (
  SELECT
    h.SessionKey,
    ${config.headerParseFields.map(f => 
      `TRY_CONVERT(int, JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${f.jsonField}')) AS New${f.jsonField}`
    ).join(',\n    ')}
  FROM hdr h
  WHERE COALESCE(h.HeaderNew, h.HeaderInserted) IS NOT NULL
)`;

    selectParsedFields = config.headerParseFields.map(f => `,
  ho.Old${f.jsonField},
  jpOld${f.jsonField}.${f.displayField} AS Old${f.alias},
  hn.New${f.jsonField},
  jpNew${f.jsonField}.${f.displayField} AS New${f.alias}`).join('');

    joinParsedTables = `
LEFT JOIN hdrParsedOld ho ON ho.SessionKey = s.SessionKey
LEFT JOIN hdrParsedNew hn ON hn.SessionKey = s.SessionKey`;

    // ✅ FIX: Gunakan f.jsonField di kedua tempat
    config.headerParseFields.forEach(f => {
      joinParsedTables += `
LEFT JOIN dbo.${f.joinTable} jpOld${f.jsonField} ON jpOld${f.jsonField}.${f.joinKey} = ho.Old${f.jsonField}
LEFT JOIN dbo.${f.joinTable} jpNew${f.jsonField} ON jpNew${f.jsonField}.${f.joinKey} = hn.New${f.jsonField}`;
    });
  }

  // =============================
  // Build Scalar Fields Parsing
  // =============================
  let selectScalarFields = '';
  if (config.scalarFields && config.scalarFields.length > 0) {
    selectScalarFields = config.scalarFields.map(field => `,
  JSON_VALUE(h.HeaderOld, '$.${field}') AS Old${field},
  JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${field}') AS New${field}`).join('');
  }

  // =============================
  // Build Status Parsing
  // =============================
  let selectStatus = '';
  if (config.statusField && config.statusMapping) {
    const mapCases = Object.entries(config.statusMapping)
      .map(([key, val]) => `WHEN JSON_VALUE(h.HeaderOld, '$.${config.statusField}') = '${key}' THEN '${val}'`)
      .join('\n      ');

    const mapCasesNew = Object.entries(config.statusMapping)
      .map(([key, val]) => `WHEN JSON_VALUE(COALESCE(h.HeaderNew, h.HeaderInserted), '$.${config.statusField}') = '${key}' THEN '${val}'`)
      .join('\n      ');

    selectStatus = `,
  CASE
    ${mapCases}
    ELSE ''
  END AS OldStatusText,
  CASE
    ${mapCasesNew}
    ELSE ''
  END AS NewStatusText`;
  }

  // =============================
  // Build Detail CTE
  // =============================
  let detailCTE = '';
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

  // =============================
  // ✅ Single Aggregated Output CTE
  // =============================

 let outputCTE = '';
  
  if (config.outputTables && config.outputTables.length > 0 && config.outputDisplayConfig) {
    const outputTablesList = config.outputTables.map(t => `'${t}'`).join(',');
    
    outputCTE = `
,outputs AS (
  SELECT
    SessionKey,
    (
      SELECT TOP 1
        d.TableName,
        CASE d.TableName
          ${Object.entries(config.outputDisplayConfig).map(([tableName, cfg]) => 
            `WHEN '${tableName}' THEN '${cfg.label}'`
          ).join('\n          ')}
          ELSE d.TableName
        END AS DisplayLabel,
        CASE d.TableName
          ${Object.entries(config.outputDisplayConfig).map(([tableName, cfg]) => 
            `WHEN '${tableName}' THEN 
              COALESCE(
                JSON_VALUE(d.NewData, '$[0].${cfg.displayField}'),
                JSON_VALUE(d.OldData, '$[0].${cfg.displayField}')
              )`
          ).join('\n          ')}
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

  // =============================
  // Build Final Query
  // =============================
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
    AND JSON_VALUE(a.PK, '$.${config.pkField}') = @DocumentNo
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
    MAX(CASE WHEN TableName='${config.headerTable}' AND Action='UPDATE' THEN 1 ELSE 0 END) AS HasHeaderUpdate
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
${outputCTE}

SELECT
  s.StartTime,
  s.EndTime,
  s.Actor,
  s.RequestId,
  s.SessionKey,
  @DocumentNo AS DocumentNo,
  
  CASE
    WHEN s.HasCreate = 1 THEN 'CREATE'
    WHEN s.HasDelete = 1 THEN 'DELETE'
    ELSE 'UPDATE'
  END AS SessionAction
  
  ${selectParsedFields}
  ${selectScalarFields}
  ${selectStatus},

  h.HeaderInserted,
  h.HeaderOld,
  h.HeaderNew,
  h.HeaderDeleted,
  
  ${config.detailTable ? 'd.DetailsOldJson, d.DetailsNewJson' : 'NULL AS DetailsOldJson, NULL AS DetailsNewJson'},
  
  o.OutputChanges

FROM sessionAgg s
LEFT JOIN hdr h ON h.SessionKey = s.SessionKey
${joinParsedTables}
${config.detailTable ? 'LEFT JOIN det d ON d.SessionKey = s.SessionKey' : ''}
LEFT JOIN outputs o ON o.SessionKey = s.SessionKey
ORDER BY s.StartTime;
  `;

  const rs = await rq.query(query);
  
  return {
    module: moduleKey,
    documentNo: docNo,
    pkField: config.pkField,
    sessions: rs.recordset || [],
  };
}

module.exports = {
  getDocumentHistory,
  MODULE_CONFIG,
};
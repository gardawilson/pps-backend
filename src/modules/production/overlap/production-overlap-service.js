// services/production-overlap-service.js
const { sql, poolPromise } = require('../../../core/config/db');

// Whitelist config per modul — PERHATIKAN schema/DB sesuai yang kamu kirim: PPS.dbo untuk Washing & Gilingan.
// Broker/Crusher sesuaikan dengan real DB-mu.
function getConfig(kind) {
  switch (kind) {
    case 'broker':
      return {
        mode: 'range',
        db: 'PPS_TEST2', schema: 'dbo', table: 'BrokerProduksi_h',  // ganti ke 'PPS' jika sudah pindah
        pk: 'NoProduksi',
        dateCol: 'TglProduksi',
        idMesinCol: 'IdMesin',
        hourStartCol: 'HourStart',
        hourEndCol: 'HourEnd',
      };
    case 'crusher':
      return {
        mode: 'range',
        db: 'PPS', schema: 'dbo', table: 'CrusherProduksi_h',
        pk: 'NoCrusherProduksi',
        dateCol: 'Tanggal',
        idMesinCol: 'IdMesin',
        hourStartCol: 'HourStart',
        hourEndCol: 'HourEnd',
      };
    case 'washing':
      return {
        mode: 'range',
        db: 'PPS', schema: 'dbo', table: 'WashingProduksi_h',
        pk: 'NoProduksi',
        dateCol: 'TglProduksi',
        idMesinCol: 'IdMesin',
        hourStartCol: 'HourStart',
        hourEndCol: 'HourEnd',
      };
    case 'gilingan':
      return {
        mode: 'range',
        db: 'PPS', schema: 'dbo', table: 'GilinganProduksi_h',
        pk: 'NoProduksi',
        dateCol: 'Tanggal',
        idMesinCol: 'IdMesin',
        hourStartCol: 'HourStart',
        hourEndCol: 'HourEnd',
      };
    default:
      throw new Error('Invalid kind');
  }
}

function qIdent(db, schema, table, col) {
  if (col) return `[${col}]`;
  return `[${db}].[${schema}].[${table}]`;
}

/**
 * Cek overlap jam untuk (kind, date, idMesin).
 * - Cross-midnight didukung (end < start → +1 hari)
 * - Exclude dokumen saat edit didukung
 */
async function checkOverlapGeneric({ kind, tglProduksi, idMesin, hourStart, hourEnd, excludeNoProduksi = null }) {
  const cfg = getConfig(kind);

  const pool = await poolPromise;
  const request = pool.request();

  const FQN = qIdent(cfg.db, cfg.schema, cfg.table);
  const NO  = qIdent(null, null, null, cfg.pk);
  const DT  = qIdent(null, null, null, cfg.dateCol);
  const IMS = qIdent(null, null, null, cfg.idMesinCol);
  const HS  = qIdent(null, null, null, cfg.hourStartCol);
  const HE  = qIdent(null, null, null, cfg.hourEndCol);

  const sqlText = `
    DECLARE @tgl DATE = TRY_CONVERT(date, @TglProduksi);
    IF @tgl IS NULL
      THROW 50001, 'Invalid date format for TglProduksi', 1;

    DECLARE @startT TIME(0) = TRY_CONVERT(time(0), @HourStart);
    DECLARE @endT   TIME(0) = TRY_CONVERT(time(0), @HourEnd);
    IF @startT IS NULL OR @endT IS NULL
      THROW 50002, 'Invalid time format for HourStart/HourEnd', 1;

    DECLARE @newStartDT DATETIME =
      DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', @startT), CAST(@tgl AS datetime));

    DECLARE @newEndDT DATETIME =
      CASE WHEN @endT < @startT
           THEN DATEADD(DAY, 1, DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', @endT), CAST(@tgl AS datetime)))
           ELSE DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', @endT), CAST(@tgl AS datetime))
      END;

    ;WITH base AS (
      SELECT
        ${NO}  AS NoDoc,
        ${DT}  AS Tgl,
        ${IMS} AS IdMesin,
        ${HS}  AS HourStart,
        ${HE}  AS HourEnd
      FROM ${FQN} WITH (NOLOCK)
      WHERE ${DT} = @tgl
        AND ${IMS} = @IdMesin
        AND ( @ExcludeNo IS NULL OR ${NO} <> @ExcludeNo )
        AND ${HS} IS NOT NULL
        AND ${HE} IS NOT NULL
    )
    SELECT
      b.NoDoc,
      HourStart = CONVERT(varchar(8), TRY_CONVERT(time(0), b.HourStart), 108),
      HourEnd   = CONVERT(varchar(8), TRY_CONVERT(time(0), b.HourEnd),   108),
      StartDT   = DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', t.startT), CAST(@tgl AS datetime)),
      EndDT     = CASE WHEN t.endT < t.startT
                       THEN DATEADD(DAY, 1, DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', t.endT), CAST(@tgl AS datetime)))
                       ELSE DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', t.endT), CAST(@tgl AS datetime))
                 END
    FROM base b
    CROSS APPLY (
      SELECT
        startT = TRY_CONVERT(time(0), b.HourStart),
        endT   = TRY_CONVERT(time(0), b.HourEnd)
    ) t
    WHERE t.startT IS NOT NULL
      AND t.endT   IS NOT NULL
      -- Rule overlap: existing.start < new.end AND new.start < existing.end
      AND DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', t.startT), CAST(@tgl AS datetime)) < @newEndDT
      AND @newStartDT < CASE WHEN t.endT < t.startT
                             THEN DATEADD(DAY, 1, DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', t.endT), CAST(@tgl AS datetime)))
                             ELSE DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', t.endT), CAST(@tgl AS datetime))
                        END
    ORDER BY b.NoDoc;
  `;

  request.input('TglProduksi', sql.VarChar(10), tglProduksi); // 'YYYY-MM-DD'
  request.input('IdMesin', sql.Int, idMesin);
  request.input('HourStart', sql.VarChar(8), hourStart);      // 'HH:mm' atau 'HH:mm:ss'
  request.input('HourEnd', sql.VarChar(8), hourEnd);
  request.input('ExcludeNo', sql.VarChar(50), excludeNoProduksi);

  const rs = await request.query(sqlText);
  const conflicts = rs.recordset || [];
  return { isOverlap: conflicts.length > 0, conflicts };
}

module.exports = { checkOverlapGeneric };

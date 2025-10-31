const { sql, poolPromise } = require('../../../core/config/db');

/**
 * GET CrusherProduksi_h by date
 * - Links to MstMesin for NamaMesin
 * - Aggregates output NoCrusher from CrusherProduksiOutput → "OutputNoCrusher" (comma-separated)
 *
 * Tables:
 *  - dbo.CrusherProduksi_h       (NoCrusherProduksi, Tanggal, IdMesin, IdOperator, Jam, Shift, ...)
 *  - dbo.MstMesin                (IdMesin -> NamaMesin)
 *  - dbo.CrusherProduksiOutput   (NoCrusherProduksi -> NoCrusher)
 */
async function getProduksiByDate({ date, idMesin = null, shift = null }) {
  const pool = await poolPromise;
  const request = pool.request();

  const filters = ['CONVERT(date, h.Tanggal) = @date'];
  request.input('date', sql.Date, date);

  if (idMesin) {
    filters.push('h.IdMesin = @idMesin');
    request.input('idMesin', sql.Int, idMesin);
  }

  if (shift && shift.length > 0) {
    filters.push('h.Shift = @shift');
    request.input('shift', sql.VarChar, shift);
  }

  const whereClause = filters.join(' AND ');

  // STRING_AGG requires SQL Server 2017+, your env is SQL 2022 — good.
  const query = `
    SELECT
      h.NoCrusherProduksi,
      CONVERT(date, h.Tanggal) AS Tanggal,
      h.IdMesin,
      m.NamaMesin,
      h.IdOperator,
      h.Jam,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter,

      -- outputs connected to this produksi
      (
        SELECT STRING_AGG(cpo.NoCrusher, ', ')
        FROM dbo.CrusherProduksiOutput cpo
        WHERE cpo.NoCrusherProduksi = h.NoCrusherProduksi
      ) AS OutputNoCrusher

    FROM dbo.CrusherProduksi_h h
    LEFT JOIN dbo.MstMesin m ON m.IdMesin = h.IdMesin
    WHERE ${whereClause}
    ORDER BY h.Jam ASC, h.NoCrusherProduksi ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

/**
 * GET enabled MstCrusher (for dropdowns)
 * MstCrusher: IdCrusher, NamaCrusher, Enable
 */
async function getCrusherMasters() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      mc.IdCrusher,
      mc.NamaCrusher,
      mc.Enable
    FROM dbo.MstCrusher mc
    WHERE ISNULL(mc.Enable, 1) = 1
    ORDER BY mc.NamaCrusher;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getProduksiByDate, getCrusherMasters };

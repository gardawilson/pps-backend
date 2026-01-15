// production-shared-service.js
const { sql, poolPromise } = require('../../../core/config/db');

/**
 * Lookup label codes for production items (FWIP & BJ).
 * Users only scan main labels: BB. (FurnitureWIP) and BA. (BarangJadi)
 * Partial codes (BC., BL.) are internal tracking only, never scanned.
 * Returns standardized camelCase response matching validateLabel pattern.
 */
async function lookupLabel(labelCode) {
  const pool = await poolPromise;

  // ---------- helpers ----------
  const toCamel = (s) => {
    if (!s) return s;
    // handle snake / kebab quickly
    let out = s.replace(/[_-]+([a-zA-Z0-9])/g, (_, c) => c.toUpperCase());
    // lower-case first char (IdLokasi -> idLokasi)
    out = out.charAt(0).toLowerCase() + out.slice(1);
    return out;
  };

  const camelize = (val) => {
    if (Array.isArray(val)) return val.map(camelize);
    if (val && typeof val === 'object') {
      const o = {};
      for (const [k, v] of Object.entries(val)) {
        o[toCamel(k)] = camelize(v);
      }
      return o;
    }
    return val;
  };

  // ---------- normalize label ----------
  const raw = String(labelCode || '').trim();
  if (!raw) throw new Error('Label code is required');

  let prefix = '';
  if (raw.substring(0, 3).toUpperCase() === 'BB.') {
    prefix = 'BB.';
  } else if (raw.substring(0, 3).toUpperCase() === 'BA.') {
    prefix = 'BA.';
  } else {
    prefix = raw.substring(0, 2).toUpperCase();
  }

  let query = '';
  let tableName = '';

  // Helper eksekusi single-query
  async function run(label) {
    const req = pool.request();
    req.input('labelCode', sql.VarChar(50), label);
    const rs = await req.query(query);
    const rows = rs.recordset || [];
    return camelize({
      found: rows.length > 0,
      count: rows.length,
      prefix,
      tableName,
      data: rows,
    });
  }

  switch (prefix) {
    // =========================
    // BB. FurnitureWIP
    // =========================
    case 'BB.':
      tableName = 'FurnitureWIP';
      query = `
        ;WITH PartialAgg AS (
          SELECT
            fwp.NoFurnitureWIP,
            SUM(ISNULL(fwp.Pcs, 0)) AS PartialPcs
          FROM dbo.FurnitureWIPPartial AS fwp WITH (NOLOCK)
          GROUP BY fwp.NoFurnitureWIP
        )
        SELECT
          fw.NoFurnitureWIP,
          fw.DateCreate,
          fw.DateUsage,
          fw.Jam,
          Pcs = CASE
                  WHEN fw.Pcs - ISNULL(pa.PartialPcs, 0) < 0 THEN 0
                  ELSE fw.Pcs - ISNULL(pa.PartialPcs, 0)
                END,
          fw.IDFurnitureWIP AS idJenis,
          mcw.Nama AS namaJenis,
          fw.Berat,
          fw.IsPartial,
          fw.IdWarehouse,
          fw.IdWarna,
          fw.CreateBy,
          fw.DateTimeCreate,
          fw.Blok,
          fw.IdLokasi
        FROM dbo.FurnitureWIP fw WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoFurnitureWIP = fw.NoFurnitureWIP
        LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
          ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
        WHERE fw.NoFurnitureWIP = @labelCode
          AND fw.DateUsage IS NULL
        ORDER BY fw.NoFurnitureWIP;
      `;
      return await run(raw);

    // =========================
    // BA. BarangJadi
    // =========================
    case 'BA.':
      tableName = 'BarangJadi';
      query = `
        ;WITH PartialAgg AS (
          SELECT
            bjp.NoBJ,
            SUM(ISNULL(bjp.Pcs, 0)) AS PartialPcs
          FROM dbo.BarangJadiPartial AS bjp WITH (NOLOCK)
          GROUP BY bjp.NoBJ
        )
        SELECT
          bj.NoBJ,
          bj.IdBJ AS idJenis,
          mbj.NamaBJ AS namaJenis,
          bj.DateCreate,
          bj.DateUsage,
          bj.Jam,
          Pcs = CASE
                  WHEN bj.Pcs - ISNULL(pa.PartialPcs, 0) < 0 THEN 0
                  ELSE bj.Pcs - ISNULL(pa.PartialPcs, 0)
                END,
          bj.Berat,
          bj.IsPartial,
          bj.IdWarehouse,
          bj.CreateBy,
          bj.DateTimeCreate,
          bj.Blok,
          bj.IdLokasi
        FROM dbo.BarangJadi bj WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoBJ = bj.NoBJ
        LEFT JOIN dbo.MstBarangJadi mbj WITH (NOLOCK)
          ON mbj.IdBJ = bj.IdBJ
        WHERE bj.NoBJ = @labelCode
          AND bj.DateUsage IS NULL
        ORDER BY bj.NoBJ;
      `;
      return await run(raw);

    default:
      throw new Error(`Invalid prefix: ${prefix}. Valid prefixes: BB., BA.`);
  }
}

module.exports = {
  lookupLabel,
};
// production-shared-service.js
const { sql, poolPromise } = require('../../../core/config/db');


function inferPrefix(rawUpper) {
  // keep your existing logic (BC. / BB. / fallback first 2 chars)
  if (rawUpper.startsWith('BC.')) return 'BC.';
  if (rawUpper.startsWith('BB.')) return 'BB.';
  return rawUpper.substring(0, 2);
}

async function lookupFwipLabel(pool, raw) {
  const upper = raw.toUpperCase();
  const prefix = inferPrefix(upper);

  let tableName = '';
  let query = '';

  // helper
  async function run(label) {
    const req = pool.request();
    req.input('code', sql.VarChar(50), label);

    const rs = await req.query(query);
    const rows = rs.recordset || [];

    return {
      found: rows.length > 0,
      count: rows.length,
      prefix,
      tableName,
      data: rows,
    };
  }

  // ===== BC = partial =====
  if (prefix === 'BC.') {
    tableName = 'FurnitureWIPPartial';
    query = `
      SELECT
        fwp.NoFurnitureWIPPartial,
        fwp.NoFurnitureWIP,
        fwp.Pcs AS PcsPartial,

        fw.Pcs AS PcsHeader,
        fw.Berat,
        fw.IDFurnitureWIP AS idJenis,
        fw.IsPartial,
        fw.DateUsage,

        fw.IdWarehouse,
        fw.IdWarna,
        fw.CreateBy,
        fw.DateTimeCreate,
        fw.Blok,
        fw.IdLokasi
      FROM dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
      JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
        ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP
      WHERE fwp.NoFurnitureWIPPartial = @code
        AND fw.DateUsage IS NULL;
    `;
    return run(raw);
  }

  // ===== BB (and other) = full =====
  tableName = 'FurnitureWIP';
  query = `
    SELECT
      fw.NoFurnitureWIP,
      fw.DateCreate,
      fw.Jam,
      fw.Pcs,
      fw.IDFurnitureWIP AS idJenis,
      fw.Berat,
      fw.IsPartial,
      fw.DateUsage,
      fw.IdWarehouse,
      fw.IdWarna,
      fw.CreateBy,
      fw.DateTimeCreate,
      fw.Blok,
      fw.IdLokasi
    FROM dbo.FurnitureWIP fw WITH (NOLOCK)
    WHERE fw.NoFurnitureWIP = @code
      AND fw.DateUsage IS NULL;
  `;
  return run(raw);
}

async function lookupLabel(labelCode) {
  const pool = await poolPromise;

  const raw = String(labelCode || '').trim();
  if (!raw) throw new Error('Label code is required');

  // For now you only lookup FWIP (FurnitureWIP / FurnitureWIPPartial)
  // Later you can add branching by prefix for other modules (washing/crusher/etc).
  return lookupFwipLabel(pool, raw);
}

module.exports = {
  lookupLabel,
};

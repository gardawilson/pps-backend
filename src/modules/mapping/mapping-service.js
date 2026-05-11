const { poolPromise, sql } = require("../../core/config/db");

async function getBlokWarehouseMapping() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT TOP (1000)
      b.Blok,
      b.IdWarehouse,
      w.NamaWarehouse
    FROM [dbo].[MstBlok] b
    LEFT JOIN [dbo].[MstWarehouse] w
      ON w.IdWarehouse = b.IdWarehouse
    ORDER BY b.Blok ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

async function getLokasiByBlok(blok) {
  const pool = await poolPromise;
  const request = pool.request();
  request.input("blok", sql.VarChar(100), blok);

  const query = `
    SELECT TOP (1000)
      IdLokasi,
      Blok,
      [Description],
      Enable
    FROM [dbo].[MstLokasi]
    WHERE Blok = @blok
      AND ISNULL(Enable, 1) = 1
    ORDER BY IdLokasi ASC;
  `;

  const result = await request.query(query);
  return (result.recordset || []).map((row) => ({
    ...row,
    label: `${row.Blok} - ${row.IdLokasi}`,
  }));
}

module.exports = { getBlokWarehouseMapping, getLokasiByBlok };

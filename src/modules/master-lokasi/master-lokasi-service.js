// master-lokasi-service.js
const { sql, poolPromise } = require('../../core/config/db');

async function getAllLokasiAktif(idWarehouseList = null) {
  const pool = await poolPromise;
  const request = pool.request();

  let query = '';

  // Kalau ada query ?idWarehouse=1,2,3,5,4
  if (idWarehouseList && idWarehouseList.trim() !== '') {
    // kirim sebagai string ke SQL: "1,2,3,5,4"
    request.input('IdWarehouseList', sql.VarChar, idWarehouseList);

    query = `
      SELECT DISTINCT
        l.IdLokasi,
        l.Blok,
        l.Enable
      FROM MstLokasi l
      INNER JOIN MstBlok b
        ON b.Blok = l.Blok
      WHERE
        l.Enable = 1
        AND b.IdWarehouse IN (
          SELECT TRY_CAST(value AS int)
          FROM STRING_SPLIT(@IdWarehouseList, ',')
          WHERE TRY_CAST(value AS int) IS NOT NULL
        )
      ORDER BY l.Blok ASC;
    `;
  } else {
    // behaviour lama: tanpa filter IdWarehouse
    query = `
      SELECT IdLokasi, Blok, Enable
      FROM MstLokasi 
      WHERE Enable = 1
      ORDER BY Blok ASC;
    `;
  }

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllLokasiAktif };

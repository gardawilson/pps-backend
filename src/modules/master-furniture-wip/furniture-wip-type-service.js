// src/modules/master/furniture-wip-type-service.js
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdCabinetWIP,
      Nama,                 -- furniture WIP name
      IdCabinetWIPType,
      SaldoAwal,
      TglSaldoAwal,
      IdUOM,
      Enable,
      IdTypeFurnitureWIP,
      IdFurnitureCategory,
      PcsPerLabel,
      IsInputInjectProduksi,
      IdWarna
    FROM [dbo].[MstCabinetWIP]
    WHERE Enable = 1
    ORDER BY Nama ASC;
  `;

  const result = await request.query(query);
  return result.recordset;
}

module.exports = { getAllActive };

// services/inject-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

async function getProduksiByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      h.NoProduksi,
      h.TglProduksi,
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
      h.IdCetakan,
      h.IdWarna,
      h.EnableOffset,
      h.OffsetCurrent,
      h.OffsetNext,
      h.IdFurnitureMaterial,
      h.HourMeter,
      h.BeratProdukHasilTimbang
    FROM [dbo].[InjectProduksi_h] h
    LEFT JOIN dbo.MstMesin m ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @date
    ORDER BY h.Jam ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

// 🔹 Ambil list FurnitureWIP kandidat dari NoProduksi (bisa >1 data)
async function getFurnitureWipListByNoProduksi(noProduksi) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('noProduksi', sql.VarChar(50), noProduksi);

  const query = `
    SELECT
      h.BeratProdukHasilTimbang,
      d.IdFurnitureWIP,
      cab.Nama AS NamaFurnitureWIP
    FROM dbo.InjectProduksi_h AS h
    INNER JOIN dbo.CetakanWarnaToFurnitureWIP_d AS d
      ON d.IdCetakan = h.IdCetakan
     AND d.IdWarna   = h.IdWarna
     AND (
          (d.IdFurnitureMaterial IS NULL
              AND (h.IdFurnitureMaterial = 0 OR h.IdFurnitureMaterial IS NULL))
          OR d.IdFurnitureMaterial = h.IdFurnitureMaterial
         )
    INNER JOIN dbo.MstCabinetWIP AS cab
      ON cab.IdCabinetWIP = d.IdFurnitureWIP
    WHERE h.NoProduksi = @noProduksi
      AND h.IdCetakan IS NOT NULL
    ORDER BY cab.Nama ASC;
  `;

  const result = await request.query(query);
  // array: [{ BeratProdukHasilTimbang, IdFurnitureWIP, NamaFurnitureWIP }, ...]
  return result.recordset;
}



// 🔹 Ambil list BarangJadi (Produk) kandidat dari NoProduksi (bisa >1 data)
async function getPackingListByNoProduksi(noProduksi) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('noProduksi', sql.VarChar(50), noProduksi);

  const query = `
    SELECT
      h.BeratProdukHasilTimbang,
      d.IdBarangJadi AS IdBJ,
      mbj.NamaBJ
    FROM dbo.InjectProduksi_h AS h
    INNER JOIN dbo.CetakanWarnaToProduk_d AS d
      ON d.IdCetakan = h.IdCetakan
     AND d.IdWarna   = h.IdWarna
     AND (
          (d.IdFurnitureMaterial IS NULL AND (h.IdFurnitureMaterial = 0 OR h.IdFurnitureMaterial IS NULL))
          OR d.IdFurnitureMaterial = h.IdFurnitureMaterial
         )
    INNER JOIN dbo.MstBarangJadi AS mbj
      ON mbj.IdBJ = d.IdBarangJadi
    WHERE h.NoProduksi = @noProduksi
      AND h.IdCetakan IS NOT NULL
    ORDER BY mbj.NamaBJ ASC;
  `;

  const result = await request.query(query);
  return result.recordset;  // array: [{ BeratProdukHasilTimbang, IdBJ, NamaBJ }, ...]
}




module.exports = {
  getProduksiByDate,
  getFurnitureWipListByNoProduksi,
  getPackingListByNoProduksi
};

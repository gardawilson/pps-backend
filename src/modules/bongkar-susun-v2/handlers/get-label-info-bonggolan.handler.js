const { sql, poolPromise } = require("../../../core/config/db");

exports.getLabelInfoBonggolan = async (labelCode) => {
  const pool = await poolPromise;
  const result = await pool
    .request()
    .input("NoBonggolan", sql.VarChar(50), labelCode).query(`
      SELECT
        b.NoBonggolan   AS labelCode,
        b.IdBonggolan   AS idJenis,
        mb.NamaBonggolan AS namaJenis,
        b.IdWarehouse,
        b.IdStatus,
        b.Berat         AS totalBerat
      FROM dbo.Bonggolan b
      INNER JOIN dbo.MstBonggolan mb
        ON mb.IdBonggolan = b.IdBonggolan
      WHERE b.NoBonggolan = @NoBonggolan
        AND b.DateUsage IS NULL
    `);

  if (!result.recordset.length) {
    const e = new Error(
      `Label ${labelCode} tidak ditemukan atau sudah terpakai`,
    );
    e.statusCode = 404;
    throw e;
  }

  const row = result.recordset[0];
  return {
    labelCode: row.labelCode,
    category: "bonggolan",
    idJenis: row.idJenis,
    namaJenis: row.namaJenis,
    idWarehouse: row.IdWarehouse,
    idStatus: row.IdStatus,
    totalBerat: row.totalBerat,
  };
};

const { sql, poolPromise } = require("../../../core/config/db");

exports.getLabelInfoBonggolan = async (labelCode) => {
  const pool = await poolPromise;
  const result = await pool
    .request()
    .input("NoBonggolan", sql.VarChar(50), labelCode).query(`
      SELECT
        NoBonggolan   AS labelCode,
        IdBonggolan   AS idJenis,
        IdWarehouse,
        IdStatus,
        Berat         AS totalBerat
      FROM dbo.Bonggolan
      WHERE NoBonggolan = @NoBonggolan
        AND DateUsage IS NULL
    `);

  if (!result.recordset.length) {
    const e = new Error(`Label ${labelCode} tidak ditemukan atau sudah terpakai`);
    e.statusCode = 404;
    throw e;
  }

  const row = result.recordset[0];
  return {
    labelCode: row.labelCode,
    category: "bonggolan",
    idJenis: row.idJenis,
    idWarehouse: row.IdWarehouse,
    idStatus: row.IdStatus,
    totalBerat: row.totalBerat,
  };
};

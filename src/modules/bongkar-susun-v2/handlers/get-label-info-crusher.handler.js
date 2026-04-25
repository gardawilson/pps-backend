const { sql, poolPromise } = require("../../../core/config/db");

exports.getLabelInfoCrusher = async (labelCode) => {
  const pool = await poolPromise;

  const result = await pool
    .request()
    .input("NoCrusher", sql.VarChar(50), labelCode).query(`
      ;WITH Base AS (
      SELECT
        A.NoCrusher,
        A.DateCreate,
        A.IdCrusher AS idJenis,
        B.NamaCrusher AS namaJenis,
        A.DateUsage,
        A.Berat,
          A.CreateBy,
          G.NamaWarehouse,
          A.HasBeenPrinted
        FROM dbo.Crusher A
        INNER JOIN dbo.MstCrusher B
          ON B.IdCrusher = A.IdCrusher
        LEFT JOIN dbo.MstWarehouse G
          ON G.IdWarehouse = A.IdWarehouse
        WHERE A.NoCrusher = @NoCrusher AND A.DateUsage IS NULL
      )
      SELECT
        A.NoCrusher,
        A.DateCreate,
        A.idJenis,
        A.namaJenis,
        A.DateUsage,
        A.Berat,
        ISNULL(K.Mesin, '') AS Mesin,
        A.CreateBy,
        A.NamaWarehouse,
        ISNULL(K.Shift, 0)  AS Shift,
        A.HasBeenPrinted
      FROM Base A
      OUTER APPLY (
        SELECT TOP (1)
          src.Mesin,
          src.Shift
        FROM (
          SELECT
            E.NamaMesin AS Mesin,
            D.Shift,
            1 AS Priority
          FROM dbo.CrusherProduksiOutput C
          JOIN dbo.CrusherProduksi_h D ON D.NoCrusherProduksi = C.NoCrusherProduksi
          JOIN dbo.MstMesin E          ON E.IdMesin = D.IdMesin
          WHERE C.NoCrusher = A.NoCrusher

          UNION ALL

          SELECT
            F.NoBongkarSusun,
            0,
            2
          FROM dbo.BongkarSusunOutputCrusher F
          WHERE F.NoCrusher = A.NoCrusher

          UNION ALL

          SELECT '', 0, 3
        ) src
        ORDER BY src.Priority
      ) K
    `);

  const first = result.recordset?.[0];
  if (!first) {
    const e = new Error(`NoCrusher ${labelCode} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  return {
    labelCode: first.NoCrusher,
    category: "crusher",
    dateCreate: first.DateCreate,
    idJenis: first.idJenis,
    namaJenis: first.namaJenis,
    dateUsage: first.DateUsage,
    berat: first.Berat,
    mesin: first.Mesin,
    createBy: first.CreateBy,
    namaWarehouse: first.NamaWarehouse,
    shift: first.Shift,
    hasBeenPrinted: first.HasBeenPrinted ?? 0,
  };
};

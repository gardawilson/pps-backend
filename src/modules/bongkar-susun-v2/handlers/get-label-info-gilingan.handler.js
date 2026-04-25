const { sql, poolPromise } = require("../../../core/config/db");
const { conflict } = require("../../../core/utils/http-error");

exports.getLabelInfoGilingan = async (labelCode) => {
  const pool = await poolPromise;

  const result = await pool
    .request()
    .input("NoGilingan", sql.VarChar(50), labelCode).query(`
      SELECT
        g.NoGilingan AS labelCode,
        g.DateCreate,
        g.IdGilingan AS idJenis,
        mg.NamaGilingan AS namaJenis,
        g.IsPartial,
        CASE
          WHEN g.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0) < 0
                THEN 0
              ELSE ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0)
            END
          ELSE ISNULL(g.Berat, 0)
        END AS berat,
        ISNULL(CAST(g.HasBeenPrinted AS int), 0) AS hasBeenPrinted,
        g.CreateBy,
        COALESCE(prod.NamaMesin, bs.NoBongkarSusun, '') AS mesin,
        prod.Shift AS shift
      FROM dbo.Gilingan g
      INNER JOIN dbo.MstGilingan mg
        ON mg.IdGilingan = g.IdGilingan
      LEFT JOIN (
        SELECT NoGilingan, SUM(ISNULL(Berat, 0)) AS TotalPartial
        FROM dbo.GilinganPartial
        GROUP BY NoGilingan
      ) gp
        ON gp.NoGilingan = g.NoGilingan
      OUTER APPLY (
        SELECT TOP (1)
          m.NamaMesin,
          gh.Shift
        FROM dbo.GilinganProduksiOutput gpo
        JOIN dbo.GilinganProduksi_h gh ON gh.NoProduksi = gpo.NoProduksi
        LEFT JOIN dbo.MstMesin m ON m.IdMesin = gh.IdMesin
        WHERE gpo.NoGilingan = g.NoGilingan
      ) prod
      OUTER APPLY (
        SELECT TOP (1)
          bs.NoBongkarSusun
        FROM dbo.BongkarSusunOutputGilingan bs
        WHERE bs.NoGilingan = g.NoGilingan
      ) bs
      WHERE g.NoGilingan = @NoGilingan
    `);

  const first = result.recordset?.[0];
  if (!first) {
    const e = new Error(`NoGilingan ${labelCode} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  if (first.IsPartial === true || first.IsPartial === 1) {
    throw conflict("Tidak dapat bongkar susun label yang sudah di partial");
  }

  return {
    labelCode: first.labelCode,
    category: "gilingan",
    dateCreate: first.DateCreate,
    idGilingan: first.idJenis,
    namaJenis: first.namaJenis,
    berat: first.berat,
    hasBeenPrinted: first.hasBeenPrinted,
    createBy: first.CreateBy,
    mesin: first.mesin,
    shift: first.shift,
  };
};

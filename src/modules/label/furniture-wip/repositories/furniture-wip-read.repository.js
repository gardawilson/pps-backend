const { sql, poolPromise } = require("../../../../core/config/db");

exports.getAll = async ({ page, limit, search, includeUsed = false }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;
  const dateUsageFilter = includeUsed ? "" : "AND f.DateUsage IS NULL";

  const baseQuery = `
    SELECT
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama AS NamaFurnitureWIP,
      CASE
        WHEN f.IsPartial = 1 THEN
          CASE
            WHEN ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0) < 0 THEN 0
            ELSE ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0)
          END
        ELSE ISNULL(f.Pcs, 0)
      END AS Pcs,
      ISNULL(f.Berat, 0) AS Berat,
      f.IsPartial,
      CASE WHEN MAX(f.DateUsage) IS NULL THEN CAST(0 AS bit) ELSE CAST(1 AS bit) END AS Used,
      MAX(ISNULL(CAST(f.HasBeenPrinted AS int), 0)) AS HasBeenPrinted,
      f.IdWarna,
      f.Blok,
      f.IdLokasi,
      CASE
        WHEN MAX(hsmap.NoProduksi) IS NOT NULL THEN 'HOTSTAMPING'
        WHEN MAX(pkmap.NoProduksi) IS NOT NULL THEN 'PASANG_KUNCI'
        WHEN MAX(bsmap.NoBongkarSusun) IS NOT NULL THEN 'BONGKAR_SUSUN'
        WHEN MAX(retmap.NoRetur) IS NOT NULL THEN 'RETUR'
        WHEN MAX(spmap.NoProduksi) IS NOT NULL THEN 'SPANNER'
        WHEN MAX(injmap.NoProduksi) IS NOT NULL THEN 'INJECT'
        ELSE NULL
      END AS OutputType,
      MAX(COALESCE(hsmap.NoProduksi, pkmap.NoProduksi, spmap.NoProduksi, injmap.NoProduksi, bsmap.NoBongkarSusun, retmap.NoRetur)) AS OutputCode,
      MAX(COALESCE(mHs.NamaMesin, mPk.NamaMesin, mSp.NamaMesin, mInj.NamaMesin, CASE WHEN bsmap.NoBongkarSusun IS NOT NULL THEN 'Bongkar Susun' END, pemb.NamaPembeli)) AS OutputNamaMesin
    FROM [dbo].[FurnitureWIP] f
    LEFT JOIN (
      SELECT NoFurnitureWIP, SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
      FROM [dbo].[FurnitureWIPPartial]
      GROUP BY NoFurnitureWIP
    ) fp ON fp.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[MstCabinetWIP] cw ON cw.IdCabinetWIP = f.IdFurnitureWIP
    LEFT JOIN [dbo].[HotStampingOutputLabelFWIP] hsmap ON hsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[HotStamping_h] hsh ON hsh.NoProduksi = hsmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHs ON mHs.IdMesin = hsh.IdMesin
    LEFT JOIN [dbo].[PasangKunciOutputLabelFWIP] pkmap ON pkmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[PasangKunci_h] pkh ON pkh.NoProduksi = pkmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mPk ON mPk.IdMesin = pkh.IdMesin
    LEFT JOIN [dbo].[BongkarSusunOutputFurnitureWIP] bsmap ON bsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJReturFurnitureWIP_d] retmap ON retmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJRetur_h] bjh ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb ON pemb.IdPembeli = bjh.IdPembeli
    LEFT JOIN [dbo].[SpannerOutputLabelFWIP] spmap ON spmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[Spanner_h] sph ON sph.NoProduksi = spmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSp ON mSp.IdMesin = sph.IdMesin
    LEFT JOIN [dbo].[InjectProduksiOutputFurnitureWIP] injmap ON injmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[InjectProduksi_h] injh ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj ON mInj.IdMesin = injh.IdMesin
    WHERE 1=1
      ${dateUsageFilter}
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search
               OR ISNULL(hsmap.NoProduksi,'') LIKE @search
               OR ISNULL(pkmap.NoProduksi,'') LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'') LIKE @search
               OR ISNULL(retmap.NoRetur,'') LIKE @search
               OR ISNULL(spmap.NoProduksi,'') LIKE @search
               OR ISNULL(injmap.NoProduksi,'') LIKE @search
               OR ISNULL(mHs.NamaMesin,'') LIKE @search
               OR ISNULL(mPk.NamaMesin,'') LIKE @search
               OR ISNULL(mSp.NamaMesin,'') LIKE @search
               OR ISNULL(mInj.NamaMesin,'') LIKE @search
               OR ISNULL(pemb.NamaPembeli,'') LIKE @search
             )`
          : ""
      }
    GROUP BY
      f.NoFurnitureWIP, f.DateCreate, f.IdFurnitureWIP, cw.Nama, f.Pcs, f.Berat, f.IsPartial, f.IdWarna, f.Blok, f.IdLokasi
    ORDER BY f.NoFurnitureWIP DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT f.NoFurnitureWIP) AS total
    FROM [dbo].[FurnitureWIP] f
    LEFT JOIN [dbo].[MstCabinetWIP] cw ON cw.IdCabinetWIP = f.IdFurnitureWIP
    LEFT JOIN [dbo].[HotStampingOutputLabelFWIP] hsmap ON hsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[HotStamping_h] hsh ON hsh.NoProduksi = hsmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHs ON mHs.IdMesin = hsh.IdMesin
    LEFT JOIN [dbo].[PasangKunciOutputLabelFWIP] pkmap ON pkmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[PasangKunci_h] pkh ON pkh.NoProduksi = pkmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mPk ON mPk.IdMesin = pkh.IdMesin
    LEFT JOIN [dbo].[BongkarSusunOutputFurnitureWIP] bsmap ON bsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJReturFurnitureWIP_d] retmap ON retmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJRetur_h] bjh ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb ON pemb.IdPembeli = bjh.IdPembeli
    LEFT JOIN [dbo].[SpannerOutputLabelFWIP] spmap ON spmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[Spanner_h] sph ON sph.NoProduksi = spmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSp ON mSp.IdMesin = sph.IdMesin
    LEFT JOIN [dbo].[InjectProduksiOutputFurnitureWIP] injmap ON injmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[InjectProduksi_h] injh ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj ON mInj.IdMesin = injh.IdMesin
    WHERE 1=1
      ${dateUsageFilter}
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search
               OR ISNULL(hsmap.NoProduksi,'') LIKE @search
               OR ISNULL(pkmap.NoProduksi,'') LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'') LIKE @search
               OR ISNULL(retmap.NoRetur,'') LIKE @search
               OR ISNULL(spmap.NoProduksi,'') LIKE @search
               OR ISNULL(injmap.NoProduksi,'') LIKE @search
               OR ISNULL(mHs.NamaMesin,'') LIKE @search
               OR ISNULL(mPk.NamaMesin,'') LIKE @search
               OR ISNULL(mSp.NamaMesin,'') LIKE @search
               OR ISNULL(mInj.NamaMesin,'') LIKE @search
               OR ISNULL(pemb.NamaPembeli,'') LIKE @search
             )`
          : ""
      }
  `;

  request.input("offset", sql.Int, offset);
  request.input("limit", sql.Int, limit);
  if (search) {
    request.input("search", sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  return {
    data: dataResult.recordset || [],
    total: countResult.recordset?.[0]?.total ?? 0,
  };
};

exports.getExistingForUpdate = async (tx, noFurnitureWip) => {
  const res = await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip).query(`
    SELECT TOP 1
      NoFurnitureWIP, CONVERT(date, DateCreate) AS DateCreate, Jam, Pcs, IDFurnitureWIP,
      Berat, IsPartial, DateUsage, IdWarehouse, IdWarna, CreateBy, DateTimeCreate, Blok, IdLokasi
    FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
    WHERE NoFurnitureWIP = @NoFurnitureWIP;
  `);
  return res.recordset?.[0] || null;
};

exports.isFromBongkarSusun = async (tx, noFurnitureWip) => {
  const res = await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip).query(
    `SELECT TOP 1 1 FROM dbo.BongkarSusunOutputFurnitureWIP WHERE NoFurnitureWIP = @NoFurnitureWIP`,
  );
  return res.recordset.length > 0;
};

exports.getHeaderForDelete = async (tx, noFurnitureWip) => {
  const res = await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip).query(`
    SELECT TOP 1 NoFurnitureWIP, CONVERT(date, DateCreate) AS DateCreate, DateUsage
    FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
    WHERE NoFurnitureWIP = @NoFurnitureWIP;
  `);
  return res.recordset?.[0] || null;
};

exports.isNoFurnitureWipExists = async (tx, noFurnitureWip) => {
  const res = await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip).query(`
    SELECT 1
    FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
    WHERE NoFurnitureWIP = @NoFurnitureWIP
  `);
  return res.recordset.length > 0;
};

exports.getInjectHeaderByNoProduksi = async (tx, outputCode) => {
  const res = await new sql.Request(tx).input("NoProduksi", sql.VarChar(50), outputCode).query(`
    SELECT TOP 1 IdCetakan, IdWarna, IdFurnitureMaterial
    FROM dbo.InjectProduksi_h WITH (UPDLOCK, HOLDLOCK)
    WHERE NoProduksi = @NoProduksi
      AND IdCetakan IS NOT NULL;
  `);
  return res.recordset?.[0] || null;
};

exports.getInjectFurnitureWipMappings = async (
  tx,
  { idCetakan, idWarna, idFurnitureMaterial },
) => {
  const res = await new sql.Request(tx)
    .input("IdCetakan", sql.Int, idCetakan)
    .input("IdWarna", sql.Int, idWarna)
    .input("IdFurnitureMaterial", sql.Int, idFurnitureMaterial ?? 0).query(`
      SELECT IdFurnitureWIP
      FROM dbo.CetakanWarnaToFurnitureWIP_d
      WHERE IdCetakan = @IdCetakan
        AND IdWarna = @IdWarna
        AND (
          (IdFurnitureMaterial IS NULL AND @IdFurnitureMaterial = 0)
          OR IdFurnitureMaterial = @IdFurnitureMaterial
        );
    `);
  return res.recordset || [];
};

exports.getPartialInfoRows = async (noFurnitureWip) => {
  const pool = await poolPromise;
  const req = pool
    .request()
    .input("NoFurnitureWIP", sql.VarChar, noFurnitureWip);

  const result = await req.query(`
    ;WITH BasePartial AS (
      SELECT fwp.NoFurnitureWIPPartial, fwp.NoFurnitureWIP, fwp.Pcs
      FROM dbo.FurnitureWIPPartial fwp
      WHERE fwp.NoFurnitureWIP = @NoFurnitureWIP
    ),
    Consumed AS (
      SELECT ip.NoFurnitureWIPPartial, 'INJECT' AS SourceType, ip.NoProduksi
      FROM dbo.InjectProduksiInputFurnitureWIPPartial ip
    )
    SELECT
      bp.NoFurnitureWIPPartial, bp.NoFurnitureWIP, bp.Pcs,
      c.SourceType, c.NoProduksi,
      iph.TglProduksi, iph.IdMesin, iph.IdOperator, iph.Jam, iph.Shift,
      mm.NamaMesin
    FROM BasePartial bp
    LEFT JOIN Consumed c ON c.NoFurnitureWIPPartial = bp.NoFurnitureWIPPartial
    LEFT JOIN dbo.InjectProduksi_h iph ON iph.NoProduksi = c.NoProduksi
    LEFT JOIN dbo.MstMesin mm ON mm.IdMesin = iph.IdMesin
    ORDER BY bp.NoFurnitureWIPPartial ASC, c.NoProduksi ASC;
  `);

  return result.recordset || [];
};

exports.getByNoFurnitureWip = async (noFurnitureWip) => {
  const pool = await poolPromise;
  const result = await pool
    .request()
    .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip).query(`
      SELECT
        f.NoFurnitureWIP, f.DateCreate, f.IdFurnitureWIP, cw.Nama AS NamaFurnitureWIP, f.IsPartial,
        CASE WHEN f.IsPartial = 1 THEN
          CASE WHEN ISNULL(f.Pcs, 0) - ISNULL(fp.TotalPartialPcs, 0) < 0 THEN 0
               ELSE ISNULL(f.Pcs, 0) - ISNULL(fp.TotalPartialPcs, 0) END
        ELSE ISNULL(f.Pcs, 0) END AS Pcs,
        ISNULL(f.Berat, 0) AS Berat,
        ISNULL(CAST(f.HasBeenPrinted AS int), 0) AS HasBeenPrinted,
        f.CreateBy,
        COALESCE(outInfo.OutputNamaMesin, '') AS Mesin,
        outInfo.Shift AS Shift
      FROM dbo.FurnitureWIP f
      LEFT JOIN dbo.MstCabinetWIP cw ON cw.IdCabinetWIP = f.IdFurnitureWIP
      LEFT JOIN (
        SELECT NoFurnitureWIP, SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
        FROM dbo.FurnitureWIPPartial
        GROUP BY NoFurnitureWIP
      ) fp ON fp.NoFurnitureWIP = f.NoFurnitureWIP
      OUTER APPLY (
        SELECT TOP (1) src.OutputNamaMesin, src.Shift
        FROM (
          SELECT mHs.NamaMesin AS OutputNamaMesin, hsh.Shift, 1 AS Priority
          FROM dbo.HotStampingOutputLabelFWIP hsmap
          JOIN dbo.HotStamping_h hsh ON hsh.NoProduksi = hsmap.NoProduksi
          LEFT JOIN dbo.MstMesin mHs ON mHs.IdMesin = hsh.IdMesin
          WHERE hsmap.NoFurnitureWIP = f.NoFurnitureWIP
          UNION ALL
          SELECT mPk.NamaMesin, pkh.Shift, 2
          FROM dbo.PasangKunciOutputLabelFWIP pkmap
          JOIN dbo.PasangKunci_h pkh ON pkh.NoProduksi = pkmap.NoProduksi
          LEFT JOIN dbo.MstMesin mPk ON mPk.IdMesin = pkh.IdMesin
          WHERE pkmap.NoFurnitureWIP = f.NoFurnitureWIP
          UNION ALL
          SELECT mSp.NamaMesin, sph.Shift, 3
          FROM dbo.SpannerOutputLabelFWIP spmap
          JOIN dbo.Spanner_h sph ON sph.NoProduksi = spmap.NoProduksi
          LEFT JOIN dbo.MstMesin mSp ON mSp.IdMesin = sph.IdMesin
          WHERE spmap.NoFurnitureWIP = f.NoFurnitureWIP
          UNION ALL
          SELECT mInj.NamaMesin, injh.Shift, 4
          FROM dbo.InjectProduksiOutputFurnitureWIP injmap
          JOIN dbo.InjectProduksi_h injh ON injh.NoProduksi = injmap.NoProduksi
          LEFT JOIN dbo.MstMesin mInj ON mInj.IdMesin = injh.IdMesin
          WHERE injmap.NoFurnitureWIP = f.NoFurnitureWIP
          UNION ALL
          SELECT bsmap.NoBongkarSusun, NULL, 5
          FROM dbo.BongkarSusunOutputFurnitureWIP bsmap
          WHERE bsmap.NoFurnitureWIP = f.NoFurnitureWIP
          UNION ALL
          SELECT pemb.NamaPembeli, NULL, 6
          FROM dbo.BJReturFurnitureWIP_d retmap
          JOIN dbo.BJRetur_h bjh ON bjh.NoRetur = retmap.NoRetur
          JOIN dbo.MstPembeli pemb ON pemb.IdPembeli = bjh.IdPembeli
          WHERE retmap.NoFurnitureWIP = f.NoFurnitureWIP
        ) src
        WHERE src.OutputNamaMesin IS NOT NULL AND src.OutputNamaMesin <> ''
        ORDER BY src.Priority
      ) outInfo
      WHERE f.NoFurnitureWIP = @NoFurnitureWIP
    `);
  return result.recordset?.[0] || null;
};

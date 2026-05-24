const { sql } = require("../../../../core/config/db");

exports.insertFurnitureWipHeader = async (
  tx,
  { noFurnitureWip, header, idFurnitureWip, effectiveDateCreate, nowDateTime },
) => {
  await new sql.Request(tx)
    .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip)
    .input("DateCreate", sql.Date, effectiveDateCreate)
    .input("Jam", sql.VarChar(20), header.Jam ?? null)
    .input("Pcs", sql.Decimal(18, 3), header.Pcs ?? null)
    .input("IDFurnitureWIP", sql.Int, idFurnitureWip)
    .input("Berat", sql.Decimal(18, 3), header.Berat ?? null)
    .input("IsPartial", sql.Bit, header.IsPartial ?? 0)
    .input("IdWarehouse", sql.Int, header.IdWarehouse)
    .input("IdWarna", sql.Int, header.IdWarna ?? null)
    .input("CreateBy", sql.VarChar(50), header.CreateBy)
    .input("DateTimeCreate", sql.DateTime, nowDateTime)
    .input("Blok", sql.VarChar(50), header.Blok ?? null)
    .input("IdLokasi", sql.Int, header.IdLokasi ?? null).query(`
      INSERT INTO dbo.FurnitureWIP (
        NoFurnitureWIP, DateCreate, Jam, Pcs, IDFurnitureWIP, Berat, IsPartial, DateUsage,
        IdWarehouse, IdWarna, CreateBy, DateTimeCreate, Blok, IdLokasi
      )
      VALUES (
        @NoFurnitureWIP, @DateCreate, @Jam, @Pcs, @IDFurnitureWIP, @Berat, @IsPartial, NULL,
        @IdWarehouse, @IdWarna, @CreateBy, @DateTimeCreate, @Blok, @IdLokasi
      );
    `);
};

exports.insertOutputMapping = async (tx, { mappingTable, outputCode, noFurnitureWip }) => {
  const rqMap = new sql.Request(tx)
    .input("OutputCode", sql.VarChar(50), outputCode)
    .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip);

  if (mappingTable === "HotStampingOutputLabelFWIP") {
    return rqMap.query(
      `INSERT INTO dbo.HotStampingOutputLabelFWIP (NoProduksi, NoFurnitureWIP) VALUES (@OutputCode, @NoFurnitureWIP);`,
    );
  }
  if (mappingTable === "PasangKunciOutputLabelFWIP") {
    return rqMap.query(
      `INSERT INTO dbo.PasangKunciOutputLabelFWIP (NoProduksi, NoFurnitureWIP) VALUES (@OutputCode, @NoFurnitureWIP);`,
    );
  }
  if (mappingTable === "SpannerOutputLabelFWIP") {
    return rqMap.query(
      `INSERT INTO dbo.SpannerOutputLabelFWIP (NoProduksi, NoFurnitureWIP) VALUES (@OutputCode, @NoFurnitureWIP);`,
    );
  }
  if (mappingTable === "BongkarSusunOutputFurnitureWIP") {
    return rqMap.query(
      `INSERT INTO dbo.BongkarSusunOutputFurnitureWIP (NoBongkarSusun, NoFurnitureWIP) VALUES (@OutputCode, @NoFurnitureWIP);`,
    );
  }
  if (mappingTable === "BJReturFurnitureWIP_d") {
    return rqMap.query(
      `INSERT INTO dbo.BJReturFurnitureWIP_d (NoRetur, NoFurnitureWIP) VALUES (@OutputCode, @NoFurnitureWIP);`,
    );
  }
  if (mappingTable === "InjectProduksiOutputFurnitureWIP") {
    return rqMap.query(
      `INSERT INTO dbo.InjectProduksiOutputFurnitureWIP (NoProduksi, NoFurnitureWIP) VALUES (@OutputCode, @NoFurnitureWIP);`,
    );
  }
};

exports.deleteAllMappings = async (tx, noFurnitureWip) => {
  await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar, noFurnitureWip)
    .query(`
      DELETE FROM [dbo].[HotStampingOutputLabelFWIP]       WHERE NoFurnitureWIP = @NoFurnitureWIP;
      DELETE FROM [dbo].[PasangKunciOutputLabelFWIP]       WHERE NoFurnitureWIP = @NoFurnitureWIP;
      DELETE FROM [dbo].[BongkarSusunOutputFurnitureWIP]   WHERE NoFurnitureWIP = @NoFurnitureWIP;
      DELETE FROM [dbo].[BJReturFurnitureWIP_d]            WHERE NoFurnitureWIP = @NoFurnitureWIP;
      DELETE FROM [dbo].[SpannerOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
      DELETE FROM [dbo].[InjectProduksiOutputFurnitureWIP] WHERE NoFurnitureWIP = @NoFurnitureWIP;
    `);
};

exports.updateFurnitureWipHeader = async (
  tx,
  noFurnitureWip,
  merged,
  hasDateCreate,
  dateCreateParam,
) => {
  const rqUpdate = new sql.Request(tx)
    .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip)
    .input("IDFurnitureWIP", sql.Int, merged.IDFurnitureWIP)
    .input("Jam", sql.VarChar(20), merged.Jam ?? null)
    .input("Pcs", sql.Decimal(18, 3), merged.Pcs ?? null)
    .input("Berat", sql.Decimal(18, 3), merged.Berat ?? null)
    .input("IsPartial", sql.Bit, merged.IsPartial ?? 0)
    .input("IdWarehouse", sql.Int, merged.IdWarehouse)
    .input("IdWarna", sql.Int, merged.IdWarna ?? null)
    .input("Blok", sql.VarChar(50), merged.Blok ?? null)
    .input("IdLokasi", sql.Int, merged.IdLokasi ?? null)
    .input("CreateBy", sql.VarChar(50), merged.CreateBy ?? null);

  if (hasDateCreate) {
    rqUpdate.input("DateCreate", sql.Date, dateCreateParam);
  }

  await rqUpdate.query(`
    UPDATE dbo.FurnitureWIP
    SET
      IDFurnitureWIP = @IDFurnitureWIP,
      Jam = @Jam,
      Pcs = @Pcs,
      Berat = @Berat,
      IsPartial = @IsPartial,
      IdWarehouse = @IdWarehouse,
      IdWarna = @IdWarna,
      Blok = @Blok,
      IdLokasi = @IdLokasi,
      CreateBy = @CreateBy
      ${hasDateCreate ? ", DateCreate = @DateCreate" : ""}
    WHERE NoFurnitureWIP = @NoFurnitureWIP;
  `);
};

exports.deleteFurnitureWipPartials = async (tx, noFurnitureWip) => {
  await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip)
    .query(`DELETE FROM dbo.FurnitureWIPPartial WHERE NoFurnitureWIP = @NoFurnitureWIP;`);
};

exports.deleteFurnitureWipHeader = async (tx, noFurnitureWip) => {
  const res = await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip)
    .query(`DELETE FROM dbo.FurnitureWIP WHERE NoFurnitureWIP = @NoFurnitureWIP;`);
  return res.rowsAffected?.[0] ?? 0;
};

exports.incrementHasBeenPrinted = async (tx, noFurnitureWip) => {
  const rs = await new sql.Request(tx).input(
    "NoFurnitureWIP",
    sql.VarChar(50),
    noFurnitureWip,
  ).query(`
    DECLARE @out TABLE (NoFurnitureWIP varchar(50), HasBeenPrinted int);
    UPDATE dbo.FurnitureWIP
    SET HasBeenPrinted = ISNULL(HasBeenPrinted, 0) + 1
    OUTPUT INSERTED.NoFurnitureWIP, INSERTED.HasBeenPrinted INTO @out
    WHERE NoFurnitureWIP = @NoFurnitureWIP;
    SELECT NoFurnitureWIP, HasBeenPrinted FROM @out;
  `);
  return rs.recordset?.[0] || null;
};

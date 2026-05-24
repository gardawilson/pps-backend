const { sql } = require("../../../../core/config/db");

async function setAuditContext(tx, { actorId, requestId }) {
  return new sql.Request(tx)
    .input("actorId", sql.Int, actorId)
    .input("rid", sql.NVarChar(64), requestId).query(`
      EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
      EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
    `);
}

async function insertMixerHeaderFromBongkarSusunTx({
  tx,
  noMixer,
  nowDate,
  outputIdJenis,
  actorUsername,
  reference,
}) {
  return new sql.Request(tx)
    .input("NoMixer", sql.VarChar(50), noMixer)
    .input("DateCreate", sql.Date, nowDate)
    .input("IdMixer", sql.Int, outputIdJenis)
    .input("IdStatus", sql.Int, reference?.IdStatus ?? 1)
    .input("CreateBy", sql.VarChar(50), actorUsername || "system")
    .input("DateTimeCreate", sql.DateTime, nowDate)
    .input("Moisture", sql.Decimal(10, 3), reference?.Moisture ?? null)
    .input("MaxMeltTemp", sql.Decimal(10, 3), reference?.MaxMeltTemp ?? null)
    .input("MinMeltTemp", sql.Decimal(10, 3), reference?.MinMeltTemp ?? null)
    .input("MFI", sql.Decimal(10, 3), reference?.MFI ?? null)
    .input("Moisture2", sql.Decimal(10, 3), reference?.Moisture2 ?? null)
    .input("Moisture3", sql.Decimal(10, 3), reference?.Moisture3 ?? null)
    .input("Blok", sql.VarChar(50), reference?.Blok ?? null)
    .input("IdLokasi", sql.Int, reference?.IdLokasi ?? null).query(`
      INSERT INTO dbo.Mixer_h (
        NoMixer, IdMixer, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Moisture, MaxMeltTemp, MinMeltTemp, MFI, Moisture2, Moisture3, Blok, IdLokasi
      )
      VALUES (
        @NoMixer, @IdMixer, @DateCreate, @IdStatus, @CreateBy, @DateTimeCreate,
        @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `);
}

async function insertMixerDetailsFromBongkarSusunTx({ tx, noMixer, saksJson }) {
  return new sql.Request(tx)
    .input("NoMixer", sql.VarChar(50), noMixer)
    .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
      INSERT INTO dbo.Mixer_d (NoMixer, NoSak, Berat, DateUsage, IsPartial)
      SELECT @NoMixer, j.NoSak, j.Berat, NULL, 0
      FROM OPENJSON(@SaksJson)
      WITH (NoSak int '$.NoSak', Berat decimal(18,3) '$.Berat') AS j
    `);
}

async function insertBongkarSusunOutputMixerTx({
  tx,
  noBongkarSusun,
  noMixer,
  saksJson,
}) {
  return new sql.Request(tx)
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
    .input("NoMixer", sql.VarChar(50), noMixer)
    .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
      INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
      SELECT @NoBongkarSusun, @NoMixer, j.NoSak
      FROM OPENJSON(@SaksJson)
      WITH (NoSak int '$.NoSak') AS j
    `);
}

module.exports = {
  setAuditContext,
  insertMixerHeaderFromBongkarSusunTx,
  insertMixerDetailsFromBongkarSusunTx,
  insertBongkarSusunOutputMixerTx,
  insertMixerHeader: async ({ tx, header, effectiveDateCreate, nowDateTime, idLokasiVal }) => {
    return new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), header.NoMixer)
      .input("IdMixer", sql.Int, header.IdMixer)
      .input("DateCreate", sql.Date, effectiveDateCreate)
      .input("IdStatus", sql.Int, header.IdStatus ?? 1)
      .input("CreateBy", sql.VarChar(50), header.CreateBy)
      .input("DateTimeCreate", sql.DateTime, nowDateTime)
      .input("Moisture", sql.Decimal(10, 3), header.Moisture ?? null)
      .input("MaxMeltTemp", sql.Decimal(10, 3), header.MaxMeltTemp ?? null)
      .input("MinMeltTemp", sql.Decimal(10, 3), header.MinMeltTemp ?? null)
      .input("MFI", sql.Decimal(10, 3), header.MFI ?? null)
      .input("Moisture2", sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input("Moisture3", sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input("Blok", sql.VarChar(50), header.Blok ?? null)
      .input("IdLokasi", sql.Int, idLokasiVal).query(`
        INSERT INTO dbo.Mixer_h (
          NoMixer, IdMixer, DateCreate, IdStatus, CreateBy, DateTimeCreate,
          Moisture, MaxMeltTemp, MinMeltTemp, MFI, Moisture2, Moisture3, Blok, IdLokasi
        )
        VALUES (
          @NoMixer, @IdMixer, @DateCreate, @IdStatus, @CreateBy, @DateTimeCreate,
          @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @Moisture2, @Moisture3, @Blok, @IdLokasi
        );
      `);
  },
  insertMixerDetailsBulk: async ({ tx, noMixer, detailsJson }) => {
    return new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), noMixer)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson).query(`
        INSERT INTO dbo.Mixer_d (NoMixer, NoSak, Berat, DateUsage, IsPartial)
        SELECT @NoMixer, j.NoSak, j.Berat, NULL, j.IsPartial
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak', Berat decimal(18,3) '$.Berat', IsPartial int '$.IsPartial') AS j;
      `);
  },
  insertMixerProduksiOutputBulk: async ({ tx, noProduksi, noMixer, detailsJson }) => {
    return new sql.Request(tx)
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("NoMixer", sql.VarChar(50), noMixer)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson).query(`
        INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
        SELECT @NoProduksi, @NoMixer, j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `);
  },
  insertInjectProduksiOutputBulk: async ({ tx, noProduksi, noMixer, detailsJson }) => {
    return new sql.Request(tx)
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("NoMixer", sql.VarChar(50), noMixer)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson).query(`
        INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
        SELECT @NoProduksi, @NoMixer, j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `);
  },
  insertBongkarSusunOutputBulk: async ({ tx, noBongkarSusun, noMixer, detailsJson }) => {
    return new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("NoMixer", sql.VarChar(50), noMixer)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson).query(`
        INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
        SELECT @NoBongkarSusun, @NoMixer, j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `);
  },
  updateMixerHeaderDynamic: async ({ tx, noMixer, setParts, bind }) => {
    const req = new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer);
    for (const b of bind) req.input(b.name, b.type, b.value);
    if (setParts.length === 0) return;
    await req.query(`UPDATE dbo.Mixer_h SET ${setParts.join(", ")} WHERE NoMixer = @NoMixer`);
  },
  deleteActiveDetails: async (tx, noMixer) =>
    new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer).query(
      `DELETE FROM dbo.Mixer_d WHERE NoMixer = @NoMixer AND DateUsage IS NULL`,
    ),
  clearOutputsByNoMixer: async (tx, noMixer) =>
    new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer).query(`
      DELETE FROM dbo.MixerProduksiOutput WHERE NoMixer = @NoMixer;
      DELETE FROM dbo.InjectProduksiOutputMixer WHERE NoMixer = @NoMixer;
    `),
  insertMixerProduksiOutputFromExistingDetails: async ({ tx, noProduksi, noMixer }) => {
    const r = await new sql.Request(tx)
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("NoMixer", sql.VarChar(50), noMixer).query(`
        INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
        SELECT @NoProduksi, @NoMixer, d.NoSak
        FROM dbo.Mixer_d d
        WHERE d.NoMixer = @NoMixer AND d.DateUsage IS NULL;
      `);
    return r.rowsAffected?.[0] || 0;
  },
  insertInjectOutputFromExistingDetails: async ({ tx, noProduksi, noMixer }) => {
    const r = await new sql.Request(tx)
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("NoMixer", sql.VarChar(50), noMixer).query(`
        INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
        SELECT @NoProduksi, @NoMixer, d.NoSak
        FROM dbo.Mixer_d d
        WHERE d.NoMixer = @NoMixer AND d.DateUsage IS NULL;
      `);
    return r.rowsAffected?.[0] || 0;
  },
  deleteOutputsForDeleteCascade: async (tx, noMixer) => {
    const r1 = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer)
      .query(`DELETE FROM dbo.MixerProduksiOutput WHERE NoMixer = @NoMixer`);
    const r2 = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer)
      .query(`DELETE FROM dbo.BongkarSusunOutputMixer WHERE NoMixer = @NoMixer`);
    const r3 = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer)
      .query(`DELETE FROM dbo.InjectProduksiOutputMixer WHERE NoMixer = @NoMixer`);
    return {
      mixerProduksiOutput: r1.rowsAffected?.[0] ?? 0,
      bongkarSusunOutputMixer: r2.rowsAffected?.[0] ?? 0,
      injectProduksiOutputMixer: r3.rowsAffected?.[0] ?? 0,
    };
  },
  deletePartialInputReferences: async (tx, noMixer) => {
    const rb = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer).query(`
      DELETE bip
      FROM dbo.BrokerProduksiInputMixerPartial AS bip
      INNER JOIN dbo.MixerPartial AS mp ON mp.NoMixerPartial = bip.NoMixerPartial
      WHERE mp.NoMixer = @NoMixer
    `);
    const ri = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer).query(`
      DELETE iip
      FROM dbo.InjectProduksiInputMixerPartial AS iip
      INNER JOIN dbo.MixerPartial AS mp ON mp.NoMixerPartial = iip.NoMixerPartial
      WHERE mp.NoMixer = @NoMixer
    `);
    const rm = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer).query(`
      DELETE mip
      FROM dbo.MixerProduksiInputMixerPartial AS mip
      INNER JOIN dbo.MixerPartial AS mp ON mp.NoMixerPartial = mip.NoMixerPartial
      WHERE mp.NoMixer = @NoMixer
    `);
    return {
      brokerInputPartial: rb.rowsAffected?.[0] ?? 0,
      injectInputPartial: ri.rowsAffected?.[0] ?? 0,
      mixerInputPartial: rm.rowsAffected?.[0] ?? 0,
    };
  },
  deleteMixerPartials: async (tx, noMixer) => {
    const r = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer)
      .query(`DELETE FROM dbo.MixerPartial WHERE NoMixer = @NoMixer`);
    return r.rowsAffected?.[0] ?? 0;
  },
  deleteMixerHeader: async (tx, noMixer) => {
    const r = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer)
      .query(`DELETE FROM dbo.Mixer_h WHERE NoMixer = @NoMixer`);
    return r.rowsAffected?.[0] ?? 0;
  },
  incrementHasBeenPrinted: async (tx, noMixer) => {
    const rs = await new sql.Request(tx).input("NoMixer", sql.VarChar(50), noMixer).query(`
      DECLARE @out TABLE (NoMixer varchar(50), HasBeenPrinted int);
      UPDATE dbo.Mixer_h
      SET HasBeenPrinted = ISNULL(HasBeenPrinted, 0) + 1
      OUTPUT INSERTED.NoMixer, INSERTED.HasBeenPrinted INTO @out
      WHERE NoMixer = @NoMixer;
      SELECT NoMixer, HasBeenPrinted FROM @out;
    `);
    return rs.recordset?.[0] || null;
  },
};

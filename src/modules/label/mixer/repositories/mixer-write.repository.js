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
};

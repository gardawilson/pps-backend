/* ===== [dbo].[tr_Audit_Mixer_h] ON [dbo].[Mixer_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_Mixer_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Mixer_h]
ON [dbo].[Mixer_h]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
  SET NOCOUNT ON;

  -- ✅ actor = actor_id (ID user) dari SESSION_CONTEXT (fallback actor lama, terakhir login DB)
  DECLARE @actor nvarchar(128) =
    COALESCE(
      CONVERT(nvarchar(128), TRY_CONVERT(int, SESSION_CONTEXT(N'actor_id'))),
      CAST(SESSION_CONTEXT(N'actor') AS nvarchar(128)),
      SUSER_SNAME()
    );

  DECLARE @rid nvarchar(64) =
    CAST(SESSION_CONTEXT(N'request_id') AS nvarchar(64));

  /* =====================
     INSERT
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'INSERT',
    'Mixer_h',
    @actor,
    @rid,
    CONCAT('{"NoMixer":"', i.NoMixer, '"}'),
    NULL,
    (
      SELECT
        i.NoMixer,
        i.IdMixer,
        i.DateCreate,
        i.IdWarehouse,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        CAST(i.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(i.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(i.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(i.MFI         AS decimal(18,3)) AS MFI,
        CAST(i.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(i.Moisture3   AS decimal(18,3)) AS Moisture3,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoMixer = i.NoMixer
  WHERE d.NoMixer IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'Mixer_h',
    @actor,
    @rid,
    CONCAT('{"NoMixer":"', i.NoMixer, '"}'),
    (
      SELECT
        d.NoMixer,
        d.IdMixer,
        d.DateCreate,
        d.IdWarehouse,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        CAST(d.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(d.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(d.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(d.MFI         AS decimal(18,3)) AS MFI,
        CAST(d.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(d.Moisture3   AS decimal(18,3)) AS Moisture3,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoMixer,
        i.IdMixer,
        i.DateCreate,
        i.IdWarehouse,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        CAST(i.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(i.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(i.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(i.MFI         AS decimal(18,3)) AS MFI,
        CAST(i.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(i.Moisture3   AS decimal(18,3)) AS Moisture3,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoMixer = i.NoMixer;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'Mixer_h',
    @actor,
    @rid,
    CONCAT('{"NoMixer":"', d.NoMixer, '"}'),
    (
      SELECT
        d.NoMixer,
        d.IdMixer,
        d.DateCreate,
        d.IdWarehouse,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        CAST(d.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(d.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(d.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(d.MFI         AS decimal(18,3)) AS MFI,
        CAST(d.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(d.Moisture3   AS decimal(18,3)) AS Moisture3,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoMixer = d.NoMixer
  WHERE i.NoMixer IS NULL;

END;
GO

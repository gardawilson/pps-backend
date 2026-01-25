/* ===== [dbo].[tr_Audit_Broker_h] ON [dbo].[Broker_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_Broker_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Broker_h]
ON [dbo].[Broker_h]
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
    'Broker_h',
    @actor,
    @rid,
    CONCAT('{"NoBroker":"', i.NoBroker, '"}'),
    NULL,
    (
      SELECT
        i.NoBroker,
        i.DateCreate,
        i.IdJenisPlastik,
        i.IdWarehouse,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        CAST(i.Density     AS decimal(18,3)) AS Density,
        CAST(i.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(i.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(i.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(i.MFI         AS decimal(18,3)) AS MFI,
        i.VisualNote,
        CAST(i.Density2    AS decimal(18,3)) AS Density2,
        CAST(i.Density3    AS decimal(18,3)) AS Density3,
        CAST(i.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(i.Moisture3   AS decimal(18,3)) AS Moisture3,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoBroker = i.NoBroker
  WHERE d.NoBroker IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'Broker_h',
    @actor,
    @rid,
    CONCAT('{"NoBroker":"', i.NoBroker, '"}'),
    (
      SELECT
        d.NoBroker,
        d.DateCreate,
        d.IdJenisPlastik,
        d.IdWarehouse,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        CAST(d.Density     AS decimal(18,3)) AS Density,
        CAST(d.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(d.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(d.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(d.MFI         AS decimal(18,3)) AS MFI,
        d.VisualNote,
        CAST(d.Density2    AS decimal(18,3)) AS Density2,
        CAST(d.Density3    AS decimal(18,3)) AS Density3,
        CAST(d.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(d.Moisture3   AS decimal(18,3)) AS Moisture3,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoBroker,
        i.DateCreate,
        i.IdJenisPlastik,
        i.IdWarehouse,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        CAST(i.Density     AS decimal(18,3)) AS Density,
        CAST(i.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(i.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(i.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(i.MFI         AS decimal(18,3)) AS MFI,
        i.VisualNote,
        CAST(i.Density2    AS decimal(18,3)) AS Density2,
        CAST(i.Density3    AS decimal(18,3)) AS Density3,
        CAST(i.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(i.Moisture3   AS decimal(18,3)) AS Moisture3,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoBroker = i.NoBroker;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'Broker_h',
    @actor,
    @rid,
    CONCAT('{"NoBroker":"', d.NoBroker, '"}'),
    (
      SELECT
        d.NoBroker,
        d.DateCreate,
        d.IdJenisPlastik,
        d.IdWarehouse,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        CAST(d.Density     AS decimal(18,3)) AS Density,
        CAST(d.Moisture    AS decimal(18,3)) AS Moisture,
        CAST(d.MaxMeltTemp AS decimal(18,3)) AS MaxMeltTemp,
        CAST(d.MinMeltTemp AS decimal(18,3)) AS MinMeltTemp,
        CAST(d.MFI         AS decimal(18,3)) AS MFI,
        d.VisualNote,
        CAST(d.Density2    AS decimal(18,3)) AS Density2,
        CAST(d.Density3    AS decimal(18,3)) AS Density3,
        CAST(d.Moisture2   AS decimal(18,3)) AS Moisture2,
        CAST(d.Moisture3   AS decimal(18,3)) AS Moisture3,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoBroker = d.NoBroker
  WHERE i.NoBroker IS NULL;
END;

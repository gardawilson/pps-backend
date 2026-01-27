/* ===== [dbo].[tr_Audit_RejectV2] ON [dbo].[RejectV2] ===== */
-- =============================================
-- TRIGGER: tr_Audit_RejectV2
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: NoReject
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_RejectV2]
ON [dbo].[RejectV2]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
  SET NOCOUNT ON;

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
    'RejectV2',
    @actor,
    @rid,
    CONCAT('{"NoReject":"', i.NoReject, '"}'),
    NULL,
    (
      SELECT
        i.NoReject,
        i.IdReject,
        i.DateCreate,
        i.DateUsage,
        i.IdWarehouse,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.Jam,
        i.CreateBy,
        i.DateTimeCreate,
        i.IsPartial,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoReject = i.NoReject
  WHERE d.NoReject IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'RejectV2',
    @actor,
    @rid,
    CONCAT('{"NoReject":"', i.NoReject, '"}'),
    (
      SELECT
        d.NoReject,
        d.IdReject,
        d.DateCreate,
        d.DateUsage,
        d.IdWarehouse,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.Jam,
        d.CreateBy,
        d.DateTimeCreate,
        d.IsPartial,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoReject,
        i.IdReject,
        i.DateCreate,
        i.DateUsage,
        i.IdWarehouse,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.Jam,
        i.CreateBy,
        i.DateTimeCreate,
        i.IsPartial,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoReject = i.NoReject;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'RejectV2',
    @actor,
    @rid,
    CONCAT('{"NoReject":"', d.NoReject, '"}'),
    (
      SELECT
        d.NoReject,
        d.IdReject,
        d.DateCreate,
        d.DateUsage,
        d.IdWarehouse,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.Jam,
        d.CreateBy,
        d.DateTimeCreate,
        d.IsPartial,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoReject = d.NoReject
  WHERE i.NoReject IS NULL;

END;
GO

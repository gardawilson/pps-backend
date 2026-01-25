/* ===== [dbo].[tr_Audit_Crusher] ON [dbo].[Crusher] ===== */
-- =============================================
-- TRIGGER: tr_Audit_Crusher
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Crusher]
ON [dbo].[Crusher]
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
    'Crusher',
    @actor,
    @rid,
    CONCAT('{"NoCrusher":"', i.NoCrusher, '"}'),
    NULL,
    (
      SELECT
        i.NoCrusher,
        i.DateCreate,
        i.IdCrusher,
        i.IdWarehouse,
        i.DateUsage,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoCrusher = i.NoCrusher
  WHERE d.NoCrusher IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'Crusher',
    @actor,
    @rid,
    CONCAT('{"NoCrusher":"', i.NoCrusher, '"}'),
    (
      SELECT
        d.NoCrusher,
        d.DateCreate,
        d.IdCrusher,
        d.IdWarehouse,
        d.DateUsage,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoCrusher,
        i.DateCreate,
        i.IdCrusher,
        i.IdWarehouse,
        i.DateUsage,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoCrusher = i.NoCrusher;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'Crusher',
    @actor,
    @rid,
    CONCAT('{"NoCrusher":"', d.NoCrusher, '"}'),
    (
      SELECT
        d.NoCrusher,
        d.DateCreate,
        d.IdCrusher,
        d.IdWarehouse,
        d.DateUsage,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoCrusher = d.NoCrusher
  WHERE i.NoCrusher IS NULL;
END;

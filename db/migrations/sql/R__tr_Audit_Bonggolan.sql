/* ===== [dbo].[tr_Audit_Bonggolan] ON [dbo].[Bonggolan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_Bonggolan
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: NoBonggolan
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Bonggolan]
ON [dbo].[Bonggolan]
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
    'Bonggolan',
    @actor,
    @rid,
    CONCAT('{"NoBonggolan":"', i.NoBonggolan, '"}'),
    NULL,
    (
      SELECT
        i.NoBonggolan,
        i.DateCreate,
        i.IdBonggolan,
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
  LEFT JOIN deleted d ON d.NoBonggolan = i.NoBonggolan
  WHERE d.NoBonggolan IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'Bonggolan',
    @actor,
    @rid,
    CONCAT('{"NoBonggolan":"', i.NoBonggolan, '"}'),
    (
      SELECT
        d.NoBonggolan,
        d.DateCreate,
        d.IdBonggolan,
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
        i.NoBonggolan,
        i.DateCreate,
        i.IdBonggolan,
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
  JOIN deleted d ON d.NoBonggolan = i.NoBonggolan;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'Bonggolan',
    @actor,
    @rid,
    CONCAT('{"NoBonggolan":"', d.NoBonggolan, '"}'),
    (
      SELECT
        d.NoBonggolan,
        d.DateCreate,
        d.IdBonggolan,
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
  LEFT JOIN inserted i ON i.NoBonggolan = d.NoBonggolan
  WHERE i.NoBonggolan IS NULL;

END;
GO

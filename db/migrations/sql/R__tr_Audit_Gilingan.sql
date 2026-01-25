/* ===== [dbo].[tr_Audit_Gilingan] ON [dbo].[Gilingan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_Gilingan
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Gilingan]
ON [dbo].[Gilingan]
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
    'Gilingan',
    @actor,
    @rid,
    CONCAT('{"NoGilingan":"', i.NoGilingan, '"}'),
    NULL,
    (
      SELECT
        i.NoGilingan,
        i.DateCreate,
        i.IdGilingan,
        i.DateUsage,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IsPartial,
        i.IdWarehouse,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoGilingan = i.NoGilingan
  WHERE d.NoGilingan IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'Gilingan',
    @actor,
    @rid,
    CONCAT('{"NoGilingan":"', i.NoGilingan, '"}'),
    (
      SELECT
        d.NoGilingan,
        d.DateCreate,
        d.IdGilingan,
        d.DateUsage,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IsPartial,
        d.IdWarehouse,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoGilingan,
        i.DateCreate,
        i.IdGilingan,
        i.DateUsage,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IsPartial,
        i.IdWarehouse,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoGilingan = i.NoGilingan;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'Gilingan',
    @actor,
    @rid,
    CONCAT('{"NoGilingan":"', d.NoGilingan, '"}'),
    (
      SELECT
        d.NoGilingan,
        d.DateCreate,
        d.IdGilingan,
        d.DateUsage,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IsPartial,
        d.IdWarehouse,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoGilingan = d.NoGilingan
  WHERE i.NoGilingan IS NULL;
END;
GO

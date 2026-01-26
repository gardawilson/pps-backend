/* ===== [dbo].[tr_Audit_BarangJadi] ON [dbo].[BarangJadi] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BarangJadi
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BarangJadi]
ON [dbo].[BarangJadi]
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
    'BarangJadi',
    @actor,
    @rid,
    CONCAT('{"NoBJ":"', i.NoBJ, '"}'),
    NULL,
    (
      SELECT
        i.NoBJ,
        i.IdBJ,
        i.DateCreate,
        i.DateUsage,
        i.Jam,
        i.Pcs,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IdWarehouse,
        i.CreateBy,
        i.DateTimeCreate,
        i.IsPartial,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoBJ = i.NoBJ
  WHERE d.NoBJ IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'BarangJadi',
    @actor,
    @rid,
    CONCAT('{"NoBJ":"', i.NoBJ, '"}'),
    (
      SELECT
        d.NoBJ,
        d.IdBJ,
        d.DateCreate,
        d.DateUsage,
        d.Jam,
        d.Pcs,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IdWarehouse,
        d.CreateBy,
        d.DateTimeCreate,
        d.IsPartial,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoBJ,
        i.IdBJ,
        i.DateCreate,
        i.DateUsage,
        i.Jam,
        i.Pcs,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IdWarehouse,
        i.CreateBy,
        i.DateTimeCreate,
        i.IsPartial,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoBJ = i.NoBJ;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'BarangJadi',
    @actor,
    @rid,
    CONCAT('{"NoBJ":"', d.NoBJ, '"}'),
    (
      SELECT
        d.NoBJ,
        d.IdBJ,
        d.DateCreate,
        d.DateUsage,
        d.Jam,
        d.Pcs,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IdWarehouse,
        d.CreateBy,
        d.DateTimeCreate,
        d.IsPartial,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoBJ = d.NoBJ
  WHERE i.NoBJ IS NULL;

END;
GO

/* ===== [dbo].[tr_Audit_FurnitureWIP] ON [dbo].[FurnitureWIP] ===== */
-- =============================================
-- TRIGGER: tr_Audit_FurnitureWIP
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_FurnitureWIP]
ON [dbo].[FurnitureWIP]
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
    'FurnitureWIP',
    @actor,
    @rid,
    CONCAT('{"NoFurnitureWIP":"', i.NoFurnitureWIP, '"}'),
    NULL,
    (
      SELECT
        i.NoFurnitureWIP,
        i.DateCreate,
        i.Jam,
        i.Pcs,
        i.IDFurnitureWIP,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IsPartial,
        i.DateUsage,
        i.IdWarehouse,
        i.IdWarna,
        i.CreateBy,
        i.DateTimeCreate,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoFurnitureWIP = i.NoFurnitureWIP
  WHERE d.NoFurnitureWIP IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'FurnitureWIP',
    @actor,
    @rid,
    CONCAT('{"NoFurnitureWIP":"', i.NoFurnitureWIP, '"}'),
    (
      SELECT
        d.NoFurnitureWIP,
        d.DateCreate,
        d.Jam,
        d.Pcs,
        d.IDFurnitureWIP,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IsPartial,
        d.DateUsage,
        d.IdWarehouse,
        d.IdWarna,
        d.CreateBy,
        d.DateTimeCreate,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoFurnitureWIP,
        i.DateCreate,
        i.Jam,
        i.Pcs,
        i.IDFurnitureWIP,
        CAST(i.Berat AS decimal(18,3)) AS Berat,
        i.IsPartial,
        i.DateUsage,
        i.IdWarehouse,
        i.IdWarna,
        i.CreateBy,
        i.DateTimeCreate,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoFurnitureWIP = i.NoFurnitureWIP;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'FurnitureWIP',
    @actor,
    @rid,
    CONCAT('{"NoFurnitureWIP":"', d.NoFurnitureWIP, '"}'),
    (
      SELECT
        d.NoFurnitureWIP,
        d.DateCreate,
        d.Jam,
        d.Pcs,
        d.IDFurnitureWIP,
        CAST(d.Berat AS decimal(18,3)) AS Berat,
        d.IsPartial,
        d.DateUsage,
        d.IdWarehouse,
        d.IdWarna,
        d.CreateBy,
        d.DateTimeCreate,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoFurnitureWIP = d.NoFurnitureWIP
  WHERE i.NoFurnitureWIP IS NULL;

END;
GO

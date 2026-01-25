/* ===== [dbo].[tr_Audit_Washing_h] ON [dbo].[Washing_h] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Washing_h]
ON [dbo].[Washing_h]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @actor nvarchar(128) =
    COALESCE(
      CONVERT(nvarchar(128), TRY_CONVERT(int, SESSION_CONTEXT(N'actor_id'))),
      CAST(SESSION_CONTEXT(N'actor') AS nvarchar(128)),   -- fallback kalau masih ada trigger lain
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
    'Washing_h',
    @actor,
    @rid,
    CONCAT('{"NoWashing":"', i.NoWashing, '"}'),
    NULL,
    (
      SELECT
        i.NoWashing,
        i.IdJenisPlastik,
        i.IdWarehouse,
        i.DateCreate,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        CAST(i.Density   AS decimal(18,3)) AS Density,
        CAST(i.Moisture  AS decimal(18,3)) AS Moisture,
        CAST(i.Density2  AS decimal(18,3)) AS Density2,
        CAST(i.Density3  AS decimal(18,3)) AS Density3,
        CAST(i.Moisture2 AS decimal(18,3)) AS Moisture2,
        CAST(i.Moisture3 AS decimal(18,3)) AS Moisture3,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d ON d.NoWashing = i.NoWashing
  WHERE d.NoWashing IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'Washing_h',
    @actor,
    @rid,
    CONCAT('{"NoWashing":"', i.NoWashing, '"}'),
    (
      SELECT
        d.NoWashing,
        d.IdJenisPlastik,
        d.IdWarehouse,
        d.DateCreate,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        CAST(d.Density   AS decimal(18,3)) AS Density,
        CAST(d.Moisture  AS decimal(18,3)) AS Moisture,
        CAST(d.Density2  AS decimal(18,3)) AS Density2,
        CAST(d.Density3  AS decimal(18,3)) AS Density3,
        CAST(d.Moisture2 AS decimal(18,3)) AS Moisture2,
        CAST(d.Moisture3 AS decimal(18,3)) AS Moisture3,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoWashing,
        i.IdJenisPlastik,
        i.IdWarehouse,
        i.DateCreate,
        i.IdStatus,
        i.CreateBy,
        i.DateTimeCreate,
        CAST(i.Density   AS decimal(18,3)) AS Density,
        CAST(i.Moisture  AS decimal(18,3)) AS Moisture,
        CAST(i.Density2  AS decimal(18,3)) AS Density2,
        CAST(i.Density3  AS decimal(18,3)) AS Density3,
        CAST(i.Moisture2 AS decimal(18,3)) AS Moisture2,
        CAST(i.Moisture3 AS decimal(18,3)) AS Moisture3,
        i.Blok,
        i.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d ON d.NoWashing = i.NoWashing;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'Washing_h',
    @actor,
    @rid,
    CONCAT('{"NoWashing":"', d.NoWashing, '"}'),
    (
      SELECT
        d.NoWashing,
        d.IdJenisPlastik,
        d.IdWarehouse,
        d.DateCreate,
        d.IdStatus,
        d.CreateBy,
        d.DateTimeCreate,
        CAST(d.Density   AS decimal(18,3)) AS Density,
        CAST(d.Moisture  AS decimal(18,3)) AS Moisture,
        CAST(d.Density2  AS decimal(18,3)) AS Density2,
        CAST(d.Density3  AS decimal(18,3)) AS Density3,
        CAST(d.Moisture2 AS decimal(18,3)) AS Moisture2,
        CAST(d.Moisture3 AS decimal(18,3)) AS Moisture3,
        d.Blok,
        d.IdLokasi
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i ON i.NoWashing = d.NoWashing
  WHERE i.NoWashing IS NULL;
END;

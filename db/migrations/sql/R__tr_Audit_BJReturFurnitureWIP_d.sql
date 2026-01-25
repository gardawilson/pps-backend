/* ===== [dbo].[tr_Audit_BJReturFurnitureWIP_d] ON [dbo].[BJReturFurnitureWIP_d] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BJReturFurnitureWIP_d
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: (NoRetur, NoFurnitureWIP)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJReturFurnitureWIP_d]
ON [dbo].[BJReturFurnitureWIP_d]
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
    'BJReturFurnitureWIP_d',
    @actor,
    @rid,
    CONCAT(
      '{"NoRetur":"', i.NoRetur,
      '","NoFurnitureWIP":"', i.NoFurnitureWIP, '"}'
    ),
    NULL,
    (
      SELECT
        i.NoRetur,
        i.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d
    ON d.NoRetur        = i.NoRetur
   AND d.NoFurnitureWIP = i.NoFurnitureWIP
  WHERE d.NoRetur IS NULL
    AND d.NoFurnitureWIP IS NULL;

  /* =====================
     UPDATE
     (jarang terjadi di bridge table, tapi tetap di-handle)
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'BJReturFurnitureWIP_d',
    @actor,
    @rid,
    CONCAT(
      '{"NoRetur":"', i.NoRetur,
      '","NoFurnitureWIP":"', i.NoFurnitureWIP, '"}'
    ),
    (
      SELECT
        d.NoRetur,
        d.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoRetur,
        i.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d
    ON d.NoRetur        = i.NoRetur
   AND d.NoFurnitureWIP = i.NoFurnitureWIP;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'BJReturFurnitureWIP_d',
    @actor,
    @rid,
    CONCAT(
      '{"NoRetur":"', d.NoRetur,
      '","NoFurnitureWIP":"', d.NoFurnitureWIP, '"}'
    ),
    (
      SELECT
        d.NoRetur,
        d.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i
    ON i.NoRetur        = d.NoRetur
   AND i.NoFurnitureWIP = d.NoFurnitureWIP
  WHERE i.NoRetur IS NULL
    AND i.NoFurnitureWIP IS NULL;

END;
GO

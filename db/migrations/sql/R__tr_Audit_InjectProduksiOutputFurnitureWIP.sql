/* ===== [dbo].[tr_Audit_InjectProduksiOutputFurnitureWIP] ON [dbo].[InjectProduksiOutputFurnitureWIP] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksiOutputFurnitureWIP
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: (NoProduksi, NoFurnitureWIP)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksiOutputFurnitureWIP]
ON [dbo].[InjectProduksiOutputFurnitureWIP]
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
    'InjectProduksiOutputFurnitureWIP',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', i.NoProduksi,
      '","NoFurnitureWIP":"', i.NoFurnitureWIP, '"}'
    ),
    NULL,
    (
      SELECT
        i.NoProduksi,
        i.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d
    ON d.NoProduksi      = i.NoProduksi
   AND d.NoFurnitureWIP  = i.NoFurnitureWIP
  WHERE d.NoProduksi IS NULL
    AND d.NoFurnitureWIP IS NULL;

  /* =====================
     UPDATE
     (biasanya jarang di bridge table, tapi tetap di-handle)
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'InjectProduksiOutputFurnitureWIP',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', i.NoProduksi,
      '","NoFurnitureWIP":"', i.NoFurnitureWIP, '"}'
    ),
    (
      SELECT
        d.NoProduksi,
        d.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoProduksi,
        i.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d
    ON d.NoProduksi      = i.NoProduksi
   AND d.NoFurnitureWIP  = i.NoFurnitureWIP;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'InjectProduksiOutputFurnitureWIP',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', d.NoProduksi,
      '","NoFurnitureWIP":"', d.NoFurnitureWIP, '"}'
    ),
    (
      SELECT
        d.NoProduksi,
        d.NoFurnitureWIP
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i
    ON i.NoProduksi      = d.NoProduksi
   AND i.NoFurnitureWIP  = d.NoFurnitureWIP
  WHERE i.NoProduksi IS NULL
    AND i.NoFurnitureWIP IS NULL;

END;
GO

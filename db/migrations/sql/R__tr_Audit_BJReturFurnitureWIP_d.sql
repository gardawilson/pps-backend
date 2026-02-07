/* ===== [dbo].[tr_Audit_BJReturFurnitureWIP_d]
         ON [dbo].[BJReturFurnitureWIP_d] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BJReturFurnitureWIP_d
-- PK     : NoFurnitureWIP + NoRetur
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join FurnitureWIP untuk ambil Pcs
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

  /* =========================================================
     1) INSERT-only => PRODUCE (DETAIL)
     ========================================================= */
  ;WITH insOnly AS (
    SELECT
      i.NoRetur,
      i.NoFurnitureWIP,
      fw.Pcs
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoRetur        = i.NoRetur
     AND d.NoFurnitureWIP = i.NoFurnitureWIP
    LEFT JOIN dbo.FurnitureWIP fw
      ON fw.NoFurnitureWIP = i.NoFurnitureWIP
    WHERE d.NoRetur IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'BJReturFurnitureWIP_d',
    @actor,
    @rid,
    (SELECT
       i.NoFurnitureWIP,
       i.NoRetur
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoRetur,
       i.NoFurnitureWIP,
       i.Pcs
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT
      d.NoRetur,
      d.NoFurnitureWIP,
      fw.Pcs
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoRetur        = d.NoRetur
     AND i.NoFurnitureWIP = d.NoFurnitureWIP
    LEFT JOIN dbo.FurnitureWIP fw
      ON fw.NoFurnitureWIP = d.NoFurnitureWIP
    WHERE i.NoRetur IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'BJReturFurnitureWIP_d',
    @actor,
    @rid,
    (SELECT
       d.NoFurnitureWIP,
       d.NoRetur
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoRetur,
       d.NoFurnitureWIP,
       d.Pcs
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL
  FROM delOnly d;

  /* =========================================================
     3) UPDATE => UPDATE (DETAIL)
     ========================================================= */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail
      (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJReturFurnitureWIP_d',
      @actor,
      @rid,
      (SELECT
         i.NoFurnitureWIP,
         i.NoRetur
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoRetur,
         d.NoFurnitureWIP,
         fwOld.Pcs
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoRetur,
         i.NoFurnitureWIP,
         fwNew.Pcs
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoRetur        = i.NoRetur
     AND d.NoFurnitureWIP = i.NoFurnitureWIP
    LEFT JOIN dbo.FurnitureWIP fwOld
      ON fwOld.NoFurnitureWIP = d.NoFurnitureWIP
    LEFT JOIN dbo.FurnitureWIP fwNew
      ON fwNew.NoFurnitureWIP = i.NoFurnitureWIP;
  END
END;
GO

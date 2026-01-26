/* ===== [dbo].[tr_Audit_BJReturFurnitureWIP_d] ON [dbo].[BJReturFurnitureWIP_d] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BJReturFurnitureWIP_d
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- ✅ PK: NoFurnitureWIP (parent document)
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
     ✅ Helper: PK menggunakan NoFurnitureWIP (parent)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoFurnitureWIP FROM inserted
    UNION
    SELECT NoFurnitureWIP FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoFurnitureWIP) = 1
          THEN CONCAT('{"NoFurnitureWIP":"', MAX(NoFurnitureWIP), '"}')
        ELSE
          CONCAT(
            '{"NoFurnitureWIPList":',
            (SELECT DISTINCT NoFurnitureWIP FROM x FOR JSON PATH),
            '}'
          )
      END
  FROM x;

  /* =====================
     INSERT (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'INSERT',
      'BJReturFurnitureWIP_d',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoRetur,
          i.NoFurnitureWIP
        FROM inserted i
        ORDER BY i.NoFurnitureWIP, i.NoRetur
        FOR JSON PATH
      );
  END

  /* =====================
     DELETE (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'DELETE',
      'BJReturFurnitureWIP_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoRetur,
          d.NoFurnitureWIP
        FROM deleted d
        ORDER BY d.NoFurnitureWIP, d.NoRetur
        FOR JSON PATH
      ),
      NULL;
  END

  /* =====================
     UPDATE (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJReturFurnitureWIP_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoRetur,
          d.NoFurnitureWIP
        FROM deleted d
        ORDER BY d.NoFurnitureWIP, d.NoRetur
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoRetur,
          i.NoFurnitureWIP
        FROM inserted i
        ORDER BY i.NoFurnitureWIP, i.NoRetur
        FOR JSON PATH
      );
  END
END;
GO

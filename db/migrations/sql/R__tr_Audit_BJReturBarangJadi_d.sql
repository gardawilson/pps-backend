/* ===== [dbo].[tr_Audit_BJReturBarangJadi_d] ON [dbo].[BJReturBarangJadi_d] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BJReturBarangJadi_d
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- ✅ PK: NoBJ (parent document)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJReturBarangJadi_d]
ON [dbo].[BJReturBarangJadi_d]
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
     ✅ Helper: PK menggunakan NoBJ (parent)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoBJ FROM inserted
    UNION
    SELECT NoBJ FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoBJ) = 1
          THEN CONCAT('{"NoBJ":"', MAX(NoBJ), '"}')
        ELSE
          CONCAT(
            '{"NoBJList":',
            (SELECT DISTINCT NoBJ FROM x FOR JSON PATH),
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
      'BJReturBarangJadi_d',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoRetur,
          i.NoBJ
        FROM inserted i
        ORDER BY i.NoBJ, i.NoRetur
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
      'BJReturBarangJadi_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoRetur,
          d.NoBJ
        FROM deleted d
        ORDER BY d.NoBJ, d.NoRetur
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
      'BJReturBarangJadi_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoRetur,
          d.NoBJ
        FROM deleted d
        ORDER BY d.NoBJ, d.NoRetur
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoRetur,
          i.NoBJ
        FROM inserted i
        ORDER BY i.NoBJ, i.NoRetur
        FOR JSON PATH
      );
  END
END;
GO

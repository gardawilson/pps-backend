SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunOutputCrusher] ON [dbo].[BongkarSusunOutputCrusher] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputCrusher
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- ✅ PK: NoCrusher (parent document)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputCrusher]
ON [dbo].[BongkarSusunOutputCrusher]
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
     ✅ Helper: PK ringkas (NoCrusher tunggal / list)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoCrusher FROM inserted
    UNION
    SELECT NoCrusher FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoCrusher) = 1
          THEN CONCAT('{"NoCrusher":"', MAX(NoCrusher), '"}')
        ELSE
          CONCAT(
            '{"NoCrusherList":',
            (SELECT DISTINCT NoCrusher FROM x FOR JSON PATH),
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
      'BongkarSusunOutputCrusher',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoBongkarSusun,
          i.NoCrusher
        FROM inserted i
        ORDER BY i.NoCrusher, i.NoBongkarSusun
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
      'BongkarSusunOutputCrusher',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoCrusher
        FROM deleted d
        ORDER BY d.NoCrusher, d.NoBongkarSusun
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
      'BongkarSusunOutputCrusher',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoCrusher
        FROM deleted d
        ORDER BY d.NoCrusher, d.NoBongkarSusun
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoBongkarSusun,
          i.NoCrusher
        FROM inserted i
        ORDER BY i.NoCrusher, i.NoBongkarSusun
        FOR JSON PATH
      );
  END
END;
GO

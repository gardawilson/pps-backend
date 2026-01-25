SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunOutputCrusher] ON [dbo].[BongkarSusunOutputCrusher] ===== */
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

  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoBongkarSusun FROM inserted
    UNION
    SELECT NoBongkarSusun FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoBongkarSusun) = 1
          THEN CONCAT('{"NoBongkarSusun":"', MAX(NoBongkarSusun), '"}')
        ELSE
          CONCAT(
            '{"NoBongkarSusunList":',
            (SELECT DISTINCT NoBongkarSusun FROM x FOR JSON PATH),
            '}'
          )
      END
  FROM x;

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
        SELECT i.NoBongkarSusun, i.NoCrusher
        FROM inserted i
        ORDER BY i.NoBongkarSusun, i.NoCrusher
        FOR JSON PATH
      );
  END

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
        SELECT d.NoBongkarSusun, d.NoCrusher
        FROM deleted d
        ORDER BY d.NoBongkarSusun, d.NoCrusher
        FOR JSON PATH
      ),
      NULL;
  END

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
        SELECT d.NoBongkarSusun, d.NoCrusher
        FROM deleted d
        ORDER BY d.NoBongkarSusun, d.NoCrusher
        FOR JSON PATH
      ),
      (
        SELECT i.NoBongkarSusun, i.NoCrusher
        FROM inserted i
        ORDER BY i.NoBongkarSusun, i.NoCrusher
        FOR JSON PATH
      );
  END
END;
GO

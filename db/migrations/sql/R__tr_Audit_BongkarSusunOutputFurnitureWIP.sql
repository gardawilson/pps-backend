SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunOutputFurnitureWIP] ON [dbo].[BongkarSusunOutputFurnitureWIP] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputFurnitureWIP]
ON [dbo].[BongkarSusunOutputFurnitureWIP]
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

  /* =====================
     INSERT
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'INSERT',
      'BongkarSusunOutputFurnitureWIP',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT i.NoBongkarSusun, i.NoFurnitureWIP
        FROM inserted i
        ORDER BY i.NoBongkarSusun, i.NoFurnitureWIP
        FOR JSON PATH
      );
  END

  /* =====================
     DELETE
  ===================== */
  IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'DELETE',
      'BongkarSusunOutputFurnitureWIP',
      @actor,
      @rid,
      @pk,
      (
        SELECT d.NoBongkarSusun, d.NoFurnitureWIP
        FROM deleted d
        ORDER BY d.NoBongkarSusun, d.NoFurnitureWIP
        FOR JSON PATH
      ),
      NULL;
  END

  /* =====================
     UPDATE
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BongkarSusunOutputFurnitureWIP',
      @actor,
      @rid,
      @pk,
      (
        SELECT d.NoBongkarSusun, d.NoFurnitureWIP
        FROM deleted d
        ORDER BY d.NoBongkarSusun, d.NoFurnitureWIP
        FOR JSON PATH
      ),
      (
        SELECT i.NoBongkarSusun, i.NoFurnitureWIP
        FROM inserted i
        ORDER BY i.NoBongkarSusun, i.NoFurnitureWIP
        FOR JSON PATH
      );
  END
END;
GO

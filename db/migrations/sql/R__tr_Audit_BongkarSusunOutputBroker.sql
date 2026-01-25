/* ===== [dbo].[tr_Audit_BongkarSusunOutputBroker] ON [dbo].[BongkarSusunOutputBroker] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputBroker]
ON [dbo].[BongkarSusunOutputBroker]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
  SET NOCOUNT ON;

  -- ✅ actor = actor_id (ID user) dari SESSION_CONTEXT
  DECLARE @actor nvarchar(128) =
    COALESCE(
      CONVERT(nvarchar(128), TRY_CONVERT(int, SESSION_CONTEXT(N'actor_id'))),
      CAST(SESSION_CONTEXT(N'actor') AS nvarchar(128)),  -- fallback lama
      SUSER_SNAME()
    );

  DECLARE @rid nvarchar(64) =
    CAST(SESSION_CONTEXT(N'request_id') AS nvarchar(64));

  /* =========================================================
     Helper: PK ringkas (NoBroker tunggal / list)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoBroker FROM inserted
    UNION
    SELECT NoBroker FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoBroker) = 1
          THEN CONCAT('{"NoBroker":"', MAX(NoBroker), '"}')
        ELSE
          CONCAT(
            '{"NoBrokerList":',
            (SELECT DISTINCT NoBroker FROM x FOR JSON PATH),
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
      'BongkarSusunOutputBroker',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoBongkarSusun,
          i.NoBroker,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoBroker, i.NoSak, i.NoBongkarSusun
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
      'BongkarSusunOutputBroker',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoBroker,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoBroker, d.NoSak, d.NoBongkarSusun
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
      'BongkarSusunOutputBroker',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoBroker,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoBroker, d.NoSak, d.NoBongkarSusun
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoBongkarSusun,
          i.NoBroker,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoBroker, i.NoSak, i.NoBongkarSusun
        FOR JSON PATH
      );
  END
END;

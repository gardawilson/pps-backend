/* ===== [dbo].[tr_Audit_Broker_d] ON [dbo].[Broker_d] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Broker_d]
ON [dbo].[Broker_d]
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
     Helper: bentuk PK ringkas (NoBroker tunggal / list)
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
      'Broker_d',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoBroker,
          i.NoSak,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IsPartial,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoBroker, i.NoSak
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
      'Broker_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBroker,
          d.NoSak,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IsPartial,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoBroker, d.NoSak
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
      'Broker_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBroker,
          d.NoSak,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IsPartial,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoBroker, d.NoSak
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoBroker,
          i.NoSak,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IsPartial,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoBroker, i.NoSak
        FOR JSON PATH
      );
  END
END;

/* ===== [dbo].[tr_Audit_WashingProduksiOutput] ON [dbo].[WashingProduksiOutput] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_WashingProduksiOutput]
ON [dbo].[WashingProduksiOutput]
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
     Helper: bentuk PK ringkas (NoWashing tunggal / list)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoWashing FROM inserted
    UNION
    SELECT NoWashing FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoWashing) = 1
          THEN CONCAT('{"NoWashing":"', MAX(NoWashing), '"}')
        ELSE
          CONCAT(
            '{"NoWashingList":',
            (SELECT DISTINCT NoWashing FROM x FOR JSON PATH),
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
      'WashingProduksiOutput',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoProduksi,
          i.NoWashing,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoWashing, i.NoSak, i.NoProduksi
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
      'WashingProduksiOutput',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoWashing,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoWashing, d.NoSak, d.NoProduksi
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
      'WashingProduksiOutput',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoWashing,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoWashing, d.NoSak, d.NoProduksi
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoProduksi,
          i.NoWashing,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoWashing, i.NoSak, i.NoProduksi
        FOR JSON PATH
      );
  END
END;

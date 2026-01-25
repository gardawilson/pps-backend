/* ===== [dbo].[tr_Audit_Washing_d] ON [dbo].[Washing_d] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Washing_d]
ON [dbo].[Washing_d]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
  SET NOCOUNT ON;

  -- ✅ actor = actor_id dari SESSION_CONTEXT (fallback ke actor lama, terakhir ke login DB)
  DECLARE @actor nvarchar(128) =
    COALESCE(
      CONVERT(nvarchar(128), TRY_CONVERT(int, SESSION_CONTEXT(N'actor_id'))),
      CAST(SESSION_CONTEXT(N'actor') AS nvarchar(128)),
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
      'Washing_d',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoWashing,
          i.NoSak,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoWashing, i.NoSak
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
      'Washing_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoWashing,
          d.NoSak,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoWashing, d.NoSak
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
      'Washing_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoWashing,
          d.NoSak,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoWashing, d.NoSak
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoWashing,
          i.NoSak,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoWashing, i.NoSak
        FOR JSON PATH
      );
  END
END;

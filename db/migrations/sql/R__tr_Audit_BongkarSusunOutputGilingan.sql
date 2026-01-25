/* ===== [dbo].[tr_Audit_BongkarSusunOutputGilingan] ON [dbo].[BongkarSusunOutputGilingan] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputGilingan]
ON [dbo].[BongkarSusunOutputGilingan]
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
     Helper: PK ringkas (NoBongkarSusun tunggal / list)
  ========================================================= */
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
     INSERT (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'INSERT',
      'BongkarSusunOutputGilingan',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoBongkarSusun,
          i.NoGilingan
        FROM inserted i
        ORDER BY i.NoBongkarSusun, i.NoGilingan
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
      'BongkarSusunOutputGilingan',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoGilingan
        FROM deleted d
        ORDER BY d.NoBongkarSusun, d.NoGilingan
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
      'BongkarSusunOutputGilingan',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoGilingan
        FROM deleted d
        ORDER BY d.NoBongkarSusun, d.NoGilingan
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoBongkarSusun,
          i.NoGilingan
        FROM inserted i
        ORDER BY i.NoBongkarSusun, i.NoGilingan
        FOR JSON PATH
      );
  END
END;
GO

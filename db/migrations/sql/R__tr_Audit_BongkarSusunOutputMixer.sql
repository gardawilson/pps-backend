/* ===== [dbo].[tr_Audit_BongkarSusunOutputMixer] ON [dbo].[BongkarSusunOutputMixer] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputMixer]
ON [dbo].[BongkarSusunOutputMixer]
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
     Helper: PK ringkas (NoMixer tunggal / list)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoMixer FROM inserted
    UNION
    SELECT NoMixer FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoMixer) = 1
          THEN CONCAT('{"NoMixer":"', MAX(NoMixer), '"}')
        ELSE
          CONCAT(
            '{"NoMixerList":',
            (SELECT DISTINCT NoMixer FROM x FOR JSON PATH),
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
      'BongkarSusunOutputMixer',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoBongkarSusun,
          i.NoMixer,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoMixer, i.NoSak, i.NoBongkarSusun
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
      'BongkarSusunOutputMixer',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoMixer,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoMixer, d.NoSak, d.NoBongkarSusun
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
      'BongkarSusunOutputMixer',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBongkarSusun,
          d.NoMixer,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoMixer, d.NoSak, d.NoBongkarSusun
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoBongkarSusun,
          i.NoMixer,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoMixer, i.NoSak, i.NoBongkarSusun
        FOR JSON PATH
      );
  END
END;
GO

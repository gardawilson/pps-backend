/* ===== [dbo].[tr_Audit_Mixer_d] ON [dbo].[Mixer_d] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_Mixer_d]
ON [dbo].[Mixer_d]
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
     Helper: bentuk PK ringkas (NoMixer tunggal / list)
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
      'Mixer_d',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoMixer,
          i.NoSak,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IsPartial,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoMixer, i.NoSak
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
      'Mixer_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoMixer,
          d.NoSak,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IsPartial,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoMixer, d.NoSak
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
      'Mixer_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoMixer,
          d.NoSak,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IsPartial,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoMixer, d.NoSak
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoMixer,
          i.NoSak,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IsPartial,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoMixer, i.NoSak
        FOR JSON PATH
      );
  END
END;
GO

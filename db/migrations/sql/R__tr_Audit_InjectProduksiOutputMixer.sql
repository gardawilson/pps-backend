/* ===== [dbo].[tr_Audit_InjectProduksiOutputMixer] ON [dbo].[InjectProduksiOutputMixer] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksiOutputMixer
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- ✅ PK: NoMixer (parent document)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksiOutputMixer]
ON [dbo].[InjectProduksiOutputMixer]
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
     ✅ Helper: PK menggunakan NoMixer (parent)
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
      'InjectProduksiOutputMixer',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoProduksi,
          i.NoMixer,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoMixer, i.NoProduksi, i.NoSak
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
      'InjectProduksiOutputMixer',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoMixer,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoMixer, d.NoProduksi, d.NoSak
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
      'InjectProduksiOutputMixer',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoMixer,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoMixer, d.NoProduksi, d.NoSak
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoProduksi,
          i.NoMixer,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoMixer, i.NoProduksi, i.NoSak
        FOR JSON PATH
      );
  END
END;
GO

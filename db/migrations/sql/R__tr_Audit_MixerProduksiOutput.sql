/* ===== [dbo].[tr_Audit_MixerProduksiOutput] ON [dbo].[MixerProduksiOutput] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_MixerProduksiOutput]
ON [dbo].[MixerProduksiOutput]
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
     Helper: PK ringkas (NoProduksi tunggal / list)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoProduksi FROM inserted
    UNION
    SELECT NoProduksi FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoProduksi) = 1
          THEN CONCAT('{"NoProduksi":"', MAX(NoProduksi), '"}')
        ELSE
          CONCAT(
            '{"NoProduksiList":',
            (SELECT DISTINCT NoProduksi FROM x FOR JSON PATH),
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
      'MixerProduksiOutput',
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
        ORDER BY i.NoProduksi, i.NoMixer, i.NoSak
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
      'MixerProduksiOutput',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoMixer,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoProduksi, d.NoMixer, d.NoSak
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
      'MixerProduksiOutput',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoMixer,
          d.NoSak
        FROM deleted d
        ORDER BY d.NoProduksi, d.NoMixer, d.NoSak
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoProduksi,
          i.NoMixer,
          i.NoSak
        FROM inserted i
        ORDER BY i.NoProduksi, i.NoMixer, i.NoSak
        FOR JSON PATH
      );
  END
END;
GO

/* ===== [dbo].[tr_Audit_InjectProduksiOutputMixer] 
         ON [dbo].[InjectProduksiOutputMixer] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksiOutputMixer
-- PK     : NoMixer + NoProduksi
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Mixer_d untuk ambil Berat
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
     1) INSERT-only => PRODUCE (DETAIL)
     ========================================================= */
  ;WITH insOnly AS (
    SELECT
      i.NoProduksi,
      i.NoMixer,
      i.NoSak,
      md.Berat
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoMixer    = i.NoMixer
     AND d.NoSak      = i.NoSak
    LEFT JOIN dbo.Mixer_d md
      ON md.NoMixer = i.NoMixer
     AND md.NoSak   = i.NoSak
    WHERE d.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'InjectProduksiOutputMixer',
    @actor,
    @rid,
    (SELECT
       i.NoMixer,
       i.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoProduksi,
       i.NoMixer,
       i.NoSak,
       CAST(i.Berat AS decimal(18,3)) AS Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT
      d.NoProduksi,
      d.NoMixer,
      d.NoSak,
      md.Berat
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoProduksi = d.NoProduksi
     AND i.NoMixer    = d.NoMixer
     AND i.NoSak      = d.NoSak
    LEFT JOIN dbo.Mixer_d md
      ON md.NoMixer = d.NoMixer
     AND md.NoSak   = d.NoSak
    WHERE i.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'InjectProduksiOutputMixer',
    @actor,
    @rid,
    (SELECT
       d.NoMixer,
       d.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoProduksi,
       d.NoMixer,
       d.NoSak,
       CAST(d.Berat AS decimal(18,3)) AS Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL
  FROM delOnly d;

  /* =========================================================
     3) UPDATE => UPDATE (DETAIL)
     ========================================================= */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail
      (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'InjectProduksiOutputMixer',
      @actor,
      @rid,
      (SELECT
         i.NoMixer,
         i.NoProduksi
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoProduksi,
         d.NoMixer,
         d.NoSak,
         CAST(mdOld.Berat AS decimal(18,3)) AS Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoProduksi,
         i.NoMixer,
         i.NoSak,
         CAST(mdNew.Berat AS decimal(18,3)) AS Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoMixer    = i.NoMixer
     AND d.NoSak      = i.NoSak
    LEFT JOIN dbo.Mixer_d mdOld
      ON mdOld.NoMixer = d.NoMixer
     AND mdOld.NoSak   = d.NoSak
    LEFT JOIN dbo.Mixer_d mdNew
      ON mdNew.NoMixer = i.NoMixer
     AND mdNew.NoSak   = i.NoSak;
  END
END;
GO

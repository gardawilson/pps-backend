/* ===== [dbo].[tr_Audit_InjectProduksiInputMixer] ON [dbo].[InjectProduksiInputMixer] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoMixer) per statement
   PK JSON: { NoProduksi, NoMixer }
   NewData/OldData: array of {NoProduksi, NoMixer, NoSak, Berat, IsPartial}

   Enrichment:
   - Berat & IsPartial diambil dari dbo.Mixer_d (md.Berat, md.IsPartial) berdasarkan (NoMixer, NoSak)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksiInputMixer]
ON [dbo].[InjectProduksiInputMixer]
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
     1) INSERT-only rows => CONSUME_FULL (GROUPED)
     ========================================================= */
  ;WITH insOnly AS (
    SELECT i.*
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoMixer    = i.NoMixer
     AND d.NoSak      = i.NoSak
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoMixer
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'InjectProduksiInputMixer',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoMixer,
        x.NoSak,
        CAST(md.Berat AS decimal(18,3))    AS Berat,
        CAST(md.IsPartial AS bit)          AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.Mixer_d md
        ON md.NoMixer = x.NoMixer
       AND md.NoSak   = x.NoSak
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoMixer    = gg.NoMixer
      ORDER BY x.NoSak
      FOR JSON PATH
    )
  FROM g gg;

  /* =========================================================
     2) DELETE-only rows => UNCONSUME_FULL (GROUPED)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT d.*
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoProduksi = d.NoProduksi
     AND i.NoMixer    = d.NoMixer
     AND i.NoSak      = d.NoSak
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoMixer
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'InjectProduksiInputMixer',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoMixer,
        x.NoSak,
        CAST(md.Berat AS decimal(18,3))    AS Berat,
        CAST(md.IsPartial AS bit)          AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.Mixer_d md
        ON md.NoMixer = x.NoMixer
       AND md.NoSak   = x.NoSak
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoMixer    = gg.NoMixer
      ORDER BY x.NoSak
      FOR JSON PATH
    ),
    NULL
  FROM g gg;

  /* =========================================================
     3) UPDATE rows => UPDATE (GROUPED)
     ========================================================= */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH upd AS (
      SELECT i.NoProduksi, i.NoMixer, i.NoSak
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoMixer    = i.NoMixer
       AND d.NoSak      = i.NoSak
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoMixer
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'InjectProduksiInputMixer',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoMixer,
          d.NoSak,
          CAST(md.Berat AS decimal(18,3))  AS Berat,
          CAST(md.IsPartial AS bit)        AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.Mixer_d md
          ON md.NoMixer = d.NoMixer
         AND md.NoSak   = d.NoSak
        WHERE d.NoProduksi = gg.NoProduksi
          AND d.NoMixer    = gg.NoMixer
        ORDER BY d.NoSak
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoMixer,
          i.NoSak,
          CAST(md.Berat AS decimal(18,3))  AS Berat,
          CAST(md.IsPartial AS bit)        AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.Mixer_d md
          ON md.NoMixer = i.NoMixer
         AND md.NoSak   = i.NoSak
        WHERE i.NoProduksi = gg.NoProduksi
          AND i.NoMixer    = gg.NoMixer
        ORDER BY i.NoSak
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

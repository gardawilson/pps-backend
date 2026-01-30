/* ===== [dbo].[tr_Audit_MixerProduksiInputMixerPartial] ON [dbo].[MixerProduksiInputMixerPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoProduksi, NoMixerPartial, NoMixer)
   - PK JSON: NoProduksi, NoMixerPartial, NoMixer (TANPA NoSak)
   - OldData/NewData: array of {NoProduksi, NoMixerPartial, NoMixer, NoSak, Berat}
   - NoMixer + NoSak + Berat diambil dari dbo.MixerPartial (mp.NoMixer, mp.NoSak, mp.Berat)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_MixerProduksiInputMixerPartial]
ON [dbo].[MixerProduksiInputMixerPartial]
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

  /* ======================================================
     INSERT-only => CONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH insOnly AS (
    SELECT i.NoProduksi, i.NoMixerPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoProduksi     = i.NoProduksi
        AND d.NoMixerPartial = i.NoMixerPartial
    )
  ),
  insEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoMixerPartial,
      mp.NoMixer,
      mp.NoSak,
      mp.Berat
    FROM insOnly x
    LEFT JOIN dbo.MixerPartial mp
      ON mp.NoMixerPartial = x.NoMixerPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoMixerPartial, NoMixer
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'MixerProduksiInputMixerPartial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoMixerPartial, g.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoMixerPartial,
        e.NoMixer,
        e.NoSak,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM insEnriched e
      WHERE e.NoProduksi     = g.NoProduksi
        AND e.NoMixerPartial = g.NoMixerPartial
        AND (
              (e.NoMixer = g.NoMixer)
              OR (e.NoMixer IS NULL AND g.NoMixer IS NULL)
            )
      ORDER BY e.NoMixer, e.NoSak
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoMixerPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi     = d.NoProduksi
        AND i.NoMixerPartial = d.NoMixerPartial
    )
  ),
  delEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoMixerPartial,
      mp.NoMixer,
      mp.NoSak,
      mp.Berat
    FROM delOnly x
    LEFT JOIN dbo.MixerPartial mp
      ON mp.NoMixerPartial = x.NoMixerPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoMixerPartial, NoMixer
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'MixerProduksiInputMixerPartial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoMixerPartial, g.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoMixerPartial,
        e.NoMixer,
        e.NoSak,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM delEnriched e
      WHERE e.NoProduksi     = g.NoProduksi
        AND e.NoMixerPartial = g.NoMixerPartial
        AND (
              (e.NoMixer = g.NoMixer)
              OR (e.NoMixer IS NULL AND g.NoMixer IS NULL)
            )
      ORDER BY e.NoMixer, e.NoSak
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp0 AS (
      SELECT DISTINCT i.NoProduksi, i.NoMixerPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi     = i.NoProduksi
       AND d.NoMixerPartial = i.NoMixerPartial
    ),
    grp AS (
      SELECT DISTINCT
        g0.NoProduksi,
        g0.NoMixerPartial,
        mp.NoMixer
      FROM grp0 g0
      LEFT JOIN dbo.MixerPartial mp
        ON mp.NoMixerPartial = g0.NoMixerPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'MixerProduksiInputMixerPartial',
      @actor,
      @rid,
      (SELECT g.NoProduksi, g.NoMixerPartial, g.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      COALESCE((
        SELECT
          d2.NoProduksi,
          d2.NoMixerPartial,
          mp2.NoMixer,
          mp2.NoSak,
          CAST(mp2.Berat AS decimal(18,3)) AS Berat
        FROM deleted d2
        LEFT JOIN dbo.MixerPartial mp2
          ON mp2.NoMixerPartial = d2.NoMixerPartial
        WHERE d2.NoProduksi     = g.NoProduksi
          AND d2.NoMixerPartial = g.NoMixerPartial
          AND (
                (mp2.NoMixer = g.NoMixer)
                OR (mp2.NoMixer IS NULL AND g.NoMixer IS NULL)
              )
        ORDER BY mp2.NoMixer, mp2.NoSak
        FOR JSON PATH
      ), '[]'),

      COALESCE((
        SELECT
          i2.NoProduksi,
          i2.NoMixerPartial,
          mp2.NoMixer,
          mp2.NoSak,
          CAST(mp2.Berat AS decimal(18,3)) AS Berat
        FROM inserted i2
        LEFT JOIN dbo.MixerPartial mp2
          ON mp2.NoMixerPartial = i2.NoMixerPartial
        WHERE i2.NoProduksi     = g.NoProduksi
          AND i2.NoMixerPartial = g.NoMixerPartial
          AND (
                (mp2.NoMixer = g.NoMixer)
                OR (mp2.NoMixer IS NULL AND g.NoMixer IS NULL)
              )
        ORDER BY mp2.NoMixer, mp2.NoSak
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

/* ===== [dbo].[tr_Audit_BrokerProduksiInputMixerPartial] ON [dbo].[BrokerProduksiInputMixerPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoProduksi, NoMixerPartial)
   - PK: NoProduksi + NoMixerPartial + NoMixer (TANPA NoSak)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BrokerProduksiInputMixerPartial]
ON [dbo].[BrokerProduksiInputMixerPartial]
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
      WHERE d.NoProduksi = i.NoProduksi
        AND d.NoMixerPartial = i.NoMixerPartial
    )
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoMixerPartial
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'BrokerProduksiInputMixerPartial',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', g.NoProduksi,
      '","NoMixerPartial":"', g.NoMixerPartial,
      '","NoMixer":', CASE WHEN mp.NoMixer IS NULL THEN 'null' ELSE CONCAT('"', mp.NoMixer, '"') END,
      '}'
    ),
    NULL,
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
      WHERE i2.NoProduksi = g.NoProduksi
        AND i2.NoMixerPartial = g.NoMixerPartial
      ORDER BY mp2.NoMixer, mp2.NoSak
      FOR JSON PATH
    ), '[]')
  FROM grp g
  LEFT JOIN dbo.MixerPartial mp
    ON mp.NoMixerPartial = g.NoMixerPartial;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoMixerPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi = d.NoProduksi
        AND i.NoMixerPartial = d.NoMixerPartial
    )
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoMixerPartial
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'BrokerProduksiInputMixerPartial',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', g.NoProduksi,
      '","NoMixerPartial":"', g.NoMixerPartial,
      '","NoMixer":', CASE WHEN mp.NoMixer IS NULL THEN 'null' ELSE CONCAT('"', mp.NoMixer, '"') END,
      '}'
    ),
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
      WHERE d2.NoProduksi = g.NoProduksi
        AND d2.NoMixerPartial = g.NoMixerPartial
      ORDER BY mp2.NoMixer, mp2.NoSak
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g
  LEFT JOIN dbo.MixerPartial mp
    ON mp.NoMixerPartial = g.NoMixerPartial;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp AS (
      SELECT DISTINCT i.NoProduksi, i.NoMixerPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoMixerPartial = i.NoMixerPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BrokerProduksiInputMixerPartial',
      @actor,
      @rid,
      CONCAT(
        '{"NoProduksi":"', g.NoProduksi,
        '","NoMixerPartial":"', g.NoMixerPartial,
        '","NoMixer":', CASE WHEN mp.NoMixer IS NULL THEN 'null' ELSE CONCAT('"', mp.NoMixer, '"') END,
        '}'
      ),
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
        WHERE d2.NoProduksi = g.NoProduksi
          AND d2.NoMixerPartial = g.NoMixerPartial
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
        WHERE i2.NoProduksi = g.NoProduksi
          AND i2.NoMixerPartial = g.NoMixerPartial
        ORDER BY mp2.NoMixer, mp2.NoSak
        FOR JSON PATH
      ), '[]')
    FROM grp g
    LEFT JOIN dbo.MixerPartial mp
      ON mp.NoMixerPartial = g.NoMixerPartial;
  END
END;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunInputMixer] ON [dbo].[BongkarSusunInputMixer] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoBongkarSusun, NoMixer) per statement
   PK JSON: { NoBongkarSusun, NoMixer }
   NewData/OldData: array of {NoBongkarSusun, NoMixer, NoSak, Berat, IsPartial}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunInputMixer]
ON [dbo].[BongkarSusunInputMixer]
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
      ON d.NoBongkarSusun = i.NoBongkarSusun
     AND d.NoMixer        = i.NoMixer
     AND d.NoSak          = i.NoSak
    WHERE d.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoMixer
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BongkarSusunInputMixer',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBongkarSusun,
        x.NoMixer,
        x.NoSak,
        CAST(md.Berat AS decimal(18,3)) AS Berat,
        CAST(md.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.Mixer_d md
        ON md.NoMixer = x.NoMixer AND md.NoSak = x.NoSak
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoMixer        = gg.NoMixer
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
      ON i.NoBongkarSusun = d.NoBongkarSusun
     AND i.NoMixer        = d.NoMixer
     AND i.NoSak          = d.NoSak
    WHERE i.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoMixer
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BongkarSusunInputMixer',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBongkarSusun,
        x.NoMixer,
        x.NoSak,
        CAST(md.Berat AS decimal(18,3)) AS Berat,
        CAST(md.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.Mixer_d md
        ON md.NoMixer = x.NoMixer AND md.NoSak = x.NoSak
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoMixer        = gg.NoMixer
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
      SELECT i.NoBongkarSusun, i.NoMixer, i.NoSak
      FROM inserted i
      JOIN deleted d
        ON d.NoBongkarSusun = i.NoBongkarSusun
       AND d.NoMixer        = i.NoMixer
       AND d.NoSak          = i.NoSak
    ),
    g AS (
      SELECT DISTINCT NoBongkarSusun, NoMixer
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BongkarSusunInputMixer',
      @actor,
      @rid,
      (SELECT gg.NoBongkarSusun, gg.NoMixer FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBongkarSusun,
          d.NoMixer,
          d.NoSak,
          CAST(md.Berat AS decimal(18,3)) AS Berat,
          CAST(md.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.Mixer_d md
          ON md.NoMixer = d.NoMixer AND md.NoSak = d.NoSak
        WHERE d.NoBongkarSusun = gg.NoBongkarSusun
          AND d.NoMixer        = gg.NoMixer
        ORDER BY d.NoSak
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBongkarSusun,
          i.NoMixer,
          i.NoSak,
          CAST(md.Berat AS decimal(18,3)) AS Berat,
          CAST(md.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.Mixer_d md
          ON md.NoMixer = i.NoMixer AND md.NoSak = i.NoSak
        WHERE i.NoBongkarSusun = gg.NoBongkarSusun
          AND i.NoMixer        = gg.NoMixer
        ORDER BY i.NoSak
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

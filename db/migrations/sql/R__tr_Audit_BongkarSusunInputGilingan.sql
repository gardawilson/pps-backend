SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunInputGilingan] ON [dbo].[BongkarSusunInputGilingan] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoBongkarSusun, NoGilingan) per statement
   PK JSON: { NoBongkarSusun, NoGilingan }
   NewData/OldData: array of {NoBongkarSusun, NoGilingan, Berat, IsPartial}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunInputGilingan]
ON [dbo].[BongkarSusunInputGilingan]
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
     AND d.NoGilingan     = i.NoGilingan
    WHERE d.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoGilingan
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BongkarSusunInputGilingan',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBongkarSusun,
        x.NoGilingan,
        CAST(gln.Berat AS decimal(18,3)) AS Berat,
        CAST(gln.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.Gilingan gln
        ON gln.NoGilingan = x.NoGilingan
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoGilingan     = gg.NoGilingan
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
     AND i.NoGilingan     = d.NoGilingan
    WHERE i.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoGilingan
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BongkarSusunInputGilingan',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBongkarSusun,
        x.NoGilingan,
        CAST(gln.Berat AS decimal(18,3)) AS Berat,
        CAST(gln.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.Gilingan gln
        ON gln.NoGilingan = x.NoGilingan
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoGilingan     = gg.NoGilingan
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
      SELECT i.NoBongkarSusun, i.NoGilingan
      FROM inserted i
      JOIN deleted d
        ON d.NoBongkarSusun = i.NoBongkarSusun
       AND d.NoGilingan     = i.NoGilingan
    ),
    g AS (
      SELECT DISTINCT NoBongkarSusun, NoGilingan
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BongkarSusunInputGilingan',
      @actor,
      @rid,
      (SELECT gg.NoBongkarSusun, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBongkarSusun,
          d.NoGilingan,
          CAST(gln.Berat AS decimal(18,3)) AS Berat,
          CAST(gln.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.Gilingan gln
          ON gln.NoGilingan = d.NoGilingan
        WHERE d.NoBongkarSusun = gg.NoBongkarSusun
          AND d.NoGilingan     = gg.NoGilingan
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBongkarSusun,
          i.NoGilingan,
          CAST(gln.Berat AS decimal(18,3)) AS Berat,
          CAST(gln.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.Gilingan gln
          ON gln.NoGilingan = i.NoGilingan
        WHERE i.NoBongkarSusun = gg.NoBongkarSusun
          AND i.NoGilingan     = gg.NoGilingan
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunInputBarangJadi] ON [dbo].[BongkarSusunInputBarangJadi] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoBongkarSusun, NoBJ) per statement
   PK JSON: { NoBongkarSusun, NoBJ }
   NewData/OldData: array of {NoBongkarSusun, NoBJ, Pcs, Berat, IsPartial}

   Enrichment:
   - Pcs, Berat, IsPartial diambil dari dbo.BarangJadi (bj.Pcs, bj.Berat, bj.IsPartial)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunInputBarangJadi]
ON [dbo].[BongkarSusunInputBarangJadi]
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
     AND d.NoBJ           = i.NoBJ
    WHERE d.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoBJ
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BongkarSusunInputBarangJadi',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoBJ FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBongkarSusun,
        x.NoBJ,
        CAST(bj.Pcs AS int)             AS Pcs,
        CAST(bj.Berat AS decimal(18,3)) AS Berat,
        CAST(bj.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.BarangJadi bj
        ON bj.NoBJ = x.NoBJ
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoBJ           = gg.NoBJ
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
     AND i.NoBJ           = d.NoBJ
    WHERE i.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoBJ
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BongkarSusunInputBarangJadi',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoBJ FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBongkarSusun,
        x.NoBJ,
        CAST(bj.Pcs AS int)             AS Pcs,
        CAST(bj.Berat AS decimal(18,3)) AS Berat,
        CAST(bj.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.BarangJadi bj
        ON bj.NoBJ = x.NoBJ
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoBJ           = gg.NoBJ
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
      SELECT i.NoBongkarSusun, i.NoBJ
      FROM inserted i
      JOIN deleted d
        ON d.NoBongkarSusun = i.NoBongkarSusun
       AND d.NoBJ           = i.NoBJ
    ),
    g AS (
      SELECT DISTINCT NoBongkarSusun, NoBJ
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BongkarSusunInputBarangJadi',
      @actor,
      @rid,
      (SELECT gg.NoBongkarSusun, gg.NoBJ FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBongkarSusun,
          d.NoBJ,
          CAST(bj.Pcs AS int)             AS Pcs,
          CAST(bj.Berat AS decimal(18,3)) AS Berat,
          CAST(bj.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.BarangJadi bj
          ON bj.NoBJ = d.NoBJ
        WHERE d.NoBongkarSusun = gg.NoBongkarSusun
          AND d.NoBJ           = gg.NoBJ
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBongkarSusun,
          i.NoBJ,
          CAST(bj.Pcs AS int)             AS Pcs,
          CAST(bj.Berat AS decimal(18,3)) AS Berat,
          CAST(bj.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.BarangJadi bj
          ON bj.NoBJ = i.NoBJ
        WHERE i.NoBongkarSusun = gg.NoBongkarSusun
          AND i.NoBJ           = gg.NoBJ
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

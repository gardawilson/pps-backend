SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJSortirRejectInputLabelBarangJadi] ON [dbo].[BJSortirRejectInputLabelBarangJadi] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoBJSortir, NoBJ) per statement
   PK JSON: { NoBJSortir, NoBJ }
   NewData/OldData: array of {NoBJSortir, NoBJ, Pcs, Berat, IsPartial}

   Enrichment:
   - Pcs, Berat, IsPartial diambil dari dbo.BarangJadi (bj.Pcs, bj.Berat, bj.IsPartial)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJSortirRejectInputLabelBarangJadi]
ON [dbo].[BJSortirRejectInputLabelBarangJadi]
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
      ON d.NoBJSortir = i.NoBJSortir
     AND d.NoBJ       = i.NoBJ
    WHERE d.NoBJSortir IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJSortir, NoBJ
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BJSortirRejectInputLabelBarangJadi',
    @actor,
    @rid,
    (SELECT gg.NoBJSortir, gg.NoBJ FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBJSortir,
        x.NoBJ,
        CAST(bj.Pcs AS int)             AS Pcs,
        CAST(bj.Berat AS decimal(18,3)) AS Berat,
        CAST(bj.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.BarangJadi bj
        ON bj.NoBJ = x.NoBJ
      WHERE x.NoBJSortir = gg.NoBJSortir
        AND x.NoBJ       = gg.NoBJ
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
      ON i.NoBJSortir = d.NoBJSortir
     AND i.NoBJ       = d.NoBJ
    WHERE i.NoBJSortir IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJSortir, NoBJ
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BJSortirRejectInputLabelBarangJadi',
    @actor,
    @rid,
    (SELECT gg.NoBJSortir, gg.NoBJ FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBJSortir,
        x.NoBJ,
        CAST(bj.Pcs AS int)             AS Pcs,
        CAST(bj.Berat AS decimal(18,3)) AS Berat,
        CAST(bj.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.BarangJadi bj
        ON bj.NoBJ = x.NoBJ
      WHERE x.NoBJSortir = gg.NoBJSortir
        AND x.NoBJ       = gg.NoBJ
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
      SELECT i.NoBJSortir, i.NoBJ
      FROM inserted i
      JOIN deleted d
        ON d.NoBJSortir = i.NoBJSortir
       AND d.NoBJ       = i.NoBJ
    ),
    g AS (
      SELECT DISTINCT NoBJSortir, NoBJ
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJSortirRejectInputLabelBarangJadi',
      @actor,
      @rid,
      (SELECT gg.NoBJSortir, gg.NoBJ FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBJSortir,
          d.NoBJ,
          CAST(bj.Pcs AS int)             AS Pcs,
          CAST(bj.Berat AS decimal(18,3)) AS Berat,
          CAST(bj.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.BarangJadi bj
          ON bj.NoBJ = d.NoBJ
        WHERE d.NoBJSortir = gg.NoBJSortir
          AND d.NoBJ       = gg.NoBJ
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBJSortir,
          i.NoBJ,
          CAST(bj.Pcs AS int)             AS Pcs,
          CAST(bj.Berat AS decimal(18,3)) AS Berat,
          CAST(bj.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.BarangJadi bj
          ON bj.NoBJ = i.NoBJ
        WHERE i.NoBJSortir = gg.NoBJSortir
          AND i.NoBJ       = gg.NoBJ
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

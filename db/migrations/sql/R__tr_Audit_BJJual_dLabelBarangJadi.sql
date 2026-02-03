SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJJual_dLabelBarangJadi]
   ON [dbo].[BJJual_dLabelBarangJadi]
   AFTER INSERT, UPDATE, DELETE

   Action:
   - INSERT  => CONSUME_FULL
   - DELETE  => UNCONSUME_FULL
   - UPDATE  => UPDATE

   GROUPING: 1 row audit per (NoBJJual, NoBJ) per statement
   PK JSON: { NoBJJual, NoBJ }

   Enrichment:
   - Pcs, Berat, IsPartial diambil dari dbo.BarangJadi
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJJual_dLabelBarangJadi]
ON [dbo].[BJJual_dLabelBarangJadi]
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
     1) INSERT-only rows => CONSUME_FULL
     ========================================================= */
  ;WITH insOnly AS (
    SELECT i.*
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoBJJual = i.NoBJJual
     AND d.NoBJ     = i.NoBJ
    WHERE d.NoBJJual IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJJual, NoBJ
    FROM insOnly
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BJJual_dLabelBarangJadi',
    @actor,
    @rid,
    (SELECT gg.NoBJJual, gg.NoBJ
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBJJual,
        x.NoBJ,
        CAST(bj.Pcs AS int)             AS Pcs,
        CAST(bj.Berat AS decimal(18,3)) AS Berat,
        CAST(bj.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.BarangJadi bj
        ON bj.NoBJ = x.NoBJ
      WHERE x.NoBJJual = gg.NoBJJual
        AND x.NoBJ     = gg.NoBJ
      FOR JSON PATH
    )
  FROM g gg;

  /* =========================================================
     2) DELETE-only rows => UNCONSUME_FULL
     ========================================================= */
  ;WITH delOnly AS (
    SELECT d.*
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoBJJual = d.NoBJJual
     AND i.NoBJ     = d.NoBJ
    WHERE i.NoBJJual IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJJual, NoBJ
    FROM delOnly
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BJJual_dLabelBarangJadi',
    @actor,
    @rid,
    (SELECT gg.NoBJJual, gg.NoBJ
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBJJual,
        x.NoBJ,
        CAST(bj.Pcs AS int)             AS Pcs,
        CAST(bj.Berat AS decimal(18,3)) AS Berat,
        CAST(bj.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.BarangJadi bj
        ON bj.NoBJ = x.NoBJ
      WHERE x.NoBJJual = gg.NoBJJual
        AND x.NoBJ     = gg.NoBJ
      FOR JSON PATH
    ),
    NULL
  FROM g gg;

  /* =========================================================
     3) UPDATE rows => UPDATE
     ========================================================= */
  IF EXISTS (SELECT 1 FROM inserted)
     AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH upd AS (
      SELECT i.NoBJJual, i.NoBJ
      FROM inserted i
      JOIN deleted d
        ON d.NoBJJual = i.NoBJJual
       AND d.NoBJ     = i.NoBJ
    ),
    g AS (
      SELECT DISTINCT NoBJJual, NoBJ
      FROM upd
    )
    INSERT dbo.AuditTrail
      (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJJual_dLabelBarangJadi',
      @actor,
      @rid,
      (SELECT gg.NoBJJual, gg.NoBJ
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBJJual,
          d.NoBJ,
          CAST(bj.Pcs AS int)             AS Pcs,
          CAST(bj.Berat AS decimal(18,3)) AS Berat,
          CAST(bj.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.BarangJadi bj
          ON bj.NoBJ = d.NoBJ
        WHERE d.NoBJJual = gg.NoBJJual
          AND d.NoBJ     = gg.NoBJ
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBJJual,
          i.NoBJ,
          CAST(bj.Pcs AS int)             AS Pcs,
          CAST(bj.Berat AS decimal(18,3)) AS Berat,
          CAST(bj.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.BarangJadi bj
          ON bj.NoBJ = i.NoBJ
        WHERE i.NoBJJual = gg.NoBJJual
          AND i.NoBJ     = gg.NoBJ
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

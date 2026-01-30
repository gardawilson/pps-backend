SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJSortirRejectInputLabelFurnitureWIP] ON [dbo].[BJSortirRejectInputLabelFurnitureWIP] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoBJSortir, NoFurnitureWIP) per statement
   PK JSON: { NoBJSortir, NoFurnitureWIP }
   NewData/OldData: array of {NoBJSortir, NoFurnitureWIP, Pcs, Berat, IsPartial}

   Enrichment:
   - Pcs, Berat, IsPartial diambil dari dbo.FurnitureWIP (fw.Pcs, fw.Berat, fw.IsPartial)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJSortirRejectInputLabelFurnitureWIP]
ON [dbo].[BJSortirRejectInputLabelFurnitureWIP]
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
      ON d.NoBJSortir     = i.NoBJSortir
     AND d.NoFurnitureWIP = i.NoFurnitureWIP
    WHERE d.NoBJSortir IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJSortir, NoFurnitureWIP
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BJSortirRejectInputLabelFurnitureWIP',
    @actor,
    @rid,
    (SELECT gg.NoBJSortir, gg.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBJSortir,
        x.NoFurnitureWIP,
        CAST(fw.Pcs AS int)             AS Pcs,
        CAST(fw.Berat AS decimal(18,3)) AS Berat,
        CAST(fw.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.FurnitureWIP fw
        ON fw.NoFurnitureWIP = x.NoFurnitureWIP
      WHERE x.NoBJSortir     = gg.NoBJSortir
        AND x.NoFurnitureWIP = gg.NoFurnitureWIP
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
      ON i.NoBJSortir     = d.NoBJSortir
     AND i.NoFurnitureWIP = d.NoFurnitureWIP
    WHERE i.NoBJSortir IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJSortir, NoFurnitureWIP
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BJSortirRejectInputLabelFurnitureWIP',
    @actor,
    @rid,
    (SELECT gg.NoBJSortir, gg.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBJSortir,
        x.NoFurnitureWIP,
        CAST(fw.Pcs AS int)             AS Pcs,
        CAST(fw.Berat AS decimal(18,3)) AS Berat,
        CAST(fw.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.FurnitureWIP fw
        ON fw.NoFurnitureWIP = x.NoFurnitureWIP
      WHERE x.NoBJSortir     = gg.NoBJSortir
        AND x.NoFurnitureWIP = gg.NoFurnitureWIP
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
      SELECT i.NoBJSortir, i.NoFurnitureWIP
      FROM inserted i
      JOIN deleted d
        ON d.NoBJSortir     = i.NoBJSortir
       AND d.NoFurnitureWIP = i.NoFurnitureWIP
    ),
    g AS (
      SELECT DISTINCT NoBJSortir, NoFurnitureWIP
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJSortirRejectInputLabelFurnitureWIP',
      @actor,
      @rid,
      (SELECT gg.NoBJSortir, gg.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBJSortir,
          d.NoFurnitureWIP,
          CAST(fw.Pcs AS int)             AS Pcs,
          CAST(fw.Berat AS decimal(18,3)) AS Berat,
          CAST(fw.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = d.NoFurnitureWIP
        WHERE d.NoBJSortir     = gg.NoBJSortir
          AND d.NoFurnitureWIP = gg.NoFurnitureWIP
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBJSortir,
          i.NoFurnitureWIP,
          CAST(fw.Pcs AS int)             AS Pcs,
          CAST(fw.Berat AS decimal(18,3)) AS Berat,
          CAST(fw.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = i.NoFurnitureWIP
        WHERE i.NoBJSortir     = gg.NoBJSortir
          AND i.NoFurnitureWIP = gg.NoFurnitureWIP
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

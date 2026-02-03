SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJJual_dLabelFurnitureWIP]
   ON [dbo].[BJJual_dLabelFurnitureWIP]
   AFTER INSERT, UPDATE, DELETE

   Action:
   - INSERT  => CONSUME_FULL
   - DELETE  => UNCONSUME_FULL
   - UPDATE  => UPDATE

   GROUPING: 1 row audit per (NoBJJual, NoFurnitureWIP) per statement
   PK JSON: { NoBJJual, NoFurnitureWIP }

   Enrichment:
   - Pcs, Berat, IsPartial diambil dari dbo.FurnitureWIP
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJJual_dLabelFurnitureWIP]
ON [dbo].[BJJual_dLabelFurnitureWIP]
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
      ON d.NoBJJual        = i.NoBJJual
     AND d.NoFurnitureWIP  = i.NoFurnitureWIP
    WHERE d.NoBJJual IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJJual, NoFurnitureWIP
    FROM insOnly
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BJJual_dLabelFurnitureWIP',
    @actor,
    @rid,
    (SELECT gg.NoBJJual, gg.NoFurnitureWIP
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBJJual,
        x.NoFurnitureWIP,
        CAST(fw.Pcs AS int)             AS Pcs,
        CAST(fw.Berat AS decimal(18,3)) AS Berat,
        CAST(fw.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.FurnitureWIP fw
        ON fw.NoFurnitureWIP = x.NoFurnitureWIP
      WHERE x.NoBJJual       = gg.NoBJJual
        AND x.NoFurnitureWIP = gg.NoFurnitureWIP
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
      ON i.NoBJJual        = d.NoBJJual
     AND i.NoFurnitureWIP  = d.NoFurnitureWIP
    WHERE i.NoBJJual IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBJJual, NoFurnitureWIP
    FROM delOnly
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BJJual_dLabelFurnitureWIP',
    @actor,
    @rid,
    (SELECT gg.NoBJJual, gg.NoFurnitureWIP
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBJJual,
        x.NoFurnitureWIP,
        CAST(fw.Pcs AS int)             AS Pcs,
        CAST(fw.Berat AS decimal(18,3)) AS Berat,
        CAST(fw.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.FurnitureWIP fw
        ON fw.NoFurnitureWIP = x.NoFurnitureWIP
      WHERE x.NoBJJual       = gg.NoBJJual
        AND x.NoFurnitureWIP = gg.NoFurnitureWIP
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
      SELECT i.NoBJJual, i.NoFurnitureWIP
      FROM inserted i
      JOIN deleted d
        ON d.NoBJJual        = i.NoBJJual
       AND d.NoFurnitureWIP  = i.NoFurnitureWIP
    ),
    g AS (
      SELECT DISTINCT NoBJJual, NoFurnitureWIP
      FROM upd
    )
    INSERT dbo.AuditTrail
      (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJJual_dLabelFurnitureWIP',
      @actor,
      @rid,
      (SELECT gg.NoBJJual, gg.NoFurnitureWIP
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBJJual,
          d.NoFurnitureWIP,
          CAST(fw.Pcs AS int)             AS Pcs,
          CAST(fw.Berat AS decimal(18,3)) AS Berat,
          CAST(fw.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = d.NoFurnitureWIP
        WHERE d.NoBJJual       = gg.NoBJJual
          AND d.NoFurnitureWIP = gg.NoFurnitureWIP
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBJJual,
          i.NoFurnitureWIP,
          CAST(fw.Pcs AS int)             AS Pcs,
          CAST(fw.Berat AS decimal(18,3)) AS Berat,
          CAST(fw.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = i.NoFurnitureWIP
        WHERE i.NoBJJual       = gg.NoBJJual
          AND i.NoFurnitureWIP = gg.NoFurnitureWIP
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

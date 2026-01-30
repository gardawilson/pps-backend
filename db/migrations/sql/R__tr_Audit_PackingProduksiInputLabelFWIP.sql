SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_PackingProduksiInputLabelFWIP] ON [dbo].[PackingProduksiInputLabelFWIP] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoPacking, NoFurnitureWIP) per statement
   PK JSON: { NoPacking, NoFurnitureWIP }
   NewData/OldData: array of {NoPacking, NoFurnitureWIP, Pcs, Berat, IsPartial}

   Enrichment:
   - Pcs, Berat, IsPartial diambil dari dbo.FurnitureWIP (fw.Pcs, fw.Berat, fw.IsPartial)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_PackingProduksiInputLabelFWIP]
ON [dbo].[PackingProduksiInputLabelFWIP]
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
      ON d.NoPacking       = i.NoPacking
     AND d.NoFurnitureWIP  = i.NoFurnitureWIP
    WHERE d.NoPacking IS NULL
  ),
  g AS (
    SELECT DISTINCT NoPacking, NoFurnitureWIP
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'PackingProduksiInputLabelFWIP',
    @actor,
    @rid,
    (SELECT gg.NoPacking, gg.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoPacking,
        x.NoFurnitureWIP,
        CAST(fw.Pcs AS int)             AS Pcs,
        CAST(fw.Berat AS decimal(18,3)) AS Berat,
        CAST(fw.IsPartial AS bit)       AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.FurnitureWIP fw
        ON fw.NoFurnitureWIP = x.NoFurnitureWIP
      WHERE x.NoPacking      = gg.NoPacking
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
      ON i.NoPacking       = d.NoPacking
     AND i.NoFurnitureWIP  = d.NoFurnitureWIP
    WHERE i.NoPacking IS NULL
  ),
  g AS (
    SELECT DISTINCT NoPacking, NoFurnitureWIP
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'PackingProduksiInputLabelFWIP',
    @actor,
    @rid,
    (SELECT gg.NoPacking, gg.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoPacking,
        x.NoFurnitureWIP,
        CAST(fw.Pcs AS int)             AS Pcs,
        CAST(fw.Berat AS decimal(18,3)) AS Berat,
        CAST(fw.IsPartial AS bit)       AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.FurnitureWIP fw
        ON fw.NoFurnitureWIP = x.NoFurnitureWIP
      WHERE x.NoPacking      = gg.NoPacking
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
      SELECT i.NoPacking, i.NoFurnitureWIP
      FROM inserted i
      JOIN deleted d
        ON d.NoPacking      = i.NoPacking
       AND d.NoFurnitureWIP = i.NoFurnitureWIP
    ),
    g AS (
      SELECT DISTINCT NoPacking, NoFurnitureWIP
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'PackingProduksiInputLabelFWIP',
      @actor,
      @rid,
      (SELECT gg.NoPacking, gg.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoPacking,
          d.NoFurnitureWIP,
          CAST(fw.Pcs AS int)             AS Pcs,
          CAST(fw.Berat AS decimal(18,3)) AS Berat,
          CAST(fw.IsPartial AS bit)       AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = d.NoFurnitureWIP
        WHERE d.NoPacking      = gg.NoPacking
          AND d.NoFurnitureWIP = gg.NoFurnitureWIP
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoPacking,
          i.NoFurnitureWIP,
          CAST(fw.Pcs AS int)             AS Pcs,
          CAST(fw.Berat AS decimal(18,3)) AS Berat,
          CAST(fw.IsPartial AS bit)       AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = i.NoFurnitureWIP
        WHERE i.NoPacking      = gg.NoPacking
          AND i.NoFurnitureWIP = gg.NoFurnitureWIP
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJJual_dLabelFurnitureWIPPartial]
   ON [dbo].[BJJual_dLabelFurnitureWIPPartial]
   AFTER INSERT, UPDATE, DELETE

   Action:
   - INSERT => CONSUME_PARTIAL
   - DELETE => UNCONSUME_PARTIAL
   - UPDATE => UPDATE

   NOTE:
   - 1 row audit per statement per (NoBJJual, NoFurnitureWIPPartial, NoFurnitureWIP)
   - PK JSON: {NoBJJual, NoFurnitureWIPPartial, NoFurnitureWIP}
   - OldData/NewData: array of {NoBJJual, NoFurnitureWIPPartial, NoFurnitureWIP, Pcs}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJJual_dLabelFurnitureWIPPartial]
ON [dbo].[BJJual_dLabelFurnitureWIPPartial]
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
     INSERT-only => CONSUME_PARTIAL
     ====================================================== */
  ;WITH insOnly AS (
    SELECT i.NoBJJual, i.NoFurnitureWIPPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoBJJual              = i.NoBJJual
        AND d.NoFurnitureWIPPartial = i.NoFurnitureWIPPartial
    )
  ),
  insEnriched AS (
    SELECT
      x.NoBJJual,
      x.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs
    FROM insOnly x
    LEFT JOIN dbo.FurnitureWIPPartial fwp
      ON fwp.NoFurnitureWIPPartial = x.NoFurnitureWIPPartial
  ),
  grp AS (
    SELECT DISTINCT NoBJJual, NoFurnitureWIPPartial, NoFurnitureWIP
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'BJJual_dLabelFurnitureWIPPartial',
    @actor,
    @rid,
    (SELECT g.NoBJJual, g.NoFurnitureWIPPartial, g.NoFurnitureWIP
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    COALESCE((
      SELECT
        e.NoBJJual,
        e.NoFurnitureWIPPartial,
        e.NoFurnitureWIP,
        CAST(e.Pcs AS int) AS Pcs
      FROM insEnriched e
      WHERE e.NoBJJual              = g.NoBJJual
        AND e.NoFurnitureWIPPartial = g.NoFurnitureWIPPartial
        AND (
              (e.NoFurnitureWIP = g.NoFurnitureWIP)
              OR (e.NoFurnitureWIP IS NULL AND g.NoFurnitureWIP IS NULL)
            )
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoBJJual, d.NoFurnitureWIPPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoBJJual              = d.NoBJJual
        AND i.NoFurnitureWIPPartial = d.NoFurnitureWIPPartial
    )
  ),
  delEnriched AS (
    SELECT
      x.NoBJJual,
      x.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs
    FROM delOnly x
    LEFT JOIN dbo.FurnitureWIPPartial fwp
      ON fwp.NoFurnitureWIPPartial = x.NoFurnitureWIPPartial
  ),
  grp AS (
    SELECT DISTINCT NoBJJual, NoFurnitureWIPPartial, NoFurnitureWIP
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'BJJual_dLabelFurnitureWIPPartial',
    @actor,
    @rid,
    (SELECT g.NoBJJual, g.NoFurnitureWIPPartial, g.NoFurnitureWIP
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    COALESCE((
      SELECT
        e.NoBJJual,
        e.NoFurnitureWIPPartial,
        e.NoFurnitureWIP,
        CAST(e.Pcs AS int) AS Pcs
      FROM delEnriched e
      WHERE e.NoBJJual              = g.NoBJJual
        AND e.NoFurnitureWIPPartial = g.NoFurnitureWIPPartial
        AND (
              (e.NoFurnitureWIP = g.NoFurnitureWIP)
              OR (e.NoFurnitureWIP IS NULL AND g.NoFurnitureWIP IS NULL)
            )
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g;

  /* ======================================================
     UPDATE => UPDATE
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp0 AS (
      SELECT DISTINCT i.NoBJJual, i.NoFurnitureWIPPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoBJJual              = i.NoBJJual
       AND d.NoFurnitureWIPPartial = i.NoFurnitureWIPPartial
    ),
    grp AS (
      SELECT DISTINCT
        g0.NoBJJual,
        g0.NoFurnitureWIPPartial,
        fwp.NoFurnitureWIP
      FROM grp0 g0
      LEFT JOIN dbo.FurnitureWIPPartial fwp
        ON fwp.NoFurnitureWIPPartial = g0.NoFurnitureWIPPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJJual_dLabelFurnitureWIPPartial',
      @actor,
      @rid,
      (SELECT g.NoBJJual, g.NoFurnitureWIPPartial, g.NoFurnitureWIP
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      COALESCE((
        SELECT
          d2.NoBJJual,
          d2.NoFurnitureWIPPartial,
          fwp2.NoFurnitureWIP,
          CAST(fwp2.Pcs AS int) AS Pcs
        FROM deleted d2
        LEFT JOIN dbo.FurnitureWIPPartial fwp2
          ON fwp2.NoFurnitureWIPPartial = d2.NoFurnitureWIPPartial
        WHERE d2.NoBJJual              = g.NoBJJual
          AND d2.NoFurnitureWIPPartial = g.NoFurnitureWIPPartial
          AND (
                (fwp2.NoFurnitureWIP = g.NoFurnitureWIP)
                OR (fwp2.NoFurnitureWIP IS NULL AND g.NoFurnitureWIP IS NULL)
              )
        FOR JSON PATH
      ), '[]'),

      COALESCE((
        SELECT
          i2.NoBJJual,
          i2.NoFurnitureWIPPartial,
          fwp2.NoFurnitureWIP,
          CAST(fwp2.Pcs AS int) AS Pcs
        FROM inserted i2
        LEFT JOIN dbo.FurnitureWIPPartial fwp2
          ON fwp2.NoFurnitureWIPPartial = i2.NoFurnitureWIPPartial
        WHERE i2.NoBJJual              = g.NoBJJual
          AND i2.NoFurnitureWIPPartial = g.NoFurnitureWIPPartial
          AND (
                (fwp2.NoFurnitureWIP = g.NoFurnitureWIP)
                OR (fwp2.NoFurnitureWIP IS NULL AND g.NoFurnitureWIP IS NULL)
              )
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

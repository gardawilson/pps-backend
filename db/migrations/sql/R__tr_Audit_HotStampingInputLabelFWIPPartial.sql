/* ===== [dbo].[tr_Audit_HotStampingInputLabelFWIPPartial] ON [dbo].[HotStampingInputLabelFWIPPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoProduksi, NoFurnitureWIPPartial, NoFurnitureWIP)
   - PK JSON: {NoProduksi, NoFurnitureWIPPartial, NoFurnitureWIP} (TANPA Pcs)
   - OldData/NewData: array of {NoProduksi, NoFurnitureWIPPartial, NoFurnitureWIP, Pcs}
   - NoFurnitureWIP + Pcs diambil dari dbo.FurnitureWIPPartial (fwp.NoFurnitureWIP, fwp.Pcs)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_HotStampingInputLabelFWIPPartial]
ON [dbo].[HotStampingInputLabelFWIPPartial]
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
    SELECT i.NoProduksi, i.NoFurnitureWIPPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoProduksi            = i.NoProduksi
        AND d.NoFurnitureWIPPartial = i.NoFurnitureWIPPartial
    )
  ),
  insEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs
    FROM insOnly x
    LEFT JOIN dbo.FurnitureWIPPartial fwp
      ON fwp.NoFurnitureWIPPartial = x.NoFurnitureWIPPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoFurnitureWIPPartial, NoFurnitureWIP
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'HotStampingInputLabelFWIPPartial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoFurnitureWIPPartial, g.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoFurnitureWIPPartial,
        e.NoFurnitureWIP,
        CAST(e.Pcs AS int) AS Pcs
      FROM insEnriched e
      WHERE e.NoProduksi            = g.NoProduksi
        AND e.NoFurnitureWIPPartial = g.NoFurnitureWIPPartial
        AND (
              (e.NoFurnitureWIP = g.NoFurnitureWIP)
              OR (e.NoFurnitureWIP IS NULL AND g.NoFurnitureWIP IS NULL)
            )
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoFurnitureWIPPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi            = d.NoProduksi
        AND i.NoFurnitureWIPPartial = d.NoFurnitureWIPPartial
    )
  ),
  delEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs
    FROM delOnly x
    LEFT JOIN dbo.FurnitureWIPPartial fwp
      ON fwp.NoFurnitureWIPPartial = x.NoFurnitureWIPPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoFurnitureWIPPartial, NoFurnitureWIP
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'HotStampingInputLabelFWIPPartial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoFurnitureWIPPartial, g.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoFurnitureWIPPartial,
        e.NoFurnitureWIP,
        CAST(e.Pcs AS int) AS Pcs
      FROM delEnriched e
      WHERE e.NoProduksi            = g.NoProduksi
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
     UPDATE => UPDATE (GROUPED)
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp0 AS (
      SELECT DISTINCT i.NoProduksi, i.NoFurnitureWIPPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi            = i.NoProduksi
       AND d.NoFurnitureWIPPartial = i.NoFurnitureWIPPartial
    ),
    grp AS (
      SELECT DISTINCT
        g0.NoProduksi,
        g0.NoFurnitureWIPPartial,
        fwp.NoFurnitureWIP
      FROM grp0 g0
      LEFT JOIN dbo.FurnitureWIPPartial fwp
        ON fwp.NoFurnitureWIPPartial = g0.NoFurnitureWIPPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'HotStampingInputLabelFWIPPartial',
      @actor,
      @rid,
      (SELECT g.NoProduksi, g.NoFurnitureWIPPartial, g.NoFurnitureWIP FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      COALESCE((
        SELECT
          d2.NoProduksi,
          d2.NoFurnitureWIPPartial,
          fwp2.NoFurnitureWIP,
          CAST(fwp2.Pcs AS int) AS Pcs
        FROM deleted d2
        LEFT JOIN dbo.FurnitureWIPPartial fwp2
          ON fwp2.NoFurnitureWIPPartial = d2.NoFurnitureWIPPartial
        WHERE d2.NoProduksi            = g.NoProduksi
          AND d2.NoFurnitureWIPPartial = g.NoFurnitureWIPPartial
          AND (
                (fwp2.NoFurnitureWIP = g.NoFurnitureWIP)
                OR (fwp2.NoFurnitureWIP IS NULL AND g.NoFurnitureWIP IS NULL)
              )
        FOR JSON PATH
      ), '[]'),

      COALESCE((
        SELECT
          i2.NoProduksi,
          i2.NoFurnitureWIPPartial,
          fwp2.NoFurnitureWIP,
          CAST(fwp2.Pcs AS int) AS Pcs
        FROM inserted i2
        LEFT JOIN dbo.FurnitureWIPPartial fwp2
          ON fwp2.NoFurnitureWIPPartial = i2.NoFurnitureWIPPartial
        WHERE i2.NoProduksi            = g.NoProduksi
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

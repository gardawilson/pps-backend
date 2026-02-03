SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJJual_dLabelBarangJadiPartial]
   ON [dbo].[BJJual_dLabelBarangJadiPartial]
   AFTER INSERT, UPDATE, DELETE

   Action:
   - INSERT => CONSUME_PARTIAL
   - DELETE => UNCONSUME_PARTIAL
   - UPDATE => UPDATE

   NOTE:
   - 1 row audit per statement per (NoBJJual, NoBJPartial, NoBJ)
   - PK JSON: {NoBJJual, NoBJPartial, NoBJ}
   - OldData/NewData: array of {NoBJJual, NoBJPartial, NoBJ, Pcs}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJJual_dLabelBarangJadiPartial]
ON [dbo].[BJJual_dLabelBarangJadiPartial]
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
    SELECT i.NoBJJual, i.NoBJPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoBJJual    = i.NoBJJual
        AND d.NoBJPartial = i.NoBJPartial
    )
  ),
  insEnriched AS (
    SELECT
      x.NoBJJual,
      x.NoBJPartial,
      bjp.NoBJ,
      bjp.Pcs
    FROM insOnly x
    LEFT JOIN dbo.BarangJadiPartial bjp
      ON bjp.NoBJPartial = x.NoBJPartial
  ),
  grp AS (
    SELECT DISTINCT NoBJJual, NoBJPartial, NoBJ
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'BJJual_dLabelBarangJadiPartial',
    @actor,
    @rid,
    (SELECT g.NoBJJual, g.NoBJPartial, g.NoBJ
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    COALESCE((
      SELECT
        e.NoBJJual,
        e.NoBJPartial,
        e.NoBJ,
        CAST(e.Pcs AS int) AS Pcs
      FROM insEnriched e
      WHERE e.NoBJJual    = g.NoBJJual
        AND e.NoBJPartial = g.NoBJPartial
        AND (
              (e.NoBJ = g.NoBJ)
              OR (e.NoBJ IS NULL AND g.NoBJ IS NULL)
            )
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoBJJual, d.NoBJPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoBJJual    = d.NoBJJual
        AND i.NoBJPartial = d.NoBJPartial
    )
  ),
  delEnriched AS (
    SELECT
      x.NoBJJual,
      x.NoBJPartial,
      bjp.NoBJ,
      bjp.Pcs
    FROM delOnly x
    LEFT JOIN dbo.BarangJadiPartial bjp
      ON bjp.NoBJPartial = x.NoBJPartial
  ),
  grp AS (
    SELECT DISTINCT NoBJJual, NoBJPartial, NoBJ
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'BJJual_dLabelBarangJadiPartial',
    @actor,
    @rid,
    (SELECT g.NoBJJual, g.NoBJPartial, g.NoBJ
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    COALESCE((
      SELECT
        e.NoBJJual,
        e.NoBJPartial,
        e.NoBJ,
        CAST(e.Pcs AS int) AS Pcs
      FROM delEnriched e
      WHERE e.NoBJJual    = g.NoBJJual
        AND e.NoBJPartial = g.NoBJPartial
        AND (
              (e.NoBJ = g.NoBJ)
              OR (e.NoBJ IS NULL AND g.NoBJ IS NULL)
            )
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g;

  /* ======================================================
     UPDATE => UPDATE
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted)
     AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp0 AS (
      SELECT DISTINCT i.NoBJJual, i.NoBJPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoBJJual    = i.NoBJJual
       AND d.NoBJPartial = i.NoBJPartial
    ),
    grp AS (
      SELECT DISTINCT
        g0.NoBJJual,
        g0.NoBJPartial,
        bjp.NoBJ
      FROM grp0 g0
      LEFT JOIN dbo.BarangJadiPartial bjp
        ON bjp.NoBJPartial = g0.NoBJPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BJJual_dLabelBarangJadiPartial',
      @actor,
      @rid,
      (SELECT g.NoBJJual, g.NoBJPartial, g.NoBJ
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      COALESCE((
        SELECT
          d2.NoBJJual,
          d2.NoBJPartial,
          bjp2.NoBJ,
          CAST(bjp2.Pcs AS int) AS Pcs
        FROM deleted d2
        LEFT JOIN dbo.BarangJadiPartial bjp2
          ON bjp2.NoBJPartial = d2.NoBJPartial
        WHERE d2.NoBJJual    = g.NoBJJual
          AND d2.NoBJPartial = g.NoBJPartial
          AND (
                (bjp2.NoBJ = g.NoBJ)
                OR (bjp2.NoBJ IS NULL AND g.NoBJ IS NULL)
              )
        FOR JSON PATH
      ), '[]'),

      COALESCE((
        SELECT
          i2.NoBJJual,
          i2.NoBJPartial,
          bjp2.NoBJ,
          CAST(bjp2.Pcs AS int) AS Pcs
        FROM inserted i2
        LEFT JOIN dbo.BarangJadiPartial bjp2
          ON bjp2.NoBJPartial = i2.NoBJPartial
        WHERE i2.NoBJJual    = g.NoBJJual
          AND i2.NoBJPartial = g.NoBJPartial
          AND (
                (bjp2.NoBJ = g.NoBJ)
                OR (bjp2.NoBJ IS NULL AND g.NoBJ IS NULL)
              )
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

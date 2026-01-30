/* ===== [dbo].[tr_Audit_GilinganProduksiInputBrokerPartial] ON [dbo].[GilinganProduksiInputBrokerPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoProduksi, NoBrokerPartial)
   - PK JSON: NoProduksi, NoBrokerPartial, NoBroker
   - NoBroker + NoSak + Berat diambil dari dbo.BrokerPartial (bp.NoBroker, bp.NoSak, bp.Berat)
   - NewData/OldData: array of {NoProduksi, NoBrokerPartial, NoBroker, NoSak, Berat}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_GilinganProduksiInputBrokerPartial]
ON [dbo].[GilinganProduksiInputBrokerPartial]
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
    SELECT i.NoProduksi, i.NoBrokerPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoProduksi      = i.NoProduksi
        AND d.NoBrokerPartial = i.NoBrokerPartial
    )
  ),
  insEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoBrokerPartial,
      bp.NoBroker,
      bp.NoSak,
      bp.Berat
    FROM insOnly x
    LEFT JOIN dbo.BrokerPartial bp
      ON bp.NoBrokerPartial = x.NoBrokerPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoBrokerPartial, NoBroker
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'GilinganProduksiInputBrokerPartial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoBrokerPartial, g.NoBroker FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoBrokerPartial,
        e.NoBroker,
        e.NoSak,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM insEnriched e
      WHERE e.NoProduksi      = g.NoProduksi
        AND e.NoBrokerPartial = g.NoBrokerPartial
        AND (
              (e.NoBroker = g.NoBroker)
              OR (e.NoBroker IS NULL AND g.NoBroker IS NULL)
            )
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoBrokerPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi      = d.NoProduksi
        AND i.NoBrokerPartial = d.NoBrokerPartial
    )
  ),
  delEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoBrokerPartial,
      bp.NoBroker,
      bp.NoSak,
      bp.Berat
    FROM delOnly x
    LEFT JOIN dbo.BrokerPartial bp
      ON bp.NoBrokerPartial = x.NoBrokerPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoBrokerPartial, NoBroker
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'GilinganProduksiInputBrokerPartial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoBrokerPartial, g.NoBroker FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoBrokerPartial,
        e.NoBroker,
        e.NoSak,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM delEnriched e
      WHERE e.NoProduksi      = g.NoProduksi
        AND e.NoBrokerPartial = g.NoBrokerPartial
        AND (
              (e.NoBroker = g.NoBroker)
              OR (e.NoBroker IS NULL AND g.NoBroker IS NULL)
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
      SELECT DISTINCT i.NoProduksi, i.NoBrokerPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi      = i.NoProduksi
       AND d.NoBrokerPartial = i.NoBrokerPartial
    ),
    grp AS (
      SELECT DISTINCT
        g0.NoProduksi,
        g0.NoBrokerPartial,
        bp.NoBroker
      FROM grp0 g0
      LEFT JOIN dbo.BrokerPartial bp
        ON bp.NoBrokerPartial = g0.NoBrokerPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'GilinganProduksiInputBrokerPartial',
      @actor,
      @rid,
      (SELECT g.NoProduksi, g.NoBrokerPartial, g.NoBroker FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      COALESCE((
        SELECT
          d2.NoProduksi,
          d2.NoBrokerPartial,
          bp.NoBroker,
          bp.NoSak,
          CAST(bp.Berat AS decimal(18,3)) AS Berat
        FROM deleted d2
        LEFT JOIN dbo.BrokerPartial bp
          ON bp.NoBrokerPartial = d2.NoBrokerPartial
        WHERE d2.NoProduksi      = g.NoProduksi
          AND d2.NoBrokerPartial = g.NoBrokerPartial
          AND (
                (bp.NoBroker = g.NoBroker)
                OR (bp.NoBroker IS NULL AND g.NoBroker IS NULL)
              )
        FOR JSON PATH
      ), '[]'),

      COALESCE((
        SELECT
          i2.NoProduksi,
          i2.NoBrokerPartial,
          bp.NoBroker,
          bp.NoSak,
          CAST(bp.Berat AS decimal(18,3)) AS Berat
        FROM inserted i2
        LEFT JOIN dbo.BrokerPartial bp
          ON bp.NoBrokerPartial = i2.NoBrokerPartial
        WHERE i2.NoProduksi      = g.NoProduksi
          AND i2.NoBrokerPartial = g.NoBrokerPartial
          AND (
                (bp.NoBroker = g.NoBroker)
                OR (bp.NoBroker IS NULL AND g.NoBroker IS NULL)
              )
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

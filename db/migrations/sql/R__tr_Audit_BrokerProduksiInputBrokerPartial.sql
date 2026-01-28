/* ===== [dbo].[tr_Audit_BrokerProduksiInputBrokerPartial] ON [dbo].[BrokerProduksiInputBrokerPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoProduksi, NoBrokerPartial)
   - PK: NoProduksi + NoBrokerPartial + NoBroker (TANPA NoSak)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BrokerProduksiInputBrokerPartial]
ON [dbo].[BrokerProduksiInputBrokerPartial]
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
      WHERE d.NoProduksi = i.NoProduksi
        AND d.NoBrokerPartial = i.NoBrokerPartial
    )
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoBrokerPartial
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'BrokerProduksiInputBrokerPartial',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', g.NoProduksi,
      '","NoBrokerPartial":"', g.NoBrokerPartial,
      '","NoBroker":', CASE WHEN bp.NoBroker IS NULL THEN 'null' ELSE CONCAT('"', bp.NoBroker, '"') END,
      '}'
    ),
    NULL,
    COALESCE((
      SELECT
        i2.NoProduksi,
        i2.NoBrokerPartial,
        bp2.NoBroker,
        bp2.NoSak,
        CAST(bp2.Berat AS decimal(18,3)) AS Berat
      FROM inserted i2
      LEFT JOIN dbo.BrokerPartial bp2
        ON bp2.NoBrokerPartial = i2.NoBrokerPartial
      WHERE i2.NoProduksi = g.NoProduksi
        AND i2.NoBrokerPartial = g.NoBrokerPartial
      ORDER BY bp2.NoBroker, bp2.NoSak
      FOR JSON PATH
    ), '[]')
  FROM grp g
  LEFT JOIN dbo.BrokerPartial bp
    ON bp.NoBrokerPartial = g.NoBrokerPartial;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoBrokerPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi = d.NoProduksi
        AND i.NoBrokerPartial = d.NoBrokerPartial
    )
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoBrokerPartial
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'BrokerProduksiInputBrokerPartial',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', g.NoProduksi,
      '","NoBrokerPartial":"', g.NoBrokerPartial,
      '","NoBroker":', CASE WHEN bp.NoBroker IS NULL THEN 'null' ELSE CONCAT('"', bp.NoBroker, '"') END,
      '}'
    ),
    COALESCE((
      SELECT
        d2.NoProduksi,
        d2.NoBrokerPartial,
        bp2.NoBroker,
        bp2.NoSak,
        CAST(bp2.Berat AS decimal(18,3)) AS Berat
      FROM deleted d2
      LEFT JOIN dbo.BrokerPartial bp2
        ON bp2.NoBrokerPartial = d2.NoBrokerPartial
      WHERE d2.NoProduksi = g.NoProduksi
        AND d2.NoBrokerPartial = g.NoBrokerPartial
      ORDER BY bp2.NoBroker, bp2.NoSak
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g
  LEFT JOIN dbo.BrokerPartial bp
    ON bp.NoBrokerPartial = g.NoBrokerPartial;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp AS (
      SELECT DISTINCT i.NoProduksi, i.NoBrokerPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoBrokerPartial = i.NoBrokerPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BrokerProduksiInputBrokerPartial',
      @actor,
      @rid,
      CONCAT(
        '{"NoProduksi":"', g.NoProduksi,
        '","NoBrokerPartial":"', g.NoBrokerPartial,
        '","NoBroker":', CASE WHEN bp.NoBroker IS NULL THEN 'null' ELSE CONCAT('"', bp.NoBroker, '"') END,
        '}'
      ),
      COALESCE((
        SELECT
          d2.NoProduksi,
          d2.NoBrokerPartial,
          bp2.NoBroker,
          bp2.NoSak,
          CAST(bp2.Berat AS decimal(18,3)) AS Berat
        FROM deleted d2
        LEFT JOIN dbo.BrokerPartial bp2
          ON bp2.NoBrokerPartial = d2.NoBrokerPartial
        WHERE d2.NoProduksi = g.NoProduksi
          AND d2.NoBrokerPartial = g.NoBrokerPartial
        ORDER BY bp2.NoBroker, bp2.NoSak
        FOR JSON PATH
      ), '[]'),
      COALESCE((
        SELECT
          i2.NoProduksi,
          i2.NoBrokerPartial,
          bp2.NoBroker,
          bp2.NoSak,
          CAST(bp2.Berat AS decimal(18,3)) AS Berat
        FROM inserted i2
        LEFT JOIN dbo.BrokerPartial bp2
          ON bp2.NoBrokerPartial = i2.NoBrokerPartial
        WHERE i2.NoProduksi = g.NoProduksi
          AND i2.NoBrokerPartial = g.NoBrokerPartial
        ORDER BY bp2.NoBroker, bp2.NoSak
        FOR JSON PATH
      ), '[]')
    FROM grp g
    LEFT JOIN dbo.BrokerPartial bp
      ON bp.NoBrokerPartial = g.NoBrokerPartial;
  END
END;
GO

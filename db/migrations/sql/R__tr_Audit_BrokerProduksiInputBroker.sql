/* ===== [dbo].[tr_Audit_BrokerProduksiInputBroker] ON [dbo].[BrokerProduksiInputBroker] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoBroker) per statement
   PK JSON: { NoProduksi, NoBroker }
   NewData/OldData: array of {NoProduksi, NoBroker, NoSak, Berat, IsPartial}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BrokerProduksiInputBroker]
ON [dbo].[BrokerProduksiInputBroker]
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
      ON d.NoProduksi = i.NoProduksi
     AND d.NoBroker   = i.NoBroker
     AND d.NoSak      = i.NoSak
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoBroker
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BrokerProduksiInputBroker',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoBroker FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoBroker,
        x.NoSak,
        CAST(bd.Berat AS decimal(18,3)) AS Berat,
        CAST(bd.IsPartial AS bit) AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.Broker_d bd
        ON bd.NoBroker = x.NoBroker AND bd.NoSak = x.NoSak
      WHERE x.NoProduksi = gg.NoProduksi AND x.NoBroker = gg.NoBroker
      ORDER BY x.NoSak
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
      ON i.NoProduksi = d.NoProduksi
     AND i.NoBroker   = d.NoBroker
     AND i.NoSak      = d.NoSak
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoBroker
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BrokerProduksiInputBroker',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoBroker FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoBroker,
        x.NoSak,
        CAST(bd.Berat AS decimal(18,3)) AS Berat,
        CAST(bd.IsPartial AS bit) AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.Broker_d bd
        ON bd.NoBroker = x.NoBroker AND bd.NoSak = x.NoSak
      WHERE x.NoProduksi = gg.NoProduksi AND x.NoBroker = gg.NoBroker
      ORDER BY x.NoSak
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
      SELECT i.NoProduksi, i.NoBroker, i.NoSak
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoBroker   = i.NoBroker
       AND d.NoSak      = i.NoSak
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoBroker
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BrokerProduksiInputBroker',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoBroker FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoBroker,
          d.NoSak,
          CAST(bd.Berat AS decimal(18,3)) AS Berat,
          CAST(bd.IsPartial AS bit) AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.Broker_d bd
          ON bd.NoBroker = d.NoBroker AND bd.NoSak = d.NoSak
        WHERE d.NoProduksi = gg.NoProduksi AND d.NoBroker = gg.NoBroker
        ORDER BY d.NoSak
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoBroker,
          i.NoSak,
          CAST(bd.Berat AS decimal(18,3)) AS Berat,
          CAST(bd.IsPartial AS bit) AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.Broker_d bd
          ON bd.NoBroker = i.NoBroker AND bd.NoSak = i.NoSak
        WHERE i.NoProduksi = gg.NoProduksi AND i.NoBroker = gg.NoBroker
        ORDER BY i.NoSak
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

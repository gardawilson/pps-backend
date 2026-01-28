/* ===== [dbo].[tr_Audit_BrokerProduksiInputCrusher] ON [dbo].[BrokerProduksiInputCrusher] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoCrusher) per statement
   PK JSON: { NoProduksi, NoCrusher }
   NewData/OldData: array of {NoProduksi, NoCrusher, Berat, IsPartial}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BrokerProduksiInputCrusher]
ON [dbo].[BrokerProduksiInputCrusher]
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

  /* 1) INSERT-only => CONSUME_FULL */
  ;WITH insOnly AS (
    SELECT i.*
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoCrusher  = i.NoCrusher
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoCrusher
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BrokerProduksiInputCrusher',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoCrusher FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoCrusher,
        CAST(c.Berat AS decimal(18,3)) AS Berat,
        CAST(0 AS bit)                 AS IsPartial
      FROM insOnly x
      LEFT JOIN dbo.Crusher c
        ON c.NoCrusher = x.NoCrusher
      WHERE x.NoProduksi = gg.NoProduksi AND x.NoCrusher = gg.NoCrusher
      FOR JSON PATH
    )
  FROM g gg;

  /* 2) DELETE-only => UNCONSUME_FULL */
  ;WITH delOnly AS (
    SELECT d.*
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoProduksi = d.NoProduksi
     AND i.NoCrusher  = d.NoCrusher
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoCrusher
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BrokerProduksiInputCrusher',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoCrusher FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoCrusher,
        CAST(c.Berat AS decimal(18,3)) AS Berat,
        CAST(0 AS bit)                 AS IsPartial
      FROM delOnly x
      LEFT JOIN dbo.Crusher c
        ON c.NoCrusher = x.NoCrusher
      WHERE x.NoProduksi = gg.NoProduksi AND x.NoCrusher = gg.NoCrusher
      FOR JSON PATH
    ),
    NULL
  FROM g gg;

  /* 3) UPDATE => UPDATE */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH upd AS (
      SELECT i.NoProduksi, i.NoCrusher
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoCrusher  = i.NoCrusher
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoCrusher
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BrokerProduksiInputCrusher',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoCrusher FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoCrusher,
          CAST(c.Berat AS decimal(18,3)) AS Berat,
          CAST(0 AS bit)                 AS IsPartial
        FROM deleted d
        LEFT JOIN dbo.Crusher c
          ON c.NoCrusher = d.NoCrusher
        WHERE d.NoProduksi = gg.NoProduksi AND d.NoCrusher = gg.NoCrusher
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoCrusher,
          CAST(c.Berat AS decimal(18,3)) AS Berat,
          CAST(0 AS bit)                 AS IsPartial
        FROM inserted i
        LEFT JOIN dbo.Crusher c
          ON c.NoCrusher = i.NoCrusher
        WHERE i.NoProduksi = gg.NoProduksi AND i.NoCrusher = gg.NoCrusher
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

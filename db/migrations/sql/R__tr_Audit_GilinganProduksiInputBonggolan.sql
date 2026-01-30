/* ===== [dbo].[tr_Audit_GilinganProduksiInputBonggolan] ON [dbo].[GilinganProduksiInputBonggolan] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoBonggolan) per statement
   PK JSON: { NoProduksi, NoBonggolan }
   NewData/OldData: array of {NoProduksi, NoBonggolan, DatetimeInput, Berat}
   Enrichment:
   - Berat diambil dari dbo.Bonggolan (b.Berat) berdasarkan NoBonggolan
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_GilinganProduksiInputBonggolan]
ON [dbo].[GilinganProduksiInputBonggolan]
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
      ON d.NoProduksi  = i.NoProduksi
     AND d.NoBonggolan = i.NoBonggolan
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoBonggolan
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'GilinganProduksiInputBonggolan',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoBonggolan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoBonggolan,
        x.DatetimeInput,
        CAST(b.Berat AS decimal(18,3)) AS Berat
      FROM insOnly x
      LEFT JOIN dbo.Bonggolan b
        ON b.NoBonggolan = x.NoBonggolan
      WHERE x.NoProduksi  = gg.NoProduksi
        AND x.NoBonggolan = gg.NoBonggolan
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
      ON i.NoProduksi  = d.NoProduksi
     AND i.NoBonggolan = d.NoBonggolan
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoBonggolan
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'GilinganProduksiInputBonggolan',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoBonggolan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoBonggolan,
        x.DatetimeInput,
        CAST(b.Berat AS decimal(18,3)) AS Berat
      FROM delOnly x
      LEFT JOIN dbo.Bonggolan b
        ON b.NoBonggolan = x.NoBonggolan
      WHERE x.NoProduksi  = gg.NoProduksi
        AND x.NoBonggolan = gg.NoBonggolan
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
      SELECT i.NoProduksi, i.NoBonggolan
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi  = i.NoProduksi
       AND d.NoBonggolan = i.NoBonggolan
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoBonggolan
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'GilinganProduksiInputBonggolan',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoBonggolan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoBonggolan,
          d.DatetimeInput,
          CAST(b.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.Bonggolan b
          ON b.NoBonggolan = d.NoBonggolan
        WHERE d.NoProduksi  = gg.NoProduksi
          AND d.NoBonggolan = gg.NoBonggolan
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoBonggolan,
          i.DatetimeInput,
          CAST(b.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.Bonggolan b
          ON b.NoBonggolan = i.NoBonggolan
        WHERE i.NoProduksi  = gg.NoProduksi
          AND i.NoBonggolan = gg.NoBonggolan
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

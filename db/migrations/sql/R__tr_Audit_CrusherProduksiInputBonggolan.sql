/* ===== [dbo].[tr_Audit_CrusherProduksiInputBonggolan] ON [dbo].[CrusherProduksiInputBonggolan] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoCrusherProduksi, NoBonggolan) per statement
   PK JSON: { NoCrusherProduksi, NoBonggolan }
   NewData/OldData: array of {NoCrusherProduksi, NoBonggolan, Berat}
   NOTE:
   - Table hanya punya 2 kolom, jadi untuk "Berat" kita enrichment dari tabel master Bonggolan.
     Jika beratnya bukan di dbo.Bonggolan, ganti join + field sesuai tabelmu.
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_CrusherProduksiInputBonggolan]
ON [dbo].[CrusherProduksiInputBonggolan]
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
      ON d.NoCrusherProduksi = i.NoCrusherProduksi
     AND d.NoBonggolan       = i.NoBonggolan
    WHERE d.NoCrusherProduksi IS NULL
  ),
  insEnriched AS (
    SELECT
      x.NoCrusherProduksi,
      x.NoBonggolan,
      bh.Berat
    FROM insOnly x
    LEFT JOIN dbo.Bonggolan bh
      ON bh.NoBonggolan = x.NoBonggolan
  ),
  g AS (
    SELECT DISTINCT NoCrusherProduksi, NoBonggolan
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'CrusherProduksiInputBonggolan',
    @actor,
    @rid,
    (SELECT gg.NoCrusherProduksi, gg.NoBonggolan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        e.NoCrusherProduksi,
        e.NoBonggolan,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM insEnriched e
      WHERE e.NoCrusherProduksi = gg.NoCrusherProduksi
        AND e.NoBonggolan       = gg.NoBonggolan
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
      ON i.NoCrusherProduksi = d.NoCrusherProduksi
     AND i.NoBonggolan       = d.NoBonggolan
    WHERE i.NoCrusherProduksi IS NULL
  ),
  delEnriched AS (
    SELECT
      x.NoCrusherProduksi,
      x.NoBonggolan,
      bh.Berat
    FROM delOnly x
    LEFT JOIN dbo.Bonggolan bh
      ON bh.NoBonggolan = x.NoBonggolan
  ),
  g AS (
    SELECT DISTINCT NoCrusherProduksi, NoBonggolan
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'CrusherProduksiInputBonggolan',
    @actor,
    @rid,
    (SELECT gg.NoCrusherProduksi, gg.NoBonggolan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        e.NoCrusherProduksi,
        e.NoBonggolan,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM delEnriched e
      WHERE e.NoCrusherProduksi = gg.NoCrusherProduksi
        AND e.NoBonggolan       = gg.NoBonggolan
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
      SELECT i.NoCrusherProduksi, i.NoBonggolan
      FROM inserted i
      JOIN deleted d
        ON d.NoCrusherProduksi = i.NoCrusherProduksi
       AND d.NoBonggolan       = i.NoBonggolan
    ),
    g AS (
      SELECT DISTINCT NoCrusherProduksi, NoBonggolan
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'CrusherProduksiInputBonggolan',
      @actor,
      @rid,
      (SELECT gg.NoCrusherProduksi, gg.NoBonggolan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoCrusherProduksi,
          d.NoBonggolan,
          CAST(bh.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.Bonggolan bh
          ON bh.NoBonggolan = d.NoBonggolan
        WHERE d.NoCrusherProduksi = gg.NoCrusherProduksi
          AND d.NoBonggolan       = gg.NoBonggolan
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoCrusherProduksi,
          i.NoBonggolan,
          CAST(bh.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.Bonggolan bh
          ON bh.NoBonggolan = i.NoBonggolan
        WHERE i.NoCrusherProduksi = gg.NoCrusherProduksi
          AND i.NoBonggolan       = gg.NoBonggolan
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

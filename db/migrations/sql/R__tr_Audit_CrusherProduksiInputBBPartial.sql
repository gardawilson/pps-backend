/* ===== [dbo].[tr_Audit_CrusherProduksiInputBBPartial] ON [dbo].[CrusherProduksiInputBBPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoCrusherProduksi, NoBBPartial)
   - PK JSON: NoCrusherProduksi, NoBBPartial, NoBahanBaku, NoPallet
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_CrusherProduksiInputBBPartial]
ON [dbo].[CrusherProduksiInputBBPartial]
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
    SELECT i.NoCrusherProduksi, i.NoBBPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoCrusherProduksi = i.NoCrusherProduksi
        AND d.NoBBPartial       = i.NoBBPartial
    )
  ),
  grp AS (
    SELECT DISTINCT NoCrusherProduksi, NoBBPartial
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'CrusherProduksiInputBBPartial',
    @actor,
    @rid,
    CONCAT(
      '{"NoCrusherProduksi":"', g.NoCrusherProduksi,
      '","NoBBPartial":"', g.NoBBPartial,
      '","NoBahanBaku":', CASE WHEN bb.NoBahanBaku IS NULL THEN 'null' ELSE CONCAT('"', bb.NoBahanBaku, '"') END,
      ',"NoPallet":',     CASE WHEN bb.NoPallet   IS NULL THEN 'null' ELSE CAST(bb.NoPallet AS varchar(20)) END,
      '}'
    ),
    NULL,
    COALESCE((
      SELECT
        i2.NoCrusherProduksi,
        i2.NoBBPartial,
        bb2.NoBahanBaku,
        bb2.NoPallet,
        bb2.NoSak,
        CAST(bb2.Berat AS decimal(18,3)) AS Berat
      FROM inserted i2
      LEFT JOIN dbo.BahanBakuPartial bb2
        ON bb2.NoBBPartial = i2.NoBBPartial
      WHERE i2.NoCrusherProduksi = g.NoCrusherProduksi
        AND i2.NoBBPartial       = g.NoBBPartial
      ORDER BY bb2.NoBahanBaku, bb2.NoPallet, bb2.NoSak
      FOR JSON PATH
    ), '[]')
  FROM grp g
  LEFT JOIN dbo.BahanBakuPartial bb
    ON bb.NoBBPartial = g.NoBBPartial;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoCrusherProduksi, d.NoBBPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoCrusherProduksi = d.NoCrusherProduksi
        AND i.NoBBPartial       = d.NoBBPartial
    )
  ),
  grp AS (
    SELECT DISTINCT NoCrusherProduksi, NoBBPartial
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'CrusherProduksiInputBBPartial',
    @actor,
    @rid,
    CONCAT(
      '{"NoCrusherProduksi":"', g.NoCrusherProduksi,
      '","NoBBPartial":"', g.NoBBPartial,
      '","NoBahanBaku":', CASE WHEN bb.NoBahanBaku IS NULL THEN 'null' ELSE CONCAT('"', bb.NoBahanBaku, '"') END,
      ',"NoPallet":',     CASE WHEN bb.NoPallet   IS NULL THEN 'null' ELSE CAST(bb.NoPallet AS varchar(20)) END,
      '}'
    ),
    COALESCE((
      SELECT
        d2.NoCrusherProduksi,
        d2.NoBBPartial,
        bb2.NoBahanBaku,
        bb2.NoPallet,
        bb2.NoSak,
        CAST(bb2.Berat AS decimal(18,3)) AS Berat
      FROM deleted d2
      LEFT JOIN dbo.BahanBakuPartial bb2
        ON bb2.NoBBPartial = d2.NoBBPartial
      WHERE d2.NoCrusherProduksi = g.NoCrusherProduksi
        AND d2.NoBBPartial       = g.NoBBPartial
      ORDER BY bb2.NoBahanBaku, bb2.NoPallet, bb2.NoSak
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g
  LEFT JOIN dbo.BahanBakuPartial bb
    ON bb.NoBBPartial = g.NoBBPartial;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp AS (
      SELECT DISTINCT i.NoCrusherProduksi, i.NoBBPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoCrusherProduksi = i.NoCrusherProduksi
       AND d.NoBBPartial       = i.NoBBPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'CrusherProduksiInputBBPartial',
      @actor,
      @rid,
      CONCAT(
        '{"NoCrusherProduksi":"', g.NoCrusherProduksi,
        '","NoBBPartial":"', g.NoBBPartial,
        '","NoBahanBaku":', CASE WHEN bb.NoBahanBaku IS NULL THEN 'null' ELSE CONCAT('"', bb.NoBahanBaku, '"') END,
        ',"NoPallet":',     CASE WHEN bb.NoPallet   IS NULL THEN 'null' ELSE CAST(bb.NoPallet AS varchar(20)) END,
        '}'
      ),
      COALESCE((
        SELECT
          d2.NoCrusherProduksi,
          d2.NoBBPartial,
          bb2.NoBahanBaku,
          bb2.NoPallet,
          bb2.NoSak,
          CAST(bb2.Berat AS decimal(18,3)) AS Berat
        FROM deleted d2
        LEFT JOIN dbo.BahanBakuPartial bb2
          ON bb2.NoBBPartial = d2.NoBBPartial
        WHERE d2.NoCrusherProduksi = g.NoCrusherProduksi
          AND d2.NoBBPartial       = g.NoBBPartial
        ORDER BY bb2.NoBahanBaku, bb2.NoPallet, bb2.NoSak
        FOR JSON PATH
      ), '[]'),
      COALESCE((
        SELECT
          i2.NoCrusherProduksi,
          i2.NoBBPartial,
          bb2.NoBahanBaku,
          bb2.NoPallet,
          bb2.NoSak,
          CAST(bb2.Berat AS decimal(18,3)) AS Berat
        FROM inserted i2
        LEFT JOIN dbo.BahanBakuPartial bb2
          ON bb2.NoBBPartial = i2.NoBBPartial
        WHERE i2.NoCrusherProduksi = g.NoCrusherProduksi
          AND i2.NoBBPartial       = g.NoBBPartial
        ORDER BY bb2.NoBahanBaku, bb2.NoPallet, bb2.NoSak
        FOR JSON PATH
      ), '[]')
    FROM grp g
    LEFT JOIN dbo.BahanBakuPartial bb
      ON bb.NoBBPartial = g.NoBBPartial;
  END
END;
GO

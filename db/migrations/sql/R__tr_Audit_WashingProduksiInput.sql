/* ===== [dbo].[tr_Audit_WashingProduksiInput] ON [dbo].[WashingProduksiInput] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE

   NOTE:
   - AuditTrail 1 row per statement per (NoProduksi, NoBahanBaku, NoPallet)
   - PK JSON: NoProduksi, NoBahanBaku, NoPallet (tanpa NoSak)
   - Data detail per sak disimpan dalam JSON array
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_WashingProduksiInput]
ON [dbo].[WashingProduksiInput]
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
     INSERT-only => CONSUME_FULL (GROUPED)
     1 row per (NoProduksi, NoBahanBaku, NoPallet)
  ====================================================== */
  ;WITH insOnly AS (
    SELECT i.NoProduksi, i.NoBahanBaku, i.NoPallet
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoProduksi  = i.NoProduksi
        AND d.NoBahanBaku = i.NoBahanBaku
        AND d.NoPallet    = i.NoPallet
        AND d.NoSak       = i.NoSak
    )
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoBahanBaku, NoPallet
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'WashingProduksiInput',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', g.NoProduksi,
      '","NoBahanBaku":"', g.NoBahanBaku,
      '","NoPallet":', CASE WHEN g.NoPallet IS NULL THEN 'null' ELSE CAST(g.NoPallet AS varchar(20)) END,
      '}'
    ),
    NULL,
    COALESCE((
      SELECT
        i2.NoProduksi,
        i2.NoBahanBaku,
        i2.NoPallet,
        i2.NoSak,
        CAST(bb.Berat AS decimal(18,3)) AS Berat,
        bb.IsPartial,
        bb.IsLembab
      FROM inserted i2
      LEFT JOIN dbo.BahanBaku_d bb
        ON bb.NoBahanBaku = i2.NoBahanBaku
       AND bb.NoPallet    = i2.NoPallet
       AND bb.NoSak       = i2.NoSak
      WHERE i2.NoProduksi  = g.NoProduksi
        AND i2.NoBahanBaku = g.NoBahanBaku
        AND (
              (i2.NoPallet = g.NoPallet)
              OR (i2.NoPallet IS NULL AND g.NoPallet IS NULL)
            )
      ORDER BY i2.NoSak
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_FULL (GROUPED)
     1 row per (NoProduksi, NoBahanBaku, NoPallet)
  ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoBahanBaku, d.NoPallet
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi  = d.NoProduksi
        AND i.NoBahanBaku = d.NoBahanBaku
        AND i.NoPallet    = d.NoPallet
        AND i.NoSak       = d.NoSak
    )
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoBahanBaku, NoPallet
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'WashingProduksiInput',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', g.NoProduksi,
      '","NoBahanBaku":"', g.NoBahanBaku,
      '","NoPallet":', CASE WHEN g.NoPallet IS NULL THEN 'null' ELSE CAST(g.NoPallet AS varchar(20)) END,
      '}'
    ),
    COALESCE((
      SELECT
        d2.NoProduksi,
        d2.NoBahanBaku,
        d2.NoPallet,
        d2.NoSak,
        CAST(bb.Berat AS decimal(18,3)) AS Berat,
        bb.IsPartial,
        bb.IsLembab
      FROM deleted d2
      LEFT JOIN dbo.BahanBaku_d bb
        ON bb.NoBahanBaku = d2.NoBahanBaku
       AND bb.NoPallet    = d2.NoPallet
       AND bb.NoSak       = d2.NoSak
      WHERE d2.NoProduksi  = g.NoProduksi
        AND d2.NoBahanBaku = g.NoBahanBaku
        AND (
              (d2.NoPallet = g.NoPallet)
              OR (d2.NoPallet IS NULL AND g.NoPallet IS NULL)
            )
      ORDER BY d2.NoSak
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     1 row per (NoProduksi, NoBahanBaku, NoPallet)
  ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp AS (
      SELECT DISTINCT i.NoProduksi, i.NoBahanBaku, i.NoPallet
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi  = i.NoProduksi
       AND d.NoBahanBaku = i.NoBahanBaku
       AND (
             (d.NoPallet = i.NoPallet)
             OR (d.NoPallet IS NULL AND i.NoPallet IS NULL)
           )
       AND d.NoSak       = i.NoSak
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'WashingProduksiInput',
      @actor,
      @rid,
      CONCAT(
        '{"NoProduksi":"', g.NoProduksi,
        '","NoBahanBaku":"', g.NoBahanBaku,
        '","NoPallet":', CASE WHEN g.NoPallet IS NULL THEN 'null' ELSE CAST(g.NoPallet AS varchar(20)) END,
        '}'
      ),
      COALESCE((
        SELECT
          d2.NoProduksi,
          d2.NoBahanBaku,
          d2.NoPallet,
          d2.NoSak,
          CAST(bb.Berat AS decimal(18,3)) AS Berat,
          bb.IsPartial,
          bb.IsLembab
        FROM deleted d2
        LEFT JOIN dbo.BahanBaku_d bb
          ON bb.NoBahanBaku = d2.NoBahanBaku
         AND bb.NoPallet    = d2.NoPallet
         AND bb.NoSak       = d2.NoSak
        WHERE d2.NoProduksi  = g.NoProduksi
          AND d2.NoBahanBaku = g.NoBahanBaku
          AND (
                (d2.NoPallet = g.NoPallet)
                OR (d2.NoPallet IS NULL AND g.NoPallet IS NULL)
              )
        ORDER BY d2.NoSak
        FOR JSON PATH
      ), '[]'),
      COALESCE((
        SELECT
          i2.NoProduksi,
          i2.NoBahanBaku,
          i2.NoPallet,
          i2.NoSak,
          CAST(bb.Berat AS decimal(18,3)) AS Berat,
          bb.IsPartial,
          bb.IsLembab
        FROM inserted i2
        LEFT JOIN dbo.BahanBaku_d bb
          ON bb.NoBahanBaku = i2.NoBahanBaku
         AND bb.NoPallet    = i2.NoPallet
         AND bb.NoSak       = i2.NoSak
        WHERE i2.NoProduksi  = g.NoProduksi
          AND i2.NoBahanBaku = g.NoBahanBaku
          AND (
                (i2.NoPallet = g.NoPallet)
                OR (i2.NoPallet IS NULL AND g.NoPallet IS NULL)
              )
        ORDER BY i2.NoSak
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

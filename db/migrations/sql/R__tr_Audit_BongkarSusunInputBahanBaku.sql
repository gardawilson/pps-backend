SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunInputBahanBaku] ON [dbo].[BongkarSusunInputBahanBaku] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE

   NOTE:
   - AuditTrail 1 row per statement per (NoBongkarSusun, NoBahanBaku, NoPallet)
   - PK JSON: NoBongkarSusun, NoBahanBaku, NoPallet (tanpa NoSak)
   - Data detail per sak disimpan dalam JSON array
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunInputBahanBaku]
ON [dbo].[BongkarSusunInputBahanBaku]
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
     1 row per (NoBongkarSusun, NoBahanBaku, NoPallet)
  ====================================================== */
  ;WITH insOnly AS (
    SELECT i.NoBongkarSusun, i.NoBahanBaku, i.NoPallet
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoBongkarSusun = i.NoBongkarSusun
        AND d.NoBahanBaku    = i.NoBahanBaku
        AND d.NoPallet       = i.NoPallet
        AND d.NoSak          = i.NoSak
    )
  ),
  grp AS (
    SELECT DISTINCT NoBongkarSusun, NoBahanBaku, NoPallet
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BongkarSusunInputBahanBaku',
    @actor,
    @rid,
    CONCAT(
      '{"NoBongkarSusun":"', g.NoBongkarSusun,
      '","NoBahanBaku":"', g.NoBahanBaku,
      '","NoPallet":', CASE WHEN g.NoPallet IS NULL THEN 'null' ELSE CAST(g.NoPallet AS varchar(20)) END,
      '}'
    ),
    NULL,
    COALESCE((
      SELECT
        i2.NoBongkarSusun,
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
      WHERE i2.NoBongkarSusun = g.NoBongkarSusun
        AND i2.NoBahanBaku    = g.NoBahanBaku
        AND i2.NoPallet       = g.NoPallet
      ORDER BY i2.NoSak
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_FULL (GROUPED)
     1 row per (NoBongkarSusun, NoBahanBaku, NoPallet)
  ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoBongkarSusun, d.NoBahanBaku, d.NoPallet
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoBongkarSusun = d.NoBongkarSusun
        AND i.NoBahanBaku    = d.NoBahanBaku
        AND i.NoPallet       = d.NoPallet
        AND i.NoSak          = d.NoSak
    )
  ),
  grp AS (
    SELECT DISTINCT NoBongkarSusun, NoBahanBaku, NoPallet
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BongkarSusunInputBahanBaku',
    @actor,
    @rid,
    CONCAT(
      '{"NoBongkarSusun":"', g.NoBongkarSusun,
      '","NoBahanBaku":"', g.NoBahanBaku,
      '","NoPallet":', CASE WHEN g.NoPallet IS NULL THEN 'null' ELSE CAST(g.NoPallet AS varchar(20)) END,
      '}'
    ),
    COALESCE((
      SELECT
        d2.NoBongkarSusun,
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
      WHERE d2.NoBongkarSusun = g.NoBongkarSusun
        AND d2.NoBahanBaku    = g.NoBahanBaku
        AND d2.NoPallet       = g.NoPallet
      ORDER BY d2.NoSak
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     1 row per (NoBongkarSusun, NoBahanBaku, NoPallet)
  ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp AS (
      SELECT DISTINCT i.NoBongkarSusun, i.NoBahanBaku, i.NoPallet
      FROM inserted i
      JOIN deleted d
        ON d.NoBongkarSusun = i.NoBongkarSusun
       AND d.NoBahanBaku    = i.NoBahanBaku
       AND d.NoPallet       = i.NoPallet
       AND d.NoSak          = i.NoSak
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BongkarSusunInputBahanBaku',
      @actor,
      @rid,
      CONCAT(
        '{"NoBongkarSusun":"', g.NoBongkarSusun,
        '","NoBahanBaku":"', g.NoBahanBaku,
        '","NoPallet":', CASE WHEN g.NoPallet IS NULL THEN 'null' ELSE CAST(g.NoPallet AS varchar(20)) END,
        '}'
      ),
      COALESCE((
        SELECT
          d2.NoBongkarSusun,
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
        WHERE d2.NoBongkarSusun = g.NoBongkarSusun
          AND d2.NoBahanBaku    = g.NoBahanBaku
          AND d2.NoPallet       = g.NoPallet
        ORDER BY d2.NoSak
        FOR JSON PATH
      ), '[]'),
      COALESCE((
        SELECT
          i2.NoBongkarSusun,
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
        WHERE i2.NoBongkarSusun = g.NoBongkarSusun
          AND i2.NoBahanBaku    = g.NoBahanBaku
          AND i2.NoPallet       = g.NoPallet
        ORDER BY i2.NoSak
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

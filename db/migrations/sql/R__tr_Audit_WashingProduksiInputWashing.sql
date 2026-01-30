/* ===== [dbo].[tr_Audit_WashingProduksiInputWashing] ON [dbo].[WashingProduksiInputWashing] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoWashing) per statement
   PK JSON: { NoProduksi, NoWashing }
   NewData/OldData: array of {NoProduksi, NoWashing, NoSak, Berat}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_WashingProduksiInputWashing]
ON [dbo].[WashingProduksiInputWashing]
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
     AND d.NoWashing  = i.NoWashing
     AND d.NoSak      = i.NoSak
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoWashing
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'WashingProduksiInputWashing',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoWashing FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoWashing,
        x.NoSak,
        CAST(wd.Berat AS decimal(18,3)) AS Berat
      FROM insOnly x
      LEFT JOIN dbo.Washing_d wd
        ON wd.NoWashing = x.NoWashing
       AND wd.NoSak     = x.NoSak
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoWashing  = gg.NoWashing
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
     AND i.NoWashing  = d.NoWashing
     AND i.NoSak      = d.NoSak
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoWashing
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'WashingProduksiInputWashing',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoWashing FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoWashing,
        x.NoSak,
        CAST(wd.Berat AS decimal(18,3)) AS Berat
      FROM delOnly x
      LEFT JOIN dbo.Washing_d wd
        ON wd.NoWashing = x.NoWashing
       AND wd.NoSak     = x.NoSak
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoWashing  = gg.NoWashing
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
      SELECT i.NoProduksi, i.NoWashing, i.NoSak
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoWashing  = i.NoWashing
       AND d.NoSak      = i.NoSak
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoWashing
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'WashingProduksiInputWashing',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoWashing FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoWashing,
          d.NoSak,
          CAST(wd.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.Washing_d wd
          ON wd.NoWashing = d.NoWashing
         AND wd.NoSak     = d.NoSak
        WHERE d.NoProduksi = gg.NoProduksi
          AND d.NoWashing  = gg.NoWashing
        ORDER BY d.NoSak
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoWashing,
          i.NoSak,
          CAST(wd.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.Washing_d wd
          ON wd.NoWashing = i.NoWashing
         AND wd.NoSak     = i.NoSak
        WHERE i.NoProduksi = gg.NoProduksi
          AND i.NoWashing  = gg.NoWashing
        ORDER BY i.NoSak
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

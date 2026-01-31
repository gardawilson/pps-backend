SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunInputWashing] ON [dbo].[BongkarSusunInputWashing] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoBongkarSusun, NoWashing) per statement
   PK JSON: { NoBongkarSusun, NoWashing }
   NewData/OldData: array of {NoBongkarSusun, NoWashing, NoSak, Berat}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunInputWashing]
ON [dbo].[BongkarSusunInputWashing]
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
      ON d.NoBongkarSusun = i.NoBongkarSusun
     AND d.NoWashing      = i.NoWashing
     AND d.NoSak          = i.NoSak
    WHERE d.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoWashing
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BongkarSusunInputWashing',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoWashing FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoBongkarSusun,
        x.NoWashing,
        x.NoSak,
        CAST(wd.Berat AS decimal(18,3)) AS Berat
      FROM insOnly x
      LEFT JOIN dbo.Washing_d wd
        ON wd.NoWashing = x.NoWashing AND wd.NoSak = x.NoSak
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoWashing      = gg.NoWashing
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
      ON i.NoBongkarSusun = d.NoBongkarSusun
     AND i.NoWashing      = d.NoWashing
     AND i.NoSak          = d.NoSak
    WHERE i.NoBongkarSusun IS NULL
  ),
  g AS (
    SELECT DISTINCT NoBongkarSusun, NoWashing
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BongkarSusunInputWashing',
    @actor,
    @rid,
    (SELECT gg.NoBongkarSusun, gg.NoWashing FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoBongkarSusun,
        x.NoWashing,
        x.NoSak,
        CAST(wd.Berat AS decimal(18,3)) AS Berat
      FROM delOnly x
      LEFT JOIN dbo.Washing_d wd
        ON wd.NoWashing = x.NoWashing AND wd.NoSak = x.NoSak
      WHERE x.NoBongkarSusun = gg.NoBongkarSusun
        AND x.NoWashing      = gg.NoWashing
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
      SELECT i.NoBongkarSusun, i.NoWashing, i.NoSak
      FROM inserted i
      JOIN deleted d
        ON d.NoBongkarSusun = i.NoBongkarSusun
       AND d.NoWashing      = i.NoWashing
       AND d.NoSak          = i.NoSak
    ),
    g AS (
      SELECT DISTINCT NoBongkarSusun, NoWashing
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BongkarSusunInputWashing',
      @actor,
      @rid,
      (SELECT gg.NoBongkarSusun, gg.NoWashing FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoBongkarSusun,
          d.NoWashing,
          d.NoSak,
          CAST(wd.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.Washing_d wd
          ON wd.NoWashing = d.NoWashing AND wd.NoSak = d.NoSak
        WHERE d.NoBongkarSusun = gg.NoBongkarSusun
          AND d.NoWashing      = gg.NoWashing
        ORDER BY d.NoSak
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoBongkarSusun,
          i.NoWashing,
          i.NoSak,
          CAST(wd.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.Washing_d wd
          ON wd.NoWashing = i.NoWashing AND wd.NoSak = i.NoSak
        WHERE i.NoBongkarSusun = gg.NoBongkarSusun
          AND i.NoWashing      = gg.NoWashing
        ORDER BY i.NoSak
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

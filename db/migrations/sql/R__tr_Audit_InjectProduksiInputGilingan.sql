/* ===== [dbo].[tr_Audit_InjectProduksiInputGilingan] ON [dbo].[InjectProduksiInputGilingan] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoGilingan) per statement
   PK JSON: { NoProduksi, NoGilingan }
   NewData/OldData: array of {NoProduksi, NoGilingan, Berat}
   NOTE: table ini tidak punya NoSak, jadi payload detail tidak menyertakan NoSak.
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksiInputGilingan]
ON [dbo].[InjectProduksiInputGilingan]
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
     AND d.NoGilingan = i.NoGilingan
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoGilingan
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'InjectProduksiInputGilingan',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoGilingan,
        CAST(gh.Berat AS decimal(18,3)) AS Berat
      FROM insOnly x
      LEFT JOIN dbo.Gilingan gh
        ON gh.NoGilingan = x.NoGilingan
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoGilingan = gg.NoGilingan
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
     AND i.NoGilingan = d.NoGilingan
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoGilingan
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'InjectProduksiInputGilingan',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoGilingan,
        CAST(gh.Berat AS decimal(18,3)) AS Berat
      FROM delOnly x
      LEFT JOIN dbo.Gilingan gh
        ON gh.NoGilingan = x.NoGilingan
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoGilingan = gg.NoGilingan
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
      SELECT i.NoProduksi, i.NoGilingan
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoGilingan = i.NoGilingan
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoGilingan
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'InjectProduksiInputGilingan',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoGilingan,
          CAST(gh.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.Gilingan gh
          ON gh.NoGilingan = d.NoGilingan
        WHERE d.NoProduksi = gg.NoProduksi
          AND d.NoGilingan = gg.NoGilingan
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoGilingan,
          CAST(gh.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.Gilingan gh
          ON gh.NoGilingan = i.NoGilingan
        WHERE i.NoProduksi = gg.NoProduksi
          AND i.NoGilingan = gg.NoGilingan
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

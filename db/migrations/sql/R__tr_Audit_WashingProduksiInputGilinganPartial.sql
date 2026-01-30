/* ===== [dbo].[tr_Audit_WashingProduksiInputGilinganPartial] ON [dbo].[WashingProduksiInputGilinganPartial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoGilinganPartial, NoGilingan) per statement
   PK JSON: { NoProduksi, NoGilinganPartial, NoGilingan }
   NewData/OldData: array of {NoProduksi, NoGilinganPartial, NoGilingan, Berat}

   Enrichment:
   - NoGilingan & Berat diambil dari dbo.GilinganPartial (gp.NoGilingan, gp.Berat)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_WashingProduksiInputGilinganPartial]
ON [dbo].[WashingProduksiInputGilinganPartial]
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
      ON d.NoProduksi        = i.NoProduksi
     AND d.NoGilinganPartial = i.NoGilinganPartial
    WHERE d.NoProduksi IS NULL
  ),
  insEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoGilinganPartial,
      gp.NoGilingan,
      gp.Berat
    FROM insOnly x
    LEFT JOIN dbo.GilinganPartial gp
      ON gp.NoGilinganPartial = x.NoGilinganPartial
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoGilinganPartial, NoGilingan
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'WashingProduksiInputGilinganPartial',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoGilinganPartial, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        e.NoProduksi,
        e.NoGilinganPartial,
        e.NoGilingan,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM insEnriched e
      WHERE e.NoProduksi = gg.NoProduksi
        AND e.NoGilinganPartial = gg.NoGilinganPartial
        AND (
              (e.NoGilingan = gg.NoGilingan)
              OR (e.NoGilingan IS NULL AND gg.NoGilingan IS NULL)
            )
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
      ON i.NoProduksi        = d.NoProduksi
     AND i.NoGilinganPartial = d.NoGilinganPartial
    WHERE i.NoProduksi IS NULL
  ),
  delEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoGilinganPartial,
      gp.NoGilingan,
      gp.Berat
    FROM delOnly x
    LEFT JOIN dbo.GilinganPartial gp
      ON gp.NoGilinganPartial = x.NoGilinganPartial
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoGilinganPartial, NoGilingan
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'WashingProduksiInputGilinganPartial',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoGilinganPartial, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        e.NoProduksi,
        e.NoGilinganPartial,
        e.NoGilingan,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM delEnriched e
      WHERE e.NoProduksi = gg.NoProduksi
        AND e.NoGilinganPartial = gg.NoGilinganPartial
        AND (
              (e.NoGilingan = gg.NoGilingan)
              OR (e.NoGilingan IS NULL AND gg.NoGilingan IS NULL)
            )
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
      SELECT i.NoProduksi, i.NoGilinganPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi        = i.NoProduksi
       AND d.NoGilinganPartial = i.NoGilinganPartial
    ),
    g AS (
      SELECT DISTINCT
        u.NoProduksi,
        u.NoGilinganPartial,
        gp.NoGilingan
      FROM upd u
      LEFT JOIN dbo.GilinganPartial gp
        ON gp.NoGilinganPartial = u.NoGilinganPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'WashingProduksiInputGilinganPartial',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoGilinganPartial, gg.NoGilingan FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoGilinganPartial,
          gp.NoGilingan,
          CAST(gp.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.GilinganPartial gp
          ON gp.NoGilinganPartial = d.NoGilinganPartial
        WHERE d.NoProduksi = gg.NoProduksi
          AND d.NoGilinganPartial = gg.NoGilinganPartial
          AND (
                (gp.NoGilingan = gg.NoGilingan)
                OR (gp.NoGilingan IS NULL AND gg.NoGilingan IS NULL)
              )
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoGilinganPartial,
          gp.NoGilingan,
          CAST(gp.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.GilinganPartial gp
          ON gp.NoGilinganPartial = i.NoGilinganPartial
        WHERE i.NoProduksi = gg.NoProduksi
          AND i.NoGilinganPartial = gg.NoGilinganPartial
          AND (
                (gp.NoGilingan = gg.NoGilingan)
                OR (gp.NoGilingan IS NULL AND gg.NoGilingan IS NULL)
              )
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

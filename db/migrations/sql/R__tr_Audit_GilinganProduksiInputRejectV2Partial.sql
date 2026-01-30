/* ===== [dbo].[tr_Audit_GilinganProduksiInputRejectV2Partial] ON [dbo].[GilinganProduksiInputRejectV2Partial] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_PARTIAL / UNCONSUME_PARTIAL / UPDATE

   NOTE:
   - AuditTrail hanya 1 row per statement per (NoProduksi, NoRejectPartial)
   - PK JSON: { NoProduksi, NoRejectPartial, NoReject }
   - NoReject + Berat diambil dari tabel master partial (diasumsikan dbo.RejectV2Partial)
   - NewData/OldData: array of {NoProduksi, NoRejectPartial, NoReject, Berat}
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_GilinganProduksiInputRejectV2Partial]
ON [dbo].[GilinganProduksiInputRejectV2Partial]
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
    SELECT i.NoProduksi, i.NoRejectPartial
    FROM inserted i
    WHERE NOT EXISTS (
      SELECT 1
      FROM deleted d
      WHERE d.NoProduksi      = i.NoProduksi
        AND d.NoRejectPartial = i.NoRejectPartial
    )
  ),
  insEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoRejectPartial,
      rp.NoReject,
      rp.Berat
    FROM insOnly x
    LEFT JOIN dbo.RejectV2Partial rp
      ON rp.NoRejectPartial = x.NoRejectPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoRejectPartial, NoReject
    FROM insEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_PARTIAL',
    'GilinganProduksiInputRejectV2Partial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoRejectPartial, g.NoReject FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoRejectPartial,
        e.NoReject,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM insEnriched e
      WHERE e.NoProduksi      = g.NoProduksi
        AND e.NoRejectPartial = g.NoRejectPartial
        AND (
              (e.NoReject = g.NoReject)
              OR (e.NoReject IS NULL AND g.NoReject IS NULL)
            )
      FOR JSON PATH
    ), '[]')
  FROM grp g;

  /* ======================================================
     DELETE-only => UNCONSUME_PARTIAL (GROUPED)
     ====================================================== */
  ;WITH delOnly AS (
    SELECT d.NoProduksi, d.NoRejectPartial
    FROM deleted d
    WHERE NOT EXISTS (
      SELECT 1
      FROM inserted i
      WHERE i.NoProduksi      = d.NoProduksi
        AND i.NoRejectPartial = d.NoRejectPartial
    )
  ),
  delEnriched AS (
    SELECT
      x.NoProduksi,
      x.NoRejectPartial,
      rp.NoReject,
      rp.Berat
    FROM delOnly x
    LEFT JOIN dbo.RejectV2Partial rp
      ON rp.NoRejectPartial = x.NoRejectPartial
  ),
  grp AS (
    SELECT DISTINCT NoProduksi, NoRejectPartial, NoReject
    FROM delEnriched
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_PARTIAL',
    'GilinganProduksiInputRejectV2Partial',
    @actor,
    @rid,
    (SELECT g.NoProduksi, g.NoRejectPartial, g.NoReject FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    COALESCE((
      SELECT
        e.NoProduksi,
        e.NoRejectPartial,
        e.NoReject,
        CAST(e.Berat AS decimal(18,3)) AS Berat
      FROM delEnriched e
      WHERE e.NoProduksi      = g.NoProduksi
        AND e.NoRejectPartial = g.NoRejectPartial
        AND (
              (e.NoReject = g.NoReject)
              OR (e.NoReject IS NULL AND g.NoReject IS NULL)
            )
      FOR JSON PATH
    ), '[]'),
    NULL
  FROM grp g;

  /* ======================================================
     UPDATE => UPDATE (GROUPED)
     ====================================================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    ;WITH grp0 AS (
      SELECT DISTINCT i.NoProduksi, i.NoRejectPartial
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi      = i.NoProduksi
       AND d.NoRejectPartial = i.NoRejectPartial
    ),
    grp AS (
      SELECT DISTINCT
        g0.NoProduksi,
        g0.NoRejectPartial,
        rp.NoReject
      FROM grp0 g0
      LEFT JOIN dbo.RejectV2Partial rp
        ON rp.NoRejectPartial = g0.NoRejectPartial
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'GilinganProduksiInputRejectV2Partial',
      @actor,
      @rid,
      (SELECT g.NoProduksi, g.NoRejectPartial, g.NoReject FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      COALESCE((
        SELECT
          d2.NoProduksi,
          d2.NoRejectPartial,
          rp.NoReject,
          CAST(rp.Berat AS decimal(18,3)) AS Berat
        FROM deleted d2
        LEFT JOIN dbo.RejectV2Partial rp
          ON rp.NoRejectPartial = d2.NoRejectPartial
        WHERE d2.NoProduksi      = g.NoProduksi
          AND d2.NoRejectPartial = g.NoRejectPartial
          AND (
                (rp.NoReject = g.NoReject)
                OR (rp.NoReject IS NULL AND g.NoReject IS NULL)
              )
        FOR JSON PATH
      ), '[]'),

      COALESCE((
        SELECT
          i2.NoProduksi,
          i2.NoRejectPartial,
          rp.NoReject,
          CAST(rp.Berat AS decimal(18,3)) AS Berat
        FROM inserted i2
        LEFT JOIN dbo.RejectV2Partial rp
          ON rp.NoRejectPartial = i2.NoRejectPartial
        WHERE i2.NoProduksi      = g.NoProduksi
          AND i2.NoRejectPartial = g.NoRejectPartial
          AND (
                (rp.NoReject = g.NoReject)
                OR (rp.NoReject IS NULL AND g.NoReject IS NULL)
              )
        FOR JSON PATH
      ), '[]')
    FROM grp g;
  END
END;
GO

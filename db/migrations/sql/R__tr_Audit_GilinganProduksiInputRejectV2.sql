/* ===== [dbo].[tr_Audit_GilinganProduksiInputRejectV2] ON [dbo].[GilinganProduksiInputRejectV2] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   GROUPING: 1 row audit per (NoProduksi, NoReject) per statement
   PK JSON: { NoProduksi, NoReject }
   NewData/OldData: array of {NoProduksi, NoReject, DatetimeInput, Berat}
   Enrichment:
   - Berat diambil dari dbo.RejectV2 (r.Berat) berdasarkan NoReject
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_GilinganProduksiInputRejectV2]
ON [dbo].[GilinganProduksiInputRejectV2]
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
     AND d.NoReject   = i.NoReject
    WHERE d.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoReject
    FROM insOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'GilinganProduksiInputRejectV2',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoReject FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (
      SELECT
        x.NoProduksi,
        x.NoReject,
        x.DatetimeInput,
        CAST(r.Berat AS decimal(18,3)) AS Berat
      FROM insOnly x
      LEFT JOIN dbo.RejectV2 r
        ON r.NoReject = x.NoReject
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoReject   = gg.NoReject
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
     AND i.NoReject   = d.NoReject
    WHERE i.NoProduksi IS NULL
  ),
  g AS (
    SELECT DISTINCT NoProduksi, NoReject
    FROM delOnly
  )
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'GilinganProduksiInputRejectV2',
    @actor,
    @rid,
    (SELECT gg.NoProduksi, gg.NoReject FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (
      SELECT
        x.NoProduksi,
        x.NoReject,
        x.DatetimeInput,
        CAST(r.Berat AS decimal(18,3)) AS Berat
      FROM delOnly x
      LEFT JOIN dbo.RejectV2 r
        ON r.NoReject = x.NoReject
      WHERE x.NoProduksi = gg.NoProduksi
        AND x.NoReject   = gg.NoReject
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
      SELECT i.NoProduksi, i.NoReject
      FROM inserted i
      JOIN deleted d
        ON d.NoProduksi = i.NoProduksi
       AND d.NoReject   = i.NoReject
    ),
    g AS (
      SELECT DISTINCT NoProduksi, NoReject
      FROM upd
    )
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'GilinganProduksiInputRejectV2',
      @actor,
      @rid,
      (SELECT gg.NoProduksi, gg.NoReject FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),

      (
        SELECT
          d.NoProduksi,
          d.NoReject,
          d.DatetimeInput,
          CAST(r.Berat AS decimal(18,3)) AS Berat
        FROM deleted d
        LEFT JOIN dbo.RejectV2 r
          ON r.NoReject = d.NoReject
        WHERE d.NoProduksi = gg.NoProduksi
          AND d.NoReject   = gg.NoReject
        FOR JSON PATH
      ),

      (
        SELECT
          i.NoProduksi,
          i.NoReject,
          i.DatetimeInput,
          CAST(r.Berat AS decimal(18,3)) AS Berat
        FROM inserted i
        LEFT JOIN dbo.RejectV2 r
          ON r.NoReject = i.NoReject
        WHERE i.NoProduksi = gg.NoProduksi
          AND i.NoReject   = gg.NoReject
        FOR JSON PATH
      )
    FROM g gg;
  END
END;
GO

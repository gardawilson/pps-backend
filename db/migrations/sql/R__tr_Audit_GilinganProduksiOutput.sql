/* ===== [dbo].[tr_Audit_GilinganProduksiOutput] 
         ON [dbo].[GilinganProduksiOutput] ===== */
-- =============================================
-- TRIGGER: tr_Audit_GilinganProduksiOutput
-- PK     : NoGilingan + NoProduksi
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Gilingan untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_GilinganProduksiOutput]
ON [dbo].[GilinganProduksiOutput]
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
     1) INSERT-only => PRODUCE (DETAIL)
     ========================================================= */
  ;WITH insOnly AS (
    SELECT
      i.NoProduksi,
      i.NoGilingan,
      g.Berat
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoGilingan = i.NoGilingan
    LEFT JOIN dbo.Gilingan g
      ON g.NoGilingan = i.NoGilingan
    WHERE d.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'GilinganProduksiOutput',
    @actor,
    @rid,
    (SELECT
       i.NoGilingan,
       i.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoProduksi,
       i.NoGilingan,
       CAST(i.Berat AS decimal(18,3)) AS Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT
      d.NoProduksi,
      d.NoGilingan,
      g.Berat
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoProduksi = d.NoProduksi
     AND i.NoGilingan = d.NoGilingan
    LEFT JOIN dbo.Gilingan g
      ON g.NoGilingan = d.NoGilingan
    WHERE i.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'GilinganProduksiOutput',
    @actor,
    @rid,
    (SELECT
       d.NoGilingan,
       d.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoProduksi,
       d.NoGilingan,
       CAST(d.Berat AS decimal(18,3)) AS Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL
  FROM delOnly d;

  /* =========================================================
     3) UPDATE => UPDATE (DETAIL)
     ========================================================= */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail
      (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'GilinganProduksiOutput',
      @actor,
      @rid,
      (SELECT
         i.NoGilingan,
         i.NoProduksi
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoProduksi,
         d.NoGilingan,
         CAST(gOld.Berat AS decimal(18,3)) AS Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoProduksi,
         i.NoGilingan,
         CAST(gNew.Berat AS decimal(18,3)) AS Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoGilingan = i.NoGilingan
    LEFT JOIN dbo.Gilingan gOld
      ON gOld.NoGilingan = d.NoGilingan
    LEFT JOIN dbo.Gilingan gNew
      ON gNew.NoGilingan = i.NoGilingan;
  END
END;
GO

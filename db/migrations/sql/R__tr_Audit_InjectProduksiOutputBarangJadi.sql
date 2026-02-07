/* ===== [dbo].[tr_Audit_InjectProduksiOutputBarangJadi]
         ON [dbo].[InjectProduksiOutputBarangJadi] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksiOutputBarangJadi
-- PK     : NoBJ + NoProduksi
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join BarangJadi untuk ambil Pcs
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksiOutputBarangJadi]
ON [dbo].[InjectProduksiOutputBarangJadi]
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
      i.NoBJ,
      bj.Pcs
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoBJ       = i.NoBJ
    LEFT JOIN dbo.BarangJadi bj
      ON bj.NoBJ = i.NoBJ
    WHERE d.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'InjectProduksiOutputBarangJadi',
    @actor,
    @rid,
    (SELECT
       i.NoBJ,
       i.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoProduksi,
       i.NoBJ,
       i.Pcs
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT
      d.NoProduksi,
      d.NoBJ,
      bj.Pcs
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoProduksi = d.NoProduksi
     AND i.NoBJ       = d.NoBJ
    LEFT JOIN dbo.BarangJadi bj
      ON bj.NoBJ = d.NoBJ
    WHERE i.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'InjectProduksiOutputBarangJadi',
    @actor,
    @rid,
    (SELECT
       d.NoBJ,
       d.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoProduksi,
       d.NoBJ,
       d.Pcs
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
      'InjectProduksiOutputBarangJadi',
      @actor,
      @rid,
      (SELECT
         i.NoBJ,
         i.NoProduksi
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoProduksi,
         d.NoBJ,
         bjOld.Pcs
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoProduksi,
         i.NoBJ,
         bjNew.Pcs
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoBJ       = i.NoBJ
    LEFT JOIN dbo.BarangJadi bjOld
      ON bjOld.NoBJ = d.NoBJ
    LEFT JOIN dbo.BarangJadi bjNew
      ON bjNew.NoBJ = i.NoBJ;
  END
END;
GO

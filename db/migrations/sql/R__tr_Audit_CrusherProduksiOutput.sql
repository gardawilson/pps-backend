/* ===== [dbo].[tr_Audit_CrusherProduksiOutput] ON [dbo].[CrusherProduksiOutput] ===== */
-- =============================================
-- TRIGGER: tr_Audit_CrusherProduksiOutput
-- PK     : NoCrusher + NoCrusherProduksi
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Crusher untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_CrusherProduksiOutput]
ON [dbo].[CrusherProduksiOutput]
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
      i.NoCrusher,
      i.NoCrusherProduksi,
      c.Berat
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoCrusherProduksi = i.NoCrusherProduksi
     AND d.NoCrusher         = i.NoCrusher
    LEFT JOIN dbo.Crusher c
      ON c.NoCrusher = i.NoCrusher
    WHERE d.NoCrusherProduksi IS NULL
  )
  INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'CrusherProduksiOutput',
    @actor,
    @rid,
    (SELECT
       i.NoCrusher,
       i.NoCrusherProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoCrusher,
       i.NoCrusherProduksi,
       i.Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT 
      d.NoCrusher,
      d.NoCrusherProduksi,
      c.Berat
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoCrusherProduksi = d.NoCrusherProduksi
     AND i.NoCrusher         = d.NoCrusher
    LEFT JOIN dbo.Crusher c
      ON c.NoCrusher = d.NoCrusher
    WHERE i.NoCrusherProduksi IS NULL
  )
  INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'CrusherProduksiOutput',
    @actor,
    @rid,
    (SELECT
       d.NoCrusher,
       d.NoCrusherProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoCrusher,
       d.NoCrusherProduksi,
       d.Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL
  FROM delOnly d;

  /* =========================================================
     3) UPDATE => UPDATE (DETAIL)
     ========================================================= */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'CrusherProduksiOutput',
      @actor,
      @rid,
      (SELECT
         i.NoCrusher,
         i.NoCrusherProduksi
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoCrusher,
         d.NoCrusherProduksi,
         cOld.Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoCrusher,
         i.NoCrusherProduksi,
         cNew.Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoCrusherProduksi = i.NoCrusherProduksi
     AND d.NoCrusher         = i.NoCrusher
    LEFT JOIN dbo.Crusher cOld
      ON cOld.NoCrusher = d.NoCrusher
    LEFT JOIN dbo.Crusher cNew
      ON cNew.NoCrusher = i.NoCrusher;
  END
END;
GO

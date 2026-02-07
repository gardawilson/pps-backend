/* ===== [dbo].[tr_Audit_InjectProduksiOutputBonggolan] 
         ON [dbo].[InjectProduksiOutputBonggolan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksiOutputBonggolan
-- PK     : NoBonggolan + NoProduksi
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Bonggolan untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksiOutputBonggolan]
ON [dbo].[InjectProduksiOutputBonggolan]
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
      i.NoBonggolan,
      b.Berat
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoProduksi  = i.NoProduksi
     AND d.NoBonggolan = i.NoBonggolan
    LEFT JOIN dbo.Bonggolan b
      ON b.NoBonggolan = i.NoBonggolan
    WHERE d.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'InjectProduksiOutputBonggolan',
    @actor,
    @rid,
    (SELECT
       i.NoBonggolan,
       i.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoProduksi,
       i.NoBonggolan,
       i.Berat
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT
      d.NoProduksi,
      d.NoBonggolan,
      b.Berat
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoProduksi  = d.NoProduksi
     AND i.NoBonggolan = d.NoBonggolan
    LEFT JOIN dbo.Bonggolan b
      ON b.NoBonggolan = d.NoBonggolan
    WHERE i.NoProduksi IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'InjectProduksiOutputBonggolan',
    @actor,
    @rid,
    (SELECT
       d.NoBonggolan,
       d.NoProduksi
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoProduksi,
       d.NoBonggolan,
       d.Berat
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
      'InjectProduksiOutputBonggolan',
      @actor,
      @rid,
      (SELECT
         i.NoBonggolan,
         i.NoProduksi
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoProduksi,
         d.NoBonggolan,
         bOld.Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoProduksi,
         i.NoBonggolan,
         bNew.Berat
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoProduksi  = i.NoProduksi
     AND d.NoBonggolan = i.NoBonggolan
    LEFT JOIN dbo.Bonggolan bOld
      ON bOld.NoBonggolan = d.NoBonggolan
    LEFT JOIN dbo.Bonggolan bNew
      ON bNew.NoBonggolan = i.NoBonggolan;
  END
END;
GO

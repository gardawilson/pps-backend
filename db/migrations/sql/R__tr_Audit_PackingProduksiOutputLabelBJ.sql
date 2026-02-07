/* ===== [dbo].[tr_Audit_PackingProduksiOutputLabelBJ]
         ON [dbo].[PackingProduksiOutputLabelBJ] ===== */
-- =============================================
-- TRIGGER: tr_Audit_PackingProduksiOutputLabelBJ
-- PK     : NoBJ + NoPacking
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join BarangJadi untuk ambil Pcs
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_PackingProduksiOutputLabelBJ]
ON [dbo].[PackingProduksiOutputLabelBJ]
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
      i.NoPacking,
      i.NoBJ,
      bj.Pcs
    FROM inserted i
    LEFT JOIN deleted d
      ON d.NoPacking = i.NoPacking
     AND d.NoBJ      = i.NoBJ
    LEFT JOIN dbo.BarangJadi bj
      ON bj.NoBJ = i.NoBJ
    WHERE d.NoPacking IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'PRODUCE',
    'PackingProduksiOutputLabelBJ',
    @actor,
    @rid,
    (SELECT
       i.NoBJ,
       i.NoPacking
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    NULL,
    (SELECT
       i.NoPacking,
       i.NoBJ,
       i.Pcs
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
  FROM insOnly i;

  /* =========================================================
     2) DELETE-only => UNPRODUCE (DETAIL)
     ========================================================= */
  ;WITH delOnly AS (
    SELECT
      d.NoPacking,
      d.NoBJ,
      bj.Pcs
    FROM deleted d
    LEFT JOIN inserted i
      ON i.NoPacking = d.NoPacking
     AND i.NoBJ      = d.NoBJ
    LEFT JOIN dbo.BarangJadi bj
      ON bj.NoBJ = d.NoBJ
    WHERE i.NoPacking IS NULL
  )
  INSERT dbo.AuditTrail
    (Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNPRODUCE',
    'PackingProduksiOutputLabelBJ',
    @actor,
    @rid,
    (SELECT
       d.NoBJ,
       d.NoPacking
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
    (SELECT
       d.NoPacking,
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
      'PackingProduksiOutputLabelBJ',
      @actor,
      @rid,
      (SELECT
         i.NoBJ,
         i.NoPacking
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         d.NoPacking,
         d.NoBJ,
         bjOld.Pcs
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
      (SELECT
         i.NoPacking,
         i.NoBJ,
         bjNew.Pcs
       FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM inserted i
    JOIN deleted d
      ON d.NoPacking = i.NoPacking
     AND d.NoBJ      = i.NoBJ
    LEFT JOIN dbo.BarangJadi bjOld
      ON bjOld.NoBJ = d.NoBJ
    LEFT JOIN dbo.BarangJadi bjNew
      ON bjNew.NoBJ = i.NoBJ;
  END
END;
GO

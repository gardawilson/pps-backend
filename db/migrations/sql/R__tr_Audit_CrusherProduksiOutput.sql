/* ===== [dbo].[tr_Audit_CrusherProduksiOutput] ON [dbo].[CrusherProduksiOutput] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_CrusherProduksiOutput]
ON [dbo].[CrusherProduksiOutput]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
  SET NOCOUNT ON;

  -- ✅ actor = actor_id (ID user) dari SESSION_CONTEXT
  DECLARE @actor nvarchar(128) =
    COALESCE(
      CONVERT(nvarchar(128), TRY_CONVERT(int, SESSION_CONTEXT(N'actor_id'))),
      CAST(SESSION_CONTEXT(N'actor') AS nvarchar(128)),  -- fallback lama
      SUSER_SNAME()
    );

  DECLARE @rid nvarchar(64) =
    CAST(SESSION_CONTEXT(N'request_id') AS nvarchar(64));

  /* =========================================================
     Helper: PK ringkas (NoCrusher tunggal / list)
     - jika dalam 1 statement hanya 1 NoCrusher -> {"NoCrusher":"..."}
     - kalau banyak -> {"NoCrusherList":[{"NoCrusher":"..."}, ...]}
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoCrusher FROM inserted
    UNION
    SELECT NoCrusher FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoCrusher) = 1
          THEN CONCAT('{"NoCrusher":"', MAX(NoCrusher), '"}')
        ELSE
          CONCAT(
            '{"NoCrusherList":',
            (SELECT DISTINCT NoCrusher FROM x FOR JSON PATH),
            '}'
          )
      END
  FROM x;

  /* =====================
     INSERT (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'INSERT',
      'CrusherProduksiOutput',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoCrusher,
          i.NoCrusherProduksi
        FROM inserted i
        ORDER BY i.NoCrusher, i.NoCrusherProduksi
        FOR JSON PATH
      );
  END

  /* =====================
     DELETE (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'DELETE',
      'CrusherProduksiOutput',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoCrusher,
          d.NoCrusherProduksi
        FROM deleted d
        ORDER BY d.NoCrusher, d.NoCrusherProduksi
        FOR JSON PATH
      ),
      NULL;
  END

  /* =====================
     UPDATE (1 row audit)
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'CrusherProduksiOutput',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoCrusher,
          d.NoCrusherProduksi
        FROM deleted d
        ORDER BY d.NoCrusher, d.NoCrusherProduksi
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoCrusher,
          i.NoCrusherProduksi
        FROM inserted i
        ORDER BY i.NoCrusher, i.NoCrusherProduksi
        FOR JSON PATH
      );
  END
END;

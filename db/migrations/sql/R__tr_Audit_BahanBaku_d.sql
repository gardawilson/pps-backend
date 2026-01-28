/* ===== [dbo].[tr_Audit_BahanBaku_d] ON [dbo].[BahanBaku_d] ===== */
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BahanBaku_d]
ON [dbo].[BahanBaku_d]
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
     Helper: bentuk PK ringkas (single / list)
     PK yang kamu mau: NoBahanBaku + NoPallet
     - single: {"NoBahanBaku":"...","NoPallet":1}
     - multi : {"PKList":[{"NoBahanBaku":"...","NoPallet":1}, ...]}
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoBahanBaku, NoPallet FROM inserted
    UNION
    SELECT NoBahanBaku, NoPallet FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(1) = 1
          THEN CONCAT(
            '{"NoBahanBaku":"', MAX(NoBahanBaku),
            '","NoPallet":', CAST(MAX(NoPallet) AS nvarchar(30)),
            '}'
          )
        ELSE
          CONCAT(
            '{"PKList":',
            (SELECT DISTINCT NoBahanBaku, NoPallet FROM x FOR JSON PATH),
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
      'BahanBaku_d',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoBahanBaku,
          i.NoPallet,
          i.NoSak,
          i.TimeCreate,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IsLembab,
          i.IsPartial,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoBahanBaku, i.NoPallet, i.NoSak
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
      'BahanBaku_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBahanBaku,
          d.NoPallet,
          d.NoSak,
          d.TimeCreate,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IsLembab,
          d.IsPartial,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoBahanBaku, d.NoPallet, d.NoSak
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
      'BahanBaku_d',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoBahanBaku,
          d.NoPallet,
          d.NoSak,
          d.TimeCreate,
          CAST(d.Berat AS decimal(18,3)) AS Berat,
          d.DateUsage,
          d.IsLembab,
          d.IsPartial,
          d.IdLokasi
        FROM deleted d
        ORDER BY d.NoBahanBaku, d.NoPallet, d.NoSak
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoBahanBaku,
          i.NoPallet,
          i.NoSak,
          i.TimeCreate,
          CAST(i.Berat AS decimal(18,3)) AS Berat,
          i.DateUsage,
          i.IsLembab,
          i.IsPartial,
          i.IdLokasi
        FROM inserted i
        ORDER BY i.NoBahanBaku, i.NoPallet, i.NoSak
        FOR JSON PATH
      );
  END
END;
GO

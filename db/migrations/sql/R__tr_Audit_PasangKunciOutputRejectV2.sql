/* ===== [dbo].[tr_Audit_PasangKunciOutputRejectV2] ON [dbo].[PasangKunciOutputRejectV2] ===== */
-- =============================================
-- TRIGGER: tr_Audit_PasangKunciOutputRejectV2
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- ✅ PK: NoReject (parent document)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_PasangKunciOutputRejectV2]
ON [dbo].[PasangKunciOutputRejectV2]
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
     ✅ Helper: PK menggunakan NoReject (parent)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoReject FROM inserted
    UNION
    SELECT NoReject FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoReject) = 1
          THEN CONCAT('{"NoReject":"', MAX(NoReject), '"}')
        ELSE
          CONCAT(
            '{"NoRejectList":',
            (SELECT DISTINCT NoReject FROM x FOR JSON PATH),
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
      'PasangKunciOutputRejectV2',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoProduksi,
          i.NoReject
        FROM inserted i
        ORDER BY i.NoReject, i.NoProduksi
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
      'PasangKunciOutputRejectV2',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoReject
        FROM deleted d
        ORDER BY d.NoReject, d.NoProduksi
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
      'PasangKunciOutputRejectV2',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoReject
        FROM deleted d
        ORDER BY d.NoReject, d.NoProduksi
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoProduksi,
          i.NoReject
        FROM inserted i
        ORDER BY i.NoReject, i.NoProduksi
        FOR JSON PATH
      );
  END
END;
GO

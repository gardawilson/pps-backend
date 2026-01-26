/* ===== [dbo].[tr_Audit_BrokerProduksiOutputBonggolan] ON [dbo].[BrokerProduksiOutputBonggolan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BrokerProduksiOutputBonggolan
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- ✅ PK: NoBonggolan (parent document)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BrokerProduksiOutputBonggolan]
ON [dbo].[BrokerProduksiOutputBonggolan]
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
     ✅ Helper: PK menggunakan NoBonggolan (parent)
  ========================================================= */
  DECLARE @pk nvarchar(max);

  ;WITH x AS (
    SELECT NoBonggolan FROM inserted
    UNION
    SELECT NoBonggolan FROM deleted
  )
  SELECT
    @pk =
      CASE
        WHEN COUNT(DISTINCT NoBonggolan) = 1
          THEN CONCAT('{"NoBonggolan":"', MAX(NoBonggolan), '"}')
        ELSE
          CONCAT(
            '{"NoBonggolanList":',
            (SELECT DISTINCT NoBonggolan FROM x FOR JSON PATH),
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
      'BrokerProduksiOutputBonggolan',
      @actor,
      @rid,
      @pk,
      NULL,
      (
        SELECT
          i.NoProduksi,
          i.NoBonggolan
        FROM inserted i
        ORDER BY i.NoBonggolan, i.NoProduksi
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
      'BrokerProduksiOutputBonggolan',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoBonggolan
        FROM deleted d
        ORDER BY d.NoBonggolan, d.NoProduksi
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
      'BrokerProduksiOutputBonggolan',
      @actor,
      @rid,
      @pk,
      (
        SELECT
          d.NoProduksi,
          d.NoBonggolan
        FROM deleted d
        ORDER BY d.NoBonggolan, d.NoProduksi
        FOR JSON PATH
      ),
      (
        SELECT
          i.NoProduksi,
          i.NoBonggolan
        FROM inserted i
        ORDER BY i.NoBonggolan, i.NoProduksi
        FOR JSON PATH
      );
  END
END;
GO

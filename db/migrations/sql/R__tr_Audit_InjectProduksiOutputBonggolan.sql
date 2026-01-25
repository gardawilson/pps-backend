/* ===== [dbo].[tr_Audit_InjectProduksiOutputBonggolan] ON [dbo].[InjectProduksiOutputBonggolan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksiOutputBonggolan
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: (NoProduksi, NoBonggolan)
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

  /* =====================
     INSERT
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'INSERT',
    'InjectProduksiOutputBonggolan',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', i.NoProduksi,
      '","NoBonggolan":"', i.NoBonggolan, '"}'
    ),
    NULL,
    (
      SELECT
        i.NoProduksi,
        i.NoBonggolan
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d
    ON d.NoProduksi  = i.NoProduksi
   AND d.NoBonggolan = i.NoBonggolan
  WHERE d.NoProduksi IS NULL
    AND d.NoBonggolan IS NULL;

  /* =====================
     UPDATE
     (biasanya jarang di bridge table, tapi tetap di-handle)
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'InjectProduksiOutputBonggolan',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', i.NoProduksi,
      '","NoBonggolan":"', i.NoBonggolan, '"}'
    ),
    (
      SELECT
        d.NoProduksi,
        d.NoBonggolan
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoProduksi,
        i.NoBonggolan
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d
    ON d.NoProduksi  = i.NoProduksi
   AND d.NoBonggolan = i.NoBonggolan;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'InjectProduksiOutputBonggolan',
    @actor,
    @rid,
    CONCAT(
      '{"NoProduksi":"', d.NoProduksi,
      '","NoBonggolan":"', d.NoBonggolan, '"}'
    ),
    (
      SELECT
        d.NoProduksi,
        d.NoBonggolan
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i
    ON i.NoProduksi  = d.NoProduksi
   AND i.NoBonggolan = d.NoBonggolan
  WHERE i.NoProduksi IS NULL
    AND i.NoBonggolan IS NULL;

END;
GO

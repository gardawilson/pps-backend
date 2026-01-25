/* ===== [dbo].[tr_Audit_BongkarSusunOutputBonggolan] ON [dbo].[BongkarSusunOutputBonggolan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputBonggolan
-- AFTER INSERT, UPDATE, DELETE
-- ✅ UPDATED: Always returns JSON array for consistency
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: NoBonggolan (aggregated by NoBonggolan for audit grouping)
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputBonggolan]
ON [dbo].[BongkarSusunOutputBonggolan]
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
     ✅ INSERT - Group by NoBonggolan
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'INSERT',
    'BongkarSusunOutputBonggolan',
    @actor,
    @rid,
    CONCAT('{"NoBonggolan":"', i.NoBonggolan, '"}'),
    NULL,
    (
      SELECT
        ins.NoBongkarSusun,
        ins.NoBonggolan
      FROM inserted ins
      WHERE ins.NoBonggolan = i.NoBonggolan
      FOR JSON PATH  -- ✅ Returns array (remove WITHOUT_ARRAY_WRAPPER)
    )
  FROM inserted i
  LEFT JOIN deleted d
    ON d.NoBongkarSusun = i.NoBongkarSusun
   AND d.NoBonggolan    = i.NoBonggolan
  WHERE d.NoBongkarSusun IS NULL
    AND d.NoBonggolan IS NULL
  GROUP BY i.NoBonggolan;  -- ✅ Group by document number

  /* =====================
     ✅ UPDATE - Group by NoBonggolan
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'BongkarSusunOutputBonggolan',
    @actor,
    @rid,
    CONCAT('{"NoBonggolan":"', i.NoBonggolan, '"}'),
    (
      SELECT
        del.NoBongkarSusun,
        del.NoBonggolan
      FROM deleted del
      WHERE del.NoBonggolan = i.NoBonggolan
      FOR JSON PATH  -- ✅ Returns array
    ),
    (
      SELECT
        ins.NoBongkarSusun,
        ins.NoBonggolan
      FROM inserted ins
      WHERE ins.NoBonggolan = i.NoBonggolan
      FOR JSON PATH  -- ✅ Returns array
    )
  FROM inserted i
  JOIN deleted d
    ON d.NoBongkarSusun = i.NoBongkarSusun
   AND d.NoBonggolan    = i.NoBonggolan
  GROUP BY i.NoBonggolan;  -- ✅ Group by document number

  /* =====================
     ✅ DELETE - Group by NoBonggolan
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'BongkarSusunOutputBonggolan',
    @actor,
    @rid,
    CONCAT('{"NoBonggolan":"', d.NoBonggolan, '"}'),
    (
      SELECT
        del.NoBongkarSusun,
        del.NoBonggolan
      FROM deleted del
      WHERE del.NoBonggolan = d.NoBonggolan
      FOR JSON PATH  -- ✅ Returns array
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i
    ON i.NoBongkarSusun = d.NoBongkarSusun
   AND i.NoBonggolan    = d.NoBonggolan
  WHERE i.NoBongkarSusun IS NULL
    AND i.NoBonggolan IS NULL
  GROUP BY d.NoBonggolan;  -- ✅ Group by document number

END;
GO
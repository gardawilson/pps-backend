SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_CrusherProduksi_h] ON [dbo].[CrusherProduksi_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_CrusherProduksi_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: {"NoCrusherProduksi":"..."}
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_CrusherProduksi_h]
ON [dbo].[CrusherProduksi_h]
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
    'CrusherProduksi_h',
    @actor,
    @rid,
    CONCAT('{"NoCrusherProduksi":"', i.NoCrusherProduksi, '"}'),
    NULL,
    (
      SELECT
        i.NoCrusherProduksi,
        i.Tanggal,
        i.IdMesin,
        i.IdOperator,
        i.Jam,
        i.Shift,
        i.CreateBy,
        i.CheckBy1,
        i.CheckBy2,
        i.ApproveBy,
        i.JmlhAnggota,
        i.Hadir,
        i.HourMeter,
        i.HourStart,
        i.HourEnd
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  LEFT JOIN deleted d 
    ON d.NoCrusherProduksi = i.NoCrusherProduksi
  WHERE d.NoCrusherProduksi IS NULL;

  /* =====================
     UPDATE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UPDATE',
    'CrusherProduksi_h',
    @actor,
    @rid,
    CONCAT('{"NoCrusherProduksi":"', i.NoCrusherProduksi, '"}'),
    (
      SELECT
        d.NoCrusherProduksi,
        d.Tanggal,
        d.IdMesin,
        d.IdOperator,
        d.Jam,
        d.Shift,
        d.CreateBy,
        d.CheckBy1,
        d.CheckBy2,
        d.ApproveBy,
        d.JmlhAnggota,
        d.Hadir,
        d.HourMeter,
        d.HourStart,
        d.HourEnd
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    (
      SELECT
        i.NoCrusherProduksi,
        i.Tanggal,
        i.IdMesin,
        i.IdOperator,
        i.Jam,
        i.Shift,
        i.CreateBy,
        i.CheckBy1,
        i.CheckBy2,
        i.ApproveBy,
        i.JmlhAnggota,
        i.Hadir,
        i.HourMeter,
        i.HourStart,
        i.HourEnd
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
  FROM inserted i
  JOIN deleted d 
    ON d.NoCrusherProduksi = i.NoCrusherProduksi;

  /* =====================
     DELETE
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'DELETE',
    'CrusherProduksi_h',
    @actor,
    @rid,
    CONCAT('{"NoCrusherProduksi":"', d.NoCrusherProduksi, '"}'),
    (
      SELECT
        d.NoCrusherProduksi,
        d.Tanggal,
        d.IdMesin,
        d.IdOperator,
        d.Jam,
        d.Shift,
        d.CreateBy,
        d.CheckBy1,
        d.CheckBy2,
        d.ApproveBy,
        d.JmlhAnggota,
        d.Hadir,
        d.HourMeter,
        d.HourStart,
        d.HourEnd
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ),
    NULL
  FROM deleted d
  LEFT JOIN inserted i 
    ON i.NoCrusherProduksi = d.NoCrusherProduksi
  WHERE i.NoCrusherProduksi IS NULL;

END;
GO

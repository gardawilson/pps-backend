SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_PackingProduksi_h] ON [dbo].[PackingProduksi_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_PackingProduksi_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: {"NoPacking":"..."}
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_PackingProduksi_h]
ON [dbo].[PackingProduksi_h]
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
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'INSERT',
        'PackingProduksi_h',
        @actor,
        @rid,
        CONCAT('{"NoPacking":"', i.NoPacking, '"}'),
        NULL,
        (
            SELECT
                i.NoPacking,
                i.Tanggal,
                i.IdMesin,
                i.IdOperator,
                i.Shift,
                i.JamKerja,
                i.CreateBy,
                i.CheckBy1,
                i.CheckBy2,
                i.ApproveBy,
                i.HourMeter,
                i.HourStart,
                i.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    LEFT JOIN deleted d ON d.NoPacking = i.NoPacking
    WHERE d.NoPacking IS NULL;

    /* =====================
       UPDATE
    ===================== */
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UPDATE',
        'PackingProduksi_h',
        @actor,
        @rid,
        CONCAT('{"NoPacking":"', i.NoPacking, '"}'),
        (
            SELECT
                d.NoPacking,
                d.Tanggal,
                d.IdMesin,
                d.IdOperator,
                d.Shift,
                d.JamKerja,
                d.CreateBy,
                d.CheckBy1,
                d.CheckBy2,
                d.ApproveBy,
                d.HourMeter,
                d.HourStart,
                d.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                i.NoPacking,
                i.Tanggal,
                i.IdMesin,
                i.IdOperator,
                i.Shift,
                i.JamKerja,
                i.CreateBy,
                i.CheckBy1,
                i.CheckBy2,
                i.ApproveBy,
                i.HourMeter,
                i.HourStart,
                i.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    JOIN deleted d ON d.NoPacking = i.NoPacking;

    /* =====================
       DELETE
    ===================== */
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'DELETE',
        'PackingProduksi_h',
        @actor,
        @rid,
        CONCAT('{"NoPacking":"', d.NoPacking, '"}'),
        (
            SELECT
                d.NoPacking,
                d.Tanggal,
                d.IdMesin,
                d.IdOperator,
                d.Shift,
                d.JamKerja,
                d.CreateBy,
                d.CheckBy1,
                d.CheckBy2,
                d.ApproveBy,
                d.HourMeter,
                d.HourStart,
                d.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL
    FROM deleted d
    LEFT JOIN inserted i ON i.NoPacking = d.NoPacking
    WHERE i.NoPacking IS NULL;
END;
GO

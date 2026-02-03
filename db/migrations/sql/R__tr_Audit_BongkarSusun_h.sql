SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BongkarSusun_h] ON [dbo].[BongkarSusun_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusun_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: {"NoBongkarSusun":"..."}
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusun_h]
ON [dbo].[BongkarSusun_h]
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @actor NVARCHAR(128) =
        COALESCE(
            CONVERT(NVARCHAR(128), TRY_CONVERT(INT, SESSION_CONTEXT(N'actor_id'))),
            CAST(SESSION_CONTEXT(N'actor') AS NVARCHAR(128)),
            SUSER_SNAME()
        );

    DECLARE @rid NVARCHAR(64) =
        CAST(SESSION_CONTEXT(N'request_id') AS NVARCHAR(64));

    /* =====================
       INSERT
    ===================== */
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'INSERT',
        'BongkarSusun_h',
        @actor,
        @rid,
        CONCAT('{"NoBongkarSusun":"', i.NoBongkarSusun, '"}'),
        NULL,
        (
            SELECT
                i.NoBongkarSusun,
                i.Tanggal,
                i.IdUsername,
                i.Note
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    LEFT JOIN deleted d ON d.NoBongkarSusun = i.NoBongkarSusun
    WHERE d.NoBongkarSusun IS NULL;

    /* =====================
       UPDATE
    ===================== */
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UPDATE',
        'BongkarSusun_h',
        @actor,
        @rid,
        CONCAT('{"NoBongkarSusun":"', i.NoBongkarSusun, '"}'),
        (
            SELECT
                d.NoBongkarSusun,
                d.Tanggal,
                d.IdUsername,
                d.Note
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                i.NoBongkarSusun,
                i.Tanggal,
                i.IdUsername,
                i.Note
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    JOIN deleted d ON d.NoBongkarSusun = i.NoBongkarSusun;

    /* =====================
       DELETE
    ===================== */
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'DELETE',
        'BongkarSusun_h',
        @actor,
        @rid,
        CONCAT('{"NoBongkarSusun":"', d.NoBongkarSusun, '"}'),
        (
            SELECT
                d.NoBongkarSusun,
                d.Tanggal,
                d.IdUsername,
                d.Note
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL
    FROM deleted d
    LEFT JOIN inserted i ON i.NoBongkarSusun = d.NoBongkarSusun
    WHERE i.NoBongkarSusun IS NULL;

END;
GO

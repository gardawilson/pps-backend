SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJSortirReject_h] ON [dbo].[BJSortirReject_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BJSortirReject_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: {"NoBJSortir":"..."}
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJSortirReject_h]
ON [dbo].[BJSortirReject_h]
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
        'BJSortirReject_h',
        @actor,
        @rid,
        CONCAT('{"NoBJSortir":"', i.NoBJSortir, '"}'),
        NULL,
        (
            SELECT
                i.NoBJSortir,
                i.TglBJSortir,
                i.IdWarehouse,
                i.IdUsername
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    LEFT JOIN deleted d ON d.NoBJSortir = i.NoBJSortir
    WHERE d.NoBJSortir IS NULL;

    /* =====================
       UPDATE
    ===================== */
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UPDATE',
        'BJSortirReject_h',
        @actor,
        @rid,
        CONCAT('{"NoBJSortir":"', i.NoBJSortir, '"}'),
        (
            SELECT
                d.NoBJSortir,
                d.TglBJSortir,
                d.IdWarehouse,
                d.IdUsername
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                i.NoBJSortir,
                i.TglBJSortir,
                i.IdWarehouse,
                i.IdUsername
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    JOIN deleted d ON d.NoBJSortir = i.NoBJSortir;

    /* =====================
       DELETE
    ===================== */
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'DELETE',
        'BJSortirReject_h',
        @actor,
        @rid,
        CONCAT('{"NoBJSortir":"', d.NoBJSortir, '"}'),
        (
            SELECT
                d.NoBJSortir,
                d.TglBJSortir,
                d.IdWarehouse,
                d.IdUsername
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL
    FROM deleted d
    LEFT JOIN inserted i ON i.NoBJSortir = d.NoBJSortir
    WHERE i.NoBJSortir IS NULL;
END;
GO

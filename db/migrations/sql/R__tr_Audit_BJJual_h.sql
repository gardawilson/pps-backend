SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_BJJual_h] ON [dbo].[BJJual_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BJJual_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: {"NoBJJual":"..."}
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BJJual_h]
ON [dbo].[BJJual_h]
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
        'BJJual_h',
        @actor,
        @rid,
        CONCAT('{"NoBJJual":"', i.NoBJJual, '"}'),
        NULL,
        (
            SELECT
                i.NoBJJual,
                i.Tanggal,
                i.IdPembeli,
                i.IdWarehouse,
                i.Remark
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    LEFT JOIN deleted d ON d.NoBJJual = i.NoBJJual
    WHERE d.NoBJJual IS NULL;

    /* =====================
       UPDATE
    ===================== */
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UPDATE',
        'BJJual_h',
        @actor,
        @rid,
        CONCAT('{"NoBJJual":"', i.NoBJJual, '"}'),
        (
            SELECT
                d.NoBJJual,
                d.Tanggal,
                d.IdPembeli,
                d.IdWarehouse,
                d.Remark
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                i.NoBJJual,
                i.Tanggal,
                i.IdPembeli,
                i.IdWarehouse,
                i.Remark
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    JOIN deleted d ON d.NoBJJual = i.NoBJJual;

    /* =====================
       DELETE
    ===================== */
    INSERT dbo.AuditTrail (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'DELETE',
        'BJJual_h',
        @actor,
        @rid,
        CONCAT('{"NoBJJual":"', d.NoBJJual, '"}'),
        (
            SELECT
                d.NoBJJual,
                d.Tanggal,
                d.IdPembeli,
                d.IdWarehouse,
                d.Remark
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL
    FROM deleted d
    LEFT JOIN inserted i ON i.NoBJJual = d.NoBJJual
    WHERE i.NoBJJual IS NULL;
END;
GO

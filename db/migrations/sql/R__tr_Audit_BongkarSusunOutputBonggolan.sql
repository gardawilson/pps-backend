/* ===== [dbo].[tr_Audit_BongkarSusunOutputBonggolan]
         ON [dbo].[BongkarSusunOutputBonggolan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputBonggolan
-- PK     : NoBonggolan + NoBongkarSusun
-- MODE   : AGGREGATED
-- EXTRA  : Join Bonggolan untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputBonggolan]
ON [dbo].[BongkarSusunOutputBonggolan]
AFTER INSERT, DELETE
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

    /* =========================================================
       PRODUCE (INSERT ONLY, AGGREGATED)
       ========================================================= */
    IF EXISTS (SELECT 1 FROM inserted)
       AND NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT dbo.AuditTrail
            (Action, TableName, Actor, RequestId, PK, OldData, NewData)
        SELECT
            'PRODUCE',
            'BongkarSusunOutputBonggolan',
            @actor,
            @rid,
            (
                SELECT
                    i.NoBonggolan,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            NULL,
            (
                SELECT
                    i.NoBonggolan,
                    i.NoBongkarSusun,
                    b.Berat
                FROM inserted i
                LEFT JOIN dbo.Bonggolan b
                       ON b.NoBonggolan = i.NoBonggolan
                FOR JSON PATH
            )
        FROM inserted i
        GROUP BY i.NoBonggolan, i.NoBongkarSusun;
    END;

    /* =========================================================
       UNPRODUCE (DELETE ONLY, AGGREGATED)
       ========================================================= */
    IF EXISTS (SELECT 1 FROM deleted)
       AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT dbo.AuditTrail
            (Action, TableName, Actor, RequestId, PK, OldData, NewData)
        SELECT
            'UNPRODUCE',
            'BongkarSusunOutputBonggolan',
            @actor,
            @rid,
            (
                SELECT
                    d.NoBonggolan,
                    d.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoBonggolan,
                    d.NoBongkarSusun,
                    b.Berat
                FROM deleted d
                LEFT JOIN dbo.Bonggolan b
                       ON b.NoBonggolan = d.NoBonggolan
                FOR JSON PATH
            ),
            NULL
        FROM deleted d
        GROUP BY d.NoBonggolan, d.NoBongkarSusun;
    END;
END;
GO

/* ===== [dbo].[tr_Audit_BongkarSusunOutputWashing]
         ON [dbo].[BongkarSusunOutputWashing] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputWashing
-- PK     : NoWashing + NoBongkarSusun
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Washing_d untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputWashing]
ON [dbo].[BongkarSusunOutputWashing]
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
       PRODUCE  (INSERT ONLY, AGGREGATED)
       ========================================================= */
    IF EXISTS (SELECT 1 FROM inserted)
       AND NOT EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT dbo.AuditTrail
            (Action, TableName, Actor, RequestId, PK, OldData, NewData)
        SELECT
            'PRODUCE',
            'BongkarSusunOutputWashing',
            @actor,
            @rid,
            (
                SELECT
                    i.NoWashing,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            NULL,
            (
                SELECT
                    i.NoWashing,
                    i.NoBongkarSusun,
                    i.NoSak,
                    wd.Berat
                FROM inserted i
                LEFT JOIN dbo.Washing_d wd
                       ON wd.NoWashing = i.NoWashing
                      AND wd.NoSak     = i.NoSak
                FOR JSON PATH
            )
        FROM inserted i
        GROUP BY i.NoWashing, i.NoBongkarSusun;
    END;

    /* =========================================================
       UNPRODUCE  (DELETE ONLY, AGGREGATED)
       ========================================================= */
    IF EXISTS (SELECT 1 FROM deleted)
       AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT dbo.AuditTrail
            (Action, TableName, Actor, RequestId, PK, OldData, NewData)
        SELECT
            'UNPRODUCE',
            'BongkarSusunOutputWashing',
            @actor,
            @rid,
            (
                SELECT
                    d.NoWashing,
                    d.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoWashing,
                    d.NoBongkarSusun,
                    d.NoSak,
                    wd.Berat
                FROM deleted d
                LEFT JOIN dbo.Washing_d wd
                       ON wd.NoWashing = d.NoWashing
                      AND wd.NoSak     = d.NoSak
                FOR JSON PATH
            ),
            NULL
        FROM deleted d
        GROUP BY d.NoWashing, d.NoBongkarSusun;
    END;
END;
GO

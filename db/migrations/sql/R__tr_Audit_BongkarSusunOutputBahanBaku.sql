/* ===== [dbo].[tr_Audit_BongkarSusunOutputBahanBaku]
         ON [dbo].[BongkarSusunOutputBahanBaku] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputBahanBaku
-- PK     : NoBahanBaku + NoPallet + NoBongkarSusun
-- MODE   : AGGREGATED
-- EXTRA  : Join BahanBaku_d untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputBahanBaku]
ON [dbo].[BongkarSusunOutputBahanBaku]
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
            'BongkarSusunOutputBahanBaku',
            @actor,
            @rid,
            (
                SELECT
                    i.NoBahanBaku,
                    i.NoPallet,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            NULL,
            (
                SELECT
                    i.NoBahanBaku,
                    i.NoPallet,
                    i.NoBongkarSusun,
                    i.NoSak,
                    bb.Berat
                FROM inserted i
                LEFT JOIN dbo.BahanBaku_d bb
                       ON bb.NoBahanBaku = i.NoBahanBaku
                      AND bb.NoPallet    = i.NoPallet
                      AND bb.NoSak       = i.NoSak
                FOR JSON PATH
            )
        FROM inserted i
        GROUP BY i.NoBahanBaku, i.NoPallet, i.NoBongkarSusun;
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
            'BongkarSusunOutputBahanBaku',
            @actor,
            @rid,
            (
                SELECT
                    d.NoBahanBaku,
                    d.NoPallet,
                    d.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoBahanBaku,
                    d.NoPallet,
                    d.NoBongkarSusun,
                    d.NoSak,
                    bb.Berat
                FROM deleted d
                LEFT JOIN dbo.BahanBaku_d bb
                       ON bb.NoBahanBaku = d.NoBahanBaku
                      AND bb.NoPallet    = d.NoPallet
                      AND bb.NoSak       = d.NoSak
                FOR JSON PATH
            ),
            NULL
        FROM deleted d
        GROUP BY d.NoBahanBaku, d.NoPallet, d.NoBongkarSusun;
    END;
END;
GO

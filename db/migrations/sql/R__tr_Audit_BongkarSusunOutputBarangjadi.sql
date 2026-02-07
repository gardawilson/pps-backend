/* ===== [dbo].[tr_Audit_BongkarSusunOutputBarangjadi]
         ON [dbo].[BongkarSusunOutputBarangjadi] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputBarangjadi
-- PK     : NoBJ + NoBongkarSusun
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join BarangJadi untuk ambil Pcs
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputBarangjadi]
ON [dbo].[BongkarSusunOutputBarangjadi]
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

    /* =========================================================
       1) INSERT-only => PRODUCE (DETAIL)
       ========================================================= */
    ;WITH insOnly AS (
        SELECT
            i.NoBJ,
            i.NoBongkarSusun,
            bj.Pcs
        FROM inserted i
        LEFT JOIN deleted d
               ON d.NoBJ            = i.NoBJ
              AND d.NoBongkarSusun  = i.NoBongkarSusun
        LEFT JOIN dbo.BarangJadi bj
               ON bj.NoBJ = i.NoBJ
        WHERE d.NoBJ IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'PRODUCE',
        'BongkarSusunOutputBarangjadi',
        @actor,
        @rid,
        (
            SELECT
                i.NoBJ,
                i.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL,
        (
            SELECT
                i.NoBJ,
                i.NoBongkarSusun,
                i.Pcs
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM insOnly i;

    /* =========================================================
       2) DELETE-only => UNPRODUCE (DETAIL)
       ========================================================= */
    ;WITH delOnly AS (
        SELECT
            d.NoBJ,
            d.NoBongkarSusun,
            bj.Pcs
        FROM deleted d
        LEFT JOIN inserted i
               ON i.NoBJ            = d.NoBJ
              AND i.NoBongkarSusun  = d.NoBongkarSusun
        LEFT JOIN dbo.BarangJadi bj
               ON bj.NoBJ = d.NoBJ
        WHERE i.NoBJ IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UNPRODUCE',
        'BongkarSusunOutputBarangjadi',
        @actor,
        @rid,
        (
            SELECT
                d.NoBJ,
                d.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                d.NoBJ,
                d.NoBongkarSusun,
                d.Pcs
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL
    FROM delOnly d;

    /* =========================================================
       3) UPDATE => UPDATE (DETAIL)
       ========================================================= */
    IF EXISTS (SELECT 1 FROM inserted)
       AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT dbo.AuditTrail
            (Action, TableName, Actor, RequestId, PK, OldData, NewData)
        SELECT
            'UPDATE',
            'BongkarSusunOutputBarangjadi',
            @actor,
            @rid,
            (
                SELECT
                    i.NoBJ,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoBJ,
                    d.NoBongkarSusun,
                    bjOld.Pcs
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    i.NoBJ,
                    i.NoBongkarSusun,
                    bjNew.Pcs
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        JOIN deleted d
             ON d.NoBJ           = i.NoBJ
            AND d.NoBongkarSusun = i.NoBongkarSusun
        LEFT JOIN dbo.BarangJadi bjOld
             ON bjOld.NoBJ = d.NoBJ
        LEFT JOIN dbo.BarangJadi bjNew
             ON bjNew.NoBJ = i.NoBJ;
    END
END;
GO

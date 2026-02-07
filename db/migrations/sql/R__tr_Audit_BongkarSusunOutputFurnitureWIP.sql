/* ===== [dbo].[tr_Audit_BongkarSusunOutputFurnitureWIP]
         ON [dbo].[BongkarSusunOutputFurnitureWIP] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputFurnitureWIP
-- PK     : NoFurnitureWIP + NoBongkarSusun
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join FurnitureWIP untuk ambil Pcs
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputFurnitureWIP]
ON [dbo].[BongkarSusunOutputFurnitureWIP]
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
            i.NoFurnitureWIP,
            i.NoBongkarSusun,
            fw.Pcs
        FROM inserted i
        LEFT JOIN deleted d
               ON d.NoFurnitureWIP  = i.NoFurnitureWIP
              AND d.NoBongkarSusun  = i.NoBongkarSusun
        LEFT JOIN dbo.FurnitureWIP fw
               ON fw.NoFurnitureWIP = i.NoFurnitureWIP
        WHERE d.NoFurnitureWIP IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'PRODUCE',
        'BongkarSusunOutputFurnitureWIP',
        @actor,
        @rid,
        (
            SELECT
                i.NoFurnitureWIP,
                i.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL,
        (
            SELECT
                i.NoFurnitureWIP,
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
            d.NoFurnitureWIP,
            d.NoBongkarSusun,
            fw.Pcs
        FROM deleted d
        LEFT JOIN inserted i
               ON i.NoFurnitureWIP  = d.NoFurnitureWIP
              AND i.NoBongkarSusun  = d.NoBongkarSusun
        LEFT JOIN dbo.FurnitureWIP fw
               ON fw.NoFurnitureWIP = d.NoFurnitureWIP
        WHERE i.NoFurnitureWIP IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UNPRODUCE',
        'BongkarSusunOutputFurnitureWIP',
        @actor,
        @rid,
        (
            SELECT
                d.NoFurnitureWIP,
                d.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                d.NoFurnitureWIP,
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
            'BongkarSusunOutputFurnitureWIP',
            @actor,
            @rid,
            (
                SELECT
                    i.NoFurnitureWIP,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoFurnitureWIP,
                    d.NoBongkarSusun,
                    fwOld.Pcs
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    i.NoFurnitureWIP,
                    i.NoBongkarSusun,
                    fwNew.Pcs
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        JOIN deleted d
             ON d.NoFurnitureWIP = i.NoFurnitureWIP
            AND d.NoBongkarSusun = i.NoBongkarSusun
        LEFT JOIN dbo.FurnitureWIP fwOld
             ON fwOld.NoFurnitureWIP = d.NoFurnitureWIP
        LEFT JOIN dbo.FurnitureWIP fwNew
             ON fwNew.NoFurnitureWIP = i.NoFurnitureWIP;
    END
END;
GO

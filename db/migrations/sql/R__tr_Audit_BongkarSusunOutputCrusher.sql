/* ===== [dbo].[tr_Audit_BongkarSusunOutputCrusher]
         ON [dbo].[BongkarSusunOutputCrusher] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputCrusher
-- PK     : NoCrusher + NoBongkarSusun
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Crusher untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputCrusher]
ON [dbo].[BongkarSusunOutputCrusher]
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
            i.NoCrusher,
            i.NoBongkarSusun,
            c.Berat
        FROM inserted i
        LEFT JOIN deleted d
               ON d.NoCrusher        = i.NoCrusher
              AND d.NoBongkarSusun  = i.NoBongkarSusun
        LEFT JOIN dbo.Crusher c
               ON c.NoCrusher = i.NoCrusher
        WHERE d.NoCrusher IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'PRODUCE',
        'BongkarSusunOutputCrusher',
        @actor,
        @rid,
        (
            SELECT
                i.NoCrusher,
                i.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL,
        (
            SELECT
                i.NoCrusher,
                i.NoBongkarSusun,
                i.Berat
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM insOnly i;

    /* =========================================================
       2) DELETE-only => UNPRODUCE (DETAIL)
       ========================================================= */
    ;WITH delOnly AS (
        SELECT
            d.NoCrusher,
            d.NoBongkarSusun,
            c.Berat
        FROM deleted d
        LEFT JOIN inserted i
               ON i.NoCrusher        = d.NoCrusher
              AND i.NoBongkarSusun  = d.NoBongkarSusun
        LEFT JOIN dbo.Crusher c
               ON c.NoCrusher = d.NoCrusher
        WHERE i.NoCrusher IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UNPRODUCE',
        'BongkarSusunOutputCrusher',
        @actor,
        @rid,
        (
            SELECT
                d.NoCrusher,
                d.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                d.NoCrusher,
                d.NoBongkarSusun,
                d.Berat
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
            'BongkarSusunOutputCrusher',
            @actor,
            @rid,
            (
                SELECT
                    i.NoCrusher,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoCrusher,
                    d.NoBongkarSusun,
                    cOld.Berat
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    i.NoCrusher,
                    i.NoBongkarSusun,
                    cNew.Berat
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        JOIN deleted d
             ON d.NoCrusher        = i.NoCrusher
            AND d.NoBongkarSusun  = i.NoBongkarSusun
        LEFT JOIN dbo.Crusher cOld
             ON cOld.NoCrusher = d.NoCrusher
        LEFT JOIN dbo.Crusher cNew
             ON cNew.NoCrusher = i.NoCrusher;
    END
END;
GO

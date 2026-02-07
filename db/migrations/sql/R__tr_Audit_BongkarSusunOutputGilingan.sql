/* ===== [dbo].[tr_Audit_BongkarSusunOutputGilingan]
         ON [dbo].[BongkarSusunOutputGilingan] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputGilingan
-- PK     : NoGilingan + NoBongkarSusun
-- MODE   : DETAIL (1 row = 1 audit)
-- EXTRA  : Join Gilingan untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputGilingan]
ON [dbo].[BongkarSusunOutputGilingan]
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
            i.NoGilingan,
            i.NoBongkarSusun,
            g.Berat
        FROM inserted i
        LEFT JOIN deleted d
               ON d.NoGilingan       = i.NoGilingan
              AND d.NoBongkarSusun   = i.NoBongkarSusun
        LEFT JOIN dbo.Gilingan g
               ON g.NoGilingan = i.NoGilingan
        WHERE d.NoGilingan IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'PRODUCE',
        'BongkarSusunOutputGilingan',
        @actor,
        @rid,
        (
            SELECT
                i.NoGilingan,
                i.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL,
        (
            SELECT
                i.NoGilingan,
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
            d.NoGilingan,
            d.NoBongkarSusun,
            g.Berat
        FROM deleted d
        LEFT JOIN inserted i
               ON i.NoGilingan       = d.NoGilingan
              AND i.NoBongkarSusun   = d.NoBongkarSusun
        LEFT JOIN dbo.Gilingan g
               ON g.NoGilingan = d.NoGilingan
        WHERE i.NoGilingan IS NULL
    )
    INSERT dbo.AuditTrail
        (Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UNPRODUCE',
        'BongkarSusunOutputGilingan',
        @actor,
        @rid,
        (
            SELECT
                d.NoGilingan,
                d.NoBongkarSusun
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                d.NoGilingan,
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
            'BongkarSusunOutputGilingan',
            @actor,
            @rid,
            (
                SELECT
                    i.NoGilingan,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoGilingan,
                    d.NoBongkarSusun,
                    gOld.Berat
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    i.NoGilingan,
                    i.NoBongkarSusun,
                    gNew.Berat
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM inserted i
        JOIN deleted d
             ON d.NoGilingan       = i.NoGilingan
            AND d.NoBongkarSusun   = i.NoBongkarSusun
        LEFT JOIN dbo.Gilingan gOld
             ON gOld.NoGilingan = d.NoGilingan
        LEFT JOIN dbo.Gilingan gNew
             ON gNew.NoGilingan = i.NoGilingan;
    END
END;
GO
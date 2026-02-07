/* ===== [dbo].[tr_Audit_BongkarSusunOutputMixer]
         ON [dbo].[BongkarSusunOutputMixer] ===== */
-- =============================================
-- TRIGGER: tr_Audit_BongkarSusunOutputMixer
-- PK     : NoMixer + NoBongkarSusun
-- MODE   : AGGREGATED
-- EXTRA  : Join Mixer_d untuk ambil Berat
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BongkarSusunOutputMixer]
ON [dbo].[BongkarSusunOutputMixer]
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
            'BongkarSusunOutputMixer',
            @actor,
            @rid,
            (
                SELECT
                    i.NoMixer,
                    i.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            NULL,
            (
                SELECT
                    i.NoMixer,
                    i.NoBongkarSusun,
                    i.NoSak,
                    md.Berat
                FROM inserted i
                LEFT JOIN dbo.Mixer_d md
                       ON md.NoMixer = i.NoMixer
                      AND md.NoSak   = i.NoSak
                FOR JSON PATH
            )
        FROM inserted i
        GROUP BY i.NoMixer, i.NoBongkarSusun;
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
            'BongkarSusunOutputMixer',
            @actor,
            @rid,
            (
                SELECT
                    d.NoMixer,
                    d.NoBongkarSusun
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            (
                SELECT
                    d.NoMixer,
                    d.NoBongkarSusun,
                    d.NoSak,
                    md.Berat
                FROM deleted d
                LEFT JOIN dbo.Mixer_d md
                       ON md.NoMixer = d.NoMixer
                      AND md.NoSak   = d.NoSak
                FOR JSON PATH
            ),
            NULL
        FROM deleted d
        GROUP BY d.NoMixer, d.NoBongkarSusun;
    END;
END;
GO

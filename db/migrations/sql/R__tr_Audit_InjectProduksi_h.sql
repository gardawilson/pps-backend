SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/* ===== [dbo].[tr_Audit_InjectProduksi_h] ON [dbo].[InjectProduksi_h] ===== */
-- =============================================
-- TRIGGER: tr_Audit_InjectProduksi_h
-- AFTER INSERT, UPDATE, DELETE
-- Actor: SESSION_CONTEXT('actor_id') fallback SESSION_CONTEXT('actor') fallback SUSER_SNAME()
-- RequestId: SESSION_CONTEXT('request_id')
-- PK: {"NoProduksi":"..."}
-- =============================================
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_InjectProduksi_h]
ON [dbo].[InjectProduksi_h]
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
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'INSERT',
        'InjectProduksi_h',
        @actor,
        @rid,
        CONCAT('{"NoProduksi":"', i.NoProduksi, '"}'),
        NULL,
        (
            SELECT
                i.NoProduksi,
                i.IdOperator,
                i.IdMesin,
                i.TglProduksi,
                i.Jam,
                i.Shift,
                i.CreateBy,
                i.CheckBy1,
                i.CheckBy2,
                i.ApproveBy,
                i.JmlhAnggota,
                i.Hadir,
                i.IdCetakan,
                i.IdWarna,
                i.EnableOffset,
                i.OffsetCurrent,
                i.OffsetNext,
                i.IdFurnitureMaterial,
                i.HourMeter,
                i.BeratProdukHasilTimbang,
                i.HourStart,
                i.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    LEFT JOIN deleted d ON d.NoProduksi = i.NoProduksi
    WHERE d.NoProduksi IS NULL;

    /* =====================
       UPDATE
    ===================== */
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'UPDATE',
        'InjectProduksi_h',
        @actor,
        @rid,
        CONCAT('{"NoProduksi":"', i.NoProduksi, '"}'),
        (
            SELECT
                d.NoProduksi,
                d.IdOperator,
                d.IdMesin,
                d.TglProduksi,
                d.Jam,
                d.Shift,
                d.CreateBy,
                d.CheckBy1,
                d.CheckBy2,
                d.ApproveBy,
                d.JmlhAnggota,
                d.Hadir,
                d.IdCetakan,
                d.IdWarna,
                d.EnableOffset,
                d.OffsetCurrent,
                d.OffsetNext,
                d.IdFurnitureMaterial,
                d.HourMeter,
                d.BeratProdukHasilTimbang,
                d.HourStart,
                d.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        (
            SELECT
                i.NoProduksi,
                i.IdOperator,
                i.IdMesin,
                i.TglProduksi,
                i.Jam,
                i.Shift,
                i.CreateBy,
                i.CheckBy1,
                i.CheckBy2,
                i.ApproveBy,
                i.JmlhAnggota,
                i.Hadir,
                i.IdCetakan,
                i.IdWarna,
                i.EnableOffset,
                i.OffsetCurrent,
                i.OffsetNext,
                i.IdFurnitureMaterial,
                i.HourMeter,
                i.BeratProdukHasilTimbang,
                i.HourStart,
                i.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM inserted i
    JOIN deleted d ON d.NoProduksi = i.NoProduksi;

    /* =====================
       DELETE
    ===================== */
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
        'DELETE',
        'InjectProduksi_h',
        @actor,
        @rid,
        CONCAT('{"NoProduksi":"', d.NoProduksi, '"}'),
        (
            SELECT
                d.NoProduksi,
                d.IdOperator,
                d.IdMesin,
                d.TglProduksi,
                d.Jam,
                d.Shift,
                d.CreateBy,
                d.CheckBy1,
                d.CheckBy2,
                d.ApproveBy,
                d.JmlhAnggota,
                d.Hadir,
                d.IdCetakan,
                d.IdWarna,
                d.EnableOffset,
                d.OffsetCurrent,
                d.OffsetNext,
                d.IdFurnitureMaterial,
                d.HourMeter,
                d.BeratProdukHasilTimbang,
                d.HourStart,
                d.HourEnd
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        NULL
    FROM deleted d
    LEFT JOIN inserted i ON i.NoProduksi = d.NoProduksi
    WHERE i.NoProduksi IS NULL;
END;
GO

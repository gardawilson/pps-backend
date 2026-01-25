/* ===== [dbo].[trg_MesinUpdate] ON [dbo].[MstMesinInjectV2] ===== */
CREATE OR ALTER TRIGGER [dbo].[trg_MesinUpdate]
ON [dbo].[MstMesinInjectV2]
AFTER UPDATE
AS
BEGIN
  SET NOCOUNT ON;

  -- Insert flag ke tabel MesinChangeFlag
  INSERT INTO dbo.MesinChangeFlag (ChangedAt, ChangeType)
  VALUES (GETDATE(), 'UPDATE');

  PRINT 'Trigger UPDATE fired - Flag created';
END;

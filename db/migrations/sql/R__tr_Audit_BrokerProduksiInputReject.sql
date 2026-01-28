/* ===== [dbo].[tr_Audit_BrokerProduksiInputReject] ON [dbo].[BrokerProduksiInputReject] =====
   AFTER INSERT, UPDATE, DELETE
   Action: CONSUME_FULL / UNCONSUME_FULL / UPDATE
   PK: (NoProduksi, NoReject)
*/
CREATE OR ALTER TRIGGER [dbo].[tr_Audit_BrokerProduksiInputReject]
ON [dbo].[BrokerProduksiInputReject]
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
     INSERT-only => CONSUME_FULL
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'CONSUME_FULL',
    'BrokerProduksiInputReject',
    @actor,
    @rid,
    CONCAT('{"NoProduksi":"', i.NoProduksi, '","NoReject":"', i.NoReject, '"}'),
    NULL,
    COALESCE((
      SELECT
        i.NoProduksi,
        i.NoReject,
        CAST(r.Berat AS decimal(18,3)) AS Berat
      FROM dbo.RejectV2 r
      WHERE r.NoReject = i.NoReject
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ), CONCAT('{"NoProduksi":"', i.NoProduksi, '","NoReject":"', i.NoReject, '"}'))
  FROM inserted i
  WHERE NOT EXISTS (
    SELECT 1
    FROM deleted d
    WHERE d.NoProduksi = i.NoProduksi
      AND d.NoReject   = i.NoReject
  );

  /* =====================
     DELETE-only => UNCONSUME_FULL
  ===================== */
  INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
  SELECT
    'UNCONSUME_FULL',
    'BrokerProduksiInputReject',
    @actor,
    @rid,
    CONCAT('{"NoProduksi":"', d.NoProduksi, '","NoReject":"', d.NoReject, '"}'),
    COALESCE((
      SELECT
        d.NoProduksi,
        d.NoReject,
        CAST(r.Berat AS decimal(18,3)) AS Berat
      FROM dbo.RejectV2 r
      WHERE r.NoReject = d.NoReject
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ), CONCAT('{"NoProduksi":"', d.NoProduksi, '","NoReject":"', d.NoReject, '"}')),
    NULL
  FROM deleted d
  WHERE NOT EXISTS (
    SELECT 1
    FROM inserted i
    WHERE i.NoProduksi = d.NoProduksi
      AND i.NoReject   = d.NoReject
  );

  /* =====================
     UPDATE (jarang terjadi)
     - kalau tabel ini nggak pernah update, boleh kamu hapus section ini
  ===================== */
  IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
  BEGIN
    INSERT dbo.AuditTrail(Action, TableName, Actor, RequestId, PK, OldData, NewData)
    SELECT
      'UPDATE',
      'BrokerProduksiInputReject',
      @actor,
      @rid,
      CONCAT('{"NoProduksi":"', i.NoProduksi, '","NoReject":"', i.NoReject, '"}'),
      COALESCE((
        SELECT
          d.NoProduksi,
          d.NoReject,
          CAST(r.Berat AS decimal(18,3)) AS Berat
        FROM dbo.RejectV2 r
        WHERE r.NoReject = d.NoReject
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
      ), CONCAT('{"NoProduksi":"', d.NoProduksi, '","NoReject":"', d.NoReject, '"}')),
      COALESCE((
        SELECT
          i.NoProduksi,
          i.NoReject,
          CAST(r.Berat AS decimal(18,3)) AS Berat
        FROM dbo.RejectV2 r
        WHERE r.NoReject = i.NoReject
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
      ), CONCAT('{"NoProduksi":"', i.NoProduksi, '","NoReject":"', i.NoReject, '"}'))
    FROM inserted i
    JOIN deleted d
      ON d.NoProduksi = i.NoProduksi
     AND d.NoReject   = i.NoReject;
  END
END;
GO

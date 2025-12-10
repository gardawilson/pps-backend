const express = require('express');
const request = require('supertest');

const { sql, poolPromise } = require('../../../../core/config/db');
const rejectRouter = require('../reject-routes');

// 🔒 MOCK HANYA AUTH/PERMISSION, BUKAN controller/service/db

jest.mock('../../../../core/middleware/verify-token', () =>
  jest.fn((req, res, next) => {
    req.username = 'test-user'; // simulasi user login
    next();
  })
);

jest.mock('../../../../core/middleware/attach-permissions', () =>
  jest.fn((req, res, next) => {
    req.permissions = [
      'label_crusher:read',
      'label_crusher:create',
      'label_crusher:update',
      'label_crusher:delete',
    ];
    next();
  })
);

jest.mock('../../../../core/middleware/require-permission', () =>
  jest.fn(() => (req, res, next) => next())
);

// 👉 TIDAK mock controller
// 👉 TIDAK mock service
// 👉 TIDAK mock db

function createApp() {
  const app = express();
  app.use(express.json());
  app.use('/api', rejectRouter);
  return app;
}

describe('Reject POST Integration (DB real)', () => {
  let app;
  let pool;

  beforeAll(async () => {
    app = createApp();
    pool = await poolPromise; // pakai DB TEST (PPS_TEST3)
  });

  // helper untuk hapus data test
  async function cleanupReject(noReject) {
    if (!noReject) return;

    await pool
      .request()
      .input('NoReject', sql.VarChar, noReject)
      .query(`
        DELETE FROM dbo.InjectProduksiOutputRejectV2        WHERE NoReject = @NoReject;
        DELETE FROM dbo.HotStampingOutputRejectV2          WHERE NoReject = @NoReject;
        DELETE FROM dbo.PasangKunciOutputRejectV2          WHERE NoReject = @NoReject;
        DELETE FROM dbo.SpannerOutputRejectV2              WHERE NoReject = @NoReject;
        DELETE FROM dbo.BJSortirRejectOutputLabelReject    WHERE NoReject = @NoReject;
        DELETE FROM dbo.RejectV2                           WHERE NoReject = @NoReject;
      `);
  }

  // skenario prefix → tabel mapping
  const cases = [
    { name: 'Inject',      prefix: 'S.',  mappingTable: 'InjectProduksiOutputRejectV2' },
    { name: 'HotStamping', prefix: 'BH.', mappingTable: 'HotStampingOutputRejectV2' },
    { name: 'PasangKunci', prefix: 'BI.', mappingTable: 'PasangKunciOutputRejectV2' },
    { name: 'Spanner',     prefix: 'BJ.', mappingTable: 'SpannerOutputRejectV2' },
    { name: 'BJ Sortir',   prefix: 'J.',  mappingTable: 'BJSortirRejectOutputLabelReject' },
  ];

  test.each(cases)(
    'POST /api/labels/reject (%s) harus insert ke RejectV2 + %s',
    async ({ name, prefix, mappingTable }) => {
      // ⚠️ IMPORTANT:
      // Pastikan IdReject=1 benar-benar ADA di tabel master Reject (kalau ada FK).
      // Kalau tidak, ganti ke IdReject yang valid.
      const body = {
        header: {
          IdReject: 1,
          Berat: 10.5,
          // column lain dibiarkan NULL/default supaya kecil kemungkinan error
          // DateCreate, Jam, IdWarehouse, Blok, IdLokasi, IsPartial -> biarkan kosong
        },
        outputCode: `${prefix}0000099999`,
      };

      const res = await request(app)
        .post('/api/labels/reject')
        .send(body);

      // DEBUG: kalau gagal, print supaya kelihatan error aslinya
      if (res.status !== 201) {
        // Ini akan muncul di output Jest
        // dan sangat membantu lihat message dari controller
        console.error(
          `❌ Create Reject TEST Error [${name} - ${prefix}]:`,
          res.status,
          res.body
        );
      }

      // 1) Pastikan request sukses
      expect(res.status).toBe(201);
      expect(res.body.success).toBe(true);

      // 2) Ambil NoReject dari response
      const result = res.body.data;
      expect(result).toBeDefined();
      const headers = Array.isArray(result.headers) ? result.headers : [];
      expect(headers.length).toBeGreaterThan(0);

      const noReject = headers[0].NoReject;
      expect(typeof noReject).toBe('string');

      try {
        // 3) Cek header di RejectV2
        const headerRes = await pool
          .request()
          .input('NoReject', sql.VarChar, noReject)
          .query(`
            SELECT TOP 1 *
            FROM dbo.RejectV2
            WHERE NoReject = @NoReject;
          `);

        expect(headerRes.recordset.length).toBe(1);
        expect(Number(headerRes.recordset[0].Berat)).toBeCloseTo(10.5);

        // 4) Cek mapping di tabel sesuai prefix
        const mapRes = await pool
          .request()
          .input('NoReject', sql.VarChar, noReject)
          .query(`
            SELECT TOP 1 *
            FROM dbo.${mappingTable}
            WHERE NoReject = @NoReject;
          `);

        expect(mapRes.recordset.length).toBe(1);
      } finally {
        // 5) Hapus data test
        await cleanupReject(noReject);
      }
    }
  );
});

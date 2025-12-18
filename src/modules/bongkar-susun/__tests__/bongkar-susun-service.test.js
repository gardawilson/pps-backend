// src/modules/bongkar-susun/__tests__/bongkar-susun-service.test.js

// ====== MOCK DB CONFIG (core/config/db) ======================================
jest.mock('../../../core/config/db', () => {
    const mQuery = jest.fn();
    const mInput = jest.fn();
  
    // Mock Request: new sql.Request(tx) atau pool.request() -> req
    const MockRequest = jest.fn().mockImplementation(() => {
      const req = {
        input: (...args) => {
          mInput(...args);
          return req; // chaining
        },
        query: mQuery,
      };
      return req;
    });
  
    // Mock Transaction: new sql.Transaction(pool)
    const mBegin = jest.fn();
    const mCommit = jest.fn();
    const mRollback = jest.fn();
  
    const MockTransaction = jest.fn().mockImplementation(() => ({
      begin: mBegin,
      commit: mCommit,
      rollback: mRollback,
    }));
  
    // helper kecil untuk tipe mssql (Decimal, Int, dst)
    const makeType = (name) => jest.fn(() => name);
  
    // poolPromise -> dipakai di getAll & create/update/delete
    const mPool = {
      request: () => new MockRequest(),
    };
  
    return {
      sql: {
        VarChar: makeType('VarChar'),
        Int: makeType('Int'),
        Decimal: makeType('Decimal'),
        Date: makeType('Date'),
        Bit: makeType('Bit'),
        NVarChar: makeType('NVarChar'),
        MAX: 'MAX',
        ISOLATION_LEVEL: { SERIALIZABLE: 'SERIALIZABLE' },
        Request: MockRequest,
        Transaction: MockTransaction,
      },
      poolPromise: Promise.resolve(mPool),
      __mocks: {
        mQuery,
        mInput,
        MockRequest,
        MockTransaction,
        mBegin,
        mCommit,
        mRollback,
        mPool,
      },
    };
  });
  
  // ====== IMPORT SERVICE & HELPERS =============================================
  const service = require('../bongkar-susun-service');
  const db = require('../../../core/config/db');
  const { __mocks } = db;
  const {
    mQuery,
    mInput,
    mBegin,
    mCommit,
    mRollback,
  } = __mocks;
  
  // Helper cari SQL yang mengandung fragment tertentu
  function findQueryContaining(fragment) {
    const call = mQuery.mock.calls.find(([sql]) =>
      typeof sql === 'string' && sql.includes(fragment)
    );
    return call ? call[0] : null;
  }
  
  // ============================================================================
  // TEST getAllBongkarSusun
  // ============================================================================
  describe('BongkarSusunService.getAllBongkarSusun', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('returns data and total without search', async () => {
      // 1st query -> count
      mQuery
        .mockResolvedValueOnce({
          recordset: [{ total: 2 }],
        })
        // 2nd query -> data
        .mockResolvedValueOnce({
          recordset: [
            {
              NoBongkarSusun: 'BG.0000000001',
              Tanggal: new Date('2025-01-15'),
              IdUsername: 1,
              Username: 'testuser',
              Note: 'Test note',
            },
            {
              NoBongkarSusun: 'BG.0000000002',
              Tanggal: new Date('2025-01-16'),
              IdUsername: 1,
              Username: 'testuser',
              Note: null,
            },
          ],
        });
  
      const page = 1;
      const pageSize = 20;
  
      const result = await service.getAllBongkarSusun(page, pageSize, '');
  
      expect(result.data).toHaveLength(2);
      expect(result.total).toBe(2);
      expect(result.data[0].NoBongkarSusun).toBe('BG.0000000001');
  
      // Verify pagination params
      expect(mInput).toHaveBeenCalledWith('offset', expect.anything(), 0);
      expect(mInput).toHaveBeenCalledWith('limit', expect.anything(), 20);
    });
  
    test('binds search parameter when provided', async () => {
      mQuery
        .mockResolvedValueOnce({ recordset: [{ total: 1 }] }) // count
        .mockResolvedValueOnce({
          recordset: [
            {
              NoBongkarSusun: 'BG.0000000001',
              Tanggal: new Date('2025-01-15'),
              IdUsername: 1,
              Username: 'testuser',
              Note: 'Test',
            },
          ],
        }); // data
  
      const search = 'BG.000';
      await service.getAllBongkarSusun(1, 20, search);
  
      const searchCall = mInput.mock.calls.find((c) => c[0] === 'search');
      expect(searchCall).toBeTruthy();
      expect(searchCall[2]).toBe(search);
    });
  
    test('returns empty array when no data found', async () => {
      mQuery
        .mockResolvedValueOnce({ recordset: [{ total: 0 }] }) // count
        .mockResolvedValueOnce({ recordset: [] }); // data (tidak akan dipanggil karena total=0)
  
      const result = await service.getAllBongkarSusun(1, 20, '');
  
      expect(result.data).toEqual([]);
      expect(result.total).toBe(0);
    });
  });
  
  // ============================================================================
  // TEST getByDate
  // ============================================================================
  describe('BongkarSusunService.getByDate', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('returns records for specific date', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBongkarSusun: 'BG.0000000001',
            Tanggal: new Date('2025-01-15'),
            IdUsername: 1,
            Note: 'Test',
          },
        ],
      });
  
      const result = await service.getByDate('2025-01-15');
  
      expect(result).toHaveLength(1);
      expect(result[0].NoBongkarSusun).toBe('BG.0000000001');
  
      const dateCall = mInput.mock.calls.find((c) => c[0] === 'date');
      expect(dateCall).toBeTruthy();
      expect(dateCall[2]).toBe('2025-01-15');
    });
  
    test('returns empty array when no records for date', async () => {
      mQuery.mockResolvedValueOnce({ recordset: [] });
  
      const result = await service.getByDate('2025-12-31');
  
      expect(result).toEqual([]);
    });
  });
  
  // ============================================================================
  // TEST createBongkarSusun
  // ============================================================================
  describe('BongkarSusunService.createBongkarSusun', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
      mBegin.mockReset();
      mCommit.mockReset();
      mRollback.mockReset();
    });
  
    test('throws when tanggal missing', async () => {
      const payload = {
        username: 'testuser',
        // tanggal kosong
      };
  
      await expect(service.createBongkarSusun(payload)).rejects.toThrow(
        /Field wajib: tanggal/i
      );
    });
  
    test('throws when username missing', async () => {
      const payload = {
        tanggal: '2025-01-15',
        // username kosong
      };
  
      await expect(service.createBongkarSusun(payload)).rejects.toThrow(
        /Field wajib: username/i
      );
    });
  
    test('throws when username not found in MstUsername', async () => {
      // 1) Resolve username -> tidak ditemukan
      mQuery.mockResolvedValueOnce({ recordset: [] });
  
      const payload = {
        tanggal: '2025-01-15',
        username: 'unknownuser',
      };
  
      await expect(service.createBongkarSusun(payload)).rejects.toThrow(
        /Username "unknownuser" tidak ditemukan/i
      );
  
      expect(mRollback).toHaveBeenCalled();
    });
  
    test('creates BongkarSusun_h with generated NoBongkarSusun', async () => {
      // 1) Resolve username -> found
      mQuery
        .mockResolvedValueOnce({
          recordset: [{ IdUsername: 1 }],
        })
        // 2) generateNextNoBongkarSusun -> SELECT TOP 1
        .mockResolvedValueOnce({
          recordset: [{ NoBongkarSusun: 'BG.0000000042' }],
        })
        // 3) Check existing -> tidak ada
        .mockResolvedValueOnce({ recordset: [] })
        // 4) INSERT
        .mockResolvedValueOnce({
          recordset: [
            {
              NoBongkarSusun: 'BG.0000000043',
              Tanggal: new Date('2025-01-15'),
              IdUsername: 1,
              Note: 'Test note',
            },
          ],
        });
  
      const payload = {
        tanggal: '2025-01-15',
        username: 'testuser',
        note: 'Test note',
      };
  
      const result = await service.createBongkarSusun(payload);
  
      expect(mBegin).toHaveBeenCalled();
      expect(mCommit).toHaveBeenCalled();
  
      expect(result.header).toEqual(
        expect.objectContaining({
          NoBongkarSusun: 'BG.0000000043',
          IdUsername: 1,
        })
      );
  
      // Verify INSERT query
      const insertSql = findQueryContaining('INSERT INTO dbo.BongkarSusun_h');
      expect(insertSql).toBeTruthy();
    });
  
    test('increments NoBongkarSusun when collision detected', async () => {
      // 1) Resolve username
      mQuery
        .mockResolvedValueOnce({ recordset: [{ IdUsername: 1 }] })
        // 2) First generateNext
        .mockResolvedValueOnce({
          recordset: [{ NoBongkarSusun: 'BG.0000000010' }],
        })
        // 3) Check existing -> FOUND (collision)
        .mockResolvedValueOnce({
          recordset: [{ NoBongkarSusun: 'BG.0000000011' }],
        })
        // 4) Second generateNext
        .mockResolvedValueOnce({
          recordset: [{ NoBongkarSusun: 'BG.0000000011' }],
        })
        // 5) INSERT
        .mockResolvedValueOnce({
          recordset: [
            {
              NoBongkarSusun: 'BG.0000000012',
              Tanggal: new Date('2025-01-15'),
              IdUsername: 1,
              Note: null,
            },
          ],
        });
  
      const payload = {
        tanggal: '2025-01-15',
        username: 'testuser',
      };
  
      const result = await service.createBongkarSusun(payload);
  
      expect(result.header.NoBongkarSusun).toBe('BG.0000000012');
      expect(mCommit).toHaveBeenCalled();
    });
  });
  
  // ============================================================================
  // TEST updateBongkarSusun
  // ============================================================================
  describe('BongkarSusunService.updateBongkarSusun', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('throws when noBongkarSusun missing', async () => {
      await expect(
        service.updateBongkarSusun(null, { tanggal: '2025-01-20' })
      ).rejects.toThrow(/noBongkarSusun wajib diisi/i);
    });
  
    test('throws when no fields to update', async () => {
      await expect(
        service.updateBongkarSusun('BG.0000000001', {})
      ).rejects.toThrow(/Tidak ada field yang diupdate/i);
    });
  
    test('throws when BongkarSusun not found', async () => {
      mQuery.mockResolvedValueOnce({ recordset: [] }); // UPDATE returns no rows
  
      await expect(
        service.updateBongkarSusun('BG.0000009999', { note: 'Updated' })
      ).rejects.toThrow(/BongkarSusun tidak ditemukan/i);
    });
  
    test('updates tanggal field', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBongkarSusun: 'BG.0000000001',
            Tanggal: new Date('2025-01-20'),
            IdUsername: 1,
            Note: 'Old note',
          },
        ],
      });
  
      const payload = {
        tanggal: '2025-01-20',
      };
  
      const result = await service.updateBongkarSusun('BG.0000000001', payload);
  
      expect(result.header).toEqual(
        expect.objectContaining({
          NoBongkarSusun: 'BG.0000000001',
          Tanggal: new Date('2025-01-20'),
        })
      );
  
      const updateSql = findQueryContaining('UPDATE dbo.BongkarSusun_h');
      expect(updateSql).toBeTruthy();
      expect(updateSql).toContain('Tanggal = @Tanggal');
    });
  
    test('updates multiple fields', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBongkarSusun: 'BG.0000000001',
            Tanggal: new Date('2025-01-20'),
            IdUsername: 2,
            Note: 'Updated note',
          },
        ],
      });
  
      const payload = {
        tanggal: '2025-01-20',
        idUsername: 2,
        note: 'Updated note',
      };
  
      const result = await service.updateBongkarSusun('BG.0000000001', payload);
  
      expect(result.header.IdUsername).toBe(2);
      expect(result.header.Note).toBe('Updated note');
    });
  
    test('sets note to null when empty string', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBongkarSusun: 'BG.0000000001',
            Tanggal: new Date('2025-01-15'),
            IdUsername: 1,
            Note: null,
          },
        ],
      });
  
      const payload = {
        note: '',
      };
  
      const result = await service.updateBongkarSusun('BG.0000000001', payload);
  
      expect(result.header.Note).toBeNull();
    });
  });
  
  // ============================================================================
  // TEST deleteBongkarSusun
  // ============================================================================
  describe('BongkarSusunService.deleteBongkarSusun', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('throws when noBongkarSusun missing', async () => {
      await expect(service.deleteBongkarSusun(null)).rejects.toThrow(
        /noBongkarSusun wajib diisi/i
      );
    });
  
    test('throws when BongkarSusun not found', async () => {
      mQuery.mockResolvedValueOnce({ rowsAffected: [0] }); // No rows deleted
  
      await expect(
        service.deleteBongkarSusun('BG.0000009999')
      ).rejects.toThrow(/BongkarSusun tidak ditemukan atau sudah dihapus/i);
    });
  
    test('successfully deletes BongkarSusun', async () => {
      mQuery.mockResolvedValueOnce({ rowsAffected: [1] }); // 1 row deleted
  
      const result = await service.deleteBongkarSusun('BG.0000000001');
  
      expect(result).toBe(true);
  
      const deleteSql = findQueryContaining('DELETE FROM dbo.BongkarSusun_h');
      expect(deleteSql).toBeTruthy();
    });
  });
  
  // ============================================================================
  // TEST validateLabelBongkarSusun
  // ============================================================================
  describe('BongkarSusunService.validateLabelBongkarSusun', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('throws when label code is empty', async () => {
      await expect(service.validateLabelBongkarSusun('')).rejects.toThrow(
        /Label code is required/i
      );
    });
  
    test('validates BB. prefix (FurnitureWIP)', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoFurnitureWip: 'BB.0000044512',
            Pcs: 3,
            Berat: 5,
            IdJenis: 179,
            NamaJenis: 'CUP PANEL ORANGE',
            IsPartial: 0,
          },
        ],
      });
  
      const result = await service.validateLabelBongkarSusun('BB.0000044512');
  
      expect(result.found).toBe(true);
      expect(result.prefix).toBe('BB.');
      expect(result.tableName).toBe('FurnitureWIP');
      expect(result.data).toHaveLength(1);
      expect(result.data[0].noFurnitureWip).toBe('BB.0000044512');
      expect(result.data[0].isPartial).toBe(0); // ✅ Non-partial only
    });
  
    test('validates D. prefix (Broker) - filters partial', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBroker: 'D.0000123456',
            NoSak: 1,
            Berat: 10.5,
            IdJenis: 5,
            NamaJenis: 'HDPE',
            IsPartial: 0,
          },
        ],
      });
  
      const result = await service.validateLabelBongkarSusun('D.0000123456');
  
      expect(result.found).toBe(true);
      expect(result.prefix).toBe('D.');
      expect(result.tableName).toBe('Broker_d');
  
      // Verify query filters partial
      const sql = findQueryContaining('ISNULL(ps.BeratPartial, 0) = 0');
      expect(sql).toBeTruthy();
    });
  
    test('validates A. prefix (BahanBaku) with pallet format', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBahanBaku: 'A.0000000001',
            NoPallet: 1,
            NoSak: 1,
            Berat: 25.0,
            IdJenis: 3,
            NamaJenis: 'PP',
            IsPartial: 0,
          },
        ],
      });
  
      const result = await service.validateLabelBongkarSusun('A.0000000001-1');
  
      expect(result.found).toBe(true);
      expect(result.prefix).toBe('A.');
      expect(result.tableName).toBe('BahanBaku_d');
    });
  
    test('throws error for invalid A. format (missing pallet)', async () => {
      await expect(
        service.validateLabelBongkarSusun('A.0000000001')
      ).rejects.toThrow(/Invalid format for A. prefix/i);
    });
  
    test('validates BA. prefix (BarangJadi) - filters partial', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          {
            NoBj: 'BA.0000001305',
            Pcs: 10,
            Berat: 15.5,
            IdJenis: 50,
            NamaJenis: 'CHAIR BLUE',
            IsPartial: 0,
          },
        ],
      });
  
      const result = await service.validateLabelBongkarSusun('BA.0000001305');
  
      expect(result.found).toBe(true);
      expect(result.prefix).toBe('BA.');
      expect(result.tableName).toBe('BarangJadi');
  
      // Verify query filters partial
      const sql = findQueryContaining('ISNULL(pa.PcsPartial, 0) = 0');
      expect(sql).toBeTruthy();
    });
  
    test('returns not found for invalid label', async () => {
      mQuery.mockResolvedValueOnce({ recordset: [] });
  
      const result = await service.validateLabelBongkarSusun('BB.9999999999');
  
      expect(result.found).toBe(false);
      expect(result.data).toEqual([]);
    });
  
    test('throws for invalid prefix', async () => {
      await expect(
        service.validateLabelBongkarSusun('XX.0000000001')
      ).rejects.toThrow(/Invalid prefix/i);
    });
  });
  
  // ============================================================================
  // TEST fetchInputs
  // ============================================================================
  describe('BongkarSusunService.fetchInputs', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('returns structured inputs with all categories', async () => {
      // Mock multi-recordset response
      mQuery.mockResolvedValueOnce({
        recordsets: [
          // [0] Main rows
          [
            {
              Src: 'bb',
              NoBongkarSusun: 'BG.0000000001',
              Ref1: 'A.0000000001',
              Ref2: 1,
              Ref3: 1,
              Pcs: null,
              Berat: 25.0,
              BeratAct: null,
              IsPartial: 0,
              IdJenis: 3,
              NamaJenis: 'PP',
            },
            {
              Src: 'furniture_wip',
              NoBongkarSusun: 'BG.0000000001',
              Ref1: 'BB.0000044512',
              Ref2: null,
              Ref3: null,
              Pcs: 3,
              Berat: 5.0,
              BeratAct: null,
              IsPartial: 0,
              IdJenis: 179,
              NamaJenis: 'CUP PANEL',
            },
          ],
          // [1] BB Partial
          [],
          // [2] Gilingan Partial
          [],
          // [3] Mixer Partial
          [],
          // [4] Broker Partial
          [],
          // [5] Barang Jadi Partial
          [],
          // [6] Furniture WIP Partial
          [],
        ],
      });
  
      const result = await service.fetchInputs('BG.0000000001');
  
      expect(result.bb).toHaveLength(1);
      expect(result.bb[0]).toEqual(
        expect.objectContaining({
          noBahanBaku: 'A.0000000001',
          noPallet: 1,
          noSak: 1,
          berat: 25.0,
          namaJenis: 'PP',
        })
      );
  
      expect(result.furnitureWip).toHaveLength(1);
      expect(result.furnitureWip[0]).toEqual(
        expect.objectContaining({
          noFurnitureWIP: 'BB.0000044512',
          pcs: 3,
          berat: 5.0,
          namaJenis: 'CUP PANEL',
        })
      );
  
      expect(result.summary).toEqual(
        expect.objectContaining({
          bb: 1,
          furnitureWip: 1,
          washing: 0,
          broker: 0,
        })
      );
    });
  
    test('includes partial items in results', async () => {
      mQuery.mockResolvedValueOnce({
        recordsets: [
          // [0] Main
          [],
          // [1] BB Partial
          [
            {
              NoBBPartial: 'P.00001 (1)',
              NoBahanBaku: 'A.0000000001',
              NoPallet: 1,
              NoSak: 1,
              Berat: 5.0,
              IdJenis: 3,
              NamaJenis: 'PP',
            },
          ],
          // [2-6] Other partials
          [],
          [],
          [],
          [],
          [],
        ],
      });
  
      const result = await service.fetchInputs('BG.0000000001');
  
      expect(result.bb).toHaveLength(1);
      expect(result.bb[0]).toEqual(
        expect.objectContaining({
          noBBPartial: 'P.00001 (1)',
          isPartial: true,
          berat: 5.0,
        })
      );
    });
  });
  
  // ============================================================================
  // TEST upsertInputs
  // ============================================================================
  describe('BongkarSusunService.upsertInputs', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
      mBegin.mockReset();
      mCommit.mockReset();
      mRollback.mockReset();
    });
  
    test('inserts inputs and updates DateUsage', async () => {
      // Mock stored procedure response
      mQuery.mockResolvedValueOnce({
        recordset: [
          { Section: 'broker', Inserted: 2, Skipped: 0, Invalid: 0 },
          { Section: 'bb', Inserted: 1, Skipped: 0, Invalid: 0 },
          { Section: 'furnitureWip', Inserted: 1, Skipped: 0, Invalid: 0 },
        ],
      });
  
      const payload = {
        broker: [
          { noBroker: 'D.0000123456', noSak: 1 },
          { noBroker: 'D.0000123456', noSak: 2 },
        ],
        bb: [{ noBahanBaku: 'A.0000000001', noPallet: 1, noSak: 1 }],
        furnitureWip: [{ noFurnitureWip: 'BB.0000044512' }],
      };
  
      const result = await service.upsertInputs('BG.0000000001', payload);
  
      expect(mBegin).toHaveBeenCalled();
      expect(mCommit).toHaveBeenCalled();
  
      expect(result.success).toBe(true);
      expect(result.data.summary).toEqual({
        totalInserted: 4,
        totalSkipped: 0,
        totalInvalid: 0,
      });
  
      expect(result.data.details).toEqual(
        expect.objectContaining({
          broker: { inserted: 2, skipped: 0, invalid: 0 },
          bb: { inserted: 1, skipped: 0, invalid: 0 },
          furnitureWip: { inserted: 1, skipped: 0, invalid: 0 },
        })
      );
    });
  
    test('reports skipped duplicates', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          { Section: 'broker', Inserted: 1, Skipped: 1, Invalid: 0 },
        ],
      });
  
      const payload = {
        broker: [
          { noBroker: 'D.0000123456', noSak: 1 },
          { noBroker: 'D.0000123456', noSak: 1 }, // duplicate
        ],
      };
  
      const result = await service.upsertInputs('BG.0000000001', payload);
  
      expect(result.success).toBe(true);
      expect(result.hasWarnings).toBe(true);
      expect(result.data.summary.totalSkipped).toBe(1);
    });
  
    test('reports invalid inputs (not found or already used)', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          { Section: 'broker', Inserted: 0, Skipped: 0, Invalid: 1 },
        ],
      });
  
      const payload = {
        broker: [{ noBroker: 'D.9999999999', noSak: 1 }], // not found
      };
  
      const result = await service.upsertInputs('BG.0000000001', payload);
  
      expect(result.success).toBe(false);
      expect(result.data.summary.totalInvalid).toBe(1);
    });
  
    test('rollback on error', async () => {
      mQuery.mockRejectedValueOnce(new Error('Database error'));
  
      const payload = {
        broker: [{ noBroker: 'D.0000123456', noSak: 1 }],
      };
  
      await expect(
        service.upsertInputs('BG.0000000001', payload)
      ).rejects.toThrow('Database error');
  
      expect(mRollback).toHaveBeenCalled();
    });
  });
  
  // ============================================================================
  // TEST deleteInputs
  // ============================================================================
  describe('BongkarSusunService.deleteInputs', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
      mBegin.mockReset();
      mCommit.mockReset();
      mRollback.mockReset();
    });
  
    test('deletes inputs and resets DateUsage', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [
          { Section: 'broker', Deleted: 2, NotFound: 0 },
          { Section: 'bb', Deleted: 1, NotFound: 0 },
        ],
      });
  
      const payload = {
        broker: [
          { noBroker: 'D.0000123456', noSak: 1 },
          { noBroker: 'D.0000123456', noSak: 2 },
        ],
        bb: [{ noBahanBaku: 'A.0000000001', noPallet: 1, noSak: 1 }],
      };
  
      const result = await service.deleteInputs('BG.0000000001', payload);
  
      expect(mBegin).toHaveBeenCalled();
      expect(mCommit).toHaveBeenCalled();
  
      expect(result.success).toBe(true);
      expect(result.data.summary).toEqual({
        totalDeleted: 3,
        totalNotFound: 0,
      });
    });
  
    test('reports not found items', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [{ Section: 'broker', Deleted: 1, NotFound: 1 }],
      });
  
      const payload = {
        broker: [
          { noBroker: 'D.0000123456', noSak: 1 },
          { noBroker: 'D.9999999999', noSak: 1 }, // not found
        ],
      };
  
      const result = await service.deleteInputs('BG.0000000001', payload);
  
      expect(result.success).toBe(true);
      expect(result.hasWarnings).toBe(true);
      expect(result.data.summary.totalNotFound).toBe(1);
    });
  
    test('returns success=false when nothing deleted', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [{ Section: 'broker', Deleted: 0, NotFound: 1 }],
      });
  
      const payload = {
        broker: [{ noBroker: 'D.9999999999', noSak: 1 }],
      };
  
      const result = await service.deleteInputs('BG.0000000001', payload);
  
      expect(result.success).toBe(false);
      expect(result.data.summary.totalDeleted).toBe(0);
    });
  });
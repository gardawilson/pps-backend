// src/modules/label/furniture-wip/__tests__/furniture-wip-service.test.js

// ====== MOCK DB CONFIG (core/config/db) ======================================
jest.mock('../../../../core/config/db', () => {
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
  const service = require('../furniture-wip-service');
  const { _test } = service;
  const { padLeft, generateNextNoFurnitureWip } = _test;
  
  // Ambil mocks
  const db = require('../../../../core/config/db');
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
  // TEST getAll
  // ============================================================================
  describe('FurnitureWipService.getAll', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('returns data and total without search', async () => {
      // 1st query -> data
      mQuery
        .mockResolvedValueOnce({
          recordset: [
            {
              NoFurnitureWIP: 'BB.0000000001',
              Pcs: 10,
              Berat: 5,
            },
          ],
        })
        // 2nd query -> count
        .mockResolvedValueOnce({
          recordset: [{ total: 1 }],
        });
  
      const page = 2;
      const limit = 10;
  
      const result = await service.getAll({ page, limit, search: '' });
  
      expect(result.data).toHaveLength(1);
      expect(result.total).toBe(1);
  
      // offset = (page-1)*limit = 10
      expect(mInput).toHaveBeenCalledWith(
        'offset',
        expect.anything(),
        (page - 1) * limit
      );
      expect(mInput).toHaveBeenCalledWith('limit', expect.anything(), limit);
  
      // Tidak ada input 'search'
      const searchCall = mInput.mock.calls.find((c) => c[0] === 'search');
      expect(searchCall).toBeUndefined();
    });
  
    test('binds search parameter when provided', async () => {
      mQuery
        .mockResolvedValueOnce({ recordset: [] }) // data
        .mockResolvedValueOnce({ recordset: [{ total: 0 }] }); // count
  
      const search = 'ABC';
      await service.getAll({ page: 1, limit: 20, search });
  
      const searchCall = mInput.mock.calls.find((c) => c[0] === 'search');
      expect(searchCall).toBeTruthy();
      expect(searchCall[2]).toBe(`%${search}%`);
    });
  });
  
  // ============================================================================
  // TEST padLeft
  // ============================================================================
  describe('padLeft', () => {
    test('pads number with zeros when shorter than width', () => {
      expect(padLeft(5, 3)).toBe('005');
      expect(padLeft(42, 5)).toBe('00042');
    });
  
    test('returns string unchanged when length >= width', () => {
      expect(padLeft(123, 3)).toBe('123');
      expect(padLeft(12345, 3)).toBe('12345');
    });
  
    test('works when num is string-like', () => {
      expect(padLeft('7', 2)).toBe('07');
      expect(padLeft('999', 2)).toBe('999');
    });
  });
  
  // ============================================================================
  // TEST generateNextNoFurnitureWip
  // ============================================================================
  describe('generateNextNoFurnitureWip', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
    });
  
    test('returns first code when no existing record', async () => {
      mQuery.mockResolvedValueOnce({ recordset: [] }); // SELECT TOP 1
  
      const tx = {}; // fake transaction
      const code = await generateNextNoFurnitureWip(tx, {
        prefix: 'BB.',
        width: 4,
      });
  
      expect(code).toBe('BB.0001');
      expect(mInput).toHaveBeenCalledWith(
        'prefix',
        expect.anything(),
        'BB.'
      );
    });
  
    test('increments last numeric part from existing NoFurnitureWIP', async () => {
      mQuery.mockResolvedValueOnce({
        recordset: [{ NoFurnitureWIP: 'BB.0000000042' }],
      });
  
      const tx = {};
      const code = await generateNextNoFurnitureWip(tx, {
        prefix: 'BB.',
        width: 10,
      });
  
      expect(code).toBe('BB.0000000043');
    });
  });
  
  // ============================================================================
  // TEST createFurnitureWip
  // ============================================================================
  describe('createFurnitureWip', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
      mBegin.mockReset();
      mCommit.mockReset();
      mRollback.mockReset();
    });
  
    function setupCreateMocks() {
      mQuery
        .mockResolvedValueOnce({ recordset: [] }) // generateNext -> SELECT TOP 1
        .mockResolvedValueOnce({ recordset: [] }) // cek existing
        .mockResolvedValueOnce({ recordset: [] }) // insert header
        .mockResolvedValueOnce({ recordset: [] }); // insert mapping
    }
  
    test('throws when IdFurnitureWIP missing', async () => {
      const payload = {
        header: { Pcs: 10, Berat: 5 },
        outputCode: 'BH.0000000001',
      };
  
      await expect(service.createFurnitureWip(payload)).rejects.toThrow(
        /IdFurnitureWIP is required/i
      );
    });
  
    test('throws when outputCode missing', async () => {
      const payload = {
        header: { IdFurnitureWIP: 1, Pcs: 10, Berat: 5 },
        // outputCode kosong
      };
  
      await expect(service.createFurnitureWip(payload)).rejects.toThrow(
        /outputCode is required/i
      );
    });
  
    test('throws when prefix invalid', async () => {
      const payload = {
        header: { IdFurnitureWIP: 1, Pcs: 10, Berat: 5 },
        outputCode: 'XX.0000000001',
      };
  
      await expect(service.createFurnitureWip(payload)).rejects.toThrow(
        /outputCode prefix not recognized/i
      );
    });
  
    function testPrefixMapping(prefix, expectedType, expectedTable) {
      test(`${prefix} -> mapping ke ${expectedTable}`, async () => {
        setupCreateMocks();
  
        const payload = {
          header: { IdFurnitureWIP: 1, Pcs: 10, Berat: 5 },
          outputCode: `${prefix}0000000001`,
        };
  
        const result = await service.createFurnitureWip(payload);
  
        expect(mBegin).toHaveBeenCalled();
        expect(mCommit).toHaveBeenCalled();
  
        const sql = findQueryContaining(expectedTable);
        expect(sql).toBeTruthy();
  
        expect(result.output).toEqual(
          expect.objectContaining({
            code: `${prefix}0000000001`,
            type: expectedType,
            mappingTable: expectedTable,
          })
        );
      });
    }
  
    testPrefixMapping('BH.', 'HOTSTAMPING', 'HotStampingOutputLabelFWIP');
    testPrefixMapping('BI.', 'PASANG_KUNCI', 'PasangKunciOutputLabelFWIP');
    testPrefixMapping('BG.', 'BONGKAR_SUSUN', 'BongkarSusunOutputFurnitureWIP');
    testPrefixMapping('L.', 'RETUR', 'BJReturFurnitureWIP_d');
    testPrefixMapping('BJ.', 'SPANNER', 'SpannerOutputLabelFWIP');
    testPrefixMapping('S.', 'INJECT', 'InjectProduksiOutputFurnitureWIP');
  });
  
  // ============================================================================
  // TEST updateFurnitureWip
  // ============================================================================
  describe('updateFurnitureWip', () => {
    const existingRow = {
      NoFurnitureWIP: 'BB.0000000001',
      DateCreate: new Date('2025-01-01'),
      Pcs: 10,
      IdFurnitureWIP: 1,
      Berat: 5,
      IsPartial: 0,
      DateUsage: null,
      IdWarna: 1,
      CreateBy: 'oldUser',
      DateTimeCreate: new Date('2025-01-01T10:00:00'),
      Blok: 'A',
      IdLokasi: 'A1',
    };
  
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
      mBegin.mockReset();
      mCommit.mockReset();
      mRollback.mockReset();
    });
  
    test('throws 404 when FurnitureWIP not found', async () => {
      mQuery.mockResolvedValueOnce({ recordset: [] }); // SELECT existing
  
      await expect(
        service.updateFurnitureWip('BB.0000000099', {
          header: { Pcs: 20 },
        })
      ).rejects.toThrow(/Furniture WIP not found/i);
    });
  
    test('updates header fields without touching mapping when no outputCode field', async () => {
      // 1) SELECT existing
      // 2) UPDATE header
      mQuery
        .mockResolvedValueOnce({ recordset: [existingRow] })
        .mockResolvedValueOnce({ recordset: [] });
  
      const payload = {
        header: {
          Pcs: 20,
          Berat: 8,
          Blok: 'B',
          IdLokasi: 'B2',
          CreateBy: 'newUser',
        },
        // tidak ada outputCode di payload
      };
  
      const result = await service.updateFurnitureWip(
        existingRow.NoFurnitureWIP,
        payload
      );
  
      expect(mBegin).toHaveBeenCalled();
      expect(mCommit).toHaveBeenCalled();
  
      // Pastikan UPDATE ke FurnitureWIP terjadi
      const sql = findQueryContaining('UPDATE [dbo].[FurnitureWIP]');
      expect(sql).toBeTruthy();
  
      // Output header reflect merged value
      expect(result.header).toEqual(
        expect.objectContaining({
          NoFurnitureWIP: existingRow.NoFurnitureWIP,
          Pcs: 20,
          Berat: 8,
          Blok: 'B',
          IdLokasi: 'B2',
          CreateBy: 'newUser',
        })
      );
  
      // Karena tidak ada outputCode field, block "output" tidak ada
      expect(result.output).toBeUndefined();
    });
  
    test('clears all mappings when outputCode is empty string', async () => {
      // urutan kira-kira:
      // 1) SELECT existing
      // 2) UPDATE header
      // 3) deleteAllMappings (multi-DELETE)
      mQuery
        .mockResolvedValueOnce({ recordset: [existingRow] }) // SELECT
        .mockResolvedValueOnce({ recordset: [] }) // UPDATE
        .mockResolvedValueOnce({ recordset: [] }); // deleteAllMappings
  
      const payload = {
        header: { Pcs: 30 },
        outputCode: '', // kosong -> hapus mapping
      };
  
      const result = await service.updateFurnitureWip(
        existingRow.NoFurnitureWIP,
        payload
      );
  
      expect(mCommit).toHaveBeenCalled();
  
      const deleteSql = findQueryContaining(
        'DELETE FROM [dbo].[HotStampingOutputLabelFWIP]'
      );
      expect(deleteSql).toBeTruthy();
  
      // output block tetap ada, tapi code null & type/mappingTable null
      expect(result.output).toEqual(
        expect.objectContaining({
          code: null,
          type: null,
          mappingTable: null,
        })
      );
    });
  
    test('updates mapping when outputCode has valid prefix', async () => {
      // 1) SELECT existing
      // 2) UPDATE header
      // 3) deleteAllMappings
      // 4) INSERT mapping baru
      mQuery
        .mockResolvedValueOnce({ recordset: [existingRow] })
        .mockResolvedValueOnce({ recordset: [] })
        .mockResolvedValueOnce({ recordset: [] })
        .mockResolvedValueOnce({ recordset: [] });
  
      const payload = {
        header: { Pcs: 50 },
        outputCode: 'BH.0000000123',
      };
  
      const result = await service.updateFurnitureWip(
        existingRow.NoFurnitureWIP,
        payload
      );
  
      expect(mCommit).toHaveBeenCalled();
  
      const deleteSql = findQueryContaining(
        'DELETE FROM [dbo].[HotStampingOutputLabelFWIP]'
      );
      expect(deleteSql).toBeTruthy();
  
      const insertSql = findQueryContaining(
        'INSERT INTO [dbo].[HotStampingOutputLabelFWIP]'
      );
      expect(insertSql).toBeTruthy();
  
      expect(result.output).toEqual(
        expect.objectContaining({
          code: 'BH.0000000123',
          type: 'HOTSTAMPING',
          mappingTable: 'HotStampingOutputLabelFWIP',
        })
      );
    });
  
    test('throws error when outputCode prefix invalid on update', async () => {
      mQuery.mockResolvedValueOnce({ recordset: [existingRow] }); // SELECT
  
      const payload = {
        header: {},
        outputCode: 'XX.0001',
      };
  
      await expect(
        service.updateFurnitureWip(existingRow.NoFurnitureWIP, payload)
      ).rejects.toThrow(/outputCode prefix not recognized/i);
    });
  });
  
  // ============================================================================
  // TEST deleteFurnitureWip
  // ============================================================================
  describe('deleteFurnitureWip', () => {
    beforeEach(() => {
      mQuery.mockReset();
      mInput.mockReset();
      mBegin.mockReset();
      mCommit.mockReset();
      mRollback.mockReset();
    });
  
    test('throws 404 when NoFurnitureWIP not found', async () => {
      // 1) cek header
      mQuery.mockResolvedValueOnce({ recordset: [] });
  
      await expect(
        service.deleteFurnitureWip('BB.0000009999')
      ).rejects.toThrow(/Furniture WIP not found/i);
  
      expect(mRollback).toHaveBeenCalled();
    });
  
    test('deletes mappings, partials, and header when found', async () => {
      // urutan kira-kira:
      // 1) SELECT header ada
      // 2) deleteAllMappings (multi delete)
      // 3) DELETE partial
      // 4) DELETE FurnitureWIP
      mQuery
        .mockResolvedValueOnce({
          recordset: [{ NoFurnitureWIP: 'BB.0000000001' }],
        })
        .mockResolvedValueOnce({ recordset: [] })
        .mockResolvedValueOnce({ recordset: [] })
        .mockResolvedValueOnce({ recordset: [] });
  
      const result = await service.deleteFurnitureWip('BB.0000000001');
  
      expect(mBegin).toHaveBeenCalled();
      expect(mCommit).toHaveBeenCalled();
  
      const deleteMappingSql = findQueryContaining(
        'DELETE FROM [dbo].[HotStampingOutputLabelFWIP]'
      );
      expect(deleteMappingSql).toBeTruthy();
  
      const deletePartialSql = findQueryContaining(
        'DELETE FROM [dbo].[FurnitureWIPPartial]'
      );
      expect(deletePartialSql).toBeTruthy();
  
      const deleteHeaderSql = findQueryContaining(
        'DELETE FROM [dbo].[FurnitureWIP]'
      );
      expect(deleteHeaderSql).toBeTruthy();
  
      expect(result).toEqual({
        noFurnitureWip: 'BB.0000000001',
        deleted: true,
      });
    });
  });
  
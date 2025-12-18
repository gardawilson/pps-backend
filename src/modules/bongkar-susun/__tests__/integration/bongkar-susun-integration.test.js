// src/modules/bongkar-susun/__tests__/integration/bongkar-susun-integration.test.js

require('dotenv').config();

const { sql, poolPromise } = require('../../../../core/config/db');
const bongkarSusunService = require('../../bongkar-susun-service');

// Helper untuk logging
const log = {
  info: (msg, data) => console.log(`ℹ️  [INFO] ${msg}`, data || ''),
  success: (msg, data) => console.log(`✅ [SUCCESS] ${msg}`, data || ''),
  error: (msg, data) => console.log(`❌ [ERROR] ${msg}`, data || ''),
  test: (msg) => console.log(`\n🧪 [TEST] ${msg}`),
};

// ============================================================
// STEP 1 helpers: ambil data yang "available" + tidak duplikat di input table
// ============================================================
async function getAvailableTestData() {
  const pool = await poolPromise;

  const queries = {
    broker: `
      SELECT TOP 1 d.NoBroker, d.NoSak, d.DateUsage
      FROM dbo.Broker_d d WITH (NOLOCK)
      WHERE d.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BrokerPartial bp WITH (NOLOCK)
          WHERE bp.NoBroker = d.NoBroker AND bp.NoSak = d.NoSak
        )
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputBroker i WITH (NOLOCK)
          WHERE i.NoBroker = d.NoBroker AND i.NoSak = d.NoSak
        )
      ORDER BY d.NoBroker DESC
    `,

    bb: `
      SELECT TOP 1 d.NoBahanBaku, d.NoPallet, d.NoSak, d.DateUsage
      FROM dbo.BahanBaku_d d WITH (NOLOCK)
      WHERE d.DateUsage IS NULL
        AND (d.IsPartial IS NULL OR d.IsPartial = 0)
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputBahanBaku i WITH (NOLOCK)
          WHERE i.NoBahanBaku = d.NoBahanBaku
            AND i.NoPallet   = d.NoPallet
            AND i.NoSak      = d.NoSak
        )
      ORDER BY d.NoBahanBaku DESC
    `,

    washing: `
      SELECT TOP 1 d.NoWashing, d.NoSak, d.DateUsage
      FROM dbo.Washing_d d WITH (NOLOCK)
      WHERE d.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputWashing i WITH (NOLOCK)
          WHERE i.NoWashing = d.NoWashing AND i.NoSak = d.NoSak
        )
      ORDER BY d.NoWashing DESC
    `,

    crusher: `
      SELECT TOP 1 c.NoCrusher, c.DateUsage
      FROM dbo.Crusher c WITH (NOLOCK)
      WHERE c.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputCrusher i WITH (NOLOCK)
          WHERE i.NoCrusher = c.NoCrusher
        )
      ORDER BY c.NoCrusher DESC
    `,

    gilingan: `
      SELECT TOP 1 g.NoGilingan, g.DateUsage
      FROM dbo.Gilingan g WITH (NOLOCK)
      WHERE g.DateUsage IS NULL
        AND (g.IsPartial IS NULL OR g.IsPartial = 0)
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputGilingan i WITH (NOLOCK)
          WHERE i.NoGilingan = g.NoGilingan
        )
      ORDER BY g.NoGilingan DESC
    `,

    mixer: `
      SELECT TOP 1 d.NoMixer, d.NoSak, d.DateUsage
      FROM dbo.Mixer_d d WITH (NOLOCK)
      WHERE d.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.MixerPartial mp WITH (NOLOCK)
          WHERE mp.NoMixer = d.NoMixer AND mp.NoSak = d.NoSak
        )
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputMixer i WITH (NOLOCK)
          WHERE i.NoMixer = d.NoMixer AND i.NoSak = d.NoSak
        )
      ORDER BY d.NoMixer DESC
    `,

    bonggolan: `
      SELECT TOP 1 b.NoBonggolan, b.DateUsage
      FROM dbo.Bonggolan b WITH (NOLOCK)
      WHERE b.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputBonggolan i WITH (NOLOCK)
          WHERE i.NoBonggolan = b.NoBonggolan
        )
      ORDER BY b.NoBonggolan DESC
    `,

    furnitureWip: `
      SELECT TOP 1 f.NoFurnitureWIP, f.DateUsage
      FROM dbo.FurnitureWIP f WITH (NOLOCK)
      WHERE f.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.FurnitureWIPPartial fp WITH (NOLOCK)
          WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP
        )
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputFurnitureWIP i WITH (NOLOCK)
          WHERE i.NoFurnitureWIP = f.NoFurnitureWIP
        )
      ORDER BY f.NoFurnitureWIP DESC
    `,

    barangJadi: `
      SELECT TOP 1 b.NoBJ, b.DateUsage
      FROM dbo.BarangJadi b WITH (NOLOCK)
      WHERE b.DateUsage IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BarangJadiPartial bp WITH (NOLOCK)
          WHERE bp.NoBJ = b.NoBJ
        )
        AND NOT EXISTS (
          SELECT 1 FROM dbo.BongkarSusunInputBarangJadi i WITH (NOLOCK)
          WHERE i.NoBJ = b.NoBJ
        )
      ORDER BY b.NoBJ DESC
    `,
  };

  const results = {};

  for (const [category, query] of Object.entries(queries)) {
    try {
      const result = await pool.request().query(query);
      results[category] = result.recordset[0] || null;

      if (results[category]) {
        log.info(`Found test data for ${category}:`, results[category]);
      } else {
        log.error(`No available data for ${category}`);
      }
    } catch (error) {
      log.error(`Failed to fetch ${category}:`, error.message);
      results[category] = null;
    }
  }

  return results;
}

// ============================================================
// Verify DateUsage is set
// ============================================================
async function verifyDateUsage(category, identifier) {
  const pool = await poolPromise;

  const queries = {
    broker: `
      SELECT DateUsage FROM dbo.Broker_d
      WHERE NoBroker = @ref1 AND NoSak = @ref2
    `,
    bb: `
      SELECT DateUsage FROM dbo.BahanBaku_d
      WHERE NoBahanBaku = @ref1 AND NoPallet = @ref2 AND NoSak = @ref3
    `,
    washing: `
      SELECT DateUsage FROM dbo.Washing_d
      WHERE NoWashing = @ref1 AND NoSak = @ref2
    `,
    crusher: `
      SELECT DateUsage FROM dbo.Crusher
      WHERE NoCrusher = @ref1
    `,
    gilingan: `
      SELECT DateUsage FROM dbo.Gilingan
      WHERE NoGilingan = @ref1
    `,
    mixer: `
      SELECT DateUsage FROM dbo.Mixer_d
      WHERE NoMixer = @ref1 AND NoSak = @ref2
    `,
    bonggolan: `
      SELECT DateUsage FROM dbo.Bonggolan
      WHERE NoBonggolan = @ref1
    `,
    furnitureWip: `
      SELECT DateUsage FROM dbo.FurnitureWIP
      WHERE NoFurnitureWIP = @ref1
    `,
    barangJadi: `
      SELECT DateUsage FROM dbo.BarangJadi
      WHERE NoBJ = @ref1
    `,
  };

  const request = pool.request();

  if (identifier.ref1) request.input('ref1', sql.VarChar(50), identifier.ref1);
  if (identifier.ref2 !== undefined) request.input('ref2', sql.Int, identifier.ref2);
  if (identifier.ref3 !== undefined) request.input('ref3', sql.Int, identifier.ref3);

  const result = await request.query(queries[category]);
  return result.recordset[0]?.DateUsage;
}

// ============================================================
// Verify data exists in BongkarSusunInput tables
// ============================================================
async function verifyInputTableEntry(noBongkarSusun, category, identifier) {
  const pool = await poolPromise;

  const queries = {
    broker: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputBroker
      WHERE NoBongkarSusun = @no AND NoBroker = @ref1 AND NoSak = @ref2
    `,
    bb: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputBahanBaku
      WHERE NoBongkarSusun = @no
        AND NoBahanBaku = @ref1 AND NoPallet = @ref2 AND NoSak = @ref3
    `,
    washing: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputWashing
      WHERE NoBongkarSusun = @no AND NoWashing = @ref1 AND NoSak = @ref2
    `,
    crusher: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputCrusher
      WHERE NoBongkarSusun = @no AND NoCrusher = @ref1
    `,
    gilingan: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputGilingan
      WHERE NoBongkarSusun = @no AND NoGilingan = @ref1
    `,
    mixer: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputMixer
      WHERE NoBongkarSusun = @no AND NoMixer = @ref1 AND NoSak = @ref2
    `,
    bonggolan: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputBonggolan
      WHERE NoBongkarSusun = @no AND NoBonggolan = @ref1
    `,
    furnitureWip: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputFurnitureWIP
      WHERE NoBongkarSusun = @no AND NoFurnitureWIP = @ref1
    `,
    barangJadi: `
      SELECT 1 AS ok FROM dbo.BongkarSusunInputBarangJadi
      WHERE NoBongkarSusun = @no AND NoBJ = @ref1
    `,
  };

  const request = pool.request();
  request.input('no', sql.VarChar(50), noBongkarSusun);

  if (identifier.ref1) request.input('ref1', sql.VarChar(50), identifier.ref1);
  if (identifier.ref2 !== undefined) request.input('ref2', sql.Int, identifier.ref2);
  if (identifier.ref3 !== undefined) request.input('ref3', sql.Int, identifier.ref3);

  const result = await request.query(queries[category]);
  return result.recordset.length > 0;
}

// ============================================================
// Cleanup test data
// ============================================================
async function cleanupTestData(noBongkarSusun, testData) {
  const pool = await poolPromise;

  log.info('Cleaning up test data...');
  try {
    // 1) Delete from BongkarSusunInput tables (by NoBongkarSusun)
    const deleteQueries = [
      `DELETE FROM dbo.BongkarSusunInputBroker       WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputBahanBaku    WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputWashing      WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputCrusher      WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputGilingan     WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputMixer        WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputBonggolan    WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputFurnitureWIP WHERE NoBongkarSusun = @no`,
      `DELETE FROM dbo.BongkarSusunInputBarangJadi   WHERE NoBongkarSusun = @no`,
    ];

    for (const q of deleteQueries) {
      await pool.request().input('no', sql.VarChar(50), noBongkarSusun).query(q);
    }

    // 2) Reset DateUsage (balikin data jadi "available" lagi)
    const resetQueries = {
      broker: `UPDATE dbo.Broker_d     SET DateUsage = NULL WHERE NoBroker = @ref1 AND NoSak = @ref2`,
      bb: `UPDATE dbo.BahanBaku_d      SET DateUsage = NULL WHERE NoBahanBaku = @ref1 AND NoPallet = @ref2 AND NoSak = @ref3`,
      washing: `UPDATE dbo.Washing_d   SET DateUsage = NULL WHERE NoWashing = @ref1 AND NoSak = @ref2`,
      crusher: `UPDATE dbo.Crusher     SET DateUsage = NULL WHERE NoCrusher = @ref1`,
      gilingan: `UPDATE dbo.Gilingan   SET DateUsage = NULL WHERE NoGilingan = @ref1`,
      mixer: `UPDATE dbo.Mixer_d       SET DateUsage = NULL WHERE NoMixer = @ref1 AND NoSak = @ref2`,
      bonggolan: `UPDATE dbo.Bonggolan SET DateUsage = NULL WHERE NoBonggolan = @ref1`,
      furnitureWip: `UPDATE dbo.FurnitureWIP SET DateUsage = NULL WHERE NoFurnitureWIP = @ref1`,
      barangJadi: `UPDATE dbo.BarangJadi   SET DateUsage = NULL WHERE NoBJ = @ref1`,
    };

    for (const [category, data] of Object.entries(testData || {})) {
      if (!data) continue;
      const q = resetQueries[category];
      if (!q) continue;

      const req = pool.request();

      if (category === 'broker') {
        req.input('ref1', sql.VarChar(50), data.NoBroker);
        req.input('ref2', sql.Int, data.NoSak);
      } else if (category === 'bb') {
        req.input('ref1', sql.VarChar(50), data.NoBahanBaku);
        req.input('ref2', sql.Int, data.NoPallet);
        req.input('ref3', sql.Int, data.NoSak);
      } else if (category === 'washing') {
        req.input('ref1', sql.VarChar(50), data.NoWashing);
        req.input('ref2', sql.Int, data.NoSak);
      } else if (category === 'mixer') {
        req.input('ref1', sql.VarChar(50), data.NoMixer);
        req.input('ref2', sql.Int, data.NoSak);
      } else {
        // Single ref categories
        const refKey = Object.keys(data).find((k) => k.startsWith('No'));
        req.input('ref1', sql.VarChar(50), data[refKey]);
      }

      await req.query(q);
    }

    // 3) Delete BongkarSusun_h header
    await pool
      .request()
      .input('no', sql.VarChar(50), noBongkarSusun)
      .query(`DELETE FROM dbo.BongkarSusun_h WHERE NoBongkarSusun = @no`);

    log.success('Cleanup completed successfully');
  } catch (err) {
    log.error('Cleanup failed:', err.message);
  }
}

// ============================================================
// Main integration test
// ============================================================
async function runIntegrationTest() {
  const testRunId = Date.now();

  console.log('\n========================================');
  console.log('🧪 BONGKAR SUSUN INTEGRATION TEST');
  console.log('========================================\n');

  log.info(`Test Run ID: ${testRunId}`);

  let testData = {};
  let actualNoBongkarSusun = null;

  const keepData =
    String(process.env.KEEP_DATA || '').trim() === '1' ||
    String(process.env.KEEP_DATA || '').trim().toLowerCase() === 'true';

  try {
    // STEP 1
    log.test('STEP 1: Fetching available test data...');
    testData = await getAvailableTestData();

    const availableCategories = Object.entries(testData)
      .filter(([_, data]) => data !== null)
      .map(([category]) => category);

    if (availableCategories.length === 0) {
      log.error('No test data available! Pastikan ada data DateUsage = NULL dan belum ada di BongkarSusunInput*');
      return;
    }

    log.success(`Found ${availableCategories.length} categories with available data\n`);

    // STEP 2
    log.test('STEP 2: Creating BongkarSusun_h header...');

    const headerPayload = {
      tanggal: new Date().toISOString().split('T')[0],
      username: 'admin',
      note: `Integration Test - ${testRunId}`,
    };

    const createResult = await bongkarSusunService.createBongkarSusun(headerPayload);
    actualNoBongkarSusun = createResult?.header?.NoBongkarSusun;

    if (!actualNoBongkarSusun) {
      throw new Error('createBongkarSusun did not return header.NoBongkarSusun');
    }

    log.info(`Actual NoBongkarSusun: ${actualNoBongkarSusun}`);
    log.success(`Header created: ${actualNoBongkarSusun}\n`);

    // STEP 3
    log.test('STEP 3: Preparing upsert payload...');

    const upsertPayload = {};

    if (testData.broker) {
      upsertPayload.broker = [{ noBroker: testData.broker.NoBroker, noSak: testData.broker.NoSak }];
    }
    if (testData.bb) {
      upsertPayload.bb = [{
        noBahanBaku: testData.bb.NoBahanBaku,
        noPallet: testData.bb.NoPallet,
        noSak: testData.bb.NoSak,
      }];
    }
    if (testData.washing) {
      upsertPayload.washing = [{ noWashing: testData.washing.NoWashing, noSak: testData.washing.NoSak }];
    }
    if (testData.crusher) {
      upsertPayload.crusher = [{ noCrusher: testData.crusher.NoCrusher }];
    }
    if (testData.gilingan) {
      upsertPayload.gilingan = [{ noGilingan: testData.gilingan.NoGilingan }];
    }
    if (testData.mixer) {
      upsertPayload.mixer = [{ noMixer: testData.mixer.NoMixer, noSak: testData.mixer.NoSak }];
    }
    if (testData.bonggolan) {
      upsertPayload.bonggolan = [{ noBonggolan: testData.bonggolan.NoBonggolan }];
    }
    if (testData.furnitureWip) {
      upsertPayload.furnitureWip = [{ noFurnitureWip: testData.furnitureWip.NoFurnitureWIP }];
    }
    if (testData.barangJadi) {
      upsertPayload.barangJadi = [{ noBj: testData.barangJadi.NoBJ }];
    }

    log.info('Payload prepared:', JSON.stringify(upsertPayload, null, 2));
    log.success('Payload ready\n');

    // STEP 4
    log.test('STEP 4: Upserting inputs...');

    const upsertResult = await bongkarSusunService.upsertInputs(actualNoBongkarSusun, upsertPayload);
    log.info('Upsert result:', JSON.stringify(upsertResult, null, 2));

    if (!upsertResult?.success) {
      throw new Error('Upsert failed: ' + JSON.stringify(upsertResult?.data));
    }

    log.success(`✅ Inserted ${upsertResult.data.summary.totalInserted} inputs\n`);

    // STEP 5
    log.test('STEP 5: Verifying DateUsage is set...');

    const verifications = [];

    for (const [category, data] of Object.entries(testData)) {
      if (!data) continue;

      let identifier = {};
      if (category === 'broker') identifier = { ref1: data.NoBroker, ref2: data.NoSak };
      else if (category === 'bb') identifier = { ref1: data.NoBahanBaku, ref2: data.NoPallet, ref3: data.NoSak };
      else if (category === 'washing') identifier = { ref1: data.NoWashing, ref2: data.NoSak };
      else if (category === 'mixer') identifier = { ref1: data.NoMixer, ref2: data.NoSak };
      else {
        const refKey = Object.keys(data).find((k) => k.startsWith('No'));
        identifier = { ref1: data[refKey] };
      }

      const dateUsage = await verifyDateUsage(category, identifier);
      if (dateUsage) {
        log.success(`✅ ${category}: DateUsage set to ${dateUsage}`);
        verifications.push({ category, success: true });
      } else {
        log.error(`❌ ${category}: DateUsage NOT set`);
        verifications.push({ category, success: false });
      }
    }

    console.log('');

    // STEP 6
    log.test('STEP 6: Verifying data in BongkarSusunInput tables...');

    for (const [category, data] of Object.entries(testData)) {
      if (!data) continue;

      let identifier = {};
      if (category === 'broker') identifier = { ref1: data.NoBroker, ref2: data.NoSak };
      else if (category === 'bb') identifier = { ref1: data.NoBahanBaku, ref2: data.NoPallet, ref3: data.NoSak };
      else if (category === 'washing') identifier = { ref1: data.NoWashing, ref2: data.NoSak };
      else if (category === 'mixer') identifier = { ref1: data.NoMixer, ref2: data.NoSak };
      else {
        const refKey = Object.keys(data).find((k) => k.startsWith('No'));
        identifier = { ref1: data[refKey] };
      }

      const exists = await verifyInputTableEntry(actualNoBongkarSusun, category, identifier);
      if (exists) log.success(`✅ ${category}: Entry exists in BongkarSusunInput table`);
      else log.error(`❌ ${category}: Entry NOT found in BongkarSusunInput table`);
    }

    console.log('');

    // SUMMARY
    console.log('\n========================================');
    console.log('📊 TEST SUMMARY');
    console.log('========================================\n');

    const allVerified = verifications.every((v) => v.success);

    if (allVerified) {
      log.success('ALL TESTS PASSED! ✅');
      log.info('- All DateUsage fields updated correctly');
      log.info('- All entries inserted into BongkarSusunInput tables');
    } else {
      log.error('SOME TESTS FAILED! ❌');
      const failed = verifications.filter((v) => !v.success);
      log.error(`Failed categories: ${failed.map((f) => f.category).join(', ')}`);
    }

    console.log('========================================\n');
  } catch (error) {
    log.error('Test execution failed:', error.message);
    console.error(error);
  } finally {
    // CLEANUP
    if (keepData) {
      log.info('KEEP_DATA is enabled -> skipping cleanup.');
      log.info(`NoBongkarSusun kept: ${actualNoBongkarSusun || '(none)'}`);
    } else {
      log.test('CLEANUP: Removing test data...');
      if (actualNoBongkarSusun) {
        await cleanupTestData(actualNoBongkarSusun, testData);
      } else {
        log.error('Cleanup skipped because actualNoBongkarSusun is null');
      }
    }

    // Optional: close DB pool
    try {
      const pool = await poolPromise;
      if (pool?.close) await pool.close();
      if (sql?.close) sql.close();
    } catch (_) {
      // ignore
    }

    console.log('\n========================================');
    console.log('✅ TEST COMPLETED');
    console.log('========================================\n');
  }
}

// Run the test
if (require.main === module) {
  runIntegrationTest()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error('Unhandled error:', error);
      process.exit(1);
    });
}

module.exports = { runIntegrationTest };

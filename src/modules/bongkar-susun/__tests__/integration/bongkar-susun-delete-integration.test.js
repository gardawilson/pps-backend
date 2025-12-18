// src/modules/bongkar-susun/__tests__/integration/bongkar-susun-delete-integration.test.js

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
// STEP 1 helpers: ambil data yang available dan belum ada di input table
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
      const r = await pool.request().query(query);
      results[category] = r.recordset[0] || null;

      if (results[category]) log.info(`Found test data for ${category}:`, results[category]);
      else log.error(`No available data for ${category}`);
    } catch (e) {
      log.error(`Failed to fetch ${category}:`, e.message);
      results[category] = null;
    }
  }
  return results;
}

// ============================================================
// Verify DateUsage helper
// ============================================================
async function verifyDateUsage(category, identifier) {
  const pool = await poolPromise;

  const queries = {
    broker: `SELECT DateUsage FROM dbo.Broker_d WHERE NoBroker=@ref1 AND NoSak=@ref2`,
    bb: `SELECT DateUsage FROM dbo.BahanBaku_d WHERE NoBahanBaku=@ref1 AND NoPallet=@ref2 AND NoSak=@ref3`,
    washing: `SELECT DateUsage FROM dbo.Washing_d WHERE NoWashing=@ref1 AND NoSak=@ref2`,
    crusher: `SELECT DateUsage FROM dbo.Crusher WHERE NoCrusher=@ref1`,
    gilingan: `SELECT DateUsage FROM dbo.Gilingan WHERE NoGilingan=@ref1`,
    mixer: `SELECT DateUsage FROM dbo.Mixer_d WHERE NoMixer=@ref1 AND NoSak=@ref2`,
    bonggolan: `SELECT DateUsage FROM dbo.Bonggolan WHERE NoBonggolan=@ref1`,
    furnitureWip: `SELECT DateUsage FROM dbo.FurnitureWIP WHERE NoFurnitureWIP=@ref1`,
    barangJadi: `SELECT DateUsage FROM dbo.BarangJadi WHERE NoBJ=@ref1`,
  };

  const req = pool.request();
  if (identifier.ref1) req.input('ref1', sql.VarChar(50), identifier.ref1);
  if (identifier.ref2 !== undefined) req.input('ref2', sql.Int, identifier.ref2);
  if (identifier.ref3 !== undefined) req.input('ref3', sql.Int, identifier.ref3);

  const rs = await req.query(queries[category]);
  return rs.recordset[0]?.DateUsage ?? null;
}

// ============================================================
// Verify input row exists helper
// ============================================================
async function verifyInputRowExists(noBongkarSusun, category, identifier) {
  const pool = await poolPromise;

  const queries = {
    broker: `
      SELECT 1 ok FROM dbo.BongkarSusunInputBroker
      WHERE NoBongkarSusun=@no AND NoBroker=@ref1 AND NoSak=@ref2`,
    bb: `
      SELECT 1 ok FROM dbo.BongkarSusunInputBahanBaku
      WHERE NoBongkarSusun=@no AND NoBahanBaku=@ref1 AND NoPallet=@ref2 AND NoSak=@ref3`,
    washing: `
      SELECT 1 ok FROM dbo.BongkarSusunInputWashing
      WHERE NoBongkarSusun=@no AND NoWashing=@ref1 AND NoSak=@ref2`,
    crusher: `
      SELECT 1 ok FROM dbo.BongkarSusunInputCrusher
      WHERE NoBongkarSusun=@no AND NoCrusher=@ref1`,
    gilingan: `
      SELECT 1 ok FROM dbo.BongkarSusunInputGilingan
      WHERE NoBongkarSusun=@no AND NoGilingan=@ref1`,
    mixer: `
      SELECT 1 ok FROM dbo.BongkarSusunInputMixer
      WHERE NoBongkarSusun=@no AND NoMixer=@ref1 AND NoSak=@ref2`,
    bonggolan: `
      SELECT 1 ok FROM dbo.BongkarSusunInputBonggolan
      WHERE NoBongkarSusun=@no AND NoBonggolan=@ref1`,
    furnitureWip: `
      SELECT 1 ok FROM dbo.BongkarSusunInputFurnitureWIP
      WHERE NoBongkarSusun=@no AND NoFurnitureWIP=@ref1`,
    barangJadi: `
      SELECT 1 ok FROM dbo.BongkarSusunInputBarangJadi
      WHERE NoBongkarSusun=@no AND NoBJ=@ref1`,
  };

  const req = pool.request();
  req.input('no', sql.VarChar(50), noBongkarSusun);

  if (identifier.ref1) req.input('ref1', sql.VarChar(50), identifier.ref1);
  if (identifier.ref2 !== undefined) req.input('ref2', sql.Int, identifier.ref2);
  if (identifier.ref3 !== undefined) req.input('ref3', sql.Int, identifier.ref3);

  const rs = await req.query(queries[category]);
  return (rs.recordset || []).length > 0;
}

// ============================================================
// Cleanup: delete all inputs by NoBongkarSusun, reset dateusage, delete header
// ============================================================
async function cleanupTestData(noBongkarSusun, testData) {
  const pool = await poolPromise;
  log.info('Cleaning up test data...');

  try {
    const deleteQueries = [
      `DELETE FROM dbo.BongkarSusunInputBroker       WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputBahanBaku    WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputWashing      WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputCrusher      WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputGilingan     WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputMixer        WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputBonggolan    WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputFurnitureWIP WHERE NoBongkarSusun=@no`,
      `DELETE FROM dbo.BongkarSusunInputBarangJadi   WHERE NoBongkarSusun=@no`,
    ];

    for (const q of deleteQueries) {
      await pool.request().input('no', sql.VarChar(50), noBongkarSusun).query(q);
    }

    // Reset DateUsage for picked rows (aman walau sudah NULL)
    const reset = async (q, binds) => {
      const req = pool.request();
      for (const b of binds) req.input(b.name, b.type, b.value);
      await req.query(q);
    };

    if (testData?.broker) {
      await reset(
        `UPDATE dbo.Broker_d SET DateUsage=NULL WHERE NoBroker=@a AND NoSak=@b`,
        [
          { name: 'a', type: sql.VarChar(50), value: testData.broker.NoBroker },
          { name: 'b', type: sql.Int, value: testData.broker.NoSak },
        ]
      );
    }
    if (testData?.bb) {
      await reset(
        `UPDATE dbo.BahanBaku_d SET DateUsage=NULL WHERE NoBahanBaku=@a AND NoPallet=@b AND NoSak=@c`,
        [
          { name: 'a', type: sql.VarChar(50), value: testData.bb.NoBahanBaku },
          { name: 'b', type: sql.Int, value: testData.bb.NoPallet },
          { name: 'c', type: sql.Int, value: testData.bb.NoSak },
        ]
      );
    }
    if (testData?.washing) {
      await reset(
        `UPDATE dbo.Washing_d SET DateUsage=NULL WHERE NoWashing=@a AND NoSak=@b`,
        [
          { name: 'a', type: sql.VarChar(50), value: testData.washing.NoWashing },
          { name: 'b', type: sql.Int, value: testData.washing.NoSak },
        ]
      );
    }
    if (testData?.crusher) {
      await reset(
        `UPDATE dbo.Crusher SET DateUsage=NULL WHERE NoCrusher=@a`,
        [{ name: 'a', type: sql.VarChar(50), value: testData.crusher.NoCrusher }]
      );
    }
    if (testData?.gilingan) {
      await reset(
        `UPDATE dbo.Gilingan SET DateUsage=NULL WHERE NoGilingan=@a`,
        [{ name: 'a', type: sql.VarChar(50), value: testData.gilingan.NoGilingan }]
      );
    }
    if (testData?.mixer) {
      await reset(
        `UPDATE dbo.Mixer_d SET DateUsage=NULL WHERE NoMixer=@a AND NoSak=@b`,
        [
          { name: 'a', type: sql.VarChar(50), value: testData.mixer.NoMixer },
          { name: 'b', type: sql.Int, value: testData.mixer.NoSak },
        ]
      );
    }
    if (testData?.bonggolan) {
      await reset(
        `UPDATE dbo.Bonggolan SET DateUsage=NULL WHERE NoBonggolan=@a`,
        [{ name: 'a', type: sql.VarChar(50), value: testData.bonggolan.NoBonggolan }]
      );
    }
    if (testData?.furnitureWip) {
      await reset(
        `UPDATE dbo.FurnitureWIP SET DateUsage=NULL WHERE NoFurnitureWIP=@a`,
        [{ name: 'a', type: sql.VarChar(50), value: testData.furnitureWip.NoFurnitureWIP }]
      );
    }
    if (testData?.barangJadi) {
      await reset(
        `UPDATE dbo.BarangJadi SET DateUsage=NULL WHERE NoBJ=@a`,
        [{ name: 'a', type: sql.VarChar(50), value: testData.barangJadi.NoBJ }]
      );
    }

    // Delete header
    await pool
      .request()
      .input('no', sql.VarChar(50), noBongkarSusun)
      .query(`DELETE FROM dbo.BongkarSusun_h WHERE NoBongkarSusun=@no`);

    log.success('Cleanup completed successfully');
  } catch (e) {
    log.error('Cleanup failed:', e.message);
  }
}

// ============================================================
// MAIN: Delete Integration Test
// ============================================================
async function runDeleteIntegrationTest() {
  const testRunId = Date.now();

  console.log('\n========================================');
  console.log('🧪 BONGKAR SUSUN DELETE INTEGRATION TEST');
  console.log('========================================\n');

  log.info(`Test Run ID: ${testRunId}`);

  let testData = {};
  let actualNoBongkarSusun = null;

  const keepData =
    String(process.env.KEEP_DATA || '').trim() === '1' ||
    String(process.env.KEEP_DATA || '').trim().toLowerCase() === 'true';

  // payload yang dipakai untuk insert, lalu dipakai lagi untuk delete
  let payload = {};

  try {
    // STEP 1: Pick data
    log.test('STEP 1: Fetching available test data...');
    testData = await getAvailableTestData();

    const available = Object.entries(testData).filter(([, v]) => v);
    if (available.length === 0) {
      log.error('No test data available!');
      return;
    }
    log.success(`Found ${available.length} categories with available data\n`);

    // STEP 2: Create header
    log.test('STEP 2: Creating BongkarSusun_h header...');
    const headerPayload = {
      tanggal: new Date().toISOString().split('T')[0],
      username: 'admin',
      note: `Delete Integration Test - ${testRunId}`,
    };

    const createResult = await bongkarSusunService.createBongkarSusun(headerPayload);
    actualNoBongkarSusun = createResult?.header?.NoBongkarSusun;
    if (!actualNoBongkarSusun) throw new Error('createBongkarSusun did not return header.NoBongkarSusun');

    log.success(`Header created: ${actualNoBongkarSusun}\n`);

    // STEP 3: Upsert (setup data to be deleted)
    log.test('STEP 3: Upserting inputs (setup) ...');

    payload = {};
    if (testData.broker) payload.broker = [{ noBroker: testData.broker.NoBroker, noSak: testData.broker.NoSak }];
    if (testData.bb) payload.bb = [{ noBahanBaku: testData.bb.NoBahanBaku, noPallet: testData.bb.NoPallet, noSak: testData.bb.NoSak }];
    if (testData.washing) payload.washing = [{ noWashing: testData.washing.NoWashing, noSak: testData.washing.NoSak }];
    if (testData.crusher) payload.crusher = [{ noCrusher: testData.crusher.NoCrusher }];
    if (testData.gilingan) payload.gilingan = [{ noGilingan: testData.gilingan.NoGilingan }];
    if (testData.mixer) payload.mixer = [{ noMixer: testData.mixer.NoMixer, noSak: testData.mixer.NoSak }];
    if (testData.bonggolan) payload.bonggolan = [{ noBonggolan: testData.bonggolan.NoBonggolan }];
    if (testData.furnitureWip) payload.furnitureWip = [{ noFurnitureWip: testData.furnitureWip.NoFurnitureWIP }];
    if (testData.barangJadi) payload.barangJadi = [{ noBj: testData.barangJadi.NoBJ }];

    const upsertRes = await bongkarSusunService.upsertInputs(actualNoBongkarSusun, payload);
    if (!upsertRes?.success) throw new Error('Upsert failed: ' + JSON.stringify(upsertRes?.data));

    log.success(`Inserted: ${upsertRes.data?.summary?.totalInserted || 0}\n`);

    // STEP 4: Verify DateUsage is SET (pre-delete)
    log.test('STEP 4: Verify DateUsage is set (pre-delete)...');

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

      const du = await verifyDateUsage(category, identifier);
      if (!du) throw new Error(`Pre-delete check failed: DateUsage is NULL for ${category}`);
      log.success(`✅ ${category}: DateUsage = ${du}`);
    }

    console.log('');

    // STEP 5: Call deleteInputs
    log.test('STEP 5: Deleting inputs...');
    const delRes = await bongkarSusunService.deleteInputs(actualNoBongkarSusun, payload);

    log.info('Delete result:', JSON.stringify(delRes, null, 2));
    if (!delRes?.success) throw new Error('deleteInputs returned success=false');

    log.success(
      `Deleted=${delRes.data?.summary?.totalDeleted || 0}, NotFound=${delRes.data?.summary?.totalNotFound || 0}\n`
    );

    // STEP 6: Verify inputs removed
    log.test('STEP 6: Verify BongkarSusunInput rows are removed...');

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

      const exists = await verifyInputRowExists(actualNoBongkarSusun, category, identifier);
      if (exists) throw new Error(`Post-delete failed: input row still exists for ${category}`);
      log.success(`✅ ${category}: input row removed`);
    }

    console.log('');

    // STEP 7: Verify DateUsage back to NULL
    log.test('STEP 7: Verify DateUsage is NULL (post-delete)...');

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

      const du = await verifyDateUsage(category, identifier);
      if (du !== null) throw new Error(`Post-delete failed: DateUsage NOT NULL for ${category} -> ${du}`);
      log.success(`✅ ${category}: DateUsage back to NULL`);
    }

    console.log('\n========================================');
    console.log('✅ DELETE TEST PASSED');
    console.log('========================================\n');
  } catch (e) {
    log.error('Delete test failed:', e.message);
    console.error(e);
  } finally {
    if (keepData) {
      log.info('KEEP_DATA enabled -> skipping cleanup.');
      log.info(`NoBongkarSusun kept: ${actualNoBongkarSusun || '(none)'}`);
    } else {
      log.test('CLEANUP: Removing test data...');
      if (actualNoBongkarSusun) await cleanupTestData(actualNoBongkarSusun, testData);
    }

    try {
      const pool = await poolPromise;
      if (pool?.close) await pool.close();
      if (sql?.close) sql.close();
    } catch (_) {}

    console.log('\n========================================');
    console.log('✅ TEST COMPLETED');
    console.log('========================================\n');
  }
}

// Run the test
if (require.main === module) {
  runDeleteIntegrationTest()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('Unhandled error:', err);
      process.exit(1);
    });
}

module.exports = { runDeleteIntegrationTest };

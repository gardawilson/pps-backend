// services/labels/packing-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const {
  getBlokLokasiFromKodeProduksi,
} = require('../../../core/shared/mesin-location-helper'); 

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
} = require('../../../core/shared/tutup-transaksi-guard');


exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      bj.NoBJ,
      bj.DateCreate,
      bj.IdBJ,
      mbj.NamaBJ,

      -- 🔹 Pcs sudah dikurangi partial (jika IsPartial = 1)
      CASE 
        WHEN bj.IsPartial = 1 THEN
          CASE
            WHEN ISNULL(bj.Pcs, 0) - ISNULL(MAX(bjp.TotalPartialPcs), 0) < 0 
              THEN 0
            ELSE ISNULL(bj.Pcs, 0) - ISNULL(MAX(bjp.TotalPartialPcs), 0)
          END
        ELSE ISNULL(bj.Pcs, 0)
      END AS Pcs,

      ISNULL(bj.Berat, 0) AS Berat,

      bj.IsPartial,
      bj.Blok,
      bj.IdLokasi,

      -- 🔗 TIPE SUMBER (PACKING / INJECT / BONGKAR_SUSUN / RETUR)
      CASE
        WHEN MAX(packmap.NoPacking)        IS NOT NULL THEN 'PACKING'
        WHEN MAX(injmap.NoProduksi)       IS NOT NULL THEN 'INJECT'
        WHEN MAX(bsmap.NoBongkarSusun)    IS NOT NULL THEN 'BONGKAR_SUSUN'
        WHEN MAX(retmap.NoRetur)          IS NOT NULL THEN 'RETUR'
        ELSE NULL
      END AS OutputType,

      -- 🔗 KODE SUMBER (NoPacking / NoProduksi / NoBongkarSusun / NoRetur)
      MAX(
        COALESCE(
          packmap.NoPacking,
          injmap.NoProduksi,
          bsmap.NoBongkarSusun,
          retmap.NoRetur
        )
      ) AS OutputCode,

      -- 🔗 NAMA MESIN / NAMA PEMBELI / 'Bongkar Susun'
      MAX(
        COALESCE(
          mPack.NamaMesin,
          mInj.NamaMesin,
          CASE 
            WHEN bsmap.NoBongkarSusun IS NOT NULL 
              THEN 'Bongkar Susun' 
          END,
          pemb.NamaPembeli
        )
      ) AS OutputNamaMesin

    FROM [dbo].[BarangJadi] bj

    -- 🔹 Aggregate partial per NoBJ
    LEFT JOIN (
      SELECT
        NoBJ,
        SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
      FROM [dbo].[BarangJadiPartial]
      GROUP BY NoBJ
    ) bjp
      ON bjp.NoBJ = bj.NoBJ

    -- 🔗 Master nama barang jadi
    LEFT JOIN [dbo].[MstBarangJadi] mbj
      ON mbj.IdBJ = bj.IdBJ

    ----------------------------------------------------------------------
    -- 🔗 MAPPING INJECT (S.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[InjectProduksiOutputBarangJadi] injmap
           ON injmap.NoBJ = bj.NoBJ
    LEFT JOIN [dbo].[InjectProduksi_h] injh
           ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj
           ON mInj.IdMesin = injh.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING PACKING (NoPacking)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[PackingProduksiOutputLabelBJ] packmap
           ON packmap.NoBJ = bj.NoBJ
    LEFT JOIN [dbo].[PackingProduksi_h] packh
           ON packh.NoPacking = packmap.NoPacking
    LEFT JOIN [dbo].[MstMesin] mPack
           ON mPack.IdMesin = packh.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING RETUR (L.) → pakai NamaPembeli
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[BJReturBarangJadi_d] retmap
           ON retmap.NoBJ = bj.NoBJ
    LEFT JOIN [dbo].[BJRetur_h] bjh
           ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb
           ON pemb.IdPembeli = bjh.IdPembeli

    ----------------------------------------------------------------------
    -- 🔗 MAPPING BONGKAR SUSUN
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[BongkarSusunOutputBarangjadi] bsmap
           ON bsmap.NoBJ = bj.NoBJ

    WHERE 1=1
      AND bj.DateUsage IS NULL
      ${
        search
          ? `AND (
               bj.NoBJ LIKE @search
               OR bj.Blok LIKE @search
               OR CONVERT(VARCHAR(20), bj.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), bj.IdBJ) LIKE @search
               OR ISNULL(mbj.NamaBJ,'') LIKE @search

               -- cari berdasarkan kode sumber
               OR ISNULL(packmap.NoPacking,'')        LIKE @search
               OR ISNULL(injmap.NoProduksi,'')        LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'')     LIKE @search
               OR ISNULL(retmap.NoRetur,'')           LIKE @search

               -- cari berdasarkan nama mesin / pembeli
               OR ISNULL(mPack.NamaMesin,'')          LIKE @search
               OR ISNULL(mInj.NamaMesin,'')           LIKE @search
               OR ISNULL(pemb.NamaPembeli,'')         LIKE @search
             )`
          : ''
      }
    GROUP BY
      bj.NoBJ,
      bj.DateCreate,
      bj.IdBJ,
      mbj.NamaBJ,
      bj.Pcs,
      bj.Berat,
      bj.IsPartial,
      bj.IdWarehouse,
      bj.Blok,
      bj.IdLokasi
    ORDER BY bj.NoBJ DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT bj.NoBJ) AS total
    FROM [dbo].[BarangJadi] bj

    LEFT JOIN [dbo].[MstBarangJadi] mbj
      ON mbj.IdBJ = bj.IdBJ

    LEFT JOIN [dbo].[InjectProduksiOutputBarangJadi] injmap
           ON injmap.NoBJ = bj.NoBJ
    LEFT JOIN [dbo].[InjectProduksi_h] injh
           ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj
           ON mInj.IdMesin = injh.IdMesin

    LEFT JOIN [dbo].[PackingProduksiOutputLabelBJ] packmap
           ON packmap.NoBJ = bj.NoBJ
    LEFT JOIN [dbo].[PackingProduksi_h] packh
           ON packh.NoPacking = packmap.NoPacking
    LEFT JOIN [dbo].[MstMesin] mPack
           ON mPack.IdMesin = packh.IdMesin

    LEFT JOIN [dbo].[BJReturBarangJadi_d] retmap
           ON retmap.NoBJ = bj.NoBJ
    LEFT JOIN [dbo].[BJRetur_h] bjh
           ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb
           ON pemb.IdPembeli = bjh.IdPembeli

    LEFT JOIN [dbo].[BongkarSusunOutputBarangjadi] bsmap
           ON bsmap.NoBJ = bj.NoBJ

    WHERE 1=1
      AND bj.DateUsage IS NULL
      ${
        search
          ? `AND (
               bj.NoBJ LIKE @search
               OR bj.Blok LIKE @search
               OR CONVERT(VARCHAR(20), bj.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), bj.IdBJ) LIKE @search
               OR ISNULL(mbj.NamaBJ,'') LIKE @search
               OR ISNULL(packmap.NoPacking,'')        LIKE @search
               OR ISNULL(injmap.NoProduksi,'')        LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'')     LIKE @search
               OR ISNULL(retmap.NoRetur,'')           LIKE @search
               OR ISNULL(mPack.NamaMesin,'')          LIKE @search
               OR ISNULL(mInj.NamaMesin,'')           LIKE @search
               OR ISNULL(pemb.NamaPembeli,'')         LIKE @search
             )`
          : ''
      }
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);
  if (search) {
    request.input('search', sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};



//
// ==================== CREATE (POST /labels/packing) ====================
//

// helper pad left
function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// Generate next NoBJ: e.g. 'BA.0000000002' (GANTI prefix kalau beda)
async function generateNextNoBJ(
  tx,
  { prefix = 'BA.', width = 10 } = {}
) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 bj.NoBJ
    FROM [dbo].[BarangJadi] AS bj WITH (UPDLOCK, HOLDLOCK)
    WHERE bj.NoBJ LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(bj.NoBJ, LEN(@prefix) + 1, 50)) DESC,
             bj.NoBJ DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoBJ;
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}




/**
 * Helper: insert 1 row BarangJadi + mapping ke table output
 */
async function insertSingleBarangJadi({
  tx,
  header,
  idBJ,
  outputCode,
  outputType,
  mappingTable,
}) {
  // 1) Generate NoBJ
  const generatedNo = await generateNextNoBJ(tx, {
    prefix: 'BA.', 
    width: 10,
  });

  // Double-check uniqueness (rare)
  const rqCheck = new sql.Request(tx);
  const exist = await rqCheck
    .input('NoBJ', sql.VarChar, generatedNo)
    .query(`
      SELECT 1
      FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBJ = @NoBJ
    `);

  const noBJ =
    exist.recordset.length > 0
      ? await generateNextNoBJ(tx, { prefix: 'BA.', width: 10 })
      : generatedNo;

  // 2) Insert header ke dbo.BarangJadi
  const nowDateOnly = header.DateCreate || null; // null -> GETDATE() (date only)
  const insertHeaderSql = `
    INSERT INTO [dbo].[BarangJadi] (
      NoBJ,
      IdBJ,
      DateCreate,
      DateUsage,
      Jam,
      Pcs,
      Berat,
      IdWarehouse,
      CreateBy,
      DateTimeCreate,
      IsPartial,
      Blok,
      IdLokasi
    )
    VALUES (
      @NoBJ,
      @IdBJ,
      ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
      NULL,
      @Jam,
      @Pcs,
      @Berat,
      @IdWarehouse,
      @CreateBy,
      GETDATE(),
      @IsPartial,
      @Blok,
      @IdLokasi
    );
  `;

  const rqHeader = new sql.Request(tx);

  // normalize IdLokasi
  const rawIdLokasi = header.IdLokasi;
  let idLokasiVal = null;
  if (rawIdLokasi !== undefined && rawIdLokasi !== null) {
    idLokasiVal = String(rawIdLokasi).trim();
    if (idLokasiVal.length === 0) {
      idLokasiVal = null;
    }
  }

  rqHeader
    .input('NoBJ', sql.VarChar, noBJ)
    .input('IdBJ', sql.Int, idBJ)
    .input('Pcs', sql.Decimal(18, 3), header.Pcs ?? null)
    .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
    .input('Jam', sql.VarChar, header.Jam ?? null)
    .input('IdWarehouse', sql.Int, header.IdWarehouse ?? null)
    .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
    .input('CreateBy', sql.VarChar, header.CreateBy ?? null)
    .input('Blok', sql.VarChar, header.Blok ?? null)
    .input('IdLokasi', sql.VarChar, idLokasiVal);

  if (nowDateOnly) {
    rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
  }

  await rqHeader.query(insertHeaderSql);

  // 3) Insert mapping berdasarkan outputType / mappingTable
  const rqMap = new sql.Request(tx)
    .input('OutputCode', sql.VarChar, outputCode)
    .input('NoBJ', sql.VarChar, noBJ);

  if (mappingTable === 'PackingProduksiOutputLabelBJ') {
    const q = `
      INSERT INTO [dbo].[PackingProduksiOutputLabelBJ] (NoPacking, NoBJ)
      VALUES (@OutputCode, @NoBJ);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'InjectProduksiOutputBarangJadi') {
    const q = `
      INSERT INTO [dbo].[InjectProduksiOutputBarangJadi] (NoProduksi, NoBJ)
      VALUES (@OutputCode, @NoBJ);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'BongkarSusunOutputBarangjadi') {
    const q = `
      INSERT INTO [dbo].[BongkarSusunOutputBarangjadi] (NoBongkarSusun, NoBJ)
      VALUES (@OutputCode, @NoBJ);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'BJReturBarangJadi_d') {
    const q = `
      INSERT INTO [dbo].[BJReturBarangJadi_d] (NoRetur, NoBJ)
      VALUES (@OutputCode, @NoBJ);
    `;
    await rqMap.query(q);
  }

  // Return header shape yang dikirim ke controller
  return {
    NoBJ: noBJ,
    DateCreate: nowDateOnly || 'GETDATE()',
    IdBJ: idBJ,
    Pcs: header.Pcs ?? null,
    Berat: header.Berat ?? null,
    Jam: header.Jam ?? null,
    IdWarehouse: header.IdWarehouse ?? null,
    IsPartial: header.IsPartial ?? 0,
    CreateBy: header.CreateBy ?? null,
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
    OutputCode: outputCode,
    OutputType: outputType,
  };
}


/**
 * Helper khusus: INJECT tanpa IdBJ → multi-label
 * - Cari InjectProduksi_h by NoProduksi
 * - Cari semua mapping di CetakanWarnaToProduk_d
 * - Loop insertSingleBarangJadi untuk tiap IdBarangJadi (IdBJ)
 */
async function createFromInjectMappingBJ({
  tx,
  header,
  outputCode,
  mappingTable,
  outputType,
  badReq,
  effectiveDateCreate, // ✅ tambahan
}) {
  // 1) Ambil InjectProduksi_h
  const rqInject = new sql.Request(tx);
  rqInject.input('NoProduksi', sql.VarChar, outputCode);

  const injRes = await rqInject.query(`
    SELECT TOP 1
      IdCetakan,
      IdWarna,
      IdFurnitureMaterial
    FROM dbo.InjectProduksi_h WITH (UPDLOCK, HOLDLOCK)
    WHERE NoProduksi = @NoProduksi
      AND IdCetakan IS NOT NULL;
  `);

  if (injRes.recordset.length === 0) {
    throw badReq(`InjectProduksi_h ${outputCode} not found or IdCetakan is NULL`);
  }

  const inj = injRes.recordset[0];

  // 2) Ambil mapping ke Produk (BarangJadi)
  const rqMap = new sql.Request(tx);
  rqMap
    .input('IdCetakan', sql.Int, inj.IdCetakan)
    .input('IdWarna', sql.Int, inj.IdWarna)
    .input('IdFurnitureMaterial', sql.Int, inj.IdFurnitureMaterial ?? 0);

  const mapRes = await rqMap.query(`
    SELECT IdBarangJadi
    FROM dbo.CetakanWarnaToProduk_d
    WHERE IdCetakan = @IdCetakan
      AND IdWarna = @IdWarna
      AND (
        (IdFurnitureMaterial IS NULL AND @IdFurnitureMaterial = 0)
        OR IdFurnitureMaterial = @IdFurnitureMaterial
      );
  `);

  if (mapRes.recordset.length === 0) {
    throw badReq(
      `No Produk mapping found for Inject ${outputCode} (IdCetakan=${inj.IdCetakan}, IdWarna=${inj.IdWarna})`
    );
  }

  const createdHeaders = [];

  for (const row of mapRes.recordset) {
    const idBJ = row.IdBarangJadi;

    const created = await insertSingleBarangJadi({
      tx,
      header,
      idBJ,
      outputCode,
      outputType,
      mappingTable,
      effectiveDateCreate, // ✅ pass-through UTC date-only
    });

    createdHeaders.push(created);
  }

  return createdHeaders;
}



function resolveOutputByPrefix(outputCode, badReq) {
  let outputType = null;
  let mappingTable = null;

  if (outputCode.startsWith('BD.')) {
    outputType = 'PACKING';
    mappingTable = 'PackingProduksiOutputLabelBJ';
  } else if (outputCode.startsWith('S.')) {
    outputType = 'INJECT';
    mappingTable = 'InjectProduksiOutputBarangJadi';
  } else if (outputCode.startsWith('BG.')) {
    outputType = 'BONGKAR_SUSUN';
    mappingTable = 'BongkarSusunOutputBarangjadi';
  } else if (outputCode.startsWith('L.')) {
    outputType = 'RETUR';
    mappingTable = 'BJReturBarangJadi_d';
  } else {
    throw badReq(
      'outputCode prefix not recognized (supported: BD., S., BG., L.)'
    );
  }

  return { outputType, mappingTable };
}


exports.createPacking = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = (payload?.outputCode || '').toString().trim();

  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  // ===============================
  // 0) Validasi outputCode
  // ===============================
  if (!outputCode) {
    throw badReq('outputCode is required (BD., S., BG., L., etc.)');
  }

  // Prefix → outputType + mappingTable
  const { outputType, mappingTable } = resolveOutputByPrefix(outputCode, badReq);
  const isInject = outputType === 'INJECT';

  if (!header.IdBJ && !isInject) {
    throw badReq('IdBJ is required for non-INJECT modes');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // 1) Resolve DateCreate (UTC date-only) + tutup transaksi
    // ===============================
    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate);

    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create packing',
      useLock: true,
    });

    let result;

    // ===============================
    // 2) Auto-isi Blok & IdLokasi (kalau kosong)
    // ===============================
    if (!header.Blok || !header.IdLokasi) {
      const lokasi = await getBlokLokasiFromKodeProduksi({
        kode: outputCode,
        runner: tx,
      });

      if (lokasi) {
        if (!header.Blok) header.Blok = lokasi.Blok;
        if (!header.IdLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    // ===============================
    // 3) Jalur INJECT multi-create
    // ===============================
    if (isInject && !header.IdBJ) {
      const createdHeaders = await createFromInjectMappingBJ({
        tx,
        header,
        outputCode,
        mappingTable,
        outputType,
        badReq,
        effectiveDateCreate, // ✅ penting
      });

      result = {
        headers: createdHeaders,
        output: {
          code: outputCode,
          type: outputType,
          mappingTable,
          isMulti: createdHeaders.length > 1,
          count: createdHeaders.length,
        },
      };
    } else {
      // ===============================
      // 4) Jalur single-create
      // ===============================
      const createdHeader = await insertSingleBarangJadi({
        tx,
        header,
        idBJ: header.IdBJ,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate, // ✅ penting
      });

      result = {
        headers: [createdHeader],
        output: {
          code: outputCode,
          type: outputType,
          mappingTable,
          isMulti: false,
          count: 1,
        },
      };
    }

    await tx.commit();
    return result;
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};





/**
 * UPDATE Packing / BarangJadi
 * - Edit header BarangJadi (IdBJ, Pcs, Berat, Jam, dsb.)
 * - Optional: ganti mapping output kalau outputCode dikirim
 *
 * Catatan:
 * - Tidak generate NoBJ baru
 * - Tidak pakai auto-mapping inject (CetakanWarnaToProduk_d) di sini.
 *   Kalau mau multi-create Inject, tetap pakai POST.
 */
exports.updatePacking = async (noBJ, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const rawOutputCode = (payload?.outputCode || '').toString().trim();

  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  if (!noBJ) throw badReq('NoBJ is required');
  if (!header.IdBJ) throw badReq('IdBJ is required for update');

  // Siapkan info mapping baru (jika ada outputCode baru)
  let mappingInfo = null;
  if (rawOutputCode) mappingInfo = resolveOutputByPrefix(rawOutputCode, badReq);

  const hasDateCreateField = Object.prototype.hasOwnProperty.call(header, 'DateCreate');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 1) Lock + ambil DateCreate (date-only) + ensure belum dipakai
    const existingRes = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar, noBJ)
      .query(`
        SELECT TOP 1
          NoBJ,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage,
          IdBJ, Pcs, Berat, Jam, IdWarehouse, IsPartial, Blok, IdLokasi
        FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJ = @NoBJ
          AND DateUsage IS NULL;
      `);

    if (existingRes.recordset.length === 0) {
      const e = new Error('BarangJadi not found or already used (DateUsage not NULL)');
      e.statusCode = 404;
      throw e;
    }

    const current = existingRes.recordset[0];

    // 2) TUTUP TRANSAKSI CHECK
    // - kalau DateCreate dikirim → cek tanggal baru
    // - kalau tidak dikirim → cek tanggal existing (karena tetap update transaksi itu)
    let effectiveDateUpdate = null;

    if (hasDateCreateField) {
      const v = header.DateCreate;

      if (v === null || v === '') {
        // reset ke UTC today
        effectiveDateUpdate = toDateOnly(new Date());
      } else {
        const d = toDateOnly(v);
        if (!d) {
          const e = new Error('Invalid DateCreate');
          e.statusCode = 400;
          e.meta = { field: 'DateCreate', value: v };
          throw e;
        }
        effectiveDateUpdate = d;
      }
    } else {
      effectiveDateUpdate = current.DateCreate ? toDateOnly(current.DateCreate) : null;
    }

    await assertNotLocked({
      date: effectiveDateUpdate,
      runner: tx,
      action: 'update packing',
      useLock: true,
    });

    // 3) Update header BarangJadi
    const rqUpdate = new sql.Request(tx);
    rqUpdate
      .input('NoBJ', sql.VarChar, noBJ)
      .input('IdBJ', sql.Int, header.IdBJ)
      .input('Pcs', sql.Decimal(18, 3), header.Pcs ?? null)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('Jam', sql.VarChar, header.Jam ?? null)
      .input('IdWarehouse', sql.Int, header.IdWarehouse ?? null)
      .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
      .input('Blok', sql.VarChar, header.Blok ?? null);

    // normalize IdLokasi
    const rawIdLokasi = header.IdLokasi;
    let idLokasiVal = null;
    if (rawIdLokasi !== undefined && rawIdLokasi !== null) {
      const s = String(rawIdLokasi).trim();
      idLokasiVal = s.length === 0 ? null : s;
    }
    rqUpdate.input('IdLokasi', sql.VarChar, idLokasiVal);

    let updateSql = `
      UPDATE [dbo].[BarangJadi]
      SET
        IdBJ = @IdBJ,
        Jam = @Jam,
        Pcs = @Pcs,
        Berat = @Berat,
        IdWarehouse = @IdWarehouse,
        IsPartial = @IsPartial,
        Blok = @Blok,
        IdLokasi = @IdLokasi
    `;

    // DateCreate update hanya kalau field dikirim
    if (hasDateCreateField) {
      updateSql += `, DateCreate = @DateCreate`;
      rqUpdate.input('DateCreate', sql.Date, effectiveDateUpdate); // ✅ UTC date-only
    }

    updateSql += `
      WHERE NoBJ = @NoBJ
        AND DateUsage IS NULL;
    `;

    await rqUpdate.query(updateSql);

    // 4) Mapping update (kalau outputCode dikirim dan valid)
    let output = null;

    if (mappingInfo && rawOutputCode) {
      const { outputType, mappingTable } = mappingInfo;

      // Hapus semua mapping lama
      await new sql.Request(tx)
        .input('NoBJ', sql.VarChar, noBJ)
        .query(`
          DELETE FROM [dbo].[PackingProduksiOutputLabelBJ] WHERE NoBJ = @NoBJ;
          DELETE FROM [dbo].[InjectProduksiOutputBarangJadi] WHERE NoBJ = @NoBJ;
          DELETE FROM [dbo].[BongkarSusunOutputBarangjadi] WHERE NoBJ = @NoBJ;
          DELETE FROM [dbo].[BJReturBarangJadi_d]        WHERE NoBJ = @NoBJ;
        `);

      const rqMap = new sql.Request(tx)
        .input('OutputCode', sql.VarChar, rawOutputCode)
        .input('NoBJ', sql.VarChar, noBJ);

      if (mappingTable === 'PackingProduksiOutputLabelBJ') {
        await rqMap.query(`
          INSERT INTO [dbo].[PackingProduksiOutputLabelBJ] (NoPacking, NoBJ)
          VALUES (@OutputCode, @NoBJ);
        `);
      } else if (mappingTable === 'InjectProduksiOutputBarangJadi') {
        await rqMap.query(`
          INSERT INTO [dbo].[InjectProduksiOutputBarangJadi] (NoProduksi, NoBJ)
          VALUES (@OutputCode, @NoBJ);
        `);
      } else if (mappingTable === 'BongkarSusunOutputBarangjadi') {
        await rqMap.query(`
          INSERT INTO [dbo].[BongkarSusunOutputBarangjadi] (NoBongkarSusun, NoBJ)
          VALUES (@OutputCode, @NoBJ);
        `);
      } else if (mappingTable === 'BJReturBarangJadi_d') {
        await rqMap.query(`
          INSERT INTO [dbo].[BJReturBarangJadi_d] (NoRetur, NoBJ)
          VALUES (@OutputCode, @NoBJ);
        `);
      }

      output = {
        code: rawOutputCode,
        type: outputType,
        mappingTable,
        isMulti: false,
        count: 1,
      };
    }

    await tx.commit();

    // response mirip createPacking
    return {
      headers: [
        {
          NoBJ: noBJ,
          DateCreate: hasDateCreateField ? effectiveDateUpdate : current.DateCreate,
          IdBJ: header.IdBJ,
          Pcs: header.Pcs ?? current.Pcs,
          Berat: header.Berat ?? current.Berat,
          Jam: header.Jam ?? current.Jam,
          IdWarehouse: header.IdWarehouse ?? current.IdWarehouse,
          IsPartial: header.IsPartial ?? current.IsPartial,
          Blok: header.Blok ?? current.Blok,
          IdLokasi: header.IdLokasi ?? current.IdLokasi,
        },
      ],
      output,
    };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};




/**
 * DELETE Packing / BarangJadi
 * - Hanya boleh jika DateUsage IS NULL
 * - Hapus:
 *   - BarangJadiPartial
 *   - semua mapping (Packing, Inject, Bongkar Susun, Retur)
 *   - header BarangJadi
 */
exports.deletePacking = async (noBJ) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  if (!noBJ) throw badReq('NoBJ is required');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 1) Lock & cek record masih ada dan belum dipakai + ambil DateCreate (date-only)
    const existingRes = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar, noBJ)
      .query(`
        SELECT TOP 1
          NoBJ,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage
        FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJ = @NoBJ
          AND DateUsage IS NULL;
      `);

    if (existingRes.recordset.length === 0) {
      const e = new Error('BarangJadi not found or already used (DateUsage not NULL)');
      e.statusCode = 404;
      throw e;
    }

    const current = existingRes.recordset[0];

    // 2) ✅ TUTUP TRANSAKSI CHECK (pakai tanggal transaksi record)
    const trxDate = current.DateCreate ? toDateOnly(current.DateCreate) : null;

    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: 'delete packing',
      useLock: true,
    });

    // 3) Hapus partial & mapping
    await new sql.Request(tx)
      .input('NoBJ', sql.VarChar, noBJ)
      .query(`
        DELETE FROM [dbo].[BarangJadiPartial]              WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[PackingProduksiOutputLabelBJ]   WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[InjectProduksiOutputBarangJadi] WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[BongkarSusunOutputBarangjadi]   WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[BJReturBarangJadi_d]            WHERE NoBJ = @NoBJ;
      `);

    // 4) Hapus header BarangJadi
    const delRes = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar, noBJ)
      .query(`
        DELETE FROM [dbo].[BarangJadi]
        WHERE NoBJ = @NoBJ
          AND DateUsage IS NULL;
      `);

    await tx.commit();

    if ((delRes.rowsAffected?.[0] ?? 0) === 0) {
      // harusnya tidak kejadian karena sudah lock, tapi tetap defensif
      const e = new Error('BarangJadi not found or already used (DateUsage not NULL)');
      e.statusCode = 404;
      throw e;
    }

    return { deleted: true, NoBJ: noBJ };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};



/**
 * Ambil info partial BarangJadi per NoBJ.
 *
 * Tabel yang dipakai:
 * - dbo.BarangJadiPartial                     (Base partial, Pcs)
 * - dbo.BJJual_dLabelBarangJadiPartial        (konsumsi partial -> NoBJJual)
 * - dbo.BJJual_h                              (header jual -> IdPembeli, Tanggal, Remark)
 * - dbo.MstPembeli                            (nama pembeli)
 */
exports.getPartialInfoByBJ = async (noBJ) => {
  const pool = await poolPromise;

  const req = pool
    .request()
    .input('NoBJ', sql.VarChar, noBJ);

  const query = `
    ;WITH BasePartial AS (
      SELECT
        bjp.NoBJPartial,
        bjp.NoBJ,
        bjp.Pcs
      FROM dbo.BarangJadiPartial bjp
      WHERE bjp.NoBJ = @NoBJ
    ),
    Consumed AS (
      SELECT
        d.NoBJPartial,
        'JUAL' AS SourceType,
        d.NoBJJual
      FROM dbo.BJJual_dLabelBarangJadiPartial d
    )
    SELECT
      bp.NoBJPartial,
      bp.NoBJ,
      bp.Pcs,                  -- partial pcs

      c.SourceType,            -- 'JUAL' / NULL
      c.NoBJJual,

      bjh.Tanggal,
      bjh.IdPembeli,
      bjh.Remark,

      pemb.NamaPembeli
    FROM BasePartial bp
    LEFT JOIN Consumed c
      ON c.NoBJPartial = bp.NoBJPartial

    LEFT JOIN dbo.BJJual_h bjh
      ON bjh.NoBJJual = c.NoBJJual

    LEFT JOIN dbo.MstPembeli pemb
      ON pemb.IdPembeli = bjh.IdPembeli

    ORDER BY
      bp.NoBJPartial ASC,
      c.NoBJJual ASC;
  `;

  const result = await req.query(query);

  // total partial pcs (unique per NoBJPartial)
  const seen = new Set();
  let totalPartialPcs = 0;

  for (const row of result.recordset) {
    const key = row.NoBJPartial;
    if (!seen.has(key)) {
      seen.add(key);
      const pcs =
        typeof row.Pcs === 'number'
          ? row.Pcs
          : Number(row.Pcs) || 0;
      totalPartialPcs += pcs;
    }
  }

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? '0' + n : '' + n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = result.recordset.map((r) => ({
    NoBJPartial: r.NoBJPartial,
    NoBJ: r.NoBJ,
    Pcs: r.Pcs,

    SourceType: r.SourceType || null,   // 'JUAL' | null
    NoBJJual: r.NoBJJual || null,

    TanggalJual: r.Tanggal ? formatDate(r.Tanggal) : null,
    IdPembeli: r.IdPembeli || null,
    NamaPembeli: r.NamaPembeli || null,
    Remark: r.Remark || null,
  }));

  return { totalPartialPcs, rows };
};
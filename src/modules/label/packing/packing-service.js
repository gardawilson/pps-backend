// services/labels/packing-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const {
  getBlokLokasiFromKodeProduksi,
} = require('../../../core/shared/mesin-location-helper'); 



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
    throw badReq(
      `InjectProduksi_h ${outputCode} not found or IdCetakan is NULL`
    );
  }

  const inj = injRes.recordset[0];

  // 2) Ambil mapping ke Produk (BarangJadi)
  const rqMap = new sql.Request(tx);
  rqMap
    .input('IdCetakan', sql.Int, inj.IdCetakan)
    .input('IdWarna', sql.Int, inj.IdWarna)
    .input('IdFurnitureMaterial', sql.Int, inj.IdFurnitureMaterial ?? null);

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

  // ---- validation helper
  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  // Wajib link ke salah satu sumber label (prefix-based)
  if (!outputCode) {
    throw badReq('outputCode is required (BD., S., BG., L., etc.)');
  }


  // Prefix rules
  const { outputType, mappingTable } = resolveOutputByPrefix(outputCode, badReq);

  const isInject = outputType === 'INJECT';

  // Untuk NON-INJECT, IdBJ tetap wajib.
  // Untuk INJECT, IdBJ boleh NULL → akan diproses multi-label (mapping dari Cetakan).
  if (!header.IdBJ && !isInject) {
    throw badReq('IdBJ is required for non-INJECT modes');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    let result;


    
    // 0) Auto-isi Blok & IdLokasi dari kode produksi / bongkar susun (jika header belum isi)
    if (!header.Blok || !header.IdLokasi) {
      if (outputCode) {
        const lokasi = await getBlokLokasiFromKodeProduksi({
          kode: outputCode,
          runner: tx,
        });

        if (lokasi) {
          if (!header.Blok) header.Blok = lokasi.Blok;
          if (!header.IdLokasi) header.IdLokasi = lokasi.IdLokasi;
        }
      } 
    }

    if (isInject && !header.IdBJ) {
      // 🔹 Jalur khusus: INJECT + IdBJ null → multi-create dari CetakanWarnaToProduk_d
      const createdHeaders = await createFromInjectMappingBJ({
        tx,
        header,
        outputCode,
        mappingTable,
        outputType,
        badReq,
      });

      result = {
        headers: createdHeaders, // bisa 1 atau >1
        output: {
          code: outputCode,
          type: outputType,
          mappingTable,
          isMulti: createdHeaders.length > 1,
          count: createdHeaders.length,
        },
      };
    } else {
      // 🔹 Jalur normal: single create (baik PACKING, BONGKAR SUSUN, RETUR, maupun INJECT dengan IdBJ explisit)
      const createdHeader = await insertSingleBarangJadi({
        tx,
        header,
        idBJ: header.IdBJ,
        outputCode,
        outputType,
        mappingTable,
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
    try {
      await tx.rollback();
    } catch (_) {}
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

  if (!noBJ) {
    throw badReq('NoBJ is required');
  }

  // Untuk UPDATE kita minta IdBJ ada (kecuali kamu mau support partial update)
  if (!header.IdBJ) {
    throw badReq('IdBJ is required for update');
  }

  // Siapkan info mapping baru (jika ada outputCode baru)
  let mappingInfo = null;
  if (rawOutputCode) {
    mappingInfo = resolveOutputByPrefix(rawOutputCode, badReq);
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 1) Lock & cek apakah record masih boleh diedit (DateUsage NULL)
    const rqCheck = new sql.Request(tx);
    rqCheck.input('NoBJ', sql.VarChar, noBJ);

    const existingRes = await rqCheck.query(`
      SELECT TOP 1 *
      FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBJ = @NoBJ
        AND DateUsage IS NULL;
    `);

    if (existingRes.recordset.length === 0) {
      const e = new Error('BarangJadi not found or already used (DateUsage not NULL)');
      e.statusCode = 404;
      throw e;
    }

    // 2) Update header BarangJadi
    const nowDateOnly = header.DateCreate || null;

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
      idLokasiVal = String(rawIdLokasi).trim();
      if (idLokasiVal.length === 0) idLokasiVal = null;
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

    if (nowDateOnly) {
      rqUpdate.input('DateCreate', sql.Date, new Date(nowDateOnly));
      updateSql += `,
        DateCreate = @DateCreate
      `;
    }

    updateSql += `
      WHERE NoBJ = @NoBJ
        AND DateUsage IS NULL;
    `;

    await rqUpdate.query(updateSql);

    // 3) Kalau ada outputCode baru → ganti mapping
    let output = null;

    if (mappingInfo && rawOutputCode) {
      const { outputType, mappingTable } = mappingInfo;

      // Hapus semua mapping lama (asumsi 1 source per BJ)
      const rqDel = new sql.Request(tx);
      rqDel.input('NoBJ', sql.VarChar, noBJ);
      await rqDel.query(`
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

    // bentuk response mirip createPacking
    return {
      headers: [
        {
          NoBJ: noBJ,
          DateCreate: header.DateCreate || existingRes.recordset[0].DateCreate,
          IdBJ: header.IdBJ,
          Pcs: header.Pcs ?? existingRes.recordset[0].Pcs,
          Berat: header.Berat ?? existingRes.recordset[0].Berat,
          Jam: header.Jam ?? existingRes.recordset[0].Jam,
          IdWarehouse: header.IdWarehouse ?? existingRes.recordset[0].IdWarehouse,
          IsPartial: header.IsPartial ?? existingRes.recordset[0].IsPartial,
          Blok: header.Blok ?? existingRes.recordset[0].Blok,
          IdLokasi: header.IdLokasi ?? existingRes.recordset[0].IdLokasi,
        },
      ],
      output,
    };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}
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

  if (!noBJ) {
    throw badReq('NoBJ is required');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 1) Lock & cek record masih ada dan belum dipakai
    const rqCheck = new sql.Request(tx);
    rqCheck.input('NoBJ', sql.VarChar, noBJ);

    const existingRes = await rqCheck.query(`
      SELECT TOP 1 *
      FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBJ = @NoBJ
        AND DateUsage IS NULL;
    `);

    if (existingRes.recordset.length === 0) {
      const e = new Error(
        'BarangJadi not found or already used (DateUsage not NULL)'
      );
      e.statusCode = 404;
      throw e;
    }

    // 2) Hapus partial & mapping
    const rqDel = new sql.Request(tx);
    rqDel.input('NoBJ', sql.VarChar, noBJ);

    await rqDel.query(`
      DELETE FROM [dbo].[BarangJadiPartial]              WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[PackingProduksiOutputLabelBJ]   WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[InjectProduksiOutputBarangJadi] WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[BongkarSusunOutputBarangjadi]   WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[BJReturBarangJadi_d]            WHERE NoBJ = @NoBJ;
    `);

    // 3) Hapus header BarangJadi
    const rqDelHeader = new sql.Request(tx);
    rqDelHeader.input('NoBJ', sql.VarChar, noBJ);

    await rqDelHeader.query(`
      DELETE FROM [dbo].[BarangJadi]
      WHERE NoBJ = @NoBJ
        AND DateUsage IS NULL;
    `);

    await tx.commit();

    return {
      deleted: true,
      NoBJ: noBJ,
    };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}
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
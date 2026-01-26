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

const { generateNextCode } = require('../../../core/utils/sequence-code-helper'); 
const { badReq, conflict } = require('../../../core/utils/http-error'); 

function hasOwn(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj, key);
}



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

async function insertSingleBarangJadi({
  tx,
  header,
  idBJ,
  outputCode,
  outputType,
  mappingTable,
  effectiveDateCreate,
  nowDateTime,
}) {
  const gen = async () =>
    generateNextCode(tx, {
      tableName: 'dbo.BarangJadi',
      columnName: 'NoBJ',
      prefix: 'BA.',
      width: 10,
    });

  const generatedNo = await gen();

  const exist = await new sql.Request(tx)
    .input('NoBJ', sql.VarChar(50), generatedNo)
    .query(`
      SELECT 1
      FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBJ = @NoBJ
    `);

  let noBJ = generatedNo;

  if (exist.recordset.length > 0) {
    const retryNo = await gen();
    const exist2 = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar(50), retryNo)
      .query(`
        SELECT 1
        FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJ = @NoBJ
      `);

    if (exist2.recordset.length > 0) {
      throw conflict('Gagal generate NoBJ unik, coba lagi.');
    }
    noBJ = retryNo;
  }

  const insertHeaderSql = `
    INSERT INTO [dbo].[BarangJadi] (
      NoBJ,
      IdBJ,
      DateCreate,
      Jam,
      Pcs,
      Berat,
      IsPartial,
      DateUsage,
      IdWarehouse,
      CreateBy,
      DateTimeCreate,
      Blok,
      IdLokasi
    )
    VALUES (
      @NoBJ,
      @IdBJ,
      @DateCreate,
      @Jam,
      @Pcs,
      @Berat,
      @IsPartial,
      NULL,
      @IdWarehouse,
      @CreateBy,
      @DateTimeCreate,
      @Blok,
      @IdLokasi
    );
  `;

  await new sql.Request(tx)
    .input('NoBJ', sql.VarChar(50), noBJ)
    .input('IdBJ', sql.Int, idBJ)
    .input('DateCreate', sql.Date, effectiveDateCreate)
    .input('Jam', sql.VarChar(20), header.Jam ?? null)
    .input('Pcs', sql.Decimal(18, 3), header.Pcs ?? null)
    .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
    .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
    .input('IdWarehouse', sql.Int, header.IdWarehouse)
    .input('CreateBy', sql.VarChar(50), header.CreateBy)
    .input('DateTimeCreate', sql.DateTime, nowDateTime)
    .input('Blok', sql.VarChar(50), header.Blok ?? null)
    .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
    .query(insertHeaderSql);

  const rqMap = new sql.Request(tx)
    .input('OutputCode', sql.VarChar(50), outputCode)
    .input('NoBJ', sql.VarChar(50), noBJ);

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

  return {
    NoBJ: noBJ,
    DateCreate: formatYMD(effectiveDateCreate),
    IdBJ: idBJ,
    Jam: header.Jam ?? null,
    Pcs: header.Pcs ?? null,
    Berat: header.Berat ?? null,
    IsPartial: header.IsPartial ?? 0,
    DateUsage: null,
    IdWarehouse: header.IdWarehouse,
    CreateBy: header.CreateBy,
    DateTimeCreate: nowDateTime,
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
    OutputCode: outputCode,
    OutputType: outputType,
  };
}

async function createFromInjectMapping({
  tx,
  header,
  outputCode,
  mappingTable,
  outputType,
  effectiveDateCreate,
  nowDateTime,
}) {
  const injRes = await new sql.Request(tx)
    .input('NoProduksi', sql.VarChar(50), outputCode)
    .query(`
      SELECT TOP 1 IdCetakan, IdWarna, IdFurnitureMaterial
      FROM dbo.InjectProduksi_h WITH (UPDLOCK, HOLDLOCK)
      WHERE NoProduksi = @NoProduksi
        AND IdCetakan IS NOT NULL;
    `);

  if (!injRes.recordset.length) {
    throw badReq(`InjectProduksi_h ${outputCode} tidak ditemukan atau IdCetakan NULL`);
  }

  const inj = injRes.recordset[0];

  const mapRes = await new sql.Request(tx)
    .input('IdCetakan', sql.Int, inj.IdCetakan)
    .input('IdWarna', sql.Int, inj.IdWarna)
    .input('IdFurnitureMaterial', sql.Int, inj.IdFurnitureMaterial ?? 0)
    .query(`
      SELECT IdBarangJadi
      FROM dbo.CetakanWarnaToProduk_d
      WHERE IdCetakan = @IdCetakan
        AND IdWarna = @IdWarna
        AND (
          (IdFurnitureMaterial IS NULL AND @IdFurnitureMaterial = 0)
          OR IdFurnitureMaterial = @IdFurnitureMaterial
        );
    `);

  if (!mapRes.recordset.length) {
    throw badReq(
      `Mapping Produk tidak ditemukan untuk Inject ${outputCode} (IdCetakan=${inj.IdCetakan}, IdWarna=${inj.IdWarna})`
    );
  }

  const created = [];
  for (const row of mapRes.recordset) {
    created.push(
      await insertSingleBarangJadi({
        tx,
        header,
        idBJ: row.IdBarangJadi,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate,
        nowDateTime,
      })
    );
  }

  return created;
}

exports.createPacking = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = String(payload?.outputCode || '').trim();

  if (!outputCode) throw badReq('outputCode wajib diisi (BD., S., BG., L.)');
  if (!header.CreateBy) throw badReq('CreateBy wajib diisi (controller harus overwrite dari token)');

  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  let outputType = null;
  let mappingTable = null;

  if (outputCode.startsWith('BD.')) { outputType = 'PACKING'; mappingTable = 'PackingProduksiOutputLabelBJ'; }
  else if (outputCode.startsWith('S.')) { outputType = 'INJECT'; mappingTable = 'InjectProduksiOutputBarangJadi'; }
  else if (outputCode.startsWith('BG.')) { outputType = 'BONGKAR_SUSUN'; mappingTable = 'BongkarSusunOutputBarangjadi'; }
  else if (outputCode.startsWith('L.')) { outputType = 'RETUR'; mappingTable = 'BJReturBarangJadi_d'; }
  else throw badReq('outputCode prefix tidak dikenali (BD., S., BG., L.)');

  const isInject = outputType === 'INJECT';

  const idBJSingle = header.IdBJ ?? null;
  if (!isInject && !idBJSingle) throw badReq('IdBJ wajib diisi untuk mode non-INJECT');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate);
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create packing',
      useLock: true,
    });

    const needBlok = header.Blok == null || String(header.Blok).trim() === '';
    const needLokasi = header.IdLokasi == null;

    if (needBlok || needLokasi) {
      const lokasi = await getBlokLokasiFromKodeProduksi({ kode: outputCode, runner: tx });
      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    const nowDateTime = new Date();

    let headers = [];

    if (isInject && !idBJSingle) {
      headers = await createFromInjectMapping({
        tx,
        header,
        outputCode,
        mappingTable,
        outputType,
        effectiveDateCreate,
        nowDateTime,
      });
    } else {
      const created = await insertSingleBarangJadi({
        tx,
        header,
        idBJ: idBJSingle,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate,
        nowDateTime,
      });
      headers = [created];
    }

    await tx.commit();

    return {
      headers,
      output: {
        code: outputCode,
        type: outputType,
        mappingTable,
        isMulti: headers.length > 1,
        count: headers.length,
      },
      audit: { actorId, requestId },
    };
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
async function deleteAllMappingsBJ(tx, noBJ) {
  await new sql.Request(tx)
    .input('NoBJ', sql.VarChar(50), noBJ)
    .query(`
      DELETE FROM [dbo].[PackingProduksiOutputLabelBJ] WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[InjectProduksiOutputBarangJadi] WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[BongkarSusunOutputBarangjadi] WHERE NoBJ = @NoBJ;
      DELETE FROM [dbo].[BJReturBarangJadi_d] WHERE NoBJ = @NoBJ;
    `);
}

exports.updatePacking = async (noBJ, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const hasOutputCodeField = hasOwn(payload, 'outputCode');
  const outputCode = String(payload?.outputCode || '').trim();

  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const existingRes = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar(50), noBJ)
      .query(`
        SELECT TOP 1
          NoBJ,
          CONVERT(date, DateCreate) AS DateCreate,
          Jam,
          Pcs,
          IdBJ,
          Berat,
          IsPartial,
          DateUsage,
          IdWarehouse,
          CreateBy,
          DateTimeCreate,
          Blok,
          IdLokasi
        FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJ = @NoBJ;
      `);

    if (existingRes.recordset.length === 0) {
      throw notFound('Barang Jadi not found');
    }

    const current = existingRes.recordset[0];

    const existingDateCreate = current.DateCreate ? toDateOnly(current.DateCreate) : null;

    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: 'update packing',
      useLock: true,
    });

    const merged = {
      IdBJ: header.IdBJ ?? current.IdBJ,

      Jam: hasOwn(header, 'Jam') ? header.Jam : current.Jam,
      Pcs: hasOwn(header, 'Pcs') ? header.Pcs : current.Pcs,
      Berat: hasOwn(header, 'Berat') ? header.Berat : current.Berat,
      IsPartial: hasOwn(header, 'IsPartial') ? header.IsPartial : current.IsPartial,
      IdWarehouse: hasOwn(header, 'IdWarehouse') ? header.IdWarehouse : current.IdWarehouse,
      Blok: hasOwn(header, 'Blok') ? header.Blok : current.Blok,
      IdLokasi: hasOwn(header, 'IdLokasi') ? header.IdLokasi : current.IdLokasi,

      DateCreate: hasOwn(header, 'DateCreate') ? header.DateCreate : current.DateCreate,

      CreateBy: hasOwn(header, 'CreateBy') ? header.CreateBy : current.CreateBy,
    };

    if (!merged.IdBJ) throw badReq('IdBJ cannot be empty');

    let dateCreateParam = null;

    if (hasOwn(header, 'DateCreate')) {
      if (header.DateCreate === null || header.DateCreate === '') {
        dateCreateParam = toDateOnly(new Date());
      } else {
        dateCreateParam = toDateOnly(header.DateCreate);
        if (!dateCreateParam) {
          throw badReq('Invalid DateCreate');
        }
      }

      await assertNotLocked({
        date: dateCreateParam,
        runner: tx,
        action: 'update packing (DateCreate)',
        useLock: true,
      });
    }

    const rqUpdate = new sql.Request(tx)
      .input('NoBJ', sql.VarChar(50), noBJ)
      .input('IdBJ', sql.Int, merged.IdBJ)
      .input('Jam', sql.VarChar(20), merged.Jam ?? null)
      .input('Pcs', sql.Decimal(18, 3), merged.Pcs ?? null)
      .input('Berat', sql.Decimal(18, 3), merged.Berat ?? null)
      .input('IsPartial', sql.Bit, merged.IsPartial ?? 0)
      .input('IdWarehouse', sql.Int, merged.IdWarehouse)
      .input('Blok', sql.VarChar(50), merged.Blok ?? null)
      .input('IdLokasi', sql.Int, merged.IdLokasi ?? null)
      .input('CreateBy', sql.VarChar(50), merged.CreateBy ?? null);

    if (hasOwn(header, 'DateCreate')) {
      rqUpdate.input('DateCreate', sql.Date, dateCreateParam);
    }

    const updateSql = `
      UPDATE [dbo].[BarangJadi]
      SET
        IdBJ = @IdBJ,
        Jam = @Jam,
        Pcs = @Pcs,
        Berat = @Berat,
        IsPartial = @IsPartial,
        IdWarehouse = @IdWarehouse,
        Blok = @Blok,
        IdLokasi = @IdLokasi,
        CreateBy = @CreateBy
        ${hasOwn(header, 'DateCreate') ? ', DateCreate = @DateCreate' : ''}
      WHERE NoBJ = @NoBJ;
    `;
    await rqUpdate.query(updateSql);

    let outputType = null;
    let mappingTable = null;

    if (hasOutputCodeField) {
      if (!outputCode) {
        await deleteAllMappingsBJ(tx, noBJ);
      } else {
        if (outputCode.startsWith('BD.')) { outputType = 'PACKING'; mappingTable = 'PackingProduksiOutputLabelBJ'; }
        else if (outputCode.startsWith('S.')) { outputType = 'INJECT'; mappingTable = 'InjectProduksiOutputBarangJadi'; }
        else if (outputCode.startsWith('BG.')) { outputType = 'BONGKAR_SUSUN'; mappingTable = 'BongkarSusunOutputBarangjadi'; }
        else if (outputCode.startsWith('L.')) { outputType = 'RETUR'; mappingTable = 'BJReturBarangJadi_d'; }
        else throw badReq('outputCode prefix not recognized (supported: BD., S., BG., L.)');

        await deleteAllMappingsBJ(tx, noBJ);

        const rqMap = new sql.Request(tx)
          .input('OutputCode', sql.VarChar(50), outputCode)
          .input('NoBJ', sql.VarChar(50), noBJ);

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
      }
    }

    await tx.commit();

    return {
      header: {
        NoBJ: noBJ,
        DateCreate: hasOwn(header, 'DateCreate')
          ? (dateCreateParam ? formatYMD(dateCreateParam) : null)
          : formatYMD(current.DateCreate),
        Jam: merged.Jam ?? null,
        Pcs: merged.Pcs ?? null,
        IdBJ: merged.IdBJ,
        Berat: merged.Berat ?? null,
        IsPartial: merged.IsPartial ?? 0,
        IdWarehouse: merged.IdWarehouse,
        CreateBy: merged.CreateBy ?? null,
        Blok: merged.Blok ?? null,
        IdLokasi: merged.IdLokasi ?? null,
      },
      output: hasOutputCodeField
        ? { code: outputCode || null, type: outputType, mappingTable }
        : undefined,
      audit: { actorId, requestId },
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
exports.deletePacking = async (noBJ, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  if (!noBJ) throw badReq('NoBJ is required');

  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const existingRes = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar(50), noBJ)
      .query(`
        SELECT TOP 1
          NoBJ,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage
        FROM [dbo].[BarangJadi] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJ = @NoBJ;
      `);

    if (existingRes.recordset.length === 0) {
      throw notFound('Barang Jadi not found');
    }

    const current = existingRes.recordset[0];

    const trxDate = current.DateCreate ? toDateOnly(current.DateCreate) : null;

    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: 'delete packing',
      useLock: true,
    });

    await new sql.Request(tx)
      .input('NoBJ', sql.VarChar(50), noBJ)
      .query(`
        DELETE FROM [dbo].[BarangJadiPartial] WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[PackingProduksiOutputLabelBJ] WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[InjectProduksiOutputBarangJadi] WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[BongkarSusunOutputBarangjadi] WHERE NoBJ = @NoBJ;
        DELETE FROM [dbo].[BJReturBarangJadi_d] WHERE NoBJ = @NoBJ;
      `);

    const delRes = await new sql.Request(tx)
      .input('NoBJ', sql.VarChar(50), noBJ)
      .query(`
        DELETE FROM [dbo].[BarangJadi]
        WHERE NoBJ = @NoBJ;
      `);

    await tx.commit();

    if ((delRes.rowsAffected?.[0] ?? 0) === 0) {
      throw notFound('Barang Jadi not found');
    }

    return {
      deleted: true,
      NoBJ: noBJ,
      audit: { actorId, requestId },
    };
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
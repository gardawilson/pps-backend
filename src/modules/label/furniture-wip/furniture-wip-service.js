// services/labels/furniture-wip-service.js
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
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama AS NamaFurnitureWIP,

      -- 🔹 Pcs sudah dikurangi partial (jika IsPartial = 1)
      CASE 
        WHEN f.IsPartial = 1 THEN
          CASE
            WHEN ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0) < 0 
              THEN 0
            ELSE ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0)
          END
        ELSE ISNULL(f.Pcs, 0)
      END AS Pcs,

      ISNULL(f.Berat, 0) AS Berat,

      f.IsPartial,
      f.IdWarna,
      f.Blok,
      f.IdLokasi,

      -- 🔗 TIPE SUMBER (HOTSTAMPING / PASANG_KUNCI / BONGKAR_SUSUN / RETUR / SPANNER / INJECT)
      CASE
        WHEN MAX(hsmap.NoProduksi)     IS NOT NULL THEN 'HOTSTAMPING'
        WHEN MAX(pkmap.NoProduksi)     IS NOT NULL THEN 'PASANG_KUNCI'
        WHEN MAX(bsmap.NoBongkarSusun) IS NOT NULL THEN 'BONGKAR_SUSUN'
        WHEN MAX(retmap.NoRetur)       IS NOT NULL THEN 'RETUR'
        WHEN MAX(spmap.NoProduksi)     IS NOT NULL THEN 'SPANNER'
        WHEN MAX(injmap.NoProduksi)    IS NOT NULL THEN 'INJECT'
        ELSE NULL
      END AS OutputType,

      -- 🔗 KODE SUMBER (BH./BI./BG./L./BJ./S.)
      MAX(
        COALESCE(
          hsmap.NoProduksi,
          pkmap.NoProduksi,
          spmap.NoProduksi,
          injmap.NoProduksi,
          bsmap.NoBongkarSusun,
          retmap.NoRetur
        )
      ) AS OutputCode,

      -- 🔗 NAMA MESIN / NAMA PEMBELI / 'Bongkar Susun'
      MAX(
        COALESCE(
          mHs.NamaMesin,
          mPk.NamaMesin,
          mSp.NamaMesin,
          mInj.NamaMesin,
          CASE WHEN bsmap.NoBongkarSusun IS NOT NULL THEN 'Bongkar Susun' END,
          pemb.NamaPembeli
        )
      ) AS OutputNamaMesin

    FROM [dbo].[FurnitureWIP] f

    -- 🔹 Aggregate partial per NoFurnitureWIP
    LEFT JOIN (
      SELECT
        NoFurnitureWIP,
        SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
      FROM [dbo].[FurnitureWIPPartial]
      GROUP BY NoFurnitureWIP
    ) fp
      ON fp.NoFurnitureWIP = f.NoFurnitureWIP

    -- 🔗 Master nama furniture WIP
    LEFT JOIN [dbo].[MstCabinetWIP] cw
      ON cw.IdCabinetWIP = f.IdFurnitureWIP

    ----------------------------------------------------------------------
    -- 🔗 MAPPING HOT STAMPING (BH.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[HotStampingOutputLabelFWIP] hsmap
           ON hsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[HotStamping_h] hsh
           ON hsh.NoProduksi = hsmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHs
           ON mHs.IdMesin = hsh.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING PASANG KUNCI (BI.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[PasangKunciOutputLabelFWIP] pkmap
           ON pkmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[PasangKunci_h] pkh
           ON pkh.NoProduksi = pkmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mPk
           ON mPk.IdMesin = pkh.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING BONGKAR SUSUN (BG.)
    --     Tidak ada mesin → NamaMesin = 'Bongkar Susun'
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[BongkarSusunOutputFurnitureWIP] bsmap
           ON bsmap.NoFurnitureWIP = f.NoFurnitureWIP

    ----------------------------------------------------------------------
    -- 🔗 MAPPING RETUR (L.) → pakai NamaPembeli
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[BJReturFurnitureWIP_d] retmap
           ON retmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJRetur_h] bjh
           ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb
           ON pemb.IdPembeli = bjh.IdPembeli

    ----------------------------------------------------------------------
    -- 🔗 MAPPING SPANNER (BJ.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[SpannerOutputLabelFWIP] spmap
           ON spmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[Spanner_h] sph
           ON sph.NoProduksi = spmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSp
           ON mSp.IdMesin = sph.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING INJECT PRODUKSI (S.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[InjectProduksiOutputFurnitureWIP] injmap
           ON injmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[InjectProduksi_h] injh
           ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj
           ON mInj.IdMesin = injh.IdMesin

    WHERE 1=1
      AND f.DateUsage IS NULL
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search

               -- cari berdasarkan kode sumber
               OR ISNULL(hsmap.NoProduksi,'')     LIKE @search
               OR ISNULL(pkmap.NoProduksi,'')     LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'') LIKE @search
               OR ISNULL(retmap.NoRetur,'')       LIKE @search
               OR ISNULL(spmap.NoProduksi,'')     LIKE @search
               OR ISNULL(injmap.NoProduksi,'')    LIKE @search

               -- cari berdasarkan nama mesin / pembeli
               OR ISNULL(mHs.NamaMesin,'')        LIKE @search
               OR ISNULL(mPk.NamaMesin,'')        LIKE @search
               OR ISNULL(mSp.NamaMesin,'')        LIKE @search
               OR ISNULL(mInj.NamaMesin,'')       LIKE @search
               OR ISNULL(pemb.NamaPembeli,'')     LIKE @search
             )`
          : ''
      }
    GROUP BY
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama,
      f.Pcs,
      f.Berat,
      f.IsPartial,
      f.IdWarna,
      f.Blok,
      f.IdLokasi
    ORDER BY f.NoFurnitureWIP DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT f.NoFurnitureWIP) AS total
    FROM [dbo].[FurnitureWIP] f
    LEFT JOIN [dbo].[MstCabinetWIP] cw
      ON cw.IdCabinetWIP = f.IdFurnitureWIP

    LEFT JOIN [dbo].[HotStampingOutputLabelFWIP] hsmap
           ON hsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[HotStamping_h] hsh
           ON hsh.NoProduksi = hsmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHs
           ON mHs.IdMesin = hsh.IdMesin

    LEFT JOIN [dbo].[PasangKunciOutputLabelFWIP] pkmap
           ON pkmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[PasangKunci_h] pkh
           ON pkh.NoProduksi = pkmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mPk
           ON mPk.IdMesin = pkh.IdMesin

    LEFT JOIN [dbo].[BongkarSusunOutputFurnitureWIP] bsmap
           ON bsmap.NoFurnitureWIP = f.NoFurnitureWIP

    LEFT JOIN [dbo].[BJReturFurnitureWIP_d] retmap
           ON retmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJRetur_h] bjh
           ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb
           ON pemb.IdPembeli = bjh.IdPembeli

    LEFT JOIN [dbo].[SpannerOutputLabelFWIP] spmap
           ON spmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[Spanner_h] sph
           ON sph.NoProduksi = spmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSp
           ON mSp.IdMesin = sph.IdMesin

    LEFT JOIN [dbo].[InjectProduksiOutputFurnitureWIP] injmap
           ON injmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[InjectProduksi_h] injh
           ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj
           ON mInj.IdMesin = injh.IdMesin

    WHERE 1=1
      AND f.DateUsage IS NULL
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search
               OR ISNULL(hsmap.NoProduksi,'')     LIKE @search
               OR ISNULL(pkmap.NoProduksi,'')     LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'') LIKE @search
               OR ISNULL(retmap.NoRetur,'')       LIKE @search
               OR ISNULL(spmap.NoProduksi,'')     LIKE @search
               OR ISNULL(injmap.NoProduksi,'')    LIKE @search
               OR ISNULL(mHs.NamaMesin,'')        LIKE @search
               OR ISNULL(mPk.NamaMesin,'')        LIKE @search
               OR ISNULL(mSp.NamaMesin,'')        LIKE @search
               OR ISNULL(mInj.NamaMesin,'')       LIKE @search
               OR ISNULL(pemb.NamaPembeli,'')     LIKE @search
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




function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// Generate next NoFurnitureWIP: e.g. 'BB.0000000002'
async function generateNextNoFurnitureWip(
  tx,
  { prefix = 'BB.', width = 10 } = {}
) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 f.NoFurnitureWIP
    FROM [dbo].[FurnitureWIP] AS f WITH (UPDLOCK, HOLDLOCK)
    WHERE f.NoFurnitureWIP LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(f.NoFurnitureWIP, LEN(@prefix) + 1, 50)) DESC,
             f.NoFurnitureWIP DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoFurnitureWIP;
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

/**
 * Helper: insert 1 row FurnitureWIP + mapping ke table output (HotStamp / BI / BG / L / BJ / S)
 */
async function insertSingleFurnitureWip({
  tx,
  header,
  idFurnitureWip,
  outputCode,
  outputType,
  mappingTable,
}) {
  // 1) Generate NoFurnitureWIP
  const generatedNo = await generateNextNoFurnitureWip(tx, {
    prefix: 'BB.', // ganti kalau prefix NoFurnitureWIP kamu berbeda
    width: 10,
  });

  // Double-check uniqueness (rare)
  const rqCheck = new sql.Request(tx);
  const exist = await rqCheck
    .input('NoFurnitureWIP', sql.VarChar, generatedNo)
    .query(`
      SELECT 1
      FROM [dbo].[FurnitureWIP] WITH (UPDLOCK, HOLDLOCK)
      WHERE NoFurnitureWIP = @NoFurnitureWIP
    `);

  const noFurnitureWip =
    exist.recordset.length > 0
      ? await generateNextNoFurnitureWip(tx, { prefix: 'BB.', width: 10 })
      : generatedNo;

  // 2) Insert header ke dbo.FurnitureWIP
  const nowDateOnly = header.DateCreate || null; // null -> GETDATE() (date only)
  const insertHeaderSql = `
    INSERT INTO [dbo].[FurnitureWIP] (
      NoFurnitureWIP,
      DateCreate,
      Pcs,
      IdFurnitureWIP,
      Berat,
      IsPartial,
      DateUsage,
      IdWarna,
      CreateBy,
      DateTimeCreate,
      Blok,
      IdLokasi
    )
    VALUES (
      @NoFurnitureWIP,
      ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
      @Pcs,
      @IdFurnitureWIP,
      @Berat,
      @IsPartial,
      NULL,
      @IdWarna,
      @CreateBy,
      GETDATE(),
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
    .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
    .input('Pcs', sql.Decimal(18, 3), header.Pcs ?? null)
    .input('IdFurnitureWIP', sql.Int, idFurnitureWip)
    .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
    .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
    .input('IdWarna', sql.Int, header.IdWarna ?? null)
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
    .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip);

  if (mappingTable === 'HotStampingOutputLabelFWIP') {
    const q = `
      INSERT INTO [dbo].[HotStampingOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'PasangKunciOutputLabelFWIP') {
    const q = `
      INSERT INTO [dbo].[PasangKunciOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'BongkarSusunOutputFurnitureWIP') {
    const q = `
      INSERT INTO [dbo].[BongkarSusunOutputFurnitureWIP] (NoBongkarSusun, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'BJReturFurnitureWIP_d') {
    const q = `
      INSERT INTO [dbo].[BJReturFurnitureWIP_d] (NoRetur, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'SpannerOutputLabelFWIP') {
    const q = `
      INSERT INTO [dbo].[SpannerOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `;
    await rqMap.query(q);
  } else if (mappingTable === 'InjectProduksiOutputFurnitureWIP') {
    const q = `
      INSERT INTO [dbo].[InjectProduksiOutputFurnitureWIP] (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `;
    await rqMap.query(q);
  }

  // Return header shape yang dikirim ke controller
  return {
    NoFurnitureWIP: noFurnitureWip,
    DateCreate: nowDateOnly || 'GETDATE()',
    Pcs: header.Pcs ?? null,
    IdFurnitureWIP: idFurnitureWip,
    Berat: header.Berat ?? null,
    IsPartial: header.IsPartial ?? 0,
    IdWarna: header.IdWarna ?? null,
    CreateBy: header.CreateBy ?? null,
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
    OutputCode: outputCode,
    OutputType: outputType,
  };
}

/**
 * Helper khusus: INJECT tanpa IdFurnitureWIP → multi-label
 * - Cari InjectProduksi_h by NoProduksi
 * - Cari semua mapping di CetakanWarnaToFurnitureWIP_d
 * - Loop insertSingleFurnitureWip untuk tiap IdFurnitureWIP
 */
async function createFromInjectMapping({
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

  // 2) Ambil mapping ke FurnitureWIP
  const rqMap = new sql.Request(tx);
  rqMap
    .input('IdCetakan', sql.Int, inj.IdCetakan)
    .input('IdWarna', sql.Int, inj.IdWarna)
    .input('IdFurnitureMaterial', sql.Int, inj.IdFurnitureMaterial ?? 0);

  const mapRes = await rqMap.query(`
    SELECT IdFurnitureWIP
    FROM dbo.CetakanWarnaToFurnitureWIP_d
    WHERE IdCetakan = @IdCetakan
      AND IdWarna = @IdWarna
      AND (
        (IdFurnitureMaterial IS NULL AND @IdFurnitureMaterial = 0)
        OR IdFurnitureMaterial = @IdFurnitureMaterial
      );
  `);

  if (mapRes.recordset.length === 0) {
    throw badReq(
      `No FurnitureWIP mapping found for Inject ${outputCode} (IdCetakan=${inj.IdCetakan}, IdWarna=${inj.IdWarna})`
    );
  }

  const createdHeaders = [];

  for (const row of mapRes.recordset) {
    const idFwip = row.IdFurnitureWIP;

    const created = await insertSingleFurnitureWip({
      tx,
      header,
      idFurnitureWip: idFwip,
      outputCode,
      outputType,
      mappingTable,
      effectiveDateCreate, // ✅ teruskan date-only UTC
    });

    createdHeaders.push(created);
  }

  return createdHeaders;
}


exports.createFurnitureWip = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = (payload?.outputCode || '').toString().trim();

  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  if (!outputCode) {
    throw badReq('outputCode is required (BH., BI., BG., L., BJ., S., etc.)');
  }

  let outputType = null;
  let mappingTable = null;

  if (outputCode.startsWith('BH.')) { outputType = 'HOTSTAMPING';   mappingTable = 'HotStampingOutputLabelFWIP'; }
  else if (outputCode.startsWith('BI.')) { outputType = 'PASANG_KUNCI'; mappingTable = 'PasangKunciOutputLabelFWIP'; }
  else if (outputCode.startsWith('BG.')) { outputType = 'BONGKAR_SUSUN'; mappingTable = 'BongkarSusunOutputFurnitureWIP'; }
  else if (outputCode.startsWith('L.'))  { outputType = 'RETUR';        mappingTable = 'BJReturFurnitureWIP_d'; }
  else if (outputCode.startsWith('BJ.')) { outputType = 'SPANNER';      mappingTable = 'SpannerOutputLabelFWIP'; }
  else if (outputCode.startsWith('S.'))  { outputType = 'INJECT';       mappingTable = 'InjectProduksiOutputFurnitureWIP'; }
  else {
    throw badReq('outputCode prefix not recognized (supported: BH., BI., BG., L., BJ., S.)');
  }

  const isInject = outputType === 'INJECT';

  if (!header.IdFurnitureWIP && !isInject) {
    throw badReq('IdFurnitureWIP is required for non-INJECT modes');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE) - UTC
    // ===============================
    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate); // ✅ UTC date-only
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create furniture wip',
      useLock: true,
    });

    // 0) Auto-isi Blok & IdLokasi dari kode produksi / bongkar susun (jika header belum isi)
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

    let result;

    if (isInject && !header.IdFurnitureWIP) {
      // INJECT + IdFurnitureWIP null → multi-create
      const createdHeaders = await createFromInjectMapping({
        tx,
        header,
        outputCode,
        mappingTable,
        outputType,
        badReq,
        effectiveDateCreate, // ✅ pass date-only UTC
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
      // normal single create
      const createdHeader = await insertSingleFurnitureWip({
        tx,
        header,
        idFurnitureWip: header.IdFurnitureWIP,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate, // ✅ pass date-only UTC
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




  const hasOwn = (obj, key) =>
    Object.prototype.hasOwnProperty.call(obj || {}, key);
  
  /**
   * Hapus semua mapping FurnitureWIP ke proses manapun
   */
  async function deleteAllMappings(tx, noFurnitureWip) {
    await new sql.Request(tx)
      .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
      .query(`
        DELETE FROM [dbo].[HotStampingOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[PasangKunciOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[BongkarSusunOutputFurnitureWIP]       WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[BJReturFurnitureWIP_d]                WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[SpannerOutputLabelFWIP]               WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[InjectProduksiOutputFurnitureWIP]     WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);
  }
  
  exports.updateFurnitureWip = async (noFurnitureWip, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const hasOutputCodeField = hasOwn(payload, 'outputCode');
  const outputCode = (payload?.outputCode || '').toString().trim();

  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // 1) Ambil data existing + lock (ambil DateCreate juga, date-only)
    // ===============================
    const existingRes = await new sql.Request(tx)
      .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
      .query(`
        SELECT TOP 1
          NoFurnitureWIP,
          CONVERT(date, DateCreate) AS DateCreate,
          Pcs,
          IdFurnitureWIP,
          Berat,
          IsPartial,
          DateUsage,
          IdWarna,
          CreateBy,
          DateTimeCreate,
          Blok,
          IdLokasi
        FROM [dbo].[FurnitureWIP] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    if (existingRes.recordset.length === 0) {
      const e = new Error('Furniture WIP not found');
      e.statusCode = 404;
      throw e;
    }

    const current = existingRes.recordset[0];

    // ===============================
    // 1b) TUTUP TRANSAKSI CHECK (UPDATE) - selalu cek tanggal existing
    // ===============================
    const existingDateCreate = current.DateCreate ? toDateOnly(current.DateCreate) : null;

    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: 'update furniture wip',
      useLock: true,
    });

    // ===============================
    // 2) Merge field (partial update)
    // ===============================
    const merged = {
      IdFurnitureWIP: header.IdFurnitureWIP ?? current.IdFurnitureWIP,
      Pcs: hasOwn(header, 'Pcs') ? header.Pcs : current.Pcs,
      Berat: hasOwn(header, 'Berat') ? header.Berat : current.Berat,
      IsPartial: hasOwn(header, 'IsPartial') ? header.IsPartial : current.IsPartial,
      IdWarna: hasOwn(header, 'IdWarna') ? header.IdWarna : current.IdWarna,
      Blok: hasOwn(header, 'Blok') ? header.Blok : current.Blok,
      IdLokasi: hasOwn(header, 'IdLokasi') ? header.IdLokasi : current.IdLokasi,

      // ⚠️ DateCreate bisa diupdate, tapi harus lolos tutup transaksi
      DateCreate: hasOwn(header, 'DateCreate') ? header.DateCreate : current.DateCreate,

      CreateBy: hasOwn(header, 'CreateBy') ? header.CreateBy : current.CreateBy,
    };

    if (!merged.IdFurnitureWIP) {
      throw badReq('IdFurnitureWIP cannot be empty');
    }

    // normalize IdLokasi (VarChar)
    let idLokasiVal = merged.IdLokasi;
    if (idLokasiVal !== undefined && idLokasiVal !== null) {
      idLokasiVal = String(idLokasiVal).trim();
      if (idLokasiVal.length === 0) idLokasiVal = null;
    }

    // ===============================
    // 2b) Jika DateCreate dikirim user, cek tutup transaksi untuk tanggal baru juga
    // ===============================
    let dateCreateParam = null;

    if (hasOwn(header, 'DateCreate')) {
      // behaviour: null/'' => reset to today (UTC)
      if (header.DateCreate === null || header.DateCreate === '') {
        dateCreateParam = toDateOnly(new Date());
      } else {
        dateCreateParam = toDateOnly(header.DateCreate);
        if (!dateCreateParam) {
          const e = new Error('Invalid DateCreate');
          e.statusCode = 400;
          e.meta = { field: 'DateCreate', value: header.DateCreate };
          throw e;
        }
      }

      await assertNotLocked({
        date: dateCreateParam,
        runner: tx,
        action: 'update furniture wip (DateCreate)',
        useLock: true,
      });
    }

    // ===============================
    // 3) UPDATE header
    // ===============================
    const rqUpdate = new sql.Request(tx);
    rqUpdate
      .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
      .input('IdFurnitureWIP', sql.Int, merged.IdFurnitureWIP)
      .input('Pcs', sql.Decimal(18, 3), merged.Pcs ?? null)
      .input('Berat', sql.Decimal(18, 3), merged.Berat ?? null)
      .input('IsPartial', sql.Bit, merged.IsPartial ?? 0)
      .input('IdWarna', sql.Int, merged.IdWarna ?? null)
      .input('Blok', sql.VarChar, merged.Blok ?? null)
      .input('IdLokasi', sql.VarChar, idLokasiVal)
      .input('CreateBy', sql.VarChar, merged.CreateBy ?? null);

    // kalau user update DateCreate, pakai parameter UTC date-only
    if (hasOwn(header, 'DateCreate')) {
      rqUpdate.input('DateCreate', sql.Date, dateCreateParam);
    }

    const updateSql = `
      UPDATE [dbo].[FurnitureWIP]
      SET
        IdFurnitureWIP = @IdFurnitureWIP,
        Pcs = @Pcs,
        Berat = @Berat,
        IsPartial = @IsPartial,
        IdWarna = @IdWarna,
        Blok = @Blok,
        IdLokasi = @IdLokasi,
        CreateBy = @CreateBy
        ${hasOwn(header, 'DateCreate') ? ', DateCreate = @DateCreate' : ''}
      WHERE NoFurnitureWIP = @NoFurnitureWIP;
    `;
    await rqUpdate.query(updateSql);

    // ===============================
    // 4) Mapping update (optional)
    // ===============================
    let outputType = null;
    let mappingTable = null;

    if (hasOutputCodeField) {
      if (!outputCode) {
        await deleteAllMappings(tx, noFurnitureWip);
      } else {
        if (outputCode.startsWith('BH.'))      { outputType = 'HOTSTAMPING';   mappingTable = 'HotStampingOutputLabelFWIP'; }
        else if (outputCode.startsWith('BI.')) { outputType = 'PASANG_KUNCI';  mappingTable = 'PasangKunciOutputLabelFWIP'; }
        else if (outputCode.startsWith('BG.')) { outputType = 'BONGKAR_SUSUN'; mappingTable = 'BongkarSusunOutputFurnitureWIP'; }
        else if (outputCode.startsWith('L.'))  { outputType = 'RETUR';         mappingTable = 'BJReturFurnitureWIP_d'; }
        else if (outputCode.startsWith('BJ.')) { outputType = 'SPANNER';       mappingTable = 'SpannerOutputLabelFWIP'; }
        else if (outputCode.startsWith('S.'))  { outputType = 'INJECT';        mappingTable = 'InjectProduksiOutputFurnitureWIP'; }
        else {
          throw badReq('outputCode prefix not recognized (supported: BH., BI., BG., L., BJ., S.)');
        }

        await deleteAllMappings(tx, noFurnitureWip);

        if (mappingTable === 'HotStampingOutputLabelFWIP') {
          await new sql.Request(tx)
            .input('OutputCode', sql.VarChar, outputCode)
            .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
            .query(`
              INSERT INTO [dbo].[HotStampingOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
              VALUES (@OutputCode, @NoFurnitureWIP);
            `);
        } else if (mappingTable === 'PasangKunciOutputLabelFWIP') {
          await new sql.Request(tx)
            .input('OutputCode', sql.VarChar, outputCode)
            .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
            .query(`
              INSERT INTO [dbo].[PasangKunciOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
              VALUES (@OutputCode, @NoFurnitureWIP);
            `);
        } else if (mappingTable === 'BongkarSusunOutputFurnitureWIP') {
          await new sql.Request(tx)
            .input('OutputCode', sql.VarChar, outputCode)
            .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
            .query(`
              INSERT INTO [dbo].[BongkarSusunOutputFurnitureWIP] (NoBongkarSusun, NoFurnitureWIP)
              VALUES (@OutputCode, @NoFurnitureWIP);
            `);
        } else if (mappingTable === 'BJReturFurnitureWIP_d') {
          await new sql.Request(tx)
            .input('OutputCode', sql.VarChar, outputCode)
            .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
            .query(`
              INSERT INTO [dbo].[BJReturFurnitureWIP_d] (NoRetur, NoFurnitureWIP)
              VALUES (@OutputCode, @NoFurnitureWIP);
            `);
        } else if (mappingTable === 'SpannerOutputLabelFWIP') {
          await new sql.Request(tx)
            .input('OutputCode', sql.VarChar, outputCode)
            .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
            .query(`
              INSERT INTO [dbo].[SpannerOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
              VALUES (@OutputCode, @NoFurnitureWIP);
            `);
        } else if (mappingTable === 'InjectProduksiOutputFurnitureWIP') {
          await new sql.Request(tx)
            .input('OutputCode', sql.VarChar, outputCode)
            .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
            .query(`
              INSERT INTO [dbo].[InjectProduksiOutputFurnitureWIP] (NoProduksi, NoFurnitureWIP)
              VALUES (@OutputCode, @NoFurnitureWIP);
            `);
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoFurnitureWIP: noFurnitureWip,
        DateCreate: hasOwn(header, 'DateCreate')
          ? (dateCreateParam ? dateCreateParam : null)
          : current.DateCreate,
        Pcs: merged.Pcs,
        IdFurnitureWIP: merged.IdFurnitureWIP,
        Berat: merged.Berat,
        IsPartial: merged.IsPartial,
        IdWarna: merged.IdWarna,
        CreateBy: merged.CreateBy,
        Blok: merged.Blok,
        IdLokasi: merged.IdLokasi,
      },
      output: hasOutputCodeField
        ? { code: outputCode || null, type: outputType, mappingTable }
        : undefined,
    };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};




exports.deleteFurnitureWip = async (noFurnitureWip) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoFurnitureWIP = (noFurnitureWip || '').toString().trim();

  const notFound = () => {
    const e = new Error('Furniture WIP not found');
    e.statusCode = 404;
    return e;
  };

  if (!NoFurnitureWIP) {
    const e = new Error('NoFurnitureWIP is required');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // 1) Lock header + ambil DateCreate + DateUsage (untuk guard)
    // ===============================
    const headRes = await new sql.Request(tx)
      .input('NoFurnitureWIP', sql.VarChar, NoFurnitureWIP)
      .query(`
        SELECT TOP 1
          NoFurnitureWIP,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage
        FROM [dbo].[FurnitureWIP] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    if (headRes.recordset.length === 0) throw notFound();

    const head = headRes.recordset[0];

    // ===============================
    // 1b) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    const trxDate = head.DateCreate ? toDateOnly(head.DateCreate) : null;

    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: 'delete furniture wip',
      useLock: true,
    });

    // ===============================
    // 1c) Guard: jika sudah dipakai, jangan boleh delete
    // (kalau rule kamu memang begitu)
    // ===============================
    if (head.DateUsage) {
      const e = new Error('Cannot delete: FurnitureWIP already used (DateUsage IS NOT NULL).');
      e.statusCode = 409;
      e.code = 'FWIP_ALREADY_USED';
      throw e;
    }

    // ===============================
    // 2) Hapus semua mapping (BH/BI/BG/L/BJ/S)
    // ===============================
    await deleteAllMappings(tx, NoFurnitureWIP);

    // ===============================
    // 3) Hapus partial (kalau ada)
    // ===============================
    await new sql.Request(tx)
      .input('NoFurnitureWIP', sql.VarChar, NoFurnitureWIP)
      .query(`
        DELETE FROM [dbo].[FurnitureWIPPartial]
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    // ===============================
    // 4) Hapus header
    // ===============================
    const delHead = await new sql.Request(tx)
      .input('NoFurnitureWIP', sql.VarChar, NoFurnitureWIP)
      .query(`
        DELETE FROM [dbo].[FurnitureWIP]
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    if ((delHead.rowsAffected?.[0] ?? 0) === 0) {
      throw notFound();
    }

    await tx.commit();

    return {
      noFurnitureWip: NoFurnitureWIP,
      deleted: true,
    };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}

    // FK constraint (kalau ada relasi lain yang belum kamu delete)
    if (err.number === 547) {
      err.statusCode = 409;
      err.message = err.message || 'Delete failed due to foreign key constraint.';
    }

    throw err;
  }
};



/**
 * Ambil info partial FurnitureWIP per NoFurnitureWIP.
 *
 * Tabel yang dipakai:
 * - dbo.FurnitureWIPPartial                      (Base partial, Pcs)
 * - dbo.InjectProduksiInputFurnitureWIPPartial   (konsumsi partial -> NoProduksi)
 * - dbo.InjectProduksi_h                         (header produksi inject)
 * - dbo.MstMesin                                 (nama mesin)
 */
exports.getPartialInfoByFurnitureWip = async (noFurnitureWip) => {
    const pool = await poolPromise;
  
    const req = pool
      .request()
      .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip);
  
    const query = `
      ;WITH BasePartial AS (
        SELECT
          fwp.NoFurnitureWIPPartial,
          fwp.NoFurnitureWIP,
          fwp.Pcs
        FROM dbo.FurnitureWIPPartial fwp
        WHERE fwp.NoFurnitureWIP = @NoFurnitureWIP
      ),
      Consumed AS (
        SELECT
          ip.NoFurnitureWIPPartial,
          'INJECT' AS SourceType,
          ip.NoProduksi
        FROM dbo.InjectProduksiInputFurnitureWIPPartial ip
      )
      SELECT
        bp.NoFurnitureWIPPartial,
        bp.NoFurnitureWIP,
        bp.Pcs,                  -- partial pcs
  
        c.SourceType,            -- 'INJECT' / NULL
        c.NoProduksi,
  
        iph.TglProduksi,
        iph.IdMesin,
        iph.IdOperator,
        iph.Jam,
        iph.Shift,
  
        mm.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoFurnitureWIPPartial = bp.NoFurnitureWIPPartial
  
      LEFT JOIN dbo.InjectProduksi_h iph
        ON iph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MstMesin mm
        ON mm.IdMesin = iph.IdMesin
  
      ORDER BY
        bp.NoFurnitureWIPPartial ASC,
        c.NoProduksi ASC;
    `;
  
    const result = await req.query(query);
  
    // total partial pcs (unique per NoFurnitureWIPPartial)
    const seen = new Set();
    let totalPartialPcs = 0;
  
    for (const row of result.recordset) {
      const key = row.NoFurnitureWIPPartial;
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
      NoFurnitureWIPPartial: r.NoFurnitureWIPPartial,
      NoFurnitureWIP: r.NoFurnitureWIP,
      Pcs: r.Pcs,
  
      SourceType: r.SourceType || null,     // 'INJECT' | null
      NoProduksi: r.NoProduksi || null,
  
      TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
      IdMesin: r.IdMesin || null,
      NamaMesin: r.NamaMesin || null,
      IdOperator: r.IdOperator || null,
      Jam: r.Jam || null,
      Shift: r.Shift || null,
    }));
  
    return { totalPartialPcs, rows };
  };




  // === hanya untuk unit test ===
exports._test = {
    padLeft,
    generateNextNoFurnitureWip,
  };
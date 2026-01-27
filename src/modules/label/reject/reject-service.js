// services/labels/reject-service.js
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
  return Object.prototype.hasOwnProperty.call(obj || {}, key);
}


exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    WITH RejectPartialAgg AS (
      SELECT
        NoReject,
        SUM(ISNULL(Berat, 0)) AS TotalPartialBerat
      FROM [dbo].[RejectV2Partial]
      GROUP BY NoReject
    )
    SELECT
      r.NoReject,
      r.DateCreate,
      r.IdReject,
      r.IdWarehouse,

      ------------------------------------------------------------------
      -- BERAT HEADER - TOTAL PARTIAL = BERAT NET
      ------------------------------------------------------------------
      CASE 
        WHEN ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0) < 0 
          THEN 0
        ELSE ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0)
      END AS Berat,
      -- Kalau nanti mau dipakai di FE, bisa expose juga:
      -- ISNULL(rp.TotalPartialBerat, 0) AS TotalPartialBerat,

      r.IsPartial,
      r.Blok,
      r.IdLokasi,

      ------------------------------------------------------------------
      -- NAMA JENIS REJECT
      ------------------------------------------------------------------
      MAX(mr.NamaReject) AS NamaReject,

      ------------------------------------------------------------------
      -- TIPE SUMBER: INJECT / HOT_STAMPING / SPANNER / BJ_SORTIR
      ------------------------------------------------------------------
      CASE
        WHEN MAX(injr.NoProduksi) IS NOT NULL THEN 'INJECT'
        WHEN MAX(hsr.NoProduksi) IS NOT NULL THEN 'HOT_STAMPING'
        WHEN MAX(spr.NoProduksi) IS NOT NULL THEN 'SPANNER'
        WHEN MAX(bjr.NoBJSortir) IS NOT NULL THEN 'BJ_SORTIR'
        ELSE NULL
      END AS OutputType,

      ------------------------------------------------------------------
      -- KODE SUMBER: NoProduksi / NoBJSortir (untuk BJ_SORTIR)
      ------------------------------------------------------------------
      MAX(
        COALESCE(
          injr.NoProduksi,
          hsr.NoProduksi,
          spr.NoProduksi,
          bjr.NoBJSortir
        )
      ) AS OutputCode,

      ------------------------------------------------------------------
      -- NAMA MESIN / 'BJ Sortir'
      ------------------------------------------------------------------
      MAX(
        COALESCE(
          mInject.NamaMesin,
          mHot.NamaMesin,
          mSpan.NamaMesin,
          CASE 
            WHEN bjr.NoBJSortir IS NOT NULL THEN 'BJ Sortir'
          END
        )
      ) AS OutputNamaMesin

    FROM [dbo].[RejectV2] r

    ------------------------------------------------------------------
    -- MASTER JENIS REJECT
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[MstReject] mr
      ON mr.IdReject = r.IdReject
    -- kalau DB utama kamu PPS dan master ada di PPS:
    -- LEFT JOIN .[dbo].[MstReject] mr
    --   ON mr.IdReject = r.IdReject

    ------------------------------------------------------------------
    -- AGGREGATE PARTIAL REJECT
    ------------------------------------------------------------------
    LEFT JOIN RejectPartialAgg rp
      ON rp.NoReject = r.NoReject

    ------------------------------------------------------------------
    -- MAPPING: INJECT → MESIN
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[InjectProduksiOutputRejectV2] injr
      ON injr.NoReject = r.NoReject
    LEFT JOIN [dbo].[InjectProduksi_h] injh
      ON injh.NoProduksi = injr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInject
      ON mInject.IdMesin = injh.IdMesin

    ------------------------------------------------------------------
    -- MAPPING: HOT STAMPING → MESIN
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[HotStampingOutputRejectV2] hsr
      ON hsr.NoReject = r.NoReject
    LEFT JOIN [dbo].[HotStamping_h] hsh
      ON hsh.NoProduksi = hsr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHot
      ON mHot.IdMesin = hsh.IdMesin

    ------------------------------------------------------------------
    -- MAPPING: SPANNER → MESIN
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[SpannerOutputRejectV2] spr
      ON spr.NoReject = r.NoReject
    LEFT JOIN [dbo].[Spanner_h] sph
      ON sph.NoProduksi = spr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSpan
      ON mSpan.IdMesin = sph.IdMesin

    ------------------------------------------------------------------
    -- MAPPING: BJ SORTIR (bukan dari mesin)
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[BJSortirRejectOutputLabelReject] bjr
      ON bjr.NoReject = r.NoReject

    WHERE 1 = 1
      AND r.DateUsage IS NULL
      ${
        search
          ? `AND (
               r.NoReject LIKE @search
               OR r.Blok LIKE @search
               OR CONVERT(VARCHAR(20), r.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), r.IdReject) LIKE @search

               -- cari berdasarkan kode sumber
               OR ISNULL(injr.NoProduksi,'')   LIKE @search
               OR ISNULL(hsr.NoProduksi,'')    LIKE @search
               OR ISNULL(spr.NoProduksi,'')    LIKE @search
               OR ISNULL(bjr.NoBJSortir,'')    LIKE @search

               -- cari berdasarkan nama mesin
               OR ISNULL(mInject.NamaMesin,'') LIKE @search
               OR ISNULL(mHot.NamaMesin,'')    LIKE @search
               OR ISNULL(mSpan.NamaMesin,'')   LIKE @search

               -- cari berdasarkan nama jenis reject
               OR ISNULL(mr.NamaReject,'')     LIKE @search
             )`
          : ''
      }
    GROUP BY
      r.NoReject,
      r.DateCreate,
      r.IdReject,
      r.IdWarehouse,
      r.Berat,
      r.IsPartial,
      r.Blok,
      r.IdLokasi,
      rp.TotalPartialBerat
    ORDER BY r.NoReject DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT r.NoReject) AS total
    FROM [dbo].[RejectV2] r

    LEFT JOIN [dbo].[MstReject] mr
      ON mr.IdReject = r.IdReject
    -- atau [dbo].[MstReject] mr, kalau beda DB

    LEFT JOIN [dbo].[InjectProduksiOutputRejectV2] injr
      ON injr.NoReject = r.NoReject
    LEFT JOIN [dbo].[InjectProduksi_h] injh
      ON injh.NoProduksi = injr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInject
      ON mInject.IdMesin = injh.IdMesin

    LEFT JOIN [dbo].[HotStampingOutputRejectV2] hsr
      ON hsr.NoReject = r.NoReject
    LEFT JOIN [dbo].[HotStamping_h] hsh
      ON hsh.NoProduksi = hsr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHot
      ON mHot.IdMesin = hsh.IdMesin

    LEFT JOIN [dbo].[SpannerOutputRejectV2] spr
      ON spr.NoReject = r.NoReject
    LEFT JOIN [dbo].[Spanner_h] sph
      ON sph.NoProduksi = spr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSpan
      ON mSpan.IdMesin = sph.IdMesin

    LEFT JOIN [dbo].[BJSortirRejectOutputLabelReject] bjr
      ON bjr.NoReject = r.NoReject

    WHERE 1 = 1
      AND r.DateUsage IS NULL
      ${
        search
          ? `AND (
               r.NoReject LIKE @search
               OR r.Blok LIKE @search
               OR CONVERT(VARCHAR(20), r.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), r.IdReject) LIKE @search
               OR ISNULL(injr.NoProduksi,'')   LIKE @search
               OR ISNULL(hsr.NoProduksi,'')    LIKE @search
               OR ISNULL(spr.NoProduksi,'')    LIKE @search
               OR ISNULL(bjr.NoBJSortir,'')    LIKE @search
               OR ISNULL(mInject.NamaMesin,'') LIKE @search
               OR ISNULL(mHot.NamaMesin,'')    LIKE @search
               OR ISNULL(mSpan.NamaMesin,'')   LIKE @search
               OR ISNULL(mr.NamaReject,'')     LIKE @search
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



function resolveOutputByPrefix(outputCode) {
  let outputType = null;
  let mappingTable = null;

  if (outputCode.startsWith('S.')) {
    outputType = 'INJECT';
    mappingTable = 'InjectProduksiOutputRejectV2';
  } else if (outputCode.startsWith('BH.')) {
    outputType = 'HOT_STAMPING';
    mappingTable = 'HotStampingOutputRejectV2';
  } else if (outputCode.startsWith('BI.')) {
    outputType = 'PASANG_KUNCI';
    mappingTable = 'PasangKunciOutputRejectV2';
  } else if (outputCode.startsWith('BJ.')) {
    outputType = 'SPANNER';
    mappingTable = 'SpannerOutputRejectV2';
  } else if (outputCode.startsWith('J.')) {
    outputType = 'BJ_SORTIR';
    mappingTable = 'BJSortirRejectOutputLabelReject';
  } else {
    throw badReq('outputCode prefix tidak dikenali (S., BH., BI., BJ., J.)');
  }

  return { outputType, mappingTable };
}

/**
 * insert 1 row RejectV2 + mapping output
 * mengikuti pattern insertSingleFurnitureWip
 */
async function insertSingleReject({
  tx,
  header,
  idReject,
  outputCode,
  outputType,
  mappingTable,
  effectiveDateCreate,
  nowDateTime,
}) {
  // ===============================
  // 1) Generate NoReject (pakai generateNextCode)
  // ===============================
  const gen = async () =>
    generateNextCode(tx, {
      tableName: 'dbo.RejectV2',
      columnName: 'NoReject',
      prefix: 'BF.',
      width: 10,
    });

  const generatedNo = await gen();

  // double-check belum dipakai (lock)
  const exist = await new sql.Request(tx)
    .input('NoReject', sql.VarChar(50), generatedNo)
    .query(`
      SELECT 1
      FROM dbo.RejectV2 WITH (UPDLOCK, HOLDLOCK)
      WHERE NoReject = @NoReject
    `);

  let noReject = generatedNo;

  if (exist.recordset.length > 0) {
    const retryNo = await gen();
    const exist2 = await new sql.Request(tx)
      .input('NoReject', sql.VarChar(50), retryNo)
      .query(`
        SELECT 1
        FROM dbo.RejectV2 WITH (UPDLOCK, HOLDLOCK)
        WHERE NoReject = @NoReject
      `);

    if (exist2.recordset.length > 0) {
      throw conflict('Gagal generate NoReject unik, coba lagi.');
    }
    noReject = retryNo;
  }

  // ===============================
  // 2) Insert header (DateTimeCreate dari app, bukan GETDATE())
  // ===============================
  const insertHeaderSql = `
    INSERT INTO dbo.RejectV2 (
      NoReject,
      IdReject,
      DateCreate,
      DateUsage,
      IdWarehouse,
      Berat,
      Jam,
      CreateBy,
      DateTimeCreate,
      IsPartial,
      Blok,
      IdLokasi
    )
    VALUES (
      @NoReject,
      @IdReject,
      @DateCreate,
      NULL,
      @IdWarehouse,
      @Berat,
      @Jam,
      @CreateBy,
      @DateTimeCreate,
      @IsPartial,
      @Blok,
      @IdLokasi
    );
  `;

  await new sql.Request(tx)
    .input('NoReject', sql.VarChar(50), noReject)
    .input('IdReject', sql.Int, idReject)
    .input('DateCreate', sql.Date, effectiveDateCreate)
    .input('IdWarehouse', sql.Int, header.IdWarehouse) // wajib
    .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
    .input('Jam', sql.VarChar(20), header.Jam ?? null)
    .input('CreateBy', sql.VarChar(50), header.CreateBy) // controller overwrite dari token
    .input('DateTimeCreate', sql.DateTime, nowDateTime)
    .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
    .input('Blok', sql.VarChar(50), header.Blok ?? null)
    .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
    .query(insertHeaderSql);

  // ===============================
  // 3) Insert mapping berdasarkan mappingTable
  // ===============================
  const rqMap = new sql.Request(tx)
    .input('OutputCode', sql.VarChar(50), outputCode)
    .input('NoReject', sql.VarChar(50), noReject);

  if (mappingTable === 'InjectProduksiOutputRejectV2') {
    await rqMap.query(`
      INSERT INTO dbo.InjectProduksiOutputRejectV2 (NoProduksi, NoReject)
      VALUES (@OutputCode, @NoReject);
    `);
  } else if (mappingTable === 'HotStampingOutputRejectV2') {
    await rqMap.query(`
      INSERT INTO dbo.HotStampingOutputRejectV2 (NoProduksi, NoReject)
      VALUES (@OutputCode, @NoReject);
    `);
  } else if (mappingTable === 'PasangKunciOutputRejectV2') {
    await rqMap.query(`
      INSERT INTO dbo.PasangKunciOutputRejectV2 (NoProduksi, NoReject)
      VALUES (@OutputCode, @NoReject);
    `);
  } else if (mappingTable === 'SpannerOutputRejectV2') {
    await rqMap.query(`
      INSERT INTO dbo.SpannerOutputRejectV2 (NoProduksi, NoReject)
      VALUES (@OutputCode, @NoReject);
    `);
  } else if (mappingTable === 'BJSortirRejectOutputLabelReject') {
    await rqMap.query(`
      INSERT INTO dbo.BJSortirRejectOutputLabelReject (NoBJSortir, NoReject)
      VALUES (@OutputCode, @NoReject);
    `);
  }

  return {
    NoReject: noReject,
    IdReject: idReject,
    DateCreate: effectiveDateCreate, // date-only yang sudah “effective”
    DateUsage: null,
    IdWarehouse: header.IdWarehouse,
    Berat: header.Berat ?? null,
    Jam: header.Jam ?? null,
    CreateBy: header.CreateBy,
    DateTimeCreate: nowDateTime,
    IsPartial: header.IsPartial ?? 0,
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
    OutputCode: outputCode,
    OutputType: outputType,
  };
}

exports.createReject = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = String(payload?.outputCode || '').trim();

  // =========================
  // validation dasar (samakan style FWIP)
  // =========================
  if (!outputCode) throw badReq('outputCode wajib diisi (S., BH., BI., BJ., J.)');
  if (!header.CreateBy) throw badReq('CreateBy wajib diisi (controller harus overwrite dari token)');
  if (!header.IdReject) throw badReq('IdReject wajib diisi');

  // =========================
  // [AUDIT] actorId + requestId
  // =========================
  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  const { outputType, mappingTable } = resolveOutputByPrefix(outputCode);

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =========================
    // [AUDIT CTX] set session context for triggers
    // =========================
    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE)
    // ===============================
    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate);

    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create reject',
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari kode (jika belum ada)
    // ===============================
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

    const created = await insertSingleReject({
      tx,
      header,
      idReject: header.IdReject,
      outputCode,
      outputType,
      mappingTable,
      effectiveDateCreate,
      nowDateTime,
    });

    await tx.commit();

    return {
      headers: [created],
      output: {
        code: outputCode,
        type: outputType,
        mappingTable,
        isMulti: false,
        count: 1,
      },
      audit: { actorId, requestId }, // ✅ konsisten seperti module lain
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};
  


async function deleteAllRejectMappings(tx, noReject) {
  await new sql.Request(tx)
    .input('NoReject', sql.VarChar(50), noReject)
    .query(`
      DELETE FROM dbo.InjectProduksiOutputRejectV2    WHERE NoReject = @NoReject;
      DELETE FROM dbo.HotStampingOutputRejectV2       WHERE NoReject = @NoReject;
      DELETE FROM dbo.PasangKunciOutputRejectV2       WHERE NoReject = @NoReject;
      DELETE FROM dbo.SpannerOutputRejectV2           WHERE NoReject = @NoReject;
      DELETE FROM dbo.BJSortirRejectOutputLabelReject WHERE NoReject = @NoReject;
    `);
}


  /**
 * UPDATE RejectV2 + opsional ganti mapping output
 */
exports.updateReject = async (noReject, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const hasOutputCodeField = hasOwn(payload, 'outputCode');
  const outputCode = String(payload?.outputCode || '').trim();

  if (!noReject) throw badReq('NoReject wajib diisi');

  // =========================
  // [AUDIT] actorId + requestId (WAJIB seperti create)
  // =========================
  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =========================
    // [AUDIT CTX] set session context for triggers
    // =========================
    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 1) Ambil data existing + lock
    // ===============================
    const existingRes = await new sql.Request(tx)
      .input('NoReject', sql.VarChar(50), noReject)
      .query(`
        SELECT TOP 1
          NoReject,
          IdReject,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage,
          IdWarehouse,
          Berat,
          Jam,
          CreateBy,
          DateTimeCreate,
          IsPartial,
          Blok,
          IdLokasi
        FROM dbo.RejectV2 WITH (UPDLOCK, HOLDLOCK)
        WHERE NoReject = @NoReject;
      `);

    if (existingRes.recordset.length === 0) {
      throw notFound('Reject not found');
    }

    const current = existingRes.recordset[0];

    // optional: kalau kamu mau reject yg sudah used tidak boleh diupdate
    if (current.DateUsage != null) {
      throw badReq(`Reject ${noReject} sudah dipakai (DateUsage tidak NULL)`);
    }

    // ===============================
    // 1b) TUTUP TRANSAKSI CHECK (UPDATE) - cek tanggal existing
    // ===============================
    const existingDateCreate = current.DateCreate ? toDateOnly(current.DateCreate) : null;

    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: 'update reject',
      useLock: true,
    });

    // ===============================
    // 2) Merge field (partial update)
    // ===============================
    const merged = {
      // required identity
      IdReject: hasOwn(header, 'IdReject') ? header.IdReject : current.IdReject,

      // optional
      DateCreate: hasOwn(header, 'DateCreate') ? header.DateCreate : current.DateCreate,
      IdWarehouse: hasOwn(header, 'IdWarehouse') ? header.IdWarehouse : current.IdWarehouse,
      Berat: hasOwn(header, 'Berat') ? header.Berat : current.Berat,
      Jam: hasOwn(header, 'Jam') ? header.Jam : current.Jam,
      IsPartial: hasOwn(header, 'IsPartial') ? header.IsPartial : current.IsPartial,
      Blok: hasOwn(header, 'Blok') ? header.Blok : current.Blok,
      IdLokasi: hasOwn(header, 'IdLokasi') ? header.IdLokasi : current.IdLokasi,

      // CreateBy (biasanya overwrite dari token)
      CreateBy: hasOwn(header, 'CreateBy') ? header.CreateBy : current.CreateBy,
    };

    if (merged.IdReject == null) throw badReq('IdReject cannot be empty');

    // ===============================
    // 2b) Jika DateCreate dikirim user, cek tutup transaksi untuk tanggal baru
    // ===============================
    let dateCreateParam = null;

    if (hasOwn(header, 'DateCreate')) {
      if (header.DateCreate === null || header.DateCreate === '') {
        dateCreateParam = toDateOnly(new Date());
      } else {
        dateCreateParam = toDateOnly(header.DateCreate);
        if (!dateCreateParam) throw badReq('Invalid DateCreate');
      }

      await assertNotLocked({
        date: dateCreateParam,
        runner: tx,
        action: 'update reject (DateCreate)',
        useLock: true,
      });
    }

    // normalize IdLokasi (anggap INT)
    let idLokasiParam = merged.IdLokasi;
    if (hasOwn(header, 'IdLokasi')) {
      const raw = header.IdLokasi;
      if (raw === null || String(raw).trim() === '') idLokasiParam = null;
      else {
        const n = Number(String(raw).trim());
        idLokasiParam = Number.isFinite(n) ? n : null;
      }
    }

    // ===============================
    // 3) UPDATE header
    // ===============================
    const rqUpdate = new sql.Request(tx)
      .input('NoReject', sql.VarChar(50), noReject)
      .input('IdReject', sql.Int, merged.IdReject)
      .input('IdWarehouse', sql.Int, merged.IdWarehouse ?? null)
      .input('Berat', sql.Decimal(18, 3), merged.Berat ?? null)
      .input('Jam', sql.VarChar(20), merged.Jam ?? null)
      .input('IsPartial', sql.Bit, merged.IsPartial ?? 0)
      .input('Blok', sql.VarChar(50), merged.Blok ?? null)
      .input('IdLokasi', sql.Int, idLokasiParam ?? null)
      .input('CreateBy', sql.VarChar(50), merged.CreateBy ?? null);

    if (hasOwn(header, 'DateCreate')) {
      rqUpdate.input('DateCreate', sql.Date, dateCreateParam);
    }

    const updateSql = `
      UPDATE dbo.RejectV2
      SET
        IdReject = @IdReject,
        IdWarehouse = @IdWarehouse,
        Berat = @Berat,
        Jam = @Jam,
        IsPartial = @IsPartial,
        Blok = @Blok,
        IdLokasi = @IdLokasi,
        CreateBy = @CreateBy
        ${hasOwn(header, 'DateCreate') ? ', DateCreate = @DateCreate' : ''}
      WHERE NoReject = @NoReject;
    `;
    await rqUpdate.query(updateSql);

    // ===============================
    // 4) Mapping update (optional) - sama pattern FWIP
    // ===============================
    let outputType = null;
    let mappingTable = null;

    if (hasOutputCodeField) {
      if (!outputCode) {
        // kalau user kirim outputCode: "" / null -> hapus semua mapping
        await deleteAllRejectMappings(tx, noReject);
      } else {
        const resolved = resolveOutputByPrefix(outputCode); // versi kamu yang tidak perlu badReq param
        outputType = resolved.outputType;
        mappingTable = resolved.mappingTable;

        await deleteAllRejectMappings(tx, noReject);

        const rqMap = new sql.Request(tx)
          .input('OutputCode', sql.VarChar(50), outputCode)
          .input('NoReject', sql.VarChar(50), noReject);

        if (mappingTable === 'InjectProduksiOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO dbo.InjectProduksiOutputRejectV2 (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'HotStampingOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO dbo.HotStampingOutputRejectV2 (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'PasangKunciOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO dbo.PasangKunciOutputRejectV2 (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'SpannerOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO dbo.SpannerOutputRejectV2 (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'BJSortirRejectOutputLabelReject') {
          await rqMap.query(`
            INSERT INTO dbo.BJSortirRejectOutputLabelReject (NoBJSortir, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoReject: noReject,
        DateCreate: hasOwn(header, 'DateCreate')
          ? (dateCreateParam ? formatYMD(dateCreateParam) : null)
          : formatYMD(current.DateCreate),
        IdReject: merged.IdReject,
        IdWarehouse: merged.IdWarehouse ?? null,
        Berat: merged.Berat ?? null,
        Jam: merged.Jam ?? null,
        IsPartial: merged.IsPartial ?? 0,
        Blok: merged.Blok ?? null,
        IdLokasi: idLokasiParam ?? null,
        CreateBy: merged.CreateBy ?? null,
      },
      output: hasOutputCodeField
        ? { code: outputCode || null, type: outputType, mappingTable }
        : undefined,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};



  /**
 * DELETE RejectV2 + semua mapping output
 */
exports.deleteReject = async (noReject, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoReject = String(noReject || '').trim();
  if (!NoReject) throw badReq('NoReject wajib diisi');

  // =========================
  // [AUDIT] actorId + requestId (WAJIB seperti create/update)
  // =========================
  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =========================
    // [AUDIT CTX] set session context for triggers
    // =========================
    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 1) Pastikan data ada & lock + ambil DateCreate
    // ===============================
    const checkRes = await new sql.Request(tx)
      .input('NoReject', sql.VarChar(50), NoReject)
      .query(`
        SELECT TOP 1
          NoReject,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage
        FROM dbo.RejectV2 WITH (UPDLOCK, HOLDLOCK)
        WHERE NoReject = @NoReject;
      `);

    if (checkRes.recordset.length === 0) {
      throw notFound(`Reject ${NoReject} tidak ditemukan`);
    }

    const row = checkRes.recordset[0];

    if (row.DateUsage !== null) {
      throw badReq(`Reject ${NoReject} tidak bisa dihapus karena sudah dipakai (DateUsage tidak NULL)`);
    }

    // ===============================
    // 1b) TUTUP TRANSAKSI CHECK (DELETE)
    // date yang dicek = DateCreate existing
    // ===============================
    const trxDate = row.DateCreate ? toDateOnly(row.DateCreate) : null;

    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: 'delete reject',
      useLock: true,
    });

    // ===============================
    // 2) Hapus mapping dulu
    // ===============================
    await deleteAllRejectMappings(tx, NoReject);

    // ===============================
    // 3) Hapus row utama
    // ===============================
    const delMainRes = await new sql.Request(tx)
      .input('NoReject', sql.VarChar(50), NoReject)
      .query(`
        DELETE FROM dbo.RejectV2
        WHERE NoReject = @NoReject;
      `);

    if ((delMainRes.rowsAffected?.[0] ?? 0) === 0) {
      throw notFound(`Reject ${NoReject} tidak ditemukan`);
    }

    await tx.commit();

    return {
      NoReject,
      deleted: true,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};


  /**
 * Ambil info partial Reject per NoReject.
 *
 * Tabel yang dipakai:
 * - dbo.RejectV2Partial                 (base partial, Berat)
 * - dbo.BrokerProduksiInputRejectPartial (konsumsi partial -> NoProduksi)
 * - dbo.BrokerProduksi_h                (header broker -> IdMesin, TglProduksi, Jam, Shift, ...)
 * - dbo.MstMesin                        (nama mesin)
 */
exports.getPartialInfoByReject = async (noReject) => {
    const pool = await poolPromise;
  
    const req = pool
      .request()
      .input('NoReject', sql.VarChar, noReject);
  
    const query = `
      ;WITH BasePartial AS (
        SELECT
          rp.NoRejectPartial,
          rp.NoReject,
          rp.Berat
        FROM dbo.RejectV2Partial rp
        WHERE rp.NoReject = @NoReject
      ),
      Consumed AS (
        SELECT
          bpir.NoRejectPartial,
          'BROKER' AS SourceType,
          bpir.NoProduksi
        FROM dbo.BrokerProduksiInputRejectPartial bpir
      )
      SELECT
        bp.NoRejectPartial,
        bp.NoReject,
        bp.Berat,            -- partial berat
  
        c.SourceType,        -- 'BROKER' / NULL
        c.NoProduksi,
  
        bh.TglProduksi,
        bh.IdMesin,
        bh.Jam,
        bh.Shift,
  
        m.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoRejectPartial = bp.NoRejectPartial
  
      LEFT JOIN dbo.BrokerProduksi_h bh
        ON bh.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MstMesin m
        ON m.IdMesin = bh.IdMesin
  
      ORDER BY
        bp.NoRejectPartial ASC,
        c.NoProduksi ASC;
    `;
  
    const result = await req.query(query);
  
    // total partial berat (unique per NoRejectPartial)
    const seen = new Set();
    let totalPartialBerat = 0;
  
    for (const row of result.recordset) {
      const key = row.NoRejectPartial;
      if (!seen.has(key)) {
        seen.add(key);
        const berat =
          typeof row.Berat === 'number'
            ? row.Berat
            : Number(row.Berat) || 0;
        totalPartialBerat += berat;
      }
    }
  
    const formatDate = (date) => {
      if (!date) return null;
      const d = new Date(date);
      const pad = (n) => (n < 10 ? '0' + n : '' + n);
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    };
  
    const rows = result.recordset.map((r) => ({
      NoRejectPartial: r.NoRejectPartial,
      NoReject: r.NoReject,
      Berat: r.Berat,
  
      SourceType: r.SourceType || null,       // 'BROKER' | null
      NoProduksi: r.NoProduksi || null,
  
      TanggalProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
      IdMesin: r.IdMesin || null,
      NamaMesin: r.NamaMesin || null,
      JamProduksi: r.Jam || null,
      Shift: r.Shift || null,
    }));
  
    return { totalPartialBerat, rows };
  };
  
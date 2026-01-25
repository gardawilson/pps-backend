// services/crusher-service.js
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


/**
 * Tables used:
 * - dbo.Crusher c
 * - dbo.MstCrusher mc          (IdCrusher -> NamaCrusher)
 * - dbo.MstWarehouse w         (IdWarehouse -> NamaWarehouse)
 * - dbo.CrusherProduksiOutput cpo        (NoCrusher -> NoCrusherProduksi)
 * - dbo.CrusherProduksi_h ch             (NoCrusherProduksi -> IdMesin)
 * - dbo.MstMesin m                       (IdMesin -> NamaMesin)
 * - dbo.BongkarSusunOutputCrusher bs     (NoCrusher -> NoBongkarSusun)
 */
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const whereSearch = search
    ? `
      AND (
        c.NoCrusher LIKE @search
        OR c.Blok LIKE @search
        OR CONVERT(VARCHAR(20), c.IdLokasi) LIKE @search
        OR CONVERT(VARCHAR(20), c.IdWarehouse) LIKE @search
        OR ISNULL(w.NamaWarehouse,'') LIKE @search
        OR ISNULL(mc.NamaCrusher,'') LIKE @search
        OR ISNULL(cpo.NoCrusherProduksi,'') LIKE @search
        OR ISNULL(m.NamaMesin,'') LIKE @search
        OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
      )
    `
    : '';

  const baseQuery = `
    SELECT
      c.NoCrusher,
      c.DateCreate,
      c.IdCrusher,
      mc.NamaCrusher,
      c.IdWarehouse,
      w.NamaWarehouse,
      c.Blok,
      c.IdLokasi,
      c.Berat,
      CASE
        WHEN c.IdStatus = 1 THEN 'PASS'
        WHEN c.IdStatus = 0 THEN 'HOLD'
        ELSE ''
      END AS StatusText,

      -- Joins collapsed (if multiple rows exist, pick one deterministically)
      MAX(cpo.NoCrusherProduksi) AS CrusherNoProduksi,
      MAX(m.NamaMesin)          AS CrusherNamaMesin,
      MAX(bs.NoBongkarSusun)    AS NoBongkarSusun

    FROM [dbo].[Crusher] c
    LEFT JOIN [dbo].[MstCrusher] mc
      ON mc.IdCrusher = c.IdCrusher
    LEFT JOIN [dbo].[MstWarehouse] w
      ON w.IdWarehouse = c.IdWarehouse

    -- Produksi chain
    LEFT JOIN [dbo].[CrusherProduksiOutput] cpo
      ON cpo.NoCrusher = c.NoCrusher
    LEFT JOIN [dbo].[CrusherProduksi_h] ch
      ON ch.NoCrusherProduksi = cpo.NoCrusherProduksi
    LEFT JOIN [dbo].[MstMesin] m
      ON m.IdMesin = ch.IdMesin

    -- Bongkar Susun
    LEFT JOIN [dbo].[BongkarSusunOutputCrusher] bs
      ON bs.NoCrusher = c.NoCrusher

    WHERE 1=1
      AND c.DateUsage IS NULL
      ${whereSearch}

    GROUP BY
      c.NoCrusher, c.DateCreate, c.IdCrusher, mc.NamaCrusher,
      c.IdWarehouse, w.NamaWarehouse, c.Blok, c.IdLokasi, c.Berat, c.IdStatus

    ORDER BY c.NoCrusher DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT c.NoCrusher) AS total
    FROM [dbo].[Crusher] c
    LEFT JOIN [dbo].[MstCrusher] mc
      ON mc.IdCrusher = c.IdCrusher
    LEFT JOIN [dbo].[MstWarehouse] w
      ON w.IdWarehouse = c.IdWarehouse
    LEFT JOIN [dbo].[CrusherProduksiOutput] cpo
      ON cpo.NoCrusher = c.NoCrusher
    LEFT JOIN [dbo].[CrusherProduksi_h] ch
      ON ch.NoCrusherProduksi = cpo.NoCrusherProduksi
    LEFT JOIN [dbo].[MstMesin] m
      ON m.IdMesin = ch.IdMesin
    LEFT JOIN [dbo].[BongkarSusunOutputCrusher] bs
      ON bs.NoCrusher = c.NoCrusher
    WHERE 1=1
      AND c.DateUsage IS NULL
      ${whereSearch}
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit',  sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data  = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};


// =======================================
// Crusher CREATE (cascade) — mengikuti pattern Broker (trigger + session_context + generateNextCode)
// =======================================

exports.createCrusherCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const processedCode = (payload?.ProcessedCode || '').toString().trim(); // '', 'G.****', 'BG.****'

  // ---- validation dasar
  if (!header.IdCrusher) throw badReq('IdCrusher wajib diisi');
  if (!header.IdWarehouse) throw badReq('IdWarehouse wajib diisi');
  if (!header.CreateBy) throw badReq('CreateBy wajib diisi'); // controller overwrite dari token

  // Identify target from ProcessedCode (optional)
  const hasProcessed = processedCode.length > 0;
  let processedType = null; // 'PRODUKSI' | 'BONGKAR'
  if (hasProcessed) {
    if (processedCode.startsWith('G.')) processedType = 'PRODUKSI';
    else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';
    else throw badReq('ProcessedCode prefix tidak dikenali (pakai G. atau BG.)');
  }

  // =====================================================
  // [AUDIT] Pakai actorId dari controller (token)
  // =====================================================
  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;

  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =====================================================
    // [AUDIT CTX] Set actor_id + request_id untuk trigger audit
    // =====================================================
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
    const effectiveDateCreate = resolveEffectiveDateForCreate(header.DateCreate); // date-only
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: 'create crusher',
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari kode (produksi / bongkar) kalau header belum isi
    // ===============================
    const needBlok = header.Blok == null || String(header.Blok).trim() === '';
    const needLokasi = header.IdLokasi == null;

    if (needBlok || needLokasi) {
      const kodeRef = hasProcessed ? processedCode : null;

      let lokasi = null;
      if (kodeRef) {
        lokasi = await getBlokLokasiFromKodeProduksi({ kode: kodeRef, runner: tx });
      }

      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    // ===============================
    // 1) Generate NoCrusher (PAKAI generateNextCode seperti broker/washing)
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: 'Crusher',
        columnName: 'NoCrusher',
        prefix: 'F.',
        width: 10,
      });

    const generatedNo = await gen();

    // 2) Double-check belum dipakai (lock supaya konsisten)
    const exist = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), generatedNo)
      .query(`SELECT 1 FROM dbo.Crusher WITH (UPDLOCK, HOLDLOCK) WHERE NoCrusher = @NoCrusher`);

    if (exist.recordset.length > 0) {
      const retryNo = await gen();
      const exist2 = await new sql.Request(tx)
        .input('NoCrusher', sql.VarChar(50), retryNo)
        .query(`SELECT 1 FROM dbo.Crusher WITH (UPDLOCK, HOLDLOCK) WHERE NoCrusher = @NoCrusher`);

      if (exist2.recordset.length > 0) {
        throw conflict('Gagal generate NoCrusher unik, coba lagi.');
      }
      header.NoCrusher = retryNo;
    } else {
      header.NoCrusher = generatedNo;
    }

    // ===============================
    // 3) Insert header (samakan pattern: pakai @DateTimeCreate dari app, bukan GETDATE())
    // ===============================
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO dbo.Crusher (
        NoCrusher, DateCreate, IdCrusher, IdWarehouse, DateUsage,
        Berat, IdStatus, Blok, IdLokasi, CreateBy, DateTimeCreate
      )
      VALUES (
        @NoCrusher, @DateCreate, @IdCrusher, @IdWarehouse, NULL,
        @Berat, @IdStatus, @Blok, @IdLokasi, @CreateBy, @DateTimeCreate
      );
    `;

    await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), header.NoCrusher)
      .input('DateCreate', sql.Date, effectiveDateCreate)
      .input('IdCrusher', sql.Int, header.IdCrusher)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1)
      .input('Blok', sql.VarChar(50), header.Blok ?? null)
      .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
      .input('CreateBy', sql.VarChar(50), header.CreateBy) // overwritten by controller
      .input('DateTimeCreate', sql.DateTime, nowDateTime)
      .query(insertHeaderSql);

    // ===============================
    // 4) Optional mapping table based on ProcessedCode prefix
    //    (ikuti broker: mapping dibuat setelah header insert)
    // ===============================
    let mappingTable = null;

    if (processedType === 'PRODUKSI') {
      await new sql.Request(tx)
        .input('NoCrusherProduksi', sql.VarChar(50), processedCode)
        .input('NoCrusher', sql.VarChar(50), header.NoCrusher)
        .query(`
          INSERT INTO dbo.CrusherProduksiOutput (NoCrusherProduksi, NoCrusher)
          VALUES (@NoCrusherProduksi, @NoCrusher);
        `);

      mappingTable = 'CrusherProduksiOutput';
    } else if (processedType === 'BONGKAR') {
      await new sql.Request(tx)
        .input('NoBongkarSusun', sql.VarChar(50), processedCode)
        .input('NoCrusher', sql.VarChar(50), header.NoCrusher)
        .query(`
          INSERT INTO dbo.BongkarSusunOutputCrusher (NoBongkarSusun, NoCrusher)
          VALUES (@NoBongkarSusun, @NoCrusher);
        `);

      mappingTable = 'BongkarSusunOutputCrusher';
    }

    await tx.commit();

    return {
      header: {
        NoCrusher: header.NoCrusher,
        DateCreate: formatYMD(effectiveDateCreate),
        IdCrusher: header.IdCrusher,
        IdWarehouse: header.IdWarehouse,
        Berat: header.Berat ?? null,
        IdStatus: header.IdStatus ?? 1,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
        CreateBy: header.CreateBy,
        DateTimeCreate: nowDateTime,
      },
      processed: {
        code: processedCode || null,
        type: processedType,
        mappingTable,
      },
      audit: { actorId, requestId }, // ✅ sama seperti broker
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};


// =======================================
// Crusher UPDATE — mengikuti pattern updateBrokerCascade
// - SERIALIZABLE
// - actorId + requestId => sp_set_session_context (trigger audit)
// - lock existing row + ambil existing DateCreate (UPDLOCK/HOLDLOCK)
// - tutup transaksi: cek existingDateOnly + cek new DateCreate/DateUsage kalau dikirim
// - update header dynamic (partial)
// - optional mapping reset+insert jika ProcessedCode/target dikirim (idempotent)
// =======================================

exports.updateCrusherCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoCrusher = payload?.NoCrusher?.toString().trim();
  if (!NoCrusher) throw badReq('NoCrusher (path) wajib diisi');

  const header = payload?.header || {};
  const processedCode = (payload?.ProcessedCode || '').toString().trim(); // '' | 'G.****' | 'BG.****'

  // =====================================================
  // [AUDIT] actorId + requestId (ID only)
  // =====================================================
  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;

  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);
  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  // Identify processedType from ProcessedCode (optional)
  const hasProcessed = processedCode.length > 0;
  let processedType = null; // 'PRODUKSI' | 'BONGKAR' | null
  if (hasProcessed) {
    if (processedCode.startsWith('G.')) processedType = 'PRODUKSI';
    else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';
    else throw badReq('ProcessedCode prefix tidak dikenali (pakai G. atau BG.)');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =====================================================
    // [AUDIT CTX] Set actor_id + request_id untuk trigger audit
    // =====================================================
    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // 0) Pastikan header exist + ambil DateCreate existing (LOCK)
    const exist = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), NoCrusher)
      .query(`
        SELECT TOP 1 NoCrusher, DateCreate, DateUsage
        FROM dbo.Crusher WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher
      `);

    if (exist.recordset.length === 0) {
      throw notFound(`NoCrusher ${NoCrusher} tidak ditemukan`);
    }

    const existingDateCreate = exist.recordset[0]?.DateCreate;
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (UPDATE)
    // - selalu cek tanggal existing (karena row tsb "milik" tanggal itu)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `update crusher ${NoCrusher}`,
      useLock: true,
    });

    // Jika client kirim DateCreate baru, cek juga
    let newDateCreateOnly = null;
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null) throw badReq('DateCreate tidak boleh null pada UPDATE.');
      newDateCreateOnly = toDateOnly(header.DateCreate);
      if (!newDateCreateOnly) throw badReq('DateCreate tidak valid.');

      await assertNotLocked({
        date: newDateCreateOnly,
        runner: tx,
        action: `update crusher ${NoCrusher} (change DateCreate)`,
        useLock: true,
      });
    }

    // Jika client kirim DateUsage, cek juga (kalau null => allow clear, kamu bisa larang kalau mau)
    let newDateUsageOnly = null;
    if (header.DateUsage !== undefined) {
      if (header.DateUsage === null) {
        newDateUsageOnly = null; // allow clear
      } else {
        newDateUsageOnly = toDateOnly(header.DateUsage);
        if (!newDateUsageOnly) throw badReq('DateUsage tidak valid.');
        await assertNotLocked({
          date: newDateUsageOnly,
          runner: tx,
          action: `update crusher ${NoCrusher} (change DateUsage)`,
          useLock: true,
        });
      }
    }

    // ===============================
    // 1) Update header (partial/dynamic) — mirip broker
    // ===============================
    const setParts = [];
    const reqHeader = new sql.Request(tx).input('NoCrusher', sql.VarChar(50), NoCrusher);

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf('IdCrusher', 'IdCrusher', sql.Int, header.IdCrusher);
    setIf('IdWarehouse', 'IdWarehouse', sql.Int, header.IdWarehouse);

    if (header.DateCreate !== undefined) {
      setIf('DateCreate', 'DateCreate', sql.Date, newDateCreateOnly);
    }

    if (header.DateUsage !== undefined) {
      // allow clear => null
      setIf('DateUsage', 'DateUsage', sql.Date, newDateUsageOnly);
    }

    if (Object.prototype.hasOwnProperty.call(header, 'Berat')) {
      const num = header.Berat === null ? null : Number(header.Berat);
      if (num !== null && (!Number.isFinite(num) || num < 0)) throw badReq('Berat tidak valid.');
      setIf('Berat', 'Berat', sql.Decimal(18, 3), num);
    }

    setIf('IdStatus', 'IdStatus', sql.Int, header.IdStatus);

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.Crusher
        SET ${setParts.join(', ')}
        WHERE NoCrusher = @NoCrusher
      `);
    }

    // ===============================
    // 2) Optional: Processed mapping (idempotent) — mirip updateBroker outputs
    // - hanya kalau user memang "mengirim field" ProcessedCode (meski kosong)
    // - reset dulu biar gak FK/duplikat
    // ===============================
    const sentProcessedField = Object.prototype.hasOwnProperty.call(payload, 'ProcessedCode');

    let mappingTable = null;
    if (sentProcessedField) {
      // reset mapping (idempotent)
      await new sql.Request(tx)
        .input('NoCrusher', sql.VarChar(50), NoCrusher)
        .query(`DELETE FROM dbo.CrusherProduksiOutput WHERE NoCrusher = @NoCrusher`);

      await new sql.Request(tx)
        .input('NoCrusher', sql.VarChar(50), NoCrusher)
        .query(`DELETE FROM dbo.BongkarSusunOutputCrusher WHERE NoCrusher = @NoCrusher`);

      // kalau processedCode kosong => artinya user ingin "lepas relasi"
      if (hasProcessed) {
        if (processedType === 'PRODUKSI') {
          await new sql.Request(tx)
            .input('NoCrusherProduksi', sql.VarChar(50), processedCode)
            .input('NoCrusher', sql.VarChar(50), NoCrusher)
            .query(`
              INSERT INTO dbo.CrusherProduksiOutput (NoCrusherProduksi, NoCrusher)
              VALUES (@NoCrusherProduksi, @NoCrusher);
            `);
          mappingTable = 'CrusherProduksiOutput';
        } else if (processedType === 'BONGKAR') {
          await new sql.Request(tx)
            .input('NoBongkarSusun', sql.VarChar(50), processedCode)
            .input('NoCrusher', sql.VarChar(50), NoCrusher)
            .query(`
              INSERT INTO dbo.BongkarSusunOutputCrusher (NoBongkarSusun, NoCrusher)
              VALUES (@NoBongkarSusun, @NoCrusher);
            `);
          mappingTable = 'BongkarSusunOutputCrusher';
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoCrusher,
        ...header,
        existingDateCreate: existingDateOnly ? formatYMD(existingDateOnly) : null,
        ...(newDateCreateOnly ? { newDateCreate: formatYMD(newDateCreateOnly) } : {}),
        ...(header.DateUsage !== undefined
          ? { newDateUsage: newDateUsageOnly ? formatYMD(newDateUsageOnly) : null }
          : {}),
      },
      processed: sentProcessedField
        ? {
            code: processedCode || null,
            type: processedType,
            mappingTable, // null kalau lepas relasi / kosong
          }
        : undefined,
      audit: { actorId, requestId }, // ✅ ID only
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};


// =======================================
// Crusher DELETE (cascade) — mengikuti pattern deleteBrokerCascade
// - payload bisa string (legacy) atau object
// - wajib actorId + requestId (audit trigger pakai session_context)
// - SERIALIZABLE
// - lock header + ambil DateCreate existing
// - tutup transaksi check (delete)
// - (optional) block delete kalau ada pemakaian (DateUsage IS NOT NULL) -> bisa kamu keep/hapus sesuai rule bisnis
// - delete mapping dulu (avoid FK)
// - delete header
// - mapping FK error => 409
// =======================================

exports.deleteCrusherCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  // payload bisa string (legacy) atau object
  const NoCrusher =
    typeof payload === 'string'
      ? String(payload || '').trim()
      : String(payload?.NoCrusher || payload?.noCrusher || payload?.nocrusher || '').trim();

  if (!NoCrusher) throw badReq('NoCrusher wajib diisi');

  // =====================================================
  // [AUDIT] actorId + requestId (ID only)
  // =====================================================
  const actorIdNum = typeof payload === 'object' ? Number(payload?.actorId) : NaN;
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? Math.trunc(actorIdNum) : null;

  const requestId =
    typeof payload === 'object'
      ? String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`)
      : String(`${Date.now()}-${Math.random().toString(16).slice(2)}`);

  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =====================================================
    // [AUDIT CTX] Set actor_id + request_id untuk trigger audit
    // =====================================================
    await new sql.Request(tx)
      .input('actorId', sql.Int, actorId)
      .input('rid', sql.NVarChar(64), requestId)
      .query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 0) Pastikan header exist + lock + ambil DateCreate existing
    // ===============================
    const headRes = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), NoCrusher)
      .query(`
        SELECT TOP 1 NoCrusher, DateCreate, DateUsage
        FROM dbo.Crusher WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher
      `);

    if (headRes.recordset.length === 0) {
      const e = new Error(`NoCrusher ${NoCrusher} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    const existingDateCreate = headRes.recordset[0]?.DateCreate;
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `delete crusher ${NoCrusher}`,
      useLock: true,
    });

    // ===============================
    // [B] Optional: Block if already used
    // (Kalau di bisnis Crusher tidak boleh dihapus kalau sudah DateUsage terisi)
    // ===============================
    const used = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), NoCrusher)
      .query(`
        SELECT TOP 1 1
        FROM dbo.Crusher WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      throw conflict('Tidak bisa hapus: Crusher sudah terpakai (DateUsage IS NOT NULL).');
    }

    // ===============================
    // [C] Delete mappings first (avoid FK)
    // ===============================
    const delCpo = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), NoCrusher)
      .query(`DELETE FROM dbo.CrusherProduksiOutput WHERE NoCrusher = @NoCrusher`);

    const delBso = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), NoCrusher)
      .query(`DELETE FROM dbo.BongkarSusunOutputCrusher WHERE NoCrusher = @NoCrusher`);

    // ===============================
    // [D] Delete header
    // ===============================
    const delHead = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar(50), NoCrusher)
      .query(`DELETE FROM dbo.Crusher WHERE NoCrusher = @NoCrusher`);

    await tx.commit();

    return {
      NoCrusher,
      docDateCreate: existingDateOnly ? formatYMD(existingDateOnly) : null,
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        outputs: {
          CrusherProduksiOutput: delCpo.rowsAffected?.[0] ?? 0,
          BongkarSusunOutputCrusher: delBso.rowsAffected?.[0] ?? 0,
        },
      },
      audit: { actorId, requestId }, // ✅ ID only
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}

    // mapping FK error jika ada constraint lain di DB
    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || 'Gagal hapus karena constraint referensi (FK).';
    }
    throw e;
  }
};


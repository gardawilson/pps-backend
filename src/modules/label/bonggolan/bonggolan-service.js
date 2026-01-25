// services/bonggolan-service.js
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


exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      b.NoBonggolan,
      b.DateCreate,
      b.IdBonggolan,
      mb.NamaBonggolan,                -- 🆕 join ke master
      b.IdWarehouse,
      w.NamaWarehouse,
      b.Blok,
      b.IdLokasi,
      b.Berat,
      CASE 
        WHEN b.IdStatus = 1 THEN 'PASS'
        WHEN b.IdStatus = 0 THEN 'HOLD'
        ELSE ''
      END AS StatusText,

      -- Broker Output
      MAX(bpo.NoProduksi)    AS BrokerNoProduksi,
      MAX(m1.NamaMesin)      AS BrokerNamaMesin,

      -- Inject Output
      MAX(ipo.NoProduksi)    AS InjectNoProduksi,
      MAX(m2.NamaMesin)      AS InjectNamaMesin,

      -- Bongkar Susun
      MAX(bs.NoBongkarSusun) AS NoBongkarSusun

    FROM [dbo].[Bonggolan] b
    LEFT JOIN [dbo].[MstBonggolan] mb
           ON mb.IdBonggolan = b.IdBonggolan          -- 🆕
    LEFT JOIN [dbo].[MstWarehouse] w
           ON w.IdWarehouse = b.IdWarehouse

    -- Broker chain
    LEFT JOIN [dbo].[BrokerProduksiOutputBonggolan] bpo
           ON bpo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[BrokerProduksi_h] bp
           ON bp.NoProduksi = bpo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m1
           ON m1.IdMesin = bp.IdMesin

    -- Inject chain
    LEFT JOIN [dbo].[InjectProduksiOutputBonggolan] ipo
           ON ipo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[InjectProduksi_h] ip
           ON ip.NoProduksi = ipo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m2
           ON m2.IdMesin = ip.IdMesin

    -- Bongkar Susun
    LEFT JOIN [dbo].[BongkarSusunOutputBonggolan] bs
           ON bs.NoBonggolan = b.NoBonggolan

    WHERE 1=1
      AND b.DateUsage IS NULL
      ${
        search
          ? `AND (
               b.NoBonggolan LIKE @search
               OR b.Blok LIKE @search
               OR CONVERT(VARCHAR(20), b.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), b.IdWarehouse) LIKE @search
               OR ISNULL(w.NamaWarehouse,'') LIKE @search
               OR ISNULL(mb.NamaBonggolan,'') LIKE @search     -- 🆕 searchable

               OR ISNULL(bpo.NoProduksi,'') LIKE @search
               OR ISNULL(m1.NamaMesin,'') LIKE @search
               OR ISNULL(ipo.NoProduksi,'') LIKE @search
               OR ISNULL(m2.NamaMesin,'') LIKE @search
               OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
             )`
          : ''
      }
    GROUP BY
      b.NoBonggolan, b.DateCreate, b.IdBonggolan, mb.NamaBonggolan,
      b.IdWarehouse, w.NamaWarehouse, b.Blok, b.IdLokasi, b.Berat, b.IdStatus
    ORDER BY b.NoBonggolan DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT b.NoBonggolan) AS total
    FROM [dbo].[Bonggolan] b
    LEFT JOIN [dbo].[MstBonggolan] mb
           ON mb.IdBonggolan = b.IdBonggolan
    LEFT JOIN [dbo].[MstWarehouse] w
           ON w.IdWarehouse = b.IdWarehouse
    LEFT JOIN [dbo].[BrokerProduksiOutputBonggolan] bpo
           ON bpo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[BrokerProduksi_h] bp
           ON bp.NoProduksi = bpo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m1
           ON m1.IdMesin = bp.IdMesin
    LEFT JOIN [dbo].[InjectProduksiOutputBonggolan] ipo
           ON ipo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[InjectProduksi_h] ip
           ON ip.NoProduksi = ipo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m2
           ON m2.IdMesin = ip.IdMesin
    LEFT JOIN [dbo].[BongkarSusunOutputBonggolan] bs
           ON bs.NoBonggolan = b.NoBonggolan
    WHERE 1=1
      AND b.DateUsage IS NULL
      ${
        search
          ? `AND (
               b.NoBonggolan LIKE @search
               OR b.Blok LIKE @search
               OR CONVERT(VARCHAR(20), b.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), b.IdWarehouse) LIKE @search
               OR ISNULL(w.NamaWarehouse,'') LIKE @search
               OR ISNULL(mb.NamaBonggolan,'') LIKE @search      -- 🆕 searchable
               OR ISNULL(bpo.NoProduksi,'') LIKE @search
               OR ISNULL(m1.NamaMesin,'') LIKE @search
               OR ISNULL(ipo.NoProduksi,'') LIKE @search
               OR ISNULL(m2.NamaMesin,'') LIKE @search
               OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
             )`
          : ''
      }
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};




exports.createBonggolanCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const processedCode = (payload?.ProcessedCode || '').toString().trim(); // '' | 'E.****' | 'S.****' | 'BG.****'

  // ---- validation dasar (samakan crusher)
  if (!header.IdBonggolan) throw badReq('IdBonggolan wajib diisi');
  if (!header.IdWarehouse) throw badReq('IdWarehouse wajib diisi');
  if (!header.CreateBy) throw badReq('CreateBy wajib diisi'); // controller overwrite dari token

  // Identify target from ProcessedCode (optional)
  const hasProcessed = processedCode.length > 0;
  let processedType = null; // 'BROKER' | 'INJECT' | 'BONGKAR'
  if (hasProcessed) {
    if (processedCode.startsWith('E.')) processedType = 'BROKER';
    else if (processedCode.startsWith('S.')) processedType = 'INJECT';
    else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';
    else throw badReq('ProcessedCode prefix tidak dikenali (pakai E., S., atau BG.)');
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
      action: 'create bonggolan',
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari kode (processedCode) kalau header belum isi
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
    // 1) Generate NoBonggolan (PAKAI generateNextCode seperti crusher)
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: 'Bonggolan',
        columnName: 'NoBonggolan',
        prefix: 'M.',
        width: 10,
      });

    const generatedNo = await gen();

    // 2) Double-check belum dipakai (lock)
    const exist = await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), generatedNo)
      .query(`SELECT 1 FROM dbo.Bonggolan WITH (UPDLOCK, HOLDLOCK) WHERE NoBonggolan = @NoBonggolan`);

    if (exist.recordset.length > 0) {
      const retryNo = await gen();
      const exist2 = await new sql.Request(tx)
        .input('NoBonggolan', sql.VarChar(50), retryNo)
        .query(`SELECT 1 FROM dbo.Bonggolan WITH (UPDLOCK, HOLDLOCK) WHERE NoBonggolan = @NoBonggolan`);

      if (exist2.recordset.length > 0) {
        throw conflict('Gagal generate NoBonggolan unik, coba lagi.');
      }
      header.NoBonggolan = retryNo;
    } else {
      header.NoBonggolan = generatedNo;
    }

    // ===============================
    // 3) Insert header (samakan pattern: pakai @DateTimeCreate dari app, bukan GETDATE())
    // ===============================
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO dbo.Bonggolan (
        NoBonggolan, DateCreate, IdBonggolan, IdWarehouse, DateUsage,
        Berat, IdStatus, Blok, IdLokasi, CreateBy, DateTimeCreate
      )
      VALUES (
        @NoBonggolan, @DateCreate, @IdBonggolan, @IdWarehouse, NULL,
        @Berat, @IdStatus, @Blok, @IdLokasi, @CreateBy, @DateTimeCreate
      );
    `;

    await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), header.NoBonggolan)
      .input('DateCreate', sql.Date, effectiveDateCreate)
      .input('IdBonggolan', sql.Int, header.IdBonggolan)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1)
      .input('Blok', sql.VarChar(50), header.Blok ?? null)
      .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
      .input('CreateBy', sql.VarChar(50), header.CreateBy)
      .input('DateTimeCreate', sql.DateTime, nowDateTime)
      .query(insertHeaderSql);

    // ===============================
    // 4) Optional mapping based on processedType (setelah header insert)
    // ===============================
    let mappingTable = null;

    if (processedType === 'BROKER') {
      await new sql.Request(tx)
        .input('NoProduksi', sql.VarChar(50), processedCode)
        .input('NoBonggolan', sql.VarChar(50), header.NoBonggolan)
        .query(`
          INSERT INTO dbo.BrokerProduksiOutputBonggolan (NoProduksi, NoBonggolan)
          VALUES (@NoProduksi, @NoBonggolan);
        `);

      mappingTable = 'BrokerProduksiOutputBonggolan';
    } else if (processedType === 'INJECT') {
      await new sql.Request(tx)
        .input('NoProduksi', sql.VarChar(50), processedCode)
        .input('NoBonggolan', sql.VarChar(50), header.NoBonggolan)
        .query(`
          INSERT INTO dbo.InjectProduksiOutputBonggolan (NoProduksi, NoBonggolan)
          VALUES (@NoProduksi, @NoBonggolan);
        `);

      mappingTable = 'InjectProduksiOutputBonggolan';
    } else if (processedType === 'BONGKAR') {
      await new sql.Request(tx)
        .input('NoBongkarSusun', sql.VarChar(50), processedCode)
        .input('NoBonggolan', sql.VarChar(50), header.NoBonggolan)
        .query(`
          INSERT INTO dbo.BongkarSusunOutputBonggolan (NoBongkarSusun, NoBonggolan)
          VALUES (@NoBongkarSusun, @NoBonggolan);
        `);
        
      mappingTable = 'BongkarSusunOutputBonggolan';
    }

    await tx.commit();

    return {
      header: {
        NoBonggolan: header.NoBonggolan,
        DateCreate: formatYMD(effectiveDateCreate),
        IdBonggolan: header.IdBonggolan,
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
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};



exports.updateBonggolanCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBonggolan = payload?.NoBonggolan?.toString().trim();
  if (!NoBonggolan) throw badReq('NoBonggolan (path) wajib diisi');

  const header = payload?.header || {};
  const processedCode = (payload?.ProcessedCode || '').toString().trim(); // '' | 'E.****' | 'S.****' | 'BG.****'

  // =====================================================
  // [AUDIT] actorId + requestId (ID only)
  // =====================================================
  const actorIdNum = Number(payload?.actorId);
  const actorId = Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;

  const requestId = String(payload?.requestId || `${Date.now()}-${Math.random().toString(16).slice(2)}`);
  if (!actorId) throw badReq('actorId kosong. Controller harus inject payload.actorId dari token.');

  // Identify processedType from ProcessedCode (optional)
  const hasProcessed = processedCode.length > 0;
  let processedType = null; // 'BROKER' | 'INJECT' | 'BONGKAR' | null
  if (hasProcessed) {
    if (processedCode.startsWith('E.')) processedType = 'BROKER';
    else if (processedCode.startsWith('S.')) processedType = 'INJECT';
    else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';
    else throw badReq('ProcessedCode prefix tidak dikenali (pakai E., S., atau BG.)');
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
      .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
      .query(`
        SELECT TOP 1 NoBonggolan, DateCreate, DateUsage
        FROM dbo.Bonggolan WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBonggolan = @NoBonggolan
      `);

    if (exist.recordset.length === 0) {
      throw notFound(`NoBonggolan ${NoBonggolan} tidak ditemukan`);
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
      action: `update bonggolan ${NoBonggolan}`,
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
        action: `update bonggolan ${NoBonggolan} (change DateCreate)`,
        useLock: true,
      });
    }

    // Jika client kirim DateUsage, cek juga (null => allow clear)
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
          action: `update bonggolan ${NoBonggolan} (change DateUsage)`,
          useLock: true,
        });
      }
    }

    // ===============================
    // 1) Update header (partial/dynamic) — mirip crusher/broker
    // ===============================
    const setParts = [];
    const reqHeader = new sql.Request(tx).input('NoBonggolan', sql.VarChar(50), NoBonggolan);

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf('IdBonggolan', 'IdBonggolan', sql.Int, header.IdBonggolan);
    setIf('IdWarehouse', 'IdWarehouse', sql.Int, header.IdWarehouse);

    if (header.DateCreate !== undefined) {
      setIf('DateCreate', 'DateCreate', sql.Date, newDateCreateOnly);
    }

    if (header.DateUsage !== undefined) {
      setIf('DateUsage', 'DateUsage', sql.Date, newDateUsageOnly);
    }

    if (Object.prototype.hasOwnProperty.call(header, 'Berat')) {
      const num = header.Berat === null ? null : Number(header.Berat);
      if (num !== null && (!Number.isFinite(num) || num < 0)) throw badReq('Berat tidak valid.');
      setIf('Berat', 'Berat', sql.Decimal(18, 3), num);
    }

    setIf('IdStatus', 'IdStatus', sql.Int, header.IdStatus);
    setIf('Blok', 'Blok', sql.VarChar(50), header.Blok);

    // IdLokasi -> INT (samakan create bonggolan). Kalau di DB varchar, ubah ke sql.VarChar(50)
    if (header.IdLokasi !== undefined) {
      if (header.IdLokasi === null || String(header.IdLokasi).trim() === '') {
        setIf('IdLokasi', 'IdLokasi', sql.Int, null);
      } else {
        const n = Number(String(header.IdLokasi).trim());
        if (!Number.isFinite(n)) throw badReq('IdLokasi harus angka.');
        setIf('IdLokasi', 'IdLokasi', sql.Int, n);
      }
    }

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.Bonggolan
        SET ${setParts.join(', ')}
        WHERE NoBonggolan = @NoBonggolan
      `);
    }

    // ===============================
    // 2) Optional: Processed mapping (idempotent) — mirip updateCrusher outputs
    // - hanya kalau user memang "mengirim field" ProcessedCode (meski kosong)
    // - reset dulu biar gak duplikat
    // ===============================
    const sentProcessedField = Object.prototype.hasOwnProperty.call(payload, 'ProcessedCode');

    let mappingTable = null;
    if (sentProcessedField) {
      // reset mapping
      await new sql.Request(tx)
        .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
        .query(`DELETE FROM dbo.BrokerProduksiOutputBonggolan WHERE NoBonggolan = @NoBonggolan`);

      await new sql.Request(tx)
        .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
        .query(`DELETE FROM dbo.InjectProduksiOutputBonggolan WHERE NoBonggolan = @NoBonggolan`);

      await new sql.Request(tx)
        .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
        .query(`DELETE FROM dbo.BongkarSusunInputBonggolan WHERE NoBonggolan = @NoBonggolan`);

      // kalau processedCode kosong => user ingin "lepas relasi"
      if (hasProcessed) {
        if (processedType === 'BROKER') {
          await new sql.Request(tx)
            .input('NoProduksi', sql.VarChar(50), processedCode)
            .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
            .query(`
              INSERT INTO dbo.BrokerProduksiOutputBonggolan (NoProduksi, NoBonggolan)
              VALUES (@NoProduksi, @NoBonggolan);
            `);
          mappingTable = 'BrokerProduksiOutputBonggolan';
        } else if (processedType === 'INJECT') {
          await new sql.Request(tx)
            .input('NoProduksi', sql.VarChar(50), processedCode)
            .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
            .query(`
              INSERT INTO dbo.InjectProduksiOutputBonggolan (NoProduksi, NoBonggolan)
              VALUES (@NoProduksi, @NoBonggolan);
            `);
          mappingTable = 'InjectProduksiOutputBonggolan';
        } else if (processedType === 'BONGKAR') {
          await new sql.Request(tx)
            .input('NoBongkarSusun', sql.VarChar(50), processedCode)
            .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
            .query(`
              INSERT INTO dbo.BongkarSusunInputBonggolan (NoBongkarSusun, NoBonggolan)
              VALUES (@NoBongkarSusun, @NoBonggolan);
            `);
          mappingTable = 'BongkarSusunInputBonggolan';
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoBonggolan,
        ...header,
        existingDateCreate: existingDateOnly ? formatYMD(existingDateOnly) : null,
        ...(newDateCreateOnly ? { newDateCreate: formatYMD(newDateCreateOnly) } : {}),
        ...(header.DateUsage !== undefined
          ? { newDateUsage: newDateUsageOnly ? formatYMD(newDateUsageOnly) : null }
          : {}),
      },
      processed: sentProcessedField
        ? { code: processedCode || null, type: processedType, mappingTable }
        : undefined,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};




exports.deleteBonggolanCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBonggolan = payload?.NoBonggolan?.toString().trim();
  if (!NoBonggolan) throw badReq('NoBonggolan (path) wajib diisi');

  // =====================================================
  // [AUDIT] actorId + requestId (ID only)
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
    // 0) Lock + ambil DateCreate existing
    // ===============================
    const headRes = await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
      .query(`
        SELECT TOP 1 NoBonggolan, DateCreate
        FROM dbo.Bonggolan WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBonggolan = @NoBonggolan
      `);

    if (headRes.recordset.length === 0) {
      throw notFound(`NoBonggolan ${NoBonggolan} tidak ditemukan`);
    }

    const existingDateOnly = toDateOnly(headRes.recordset[0]?.DateCreate);

    // ===============================
    // 1) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `delete bonggolan ${NoBonggolan}`,
      useLock: true,
    });

    // ===============================
    // 2) delete mappings (idempotent)
    // ===============================
    await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
      .query(`DELETE FROM dbo.BrokerProduksiOutputBonggolan WHERE NoBonggolan = @NoBonggolan`);

    await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
      .query(`DELETE FROM dbo.InjectProduksiOutputBonggolan WHERE NoBonggolan = @NoBonggolan`);

    // ✅ pakai yang kamu punya di kode lama:
    await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
      .query(`DELETE FROM dbo.BongkarSusunOutputBonggolan WHERE NoBonggolan = @NoBonggolan`);

    // (kalau yang benar adalah Input, ganti jadi ini)
    // await new sql.Request(tx)
    //   .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
    //   .query(`DELETE FROM dbo.BongkarSusunInputBonggolan WHERE NoBonggolan = @NoBonggolan`);

    // ===============================
    // 3) delete header
    // ===============================
    const result = await new sql.Request(tx)
      .input('NoBonggolan', sql.VarChar(50), NoBonggolan)
      .query(`
        DELETE FROM dbo.Bonggolan
        WHERE NoBonggolan = @NoBonggolan;
      `);

    if ((result.rowsAffected?.[0] ?? 0) === 0) {
      // harusnya tidak kejadian karena sudah lock+cek di atas
      throw notFound(`NoBonggolan ${NoBonggolan} tidak ditemukan`);
    }

    await tx.commit();

    return {
      deleted: true,
      NoBonggolan,
      existingDateCreate: existingDateOnly ? formatYMD(existingDateOnly) : null,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

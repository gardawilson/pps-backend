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



function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// Generate next NoCrusher: e.g. 'F.0000000002'
async function generateNextNoCrusher(tx, { prefix = 'F.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 c.NoCrusher
    FROM [dbo].[Crusher] AS c WITH (UPDLOCK, HOLDLOCK)
    WHERE c.NoCrusher LIKE @prefix + '%'
    ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(c.NoCrusher, LEN(@prefix) + 1, 50)) DESC,
             c.NoCrusher DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoCrusher; // e.g. "F.0000000001"
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

exports.createCrusherCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const processedCode = (payload?.ProcessedCode || '').toString().trim(); // '', 'G.****', 'BG.****'

  // ---- validation
  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };
  if (!header.IdCrusher)   throw badReq('IdCrusher is required');
  if (!header.IdWarehouse) throw badReq('IdWarehouse is required');
  if (!header.CreateBy)    throw badReq('CreateBy is required');

  // Identify target from ProcessedCode (optional)
  const hasProcessed = processedCode.length > 0;
  let processedType = null; // 'PRODUKSI' | 'BONGKAR'
  if (hasProcessed) {
    if (processedCode.startsWith('G.'))       processedType = 'PRODUKSI';
    else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';
    else throw badReq('ProcessedCode prefix not recognized (use G. or BG.)');
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE) - UTC
    // ===============================
    const nowDateOnly = resolveEffectiveDateForCreate(header.DateCreate); // ✅ UTC date-only
    await assertNotLocked({
      date: nowDateOnly,
      runner: tx,
      action: 'create crusher',
      useLock: true,
    });

    // 0) Auto-isi Blok & IdLokasi dari kode produksi / bongkar susun (jika header belum isi)
    if (!header.Blok || !header.IdLokasi) {
      if (processedCode) {
        const lokasi = await getBlokLokasiFromKodeProduksi({
          kode: processedCode,
          runner: tx,
        });

        if (lokasi) {
          if (!header.Blok) header.Blok = lokasi.Blok;
          if (!header.IdLokasi) header.IdLokasi = lokasi.IdLokasi;
        }
      }
    }

    // 1) Generate NoCrusher
    const generatedNo = await generateNextNoCrusher(tx, { prefix: 'F.', width: 10 });

    // Double-check uniqueness
    const exist = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, generatedNo)
      .query(`SELECT 1 FROM [dbo].[Crusher] WITH (UPDLOCK, HOLDLOCK) WHERE NoCrusher = @NoCrusher`);

    const noCrusher = (exist.recordset.length > 0)
      ? await generateNextNoCrusher(tx, { prefix: 'F.', width: 10 })
      : generatedNo;

    // 2) Insert header into dbo.Crusher
    // ✅ selalu pakai @DateCreate supaya:
    // - tanggal yang dicek tutup transaksi = tanggal yang disimpan
    // - tidak tergantung GETDATE() server
    const insertHeaderSql = `
      INSERT INTO [dbo].[Crusher] (
        NoCrusher, DateCreate, IdCrusher, IdWarehouse, DateUsage,
        Berat, IdStatus, Blok, IdLokasi, CreateBy, DateTimeCreate
      )
      VALUES (
        @NoCrusher,
        @DateCreate,
        @IdCrusher, @IdWarehouse, NULL,
        @Berat, @IdStatus, @Blok, @IdLokasi, @CreateBy, GETDATE()
      );
    `;

    const rqHeader = new sql.Request(tx);
    rqHeader
      .input('NoCrusher', sql.VarChar, noCrusher)
      .input('DateCreate', sql.Date, nowDateOnly) // ✅ UTC date-only
      .input('IdCrusher', sql.Int, header.IdCrusher)
      .input('IdWarehouse', sql.Int, header.IdWarehouse)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('IdStatus', sql.Int, header.IdStatus ?? 1)
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.Int, header.IdLokasi ?? null)
      .input('CreateBy', sql.VarChar, header.CreateBy);

    await rqHeader.query(insertHeaderSql);

    // 3) Optional: insert mapping based on ProcessedCode prefix
    let mappingTable = null;
    if (processedType === 'PRODUKSI') {
      const q = `
        INSERT INTO [dbo].[CrusherProduksiOutput] (NoCrusherProduksi, NoCrusher)
        VALUES (@Processed, @NoCrusher);
      `;
      await new sql.Request(tx)
        .input('Processed', sql.VarChar, processedCode)
        .input('NoCrusher', sql.VarChar, noCrusher)
        .query(q);
      mappingTable = 'CrusherProduksiOutput';
    } else if (processedType === 'BONGKAR') {
      const q = `
        INSERT INTO [dbo].[BongkarSusunOutputCrusher] (NoBongkarSusun, NoCrusher)
        VALUES (@Processed, @NoCrusher);
      `;
      await new sql.Request(tx)
        .input('Processed', sql.VarChar, processedCode)
        .input('NoCrusher', sql.VarChar, noCrusher)
        .query(q);
      mappingTable = 'BongkarSusunOutputCrusher';
    }

    await tx.commit();

    return {
      header: {
        NoCrusher: noCrusher,
        DateCreate: formatYMD(nowDateOnly), // ✅ konsisten UTC string
        IdCrusher: header.IdCrusher,
        IdWarehouse: header.IdWarehouse,
        Berat: header.Berat ?? null,
        IdStatus: header.IdStatus ?? 1,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
        CreateBy: header.CreateBy,
        DateTimeCreate: 'GETDATE()',
      },
      processed: {
        code: processedCode || null,
        type: processedType,
        mappingTable,
      },
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};



exports.updateCrusher = async (noCrusher, payload = {}) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const fields = [
    { key: 'DateCreate',  type: sql.Date, isDateOnly: true },
    { key: 'IdCrusher',   type: sql.Int },
    { key: 'IdWarehouse', type: sql.Int },
    { key: 'DateUsage',   type: sql.Date, isDateOnly: true },
    { key: 'Berat',       type: sql.Decimal(18, 3) },
    { key: 'IdStatus',    type: sql.Int },
    { key: 'Blok',        type: sql.VarChar },
    { key: 'IdLokasi',    type: sql.VarChar }, // cek tipe kolom di DB (int/varchar)
  ];

  const toUpdate = fields.filter(f => payload[f.key] !== undefined);
  if (toUpdate.length === 0) {
    const e = new Error('No valid fields to update');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // Lock row + optionally fetch existing DateCreate for reference
    const head = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, noCrusher)
      .query(`
        SELECT TOP 1 NoCrusher, CONVERT(date, DateCreate) AS DateCreate
        FROM [dbo].[Crusher] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher
      `);

    if (head.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Crusher not found: ${noCrusher}`);
      e.statusCode = 404;
      throw e;
    }

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (UPDATE)
    // - kalau user mengubah DateCreate / DateUsage, cek tanggal tsb
    // ===============================
    if (payload.DateCreate !== undefined) {
      const d = (payload.DateCreate === null || payload.DateCreate === '')
        ? null
        : toDateOnly(payload.DateCreate);

      if (d) {
        await assertNotLocked({
          date: d,
          runner: tx,
          action: 'update crusher (DateCreate)',
          useLock: true,
        });
      }
      // kalau null: artinya mau clear date, biasanya tidak disarankan untuk DateCreate.
      // Jika ingin larang null, bisa throw badReq di sini.
    }

    if (payload.DateUsage !== undefined) {
      const d = (payload.DateUsage === null || payload.DateUsage === '')
        ? null
        : toDateOnly(payload.DateUsage);

      if (d) {
        await assertNotLocked({
          date: d,
          runner: tx,
          action: 'update crusher (DateUsage)',
          useLock: true,
        });
      }
    }

    // Build SET clause + bind params
    const setClauses = [];
    const rq = new sql.Request(tx);
    rq.input('NoCrusher', sql.VarChar, noCrusher);

    for (const f of toUpdate) {
      const param = `p_${f.key}`;
      setClauses.push(`[${f.key}] = @${param}`);

      // DATE (UTC date-only)
      if (f.isDateOnly) {
        if (payload[f.key] === null || payload[f.key] === '') {
          rq.input(param, f.type, null);
        } else {
          const d = toDateOnly(payload[f.key]);
          if (!d) {
            const e = new Error(`Invalid date for ${f.key}`);
            e.statusCode = 400;
            e.meta = { field: f.key, value: payload[f.key] };
            throw e;
          }
          rq.input(param, f.type, d); // ✅ UTC date-only
        }
        continue;
      }

      // DECIMAL
      if (f.type?.declaration?.startsWith('decimal')) {
        if (payload[f.key] === null || payload[f.key] === '') {
          rq.input(param, f.type, null);
        } else {
          const num = Number(payload[f.key]);
          if (Number.isNaN(num)) {
            const e = new Error(`Invalid number for ${f.key}`);
            e.statusCode = 400;
            e.meta = { field: f.key, value: payload[f.key] };
            throw e;
          }
          rq.input(param, f.type, num);
        }
        continue;
      }

      // OTHER
      rq.input(param, f.type, payload[f.key]);
    }

    await rq.query(`
      UPDATE [dbo].[Crusher]
      SET ${setClauses.join(', ')}
      WHERE NoCrusher = @NoCrusher;
    `);

    await tx.commit();
    return { updated: true, updatedFields: toUpdate.map(f => f.key) };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};


exports.deleteCrusherCascade = async (noCrusher) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const badReq = (msg) => { const e = new Error(msg); e.statusCode = 400; return e; };
  if (!noCrusher || !String(noCrusher).trim()) throw badReq('noCrusher wajib');

  try {
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    // ===============================
    // 0) Lock + ambil DateCreate untuk rule tutup transaksi
    // ===============================
    const head = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, noCrusher)
      .query(`
        SELECT TOP 1 NoCrusher, CONVERT(date, DateCreate) AS DateCreate
        FROM [dbo].[Crusher] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusher = @NoCrusher
      `);

    if (head.recordset.length === 0) {
      await tx.rollback();
      const e = new Error(`Crusher not found: ${noCrusher}`);
      e.statusCode = 404;
      throw e;
    }

    const trxDate = head.recordset[0]?.DateCreate ? toDateOnly(head.recordset[0].DateCreate) : null;

    // ===============================
    // 1) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: 'delete crusher',
      useLock: true,
    });

    // ===============================
    // 2) delete mappings (if any)
    // ===============================
    const mappingQueries = [
      `DELETE FROM [dbo].[CrusherProduksiOutput] WHERE NoCrusher = @NoCrusher`,
      `DELETE FROM [dbo].[BongkarSusunOutputCrusher] WHERE NoCrusher = @NoCrusher`,
    ];

    for (const q of mappingQueries) {
      await new sql.Request(tx)
        .input('NoCrusher', sql.VarChar, noCrusher)
        .query(q);
    }

    // ===============================
    // 3) delete header
    // ===============================
    const result = await new sql.Request(tx)
      .input('NoCrusher', sql.VarChar, noCrusher)
      .query(`
        DELETE FROM [dbo].[Crusher]
        WHERE NoCrusher = @NoCrusher;
      `);

    if ((result.rowsAffected?.[0] ?? 0) === 0) {
      await tx.rollback();
      const e = new Error(`Crusher not found: ${noCrusher}`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();
    return { deleted: true, noCrusher };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
};


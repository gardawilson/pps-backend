const { sql, poolPromise } = require('../../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
  loadDocDateOnlyFromConfig
} = require('../../../core/shared/tutup-transaksi-guard');

const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require('../../../core/utils/jam-kerja-helper');

const sharedInputService = require('../../../core/shared/produksi-input.service');

const { badReq, conflict } = require('../../../core/utils/http-error'); 

const { applyAuditContext } = require('../../../core/utils/db-audit-context');
const { generateNextCode } = require('../../../core/utils/sequence-code-helper'); 




/**
 * Paginated fetch for dbo.CrusherProduksi_h
 * Columns available:
 *  NoCrusherProduksi, Tanggal, IdMesin, IdOperator, Jam, Shift, CreateBy,
 *  CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter, HourStart, HourEnd
 *
 * We LEFT JOIN to masters and ALIAS Jam -> JamKerja for UI compatibility.
 */
async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoCrusherProduksi LIKE '%' + @search + '%')
  `;

  // 1) Count (lightweight)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.CrusherProduksi_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);

  const countRes = await countReq.query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;

  if (total === 0) return { data: [], total: 0 };

  // 2) Data + Flag Tutup Transaksi
  const dataQry = `
    ;WITH LastClosed AS (
      SELECT TOP 1
        CONVERT(date, PeriodHarian) AS LastClosedDate
      FROM dbo.MstTutupTransaksiHarian WITH (NOLOCK)
      WHERE [Lock] = 1
      ORDER BY CONVERT(date, PeriodHarian) DESC, Id DESC
    )
    SELECT
      h.NoCrusherProduksi,
      h.Tanggal,
      h.IdMesin,
      ms.NamaMesin,
      h.IdOperator,
      op.NamaOperator,
      h.Jam         AS JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd, 108) AS HourEnd,

      -- (opsional utk frontend)
      lc.LastClosedDate AS LastClosedDate,

      -- ✅ flag tutup transaksi
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.CrusherProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin     = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator  = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    -- rekomendasi: urut by tanggal + jam + no
    ORDER BY h.NoCrusherProduksi DESC, h.Tanggal DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}


/**
 * GET CrusherProduksi_h by date
 * - Links to MstMesin for NamaMesin
 * - Aggregates output NoCrusher from CrusherProduksiOutput → "OutputNoCrusher" (comma-separated)
 *
 * Tables:
 *  - dbo.CrusherProduksi_h       (NoCrusherProduksi, Tanggal, IdMesin, IdOperator, Jam, Shift, ...)
 *  - dbo.MstMesin                (IdMesin -> NamaMesin)
 *  - dbo.CrusherProduksiOutput   (NoCrusherProduksi -> NoCrusher)
 */
async function getProduksiByDate({ date, idMesin = null, shift = null }) {
  const pool = await poolPromise;
  const request = pool.request();

  const filters = ['CONVERT(date, h.Tanggal) = @date'];
  request.input('date', sql.Date, date);

  if (idMesin) {
    filters.push('h.IdMesin = @idMesin');
    request.input('idMesin', sql.Int, idMesin);
  }

  if (shift && shift.length > 0) {
    filters.push('h.Shift = @shift');
    request.input('shift', sql.VarChar, shift);
  }

  const whereClause = filters.join(' AND ');

  // STRING_AGG requires SQL Server 2017+, your env is SQL 2022 — good.
  const query = `
    SELECT
      h.NoCrusherProduksi,
      CONVERT(date, h.Tanggal) AS Tanggal,
      h.IdMesin,
      m.NamaMesin,
      h.IdOperator,
      h.Jam,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter,

      -- outputs connected to this produksi
      (
        SELECT STRING_AGG(cpo.NoCrusher, ', ')
        FROM dbo.CrusherProduksiOutput cpo
        WHERE cpo.NoCrusherProduksi = h.NoCrusherProduksi
      ) AS OutputNoCrusher

    FROM dbo.CrusherProduksi_h h
    LEFT JOIN dbo.MstMesin m ON m.IdMesin = h.IdMesin
    WHERE ${whereClause}
    ORDER BY h.Jam ASC, h.NoCrusherProduksi ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

/**
 * GET enabled MstCrusher (for dropdowns)
 * MstCrusher: IdCrusher, NamaCrusher, Enable
 */
async function getCrusherMasters() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      mc.IdCrusher,
      mc.NamaCrusher,
      mc.Enable
    FROM dbo.MstCrusher mc
    WHERE ISNULL(mc.Enable, 1) = 1
    ORDER BY mc.NamaCrusher;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}



// crusher-produksi.service.js (revised)

// pastikan import sesuai project kamu
// const { poolPromise, sql } = require('../config/db');  // atau path yang kamu pakai
// const { generateNextCode } = require('../core/utils/sequence-code-helper');
// const { applyAuditContext } = require('../services/audit/apply-audit-context'); // contoh
// const { assertNotLocked } = require('../services/lock/lock-service');           // contoh
// const { badReq, conflict } = require('../core/utils/errors');                  // contoh
// const { toDateOnly, parseJamToInt, calcJamKerjaFromStartEnd } = require('../core/utils/date-helper'); // contoh

async function createCrusherProduksi(payload, ctx) {
  // ===============================
  // 0) Validasi payload basic (business)
  // ===============================
  const body = payload && typeof payload === 'object' ? payload : {};

  const must = [];
  if (!body?.tanggal) must.push('tanggal');
  if (body?.idMesin == null) must.push('idMesin');
  if (body?.idOperator == null) must.push('idOperator');
  if (body?.shift == null) must.push('shift');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  // jam bisa dari body.jam atau dihitung dari hourStart-hourEnd
  let jamKerja = body?.jam ?? null;
  if (jamKerja == null) {
    const calc = calcJamKerjaFromStartEnd(body?.hourStart, body?.hourEnd);
    if (calc != null) jamKerja = calc;
  }
  if (jamKerja == null) throw badReq('Field wajib: jam (atau isi hourStart-hourEnd)');

  const jamInt = parseJamToInt(jamKerja);
  const docDateOnly = toDateOnly(body.tanggal);

  // ===============================
  // 1) Validasi + normalisasi ctx (audit)
  // ===============================
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
  }

  const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';
  const requestId = String(ctx?.requestId || '').trim(); // boleh kosong

  const auditCtx = {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  };

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 2) Set SESSION_CONTEXT untuk trigger audit (1x di awal tx)
    // =====================================================
    const auditReq = new sql.Request(tx);
    const audit = await applyAuditContext(auditReq, auditCtx);

    // =====================================================
    // 3) Guard tutup transaksi (CREATE = WRITE)
    // =====================================================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'create CrusherProduksi',
      useLock: true,
    });

    // =====================================================
    // 4) Generate NoCrusherProduksi via generateNextCode()
    //    Format: G.0000000001
    // =====================================================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: 'dbo.CrusherProduksi_h',
        columnName: 'NoCrusherProduksi',
        prefix: 'G.',
        width: 10,
      });

    let noCrusherProduksi = await gen();

    // optional double-check (lebih bagus kalau kolom ada UNIQUE)
    const exist = await new sql.Request(tx)
      .input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi)
      .query(`
        SELECT 1
        FROM dbo.CrusherProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusherProduksi = @NoCrusherProduksi
      `);

    if (exist.recordset.length > 0) {
      const retry = await gen();
      const exist2 = await new sql.Request(tx)
        .input('NoCrusherProduksi', sql.VarChar(50), retry)
        .query(`
          SELECT 1
          FROM dbo.CrusherProduksi_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoCrusherProduksi = @NoCrusherProduksi
        `);

      if (exist2.recordset.length > 0) {
        throw conflict('Gagal generate NoCrusherProduksi unik, coba lagi.');
      }
      noCrusherProduksi = retry;
    }

    // =====================================================
    // 5) Insert header (OUTPUT ... INTO @out) + safe time cast
    // =====================================================
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi)
      .input('Tanggal',           sql.Date,        docDateOnly)
      .input('IdMesin',           sql.Int,         body.idMesin)
      .input('IdOperator',        sql.Int,         body.idOperator)
      .input('Jam',               sql.Int,         jamInt)
      .input('Shift',             sql.Int,         body.shift)
      .input('CreateBy',          sql.VarChar(100), body.createBy) // controller overwrite
      .input('CheckBy1',          sql.VarChar(100), body.checkBy1 ?? null)
      .input('CheckBy2',          sql.VarChar(100), body.checkBy2 ?? null)
      .input('ApproveBy',         sql.VarChar(100), body.approveBy ?? null)
      .input('JmlhAnggota',       sql.Int,          body.jmlhAnggota ?? null)
      .input('Hadir',             sql.Int,          body.hadir ?? null)
      .input('HourMeter',         sql.Decimal(18, 2), body.hourMeter ?? null)
      // kirim string, biar SQL yang CAST ke time(7)
      .input('HourStart',         sql.VarChar(20),  body.hourStart ?? null)
      .input('HourEnd',           sql.VarChar(20),  body.hourEnd ?? null);

    const insertSql = `
      DECLARE @out TABLE (
        NoCrusherProduksi varchar(50),
        Tanggal           date,
        IdMesin           int,
        IdOperator        int,
        Jam               int,
        Shift             int,
        CreateBy          varchar(100),
        CheckBy1          varchar(100),
        CheckBy2          varchar(100),
        ApproveBy         varchar(100),
        JmlhAnggota       int,
        Hadir             int,
        HourMeter         decimal(18,2),
        HourStart         time(7),
        HourEnd           time(7)
      );

      INSERT INTO dbo.CrusherProduksi_h (
        NoCrusherProduksi,
        Tanggal,
        IdMesin,
        IdOperator,
        Jam,
        Shift,
        CreateBy,
        CheckBy1,
        CheckBy2,
        ApproveBy,
        JmlhAnggota,
        Hadir,
        HourMeter,
        HourStart,
        HourEnd
      )
      OUTPUT
        INSERTED.NoCrusherProduksi,
        INSERTED.Tanggal,
        INSERTED.IdMesin,
        INSERTED.IdOperator,
        INSERTED.Jam,
        INSERTED.Shift,
        INSERTED.CreateBy,
        INSERTED.CheckBy1,
        INSERTED.CheckBy2,
        INSERTED.ApproveBy,
        INSERTED.JmlhAnggota,
        INSERTED.Hadir,
        INSERTED.HourMeter,
        INSERTED.HourStart,
        INSERTED.HourEnd
      INTO @out
      VALUES (
        @NoCrusherProduksi,
        @Tanggal,
        @IdMesin,
        @IdOperator,
        @Jam,
        @Shift,
        @CreateBy,
        @CheckBy1,
        @CheckBy2,
        @ApproveBy,
        @JmlhAnggota,
        @Hadir,
        @HourMeter,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd   IS NULL OR LTRIM(RTRIM(@HourEnd))   = '' THEN NULL ELSE CAST(@HourEnd   AS time(7)) END
      );

      SELECT * FROM @out;
    `;

    const insRes = await rqIns.query(insertSql);

    await tx.commit();

    return {
      header: insRes.recordset?.[0] || null,
      audit, // optional debug/tracing
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}



/**
 * UPDATE CRUSHER PRODUCTION HEADER
 * Supports partial updates of header fields
 * Automatically syncs DateUsage for all inputs when Tanggal is changed
 */
async function updateCrusherProduksi(noCrusherProduksi, payload, ctx) {
  if (!noCrusherProduksi) throw badReq('noCrusherProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) SET AUDIT CONTEXT (PERSIS WASHING)
    // =====================================================
    const actorIdNum = Number(ctx?.actorId);
    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
    }

    const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';
    const requestId = String(ctx?.requestId || '').trim();

    const auditReq = new sql.Request(tx);
    await applyAuditContext(auditReq, {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    // =====================================================
    // 1) LOAD DOC DATE (LOCK)
    // =====================================================
    const { docDateOnly: oldDocDateOnly } =
      await loadDocDateOnlyFromConfig({
        entityKey: 'crusherProduksi',
        codeValue: noCrusherProduksi,
        runner: tx,
        useLock: true,
        throwIfNotFound: true,
      });

    // =====================================================
    // 2) HANDLE TANGGAL
    // =====================================================
    const isChangingDate = payload?.tanggal !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tanggal) throw badReq('tanggal tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tanggal);
    }

    // =====================================================
    // 3) GUARD TUTUP TRANSAKSI
    // =====================================================
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: 'update CrusherProduksi (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update CrusherProduksi (new date)',
        useLock: true,
      });
    }

    // =====================================================
    // 4) BUILD DYNAMIC SET
    // =====================================================
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('Tanggal = @Tanggal');
      rqUpd.input('Tanggal', sql.Date, newDocDateOnly);
    }

    if (payload.idMesin !== undefined) {
      sets.push('IdMesin = @IdMesin');
      rqUpd.input('IdMesin', sql.Int, payload.idMesin);
    }

    if (payload.idOperator !== undefined) {
      sets.push('IdOperator = @IdOperator');
      rqUpd.input('IdOperator', sql.Int, payload.idOperator);
    }

    if (payload.shift !== undefined) {
      sets.push('Shift = @Shift');
      rqUpd.input('Shift', sql.Int, payload.shift);
    }

    if (payload.checkBy1 !== undefined) {
      sets.push('CheckBy1 = @CheckBy1');
      rqUpd.input('CheckBy1', sql.VarChar(100), payload.checkBy1 ?? null);
    }

    if (payload.checkBy2 !== undefined) {
      sets.push('CheckBy2 = @CheckBy2');
      rqUpd.input('CheckBy2', sql.VarChar(100), payload.checkBy2 ?? null);
    }

    if (payload.approveBy !== undefined) {
      sets.push('ApproveBy = @ApproveBy');
      rqUpd.input('ApproveBy', sql.VarChar(100), payload.approveBy ?? null);
    }

    if (payload.jmlhAnggota !== undefined) {
      sets.push('JmlhAnggota = @JmlhAnggota');
      rqUpd.input('JmlhAnggota', sql.Int, payload.jmlhAnggota ?? null);
    }

    if (payload.hadir !== undefined) {
      sets.push('Hadir = @Hadir');
      rqUpd.input('Hadir', sql.Int, payload.hadir ?? null);
    }

    if (payload.hourMeter !== undefined) {
      sets.push('HourMeter = @HourMeter');
      rqUpd.input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    if (payload.jam !== undefined) {
      const jamInt = payload.jam === null ? null : parseJamToInt(payload.jam);
      sets.push('Jam = @Jam');
      rqUpd.input('Jam', sql.Int, jamInt);
    }

    if (payload.hourStart !== undefined) {
      sets.push(`
        HourStart =
          CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL
               ELSE CAST(@HourStart AS time(7)) END
      `);
      rqUpd.input('HourStart', sql.VarChar(20), payload.hourStart ?? null);
    }

    if (payload.hourEnd !== undefined) {
      sets.push(`
        HourEnd =
          CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL
               ELSE CAST(@HourEnd AS time(7)) END
      `);
      rqUpd.input('HourEnd', sql.VarChar(20), payload.hourEnd ?? null);
    }

    if (sets.length === 0) throw badReq('No fields to update');

    rqUpd.input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi);

    const sqlUpdate = `
      UPDATE dbo.CrusherProduksi_h
      SET ${sets.join(', ')}
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      SELECT *
      FROM dbo.CrusherProduksi_h
      WHERE NoCrusherProduksi = @NoCrusherProduksi;
    `;

    const resUpd = await rqUpd.query(sqlUpdate);
    const updatedHeader = resUpd.recordset?.[0] || null;

    await tx.commit();
    return { header: updatedHeader };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}



/**
 * DELETE CRUSHER PRODUCTION
 * Deletes header and all related inputs/partials
 * Validates that no outputs exist before deletion
 * Resets DateUsage and IsPartial flags for affected materials
 */
async function deleteCrusherProduksi(noCrusherProduksi, ctx) {
  if (!noCrusherProduksi) throw badReq('noCrusherProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) SET SESSION_CONTEXT (WAJIB untuk trigger audit)
    // =====================================================
    const actorIdNum = Number(ctx?.actorId);
    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
    }

    const actorUsername =
      String(ctx?.actorUsername || '').trim() || 'system';
    const requestId =
      String(ctx?.requestId || '').trim();

    const auditReq = new sql.Request(tx);
    await applyAuditContext(auditReq, {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    // -------------------------------------------------------
    // 1) AMBIL docDateOnly DARI CONFIG (LOCK HEADER)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'crusherProduksi',
      codeValue: noCrusherProduksi,
      runner: tx,
      useLock: true,          // DELETE = write
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 2) GUARD TUTUP TRANSAKSI
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete CrusherProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 3) CEK OUTPUT
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const outCheck = await rqCheck
      .input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi)
      .query(`
        SELECT COUNT(*) AS CntOutput
        FROM dbo.CrusherProduksiOutput
        WHERE NoCrusherProduksi = @NoCrusherProduksi;
      `);

    const hasOutput = (outCheck.recordset?.[0]?.CntOutput || 0) > 0;
    if (hasOutput) {
      throw badReq(
        'Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.'
      );
    }

    // -------------------------------------------------------
    // 4) DELETE INPUT + PARTIAL + RESET + DELETE HEADER
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK MENYIMPAN KEY TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BBKeys TABLE (
        NoBahanBaku varchar(50),
        NoPallet    varchar(50),
        NoSak       varchar(50)
      );

      DECLARE @BonggolanKeys TABLE (
        NoBonggolan varchar(50)
      );

      ---------------------------------------------------------
      -- 1. BAHAN BAKU (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BBKeys (NoBahanBaku, NoPallet, NoSak)
      SELECT DISTINCT bb.NoBahanBaku, bb.NoPallet, bb.NoSak
      FROM dbo.BahanBaku_d AS bb
      WHERE EXISTS (
        SELECT 1
        FROM dbo.CrusherProduksiInputBB AS map
        WHERE map.NoCrusherProduksi = @NoCrusherProduksi
          AND map.NoBahanBaku = bb.NoBahanBaku
          AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
          AND map.NoSak = bb.NoSak
      )
      OR EXISTS (
        SELECT 1
        FROM dbo.CrusherProduksiInputBBPartial AS mp
        JOIN dbo.BahanBakuPartial AS bp
          ON bp.NoBBPartial = mp.NoBBPartial
        WHERE mp.NoCrusherProduksi = @NoCrusherProduksi
          AND bp.NoBahanBaku = bb.NoBahanBaku
          AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
          AND bp.NoSak = bb.NoSak
      );

      DELETE bp
      FROM dbo.BahanBakuPartial AS bp
      JOIN dbo.CrusherProduksiInputBBPartial AS mp
        ON mp.NoBBPartial = bp.NoBBPartial
      WHERE mp.NoCrusherProduksi = @NoCrusherProduksi;

      DELETE FROM dbo.CrusherProduksiInputBBPartial
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      DELETE FROM dbo.CrusherProduksiInputBB
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      UPDATE bb
      SET bb.DateUsage = NULL,
          bb.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.BahanBakuPartial bp
              WHERE bp.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak = bb.NoSak
            ) THEN 1 ELSE 0 END
      FROM dbo.BahanBaku_d bb
      JOIN @BBKeys k
        ON k.NoBahanBaku = bb.NoBahanBaku
       AND ISNULL(k.NoPallet,'') = ISNULL(bb.NoPallet,'')
       AND k.NoSak = bb.NoSak;

      ---------------------------------------------------------
      -- 2. BONGGOLAN (FULL ONLY)
      ---------------------------------------------------------
      INSERT INTO @BonggolanKeys (NoBonggolan)
      SELECT DISTINCT map.NoBonggolan
      FROM dbo.CrusherProduksiInputBonggolan map
      WHERE map.NoCrusherProduksi = @NoCrusherProduksi;

      DELETE FROM dbo.CrusherProduksiInputBonggolan
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      UPDATE b
      SET b.DateUsage = NULL
      FROM dbo.Bonggolan b
      JOIN @BonggolanKeys k ON k.NoBonggolan = b.NoBonggolan;

      ---------------------------------------------------------
      -- 3. DELETE HEADER (TRIGGER AUDIT AKAN JALAN)
      ---------------------------------------------------------
      DELETE FROM dbo.CrusherProduksi_h
      WHERE NoCrusherProduksi = @NoCrusherProduksi;
    `;

    const res = await req.query(sqlDelete);
    if (res.rowsAffected?.[res.rowsAffected.length - 1] === 0) {
      throw notFound(`NoCrusherProduksi tidak ditemukan: ${noCrusherProduksi}`);
    }

    await tx.commit();
    return { success: true };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}



/**
 * FETCH INPUTS for Crusher Production
 * Categories: BB (with partial) + Bonggolan (no partial)
 */
async function fetchInputs(noCrusherProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noCrusherProduksi);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */
    
    /* Bahan Baku (non-partial) */
    SELECT 
      'bb' AS Src,
      ibb.NoCrusherProduksi,
      ibb.NoBahanBaku AS Ref1,
      ibb.NoPallet    AS Ref2,
      ibb.NoSak       AS Ref3,
      bb.Berat AS Berat,
      bb.BeratAct AS BeratAct,
      bb.IsPartial AS IsPartial,
      bbh.IdJenisPlastik AS IdJenis,
      jp.Jenis           AS NamaJenis
    FROM dbo.CrusherProduksiInputBB ibb WITH (NOLOCK)
    LEFT JOIN dbo.BahanBaku_d bb WITH (NOLOCK)
      ON bb.NoBahanBaku = ibb.NoBahanBaku 
      AND bb.NoPallet = ibb.NoPallet 
      AND bb.NoSak = ibb.NoSak
    LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
      ON bbh.NoBahanBaku = ibb.NoBahanBaku 
      AND bbh.NoPallet = ibb.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE ibb.NoCrusherProduksi = @no

    UNION ALL

    /* Bonggolan (no partial, no jenis plastik) */
    SELECT
      'bonggolan' AS Src,
      ib.NoCrusherProduksi,
      ib.NoBonggolan AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      b.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      b.IdBonggolan AS IdJenis,
      CAST('Bonggolan' AS varchar(100)) AS NamaJenis
    FROM dbo.CrusherProduksiInputBonggolan ib WITH (NOLOCK)
    LEFT JOIN dbo.Bonggolan b WITH (NOLOCK) 
      ON b.NoBonggolan = ib.NoBonggolan
    WHERE ib.NoCrusherProduksi = @no
    ORDER BY Ref1 DESC, Ref2 ASC;


    /* =========== [2] PARTIALS (hanya BB yang ada partial) =========== */

    /* BB partial → jenis plastik dari header pallet */
    SELECT
      pmap.NoBBPartial,
      pdet.NoBahanBaku,
      pdet.NoPallet,
      pdet.NoSak,
      pdet.Berat,
      bbh.IdJenisPlastik AS IdJenis,
      jp.Jenis           AS NamaJenis
    FROM dbo.CrusherProduksiInputBBPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BahanBakuPartial pdet WITH (NOLOCK)
      ON pdet.NoBBPartial = pmap.NoBBPartial
    LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
      ON bbh.NoBahanBaku = pdet.NoBahanBaku 
      AND bbh.NoPallet = pdet.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE pmap.NoCrusherProduksi = @no
    ORDER BY pmap.NoBBPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bbPart   = rs.recordsets?.[1] || [];

  const out = {
    bb: [],
    bonggolan: [],
    summary: { 
      bb: 0, 
      bonggolan: 0 
    },
  };

  // MAIN rows
  for (const r of mainRows) {
    const base = {
      berat: r.Berat ?? null,
      beratAct: r.BeratAct ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis: r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
    };

    switch (r.Src) {
      case 'bb':
        out.bb.push({ 
          noBahanBaku: r.Ref1, 
          noPallet: r.Ref2, 
          noSak: r.Ref3, 
          ...base 
        });
        break;
      case 'bonggolan':
        out.bonggolan.push({ 
          noBonggolan: r.Ref1, 
          ...base 
        });
        break;
    }
  }

  // PARTIAL rows (only BB)
  for (const p of bbPart) {
    out.bb.push({
      noBBPartial: p.NoBBPartial,
      noBahanBaku: p.NoBahanBaku ?? null,
      noPallet:    p.NoPallet ?? null,
      noSak:       p.NoSak ?? null,
      berat:       p.Berat ?? null,
      idJenis:     p.IdJenis ?? null,
      namaJenis:   p.NamaJenis ?? null,
    });
  }

  // Summary
  out.summary.bb = out.bb.length;
  out.summary.bonggolan = out.bonggolan.length;

  return out;
}

async function fetchOutputs(noCrusherProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noCrusherProduksi);

  const q = `
    SELECT DISTINCT
      o.NoCrusherProduksi,
      o.NoCrusher,
      ISNULL(c.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.CrusherProduksiOutput o WITH (NOLOCK)
    LEFT JOIN dbo.Crusher c WITH (NOLOCK)
      ON c.NoCrusher = o.NoCrusher
    WHERE o.NoCrusherProduksi = @no
    ORDER BY o.NoCrusher DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
}


/**
 * VALIDATE LABEL for Crusher Production
 * Only supports: A. (BahanBaku_d) and M. (Bonggolan)
 */
async function validateLabel(labelCode) {
  const pool = await poolPromise;

  // ---------- helpers ----------
  const toCamel = (s) => {
    if (!s) return s;
    // handle snake / kebab quickly
    let out = s.replace(/[_-]+([a-zA-Z0-9])/g, (_, c) => c.toUpperCase());
    // lower-case first char (IdLokasi -> idLokasi)
    out = out.charAt(0).toLowerCase() + out.slice(1);
    return out;
  };

  const camelize = (val) => {
    if (Array.isArray(val)) return val.map(camelize);
    if (val && typeof val === 'object') {
      const o = {};
      for (const [k, v] of Object.entries(val)) {
        o[toCamel(k)] = camelize(v);
      }
      return o;
    }
    return val;
  };

  // ---------- normalize label ----------
  const raw = String(labelCode || '').trim();
  if (!raw) throw new Error('Label code is required');

  const prefix = raw.substring(0, 2).toUpperCase();

  let query = '';
  let tableName = '';

  // Helper eksekusi single-query
  async function run(label) {
    const req = pool.request();
    req.input('labelCode', sql.VarChar(50), label);
    const rs = await req.query(query);
    const rows = rs.recordset || [];
    return camelize({
      found: rows.length > 0,
      count: rows.length,
      prefix,
      tableName,
      data: rows,
    });
  }

  switch (prefix) {
    // =========================
    // A. BahanBaku_d (A.xxxxx-<pallet>)
    // =========================
    case 'A.': {
      tableName = 'BahanBaku_d';
      // Format: A.0000000001-1
      const parts = raw.split('-');
      if (parts.length !== 2) {
        throw new Error('Invalid format for A. prefix. Expected: A.0000000001-1');
      }
      const noBahanBaku = parts[0].trim();
      const noPallet = parseInt(parts[1], 10);

      query = `
        ;WITH PartialAgg AS (
          SELECT
            p.NoBahanBaku,
            p.NoPallet,
            p.NoSak,
            SUM(ISNULL(p.Berat, 0)) AS PartialBerat
          FROM dbo.BahanBakuPartial AS p WITH (NOLOCK)
          GROUP BY p.NoBahanBaku, p.NoPallet, p.NoSak
        )
        SELECT
          d.NoBahanBaku,
          d.NoPallet,
          d.NoSak,
          Berat = CASE
                    WHEN ISNULL(NULLIF(d.BeratAct, 0), d.Berat) - ISNULL(pa.PartialBerat, 0) < 0
                      THEN 0
                    ELSE ISNULL(NULLIF(d.BeratAct, 0), d.Berat) - ISNULL(pa.PartialBerat, 0)
                  END,
          d.DateUsage,
          d.IsPartial,
          ph.IdJenisPlastik      AS idJenis,
          jp.Jenis               AS namaJenis

        FROM dbo.BahanBaku_d AS d WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoBahanBaku = d.NoBahanBaku
         AND pa.NoPallet    = d.NoPallet
         AND pa.NoSak       = d.NoSak
        LEFT JOIN dbo.BahanBakuPallet_h AS ph WITH (NOLOCK)
          ON ph.NoBahanBaku = d.NoBahanBaku
         AND ph.NoPallet    = d.NoPallet
        LEFT JOIN dbo.MstJenisPlastik AS jp WITH (NOLOCK)
          ON jp.IdJenisPlastik = ph.IdJenisPlastik
        WHERE d.NoBahanBaku = @noBahanBaku
          AND d.NoPallet    = @noPallet
          AND d.DateUsage IS NULL
        ORDER BY d.NoBahanBaku, d.NoPallet, d.NoSak;
      `;

      const reqA = pool.request();
      reqA.input('noBahanBaku', sql.VarChar(50), noBahanBaku);
      reqA.input('noPallet', sql.Int, noPallet);
      const rsA = await reqA.query(query);
      const rows = rsA.recordset || [];

      return camelize({
        found: rows.length > 0,
        count: rows.length,
        prefix,
        tableName,
        data: rows,
      });
    }

    // =========================
    // M. Bonggolan
    // =========================
    case 'M.':
      tableName = 'Bonggolan';
      query = `
        SELECT
          b.NoBonggolan,
          b.DateCreate,
          b.IdBonggolan      AS idJenis,
          mb.NamaBonggolan   AS namaJenis,
          b.IdWarehouse,
          b.DateUsage,
          b.Berat,
          b.IdStatus,
          b.Blok,
          b.IdLokasi,
          b.CreateBy,
          b.DateTimeCreate
        FROM dbo.Bonggolan AS b WITH (NOLOCK)
        LEFT JOIN dbo.MstBonggolan AS mb WITH (NOLOCK)
          ON mb.IdBonggolan = b.IdBonggolan
        WHERE b.NoBonggolan = @labelCode
          AND b.DateUsage IS NULL
        ORDER BY b.NoBonggolan;
      `;
      return await run(raw);

    default:
      throw new Error(`Invalid prefix: ${prefix}. Crusher production only supports A. (Bahan Baku) and M. (Bonggolan)`);
  }
}

/**
 * UPSERT INPUTS & PARTIALS for Crusher Production
 * Payload shape:
 * {
 *   bb: [{ noBahanBaku, noPallet, noSak }],
 *   bonggolan: [{ noBonggolan }],
 *   bbPartialNew: [{ noBahanBaku, noPallet, noSak, berat }]
 * }
 */

async function upsertInputsAndPartials(noProduksi, payload, ctx) {
  const no = String(noProduksi || '').trim();
  if (!no) throw badReq('noProduksi wajib diisi');

  const body = payload && typeof payload === 'object' ? payload : {};

  // ✅ ctx wajib (audit)
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
  }

  const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';

  // requestId wajib string (kalau kosong, nanti di applyAuditContext dibuat fallback juga)
  const requestId = String(ctx?.requestId || '').trim();

  // ✅ forward ctx yang sudah dinormalisasi
  return sharedInputService.upsertInputsAndPartials('crusherProduksi', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

/**
 * ✅ Delete inputs & partials dengan audit context
 */
async function deleteInputsAndPartials(noProduksi, payload, ctx) {
  const no = String(noProduksi || '').trim();
  if (!no) throw badReq('noProduksi wajib diisi');

  const body = payload && typeof payload === 'object' ? payload : {};

  // ✅ Validate audit context
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
  }

  const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';
  const requestId = String(ctx?.requestId || '').trim();

  // ✅ Forward to shared service
  return sharedInputService.deleteInputsAndPartials('crusherProduksi', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}



module.exports = { getAllProduksi, getProduksiByDate, getCrusherMasters, createCrusherProduksi, updateCrusherProduksi, deleteCrusherProduksi, fetchInputs, fetchOutputs, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials };

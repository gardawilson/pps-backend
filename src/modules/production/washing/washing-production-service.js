// services/production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
  loadDocDateOnlyFromConfig
} = require('../../../core/shared/tutup-transaksi-guard');

const sharedInputService = require('../../../core/shared/produksi-input.service');

const { badReq, conflict } = require('../../../core/utils/http-error'); 
const { applyAuditContext } = require('../../../core/utils/db-audit-context');

const { generateNextCode } = require('../../../core/utils/sequence-code-helper'); 

const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require('../../../core/utils/jam-kerja-helper');




async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20)); // batasin biar aman
  const offset = (p - 1) * ps;

  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
  `;

  // 1) Total baris (tetap sederhana)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.WashingProduksi_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);

  const countRes = await countReq.query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;

  if (total === 0) return { data: [], total: 0 };

  // 2) Data halaman + Flag Tutup Transaksi (ambil lastClosed sekali)
  const dataQry = `
    ;WITH LastClosed AS (
      SELECT TOP 1
        CONVERT(date, PeriodHarian) AS LastClosedDate
      FROM dbo.MstTutupTransaksiHarian WITH (NOLOCK)
      WHERE [Lock] = 1
      ORDER BY CONVERT(date, PeriodHarian) DESC, Id DESC
    )
    SELECT
      h.NoProduksi,
      h.IdOperator,
      op.NamaOperator,
      h.IdMesin,
      ms.NamaMesin,
      h.TglProduksi,
      h.JamKerja,
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

      -- (opsional tapi berguna untuk frontend)
      lc.LastClosedDate AS LastClosedDate,

      -- ✅ flag tutup transaksi
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.TglProduksi) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.WashingProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin     ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator  op WITH (NOLOCK) ON op.IdOperator = h.IdOperator

    -- bikin LastClosed selalu 1 row juga saat tabel tutup transaksi kosong
    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    ORDER BY h.TglProduksi DESC, h.JamKerja ASC, h.NoProduksi ASC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

async function getProduksiByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();
  const query = `
    SELECT 
      h.NoProduksi, h.IdOperator, h.IdMesin, m.NamaMesin,
      h.TglProduksi, h.JamKerja, h.Shift, h.CreateBy,
      h.CheckBy1, h.CheckBy2, h.ApproveBy,
      h.JmlhAnggota, h.Hadir, h.HourMeter
    FROM WashingProduksi_h h
    LEFT JOIN MstMesin m ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @date
    ORDER BY h.JamKerja ASC;
  `;
  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}


// =========================
//  CREATE WashingProduksi_h
// =========================
async function createWashingProduksi(payload, ctx) {
  // ===============================
  // 0) Validasi payload basic (business)
  // ===============================
  const body = payload && typeof payload === 'object' ? payload : {};

  const must = [];
  if (!body?.tglProduksi) must.push('tglProduksi');
  if (body?.idMesin == null) must.push('idMesin');
  if (body?.idOperator == null) must.push('idOperator');
  if (body?.shift == null) must.push('shift');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  // jamKerja bisa dari body.jamKerja atau dihitung dari hourStart-hourEnd (biar konsisten dg broker)
  let jamKerja = body?.jamKerja ?? null;
  if (jamKerja == null) {
    const calc = calcJamKerjaFromStartEnd(body?.hourStart, body?.hourEnd);
    if (calc != null) jamKerja = calc;
  }
  if (jamKerja == null) throw badReq('Field wajib: jamKerja (atau isi hourStart-hourEnd)');

  const jamInt = parseJamToInt(jamKerja);
  const docDateOnly = toDateOnly(body.tglProduksi);

  // ===============================
  // 1) Validasi + normalisasi ctx (audit)
  // ===============================
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
  }

  const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';
  const requestId = String(ctx?.requestId || '').trim(); // boleh kosong, applyAuditContext akan fallback

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
      action: 'create WashingProduksi',
      useLock: true,
    });

    // =====================================================
    // 4) Generate NoProduksi
    // =====================================================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: 'dbo.WashingProduksi_h',
        columnName: 'NoProduksi',
        prefix: 'C.',
        width: 10,
      });

    let noProduksi = await gen();

    // optional double-check (lebih bagus kalau kolom ada UNIQUE)
    const exist = await new sql.Request(tx)
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT 1
        FROM dbo.WashingProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (exist.recordset.length > 0) {
      const retry = await gen();
      const exist2 = await new sql.Request(tx)
        .input('NoProduksi', sql.VarChar(50), retry)
        .query(`
          SELECT 1
          FROM dbo.WashingProduksi_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoProduksi = @NoProduksi
        `);

      if (exist2.recordset.length > 0) {
        throw conflict('Gagal generate NoProduksi unik, coba lagi.');
      }
      noProduksi = retry;
    }

    // =====================================================
    // 5) Insert header (FIX: OUTPUT ... INTO @out)
    // =====================================================
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),     noProduksi)
      .input('TglProduksi', sql.Date,            docDateOnly)
      .input('IdMesin',     sql.Int,             body.idMesin)
      .input('IdOperator',  sql.Int,             body.idOperator)
      .input('JamKerja',    sql.Int,             jamInt)
      .input('Shift',       sql.Int,             body.shift)
      .input('CreateBy',    sql.VarChar(100),    body.createBy) // controller overwrite dari token
      .input('CheckBy1',    sql.VarChar(100),    body.checkBy1 ?? null)
      .input('CheckBy2',    sql.VarChar(100),    body.checkBy2 ?? null)
      .input('ApproveBy',   sql.VarChar(100),    body.approveBy ?? null)
      .input('JmlhAnggota', sql.Int,             body.jmlhAnggota ?? null)
      .input('Hadir',       sql.Int,             body.hadir ?? null)
      .input('HourMeter',   sql.Decimal(18, 2),  body.hourMeter ?? null)
      // kirim string, biar SQL yang CAST ke time(7)
      .input('HourStart',   sql.VarChar(20),     body.hourStart ?? null)
      .input('HourEnd',     sql.VarChar(20),     body.hourEnd ?? null);

    const insertSql = `
      DECLARE @out TABLE (
        NoProduksi   varchar(50),
        TglProduksi  date,
        IdMesin      int,
        IdOperator   int,
        JamKerja     int,
        Shift        int,
        CreateBy     varchar(100),
        CheckBy1     varchar(100),
        CheckBy2     varchar(100),
        ApproveBy    varchar(100),
        JmlhAnggota  int,
        Hadir        int,
        HourMeter    decimal(18,2),
        HourStart    time(7),
        HourEnd      time(7)
      );

      INSERT INTO dbo.WashingProduksi_h (
        NoProduksi,
        TglProduksi,
        IdMesin,
        IdOperator,
        JamKerja,
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
        INSERTED.NoProduksi,
        INSERTED.TglProduksi,
        INSERTED.IdMesin,
        INSERTED.IdOperator,
        INSERTED.JamKerja,
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
        @NoProduksi,
        @TglProduksi,
        @IdMesin,
        @IdOperator,
        @JamKerja,
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
      audit, // optional debug / tracing
    };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}


/**
 * Update Washing Production Header
 * 
 * Features:
 * - Dynamic SET clause (hanya update field yang dikirim)
 * - SERIALIZABLE transaction untuk data consistency
 * - Auto-sync DateUsage untuk semua input labels saat TglProduksi berubah
 * 
 * DateUsage sync untuk:
 * - Bahan Baku (Full + Partial)
 * - Washing (Full only - NO PARTIAL)
 * - Gilingan (Full + Partial)
 * 
 * PERBEDAAN dengan Broker:
 * - Field: JamKerja (bukan Jam)
 * - Tidak ada UpdateBy field (pakai CreateBy untuk tracking)
 * 
 * @param {string} noProduksi - Nomor produksi (PK)
 * @param {object} payload - Fields to update (partial)
 * @returns {object} { header: updatedRecord }
 */

async function updateWashingProduksi(noProduksi, payload, ctx) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) Set SESSION_CONTEXT untuk trigger audit (1x di awal tx)
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

    // -------------------------------------------------------
    // 1) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'washingProduksi', // pastikan sesuai config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 2) Jika user mengubah tanggal, hitung tanggal barunya (date-only)
    // -------------------------------------------------------
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq('tglProduksi tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // -------------------------------------------------------
    // 3) GUARD TUTUP TRANSAKSI
    // -------------------------------------------------------
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: 'update WashingProduksi (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update WashingProduksi (new date)',
        useLock: true,
      });
    }

    // -------------------------------------------------------
    // 4) BUILD SET DINAMIS
    // -------------------------------------------------------
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('TglProduksi = @TglProduksi');
      rqUpd.input('TglProduksi', sql.Date, newDocDateOnly);
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

    if (payload.jamKerja !== undefined) {
      const jamInt = payload.jamKerja === null ? null : parseJamToInt(payload.jamKerja);
      sets.push('JamKerja = @JamKerja');
      rqUpd.input('JamKerja', sql.Int, jamInt);
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

    // hourStart / hourEnd (lebih aman kalau null / kosong)
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

    rqUpd.input('NoProduksi', sql.VarChar(50), noProduksi);

    // -------------------------------------------------------
    // 5) UPDATE + RETURN row (FIX: OUTPUT ... INTO @out)
    // -------------------------------------------------------
    const updateSql = `
      DECLARE @out TABLE (
        NoProduksi   varchar(50),
        TglProduksi  date,
        IdMesin      int,
        IdOperator   int,
        JamKerja     int,
        Shift        int,
        CreateBy     varchar(100),
        CheckBy1     varchar(100),
        CheckBy2     varchar(100),
        ApproveBy    varchar(100),
        JmlhAnggota  int,
        Hadir        int,
        HourMeter    decimal(18,2),
        HourStart    time(7),
        HourEnd      time(7)
      );

      UPDATE dbo.WashingProduksi_h
      SET ${sets.join(', ')}
      OUTPUT
        INSERTED.NoProduksi,
        INSERTED.TglProduksi,
        INSERTED.IdMesin,
        INSERTED.IdOperator,
        INSERTED.JamKerja,
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
      WHERE NoProduksi = @NoProduksi;

      SELECT * FROM @out;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    if (!updatedHeader) throw notFound(`NoProduksi tidak ditemukan: ${noProduksi}`);

    // -------------------------------------------------------
    // 6) Jika TglProduksi berubah → (optional) sinkron DateUsage
    //    pakai tanggal hasil DB agar konsisten
    // -------------------------------------------------------
    if (isChangingDate) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.TglProduksi);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('TglProduksi', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BAHAN BAKU (FULL + PARTIAL)  [sesuaikan mapping tabelmu]
        -------------------------------------------------------
        UPDATE bb
        SET bb.DateUsage = @TglProduksi
        FROM dbo.BahanBaku_d AS bb
        WHERE bb.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInput AS map
              WHERE map.NoProduksi   = @NoProduksi
                AND map.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak        = bb.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoProduksi   = @NoProduksi
                AND bp.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak        = bb.NoSak
            )
          );

        -------------------------------------------------------
        -- (CONTOH) WASHING (kalau WashingProduksi juga consume Washing_d)
        -------------------------------------------------------
        UPDATE w
        SET w.DateUsage = @TglProduksi
        FROM dbo.Washing_d AS w
        WHERE w.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.WashingProduksiInputWashing AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoWashing  = w.NoWashing
              AND map.NoSak      = w.NoSak
          );

        -------------------------------------------------------
        -- (CONTOH) GILINGAN (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE g
        SET g.DateUsage = @TglProduksi
        FROM dbo.Gilingan AS g
        WHERE g.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputGilinganPartial AS mp
              JOIN dbo.GilinganPartial AS gp
                ON gp.NoGilinganPartial = mp.NoGilinganPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND gp.NoGilingan = g.NoGilingan
            )
          );
      `;

      await rqUsage.query(sqlUpdateUsage);
    }

    await tx.commit();
    return { header: updatedHeader };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}



/**
 * Delete washing production header + inputs + reset DateUsage
 * @param {string} noProduksi
 */
async function deleteWashingProduksi(noProduksi, ctx) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) Set SESSION_CONTEXT untuk trigger audit (1x di awal tx)
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

    // -------------------------------------------------------
    // 1) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'washingProduksi', // pastikan sesuai config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // DELETE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 2) GUARD TUTUP TRANSAKSI (DELETE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete WashingProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 3) CEK DULU: SUDAH PUNYA OUTPUT ATAU BELUM
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const outCheck = await rqCheck
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT COUNT(*) AS CntOutput
        FROM dbo.WashingProduksiOutput
        WHERE NoProduksi = @NoProduksi;
      `);

    const row = outCheck.recordset?.[0] || { CntOutput: 0 };
    const hasOutput = (row.CntOutput || 0) > 0;

    if (hasOutput) {
      throw badReq('Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.');
    }

    // -------------------------------------------------------
    // 4) DELETE INPUT + PARTIAL + RESET DATEUSAGE + DELETE HEADER (OUTPUT INTO)
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK MENYIMPAN KEY YANG TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BBKeys TABLE (
        NoBahanBaku varchar(50),
        NoPallet    varchar(50),
        NoSak       varchar(50)
      );

      DECLARE @WashingKeys TABLE ( NoWashing varchar(50) );

      DECLARE @GilinganKeys TABLE ( NoGilingan varchar(50) );

      ---------------------------------------------------------
      -- OUT TABLE: RETURN HEADER YANG TERHAPUS
      ---------------------------------------------------------
      DECLARE @outHeader TABLE (
        NoProduksi   varchar(50),
        TglProduksi  date,
        IdMesin      int,
        IdOperator   int,
        JamKerja     int,
        Shift        int,
        CreateBy     varchar(100),
        CheckBy1     varchar(100),
        CheckBy2     varchar(100),
        ApproveBy    varchar(100),
        JmlhAnggota  int,
        Hadir        int,
        HourMeter    decimal(18,2),
        HourStart    time(7),
        HourEnd      time(7)
      );

      ---------------------------------------------------------
      -- 0. PASTIKAN HEADER ADA (kalau tidak, stop cepat)
      ---------------------------------------------------------
      IF NOT EXISTS (SELECT 1 FROM dbo.WashingProduksi_h WITH (UPDLOCK, HOLDLOCK) WHERE NoProduksi = @NoProduksi)
      BEGIN
        SELECT * FROM @outHeader;
        RETURN;
      END

      ---------------------------------------------------------
      -- 1. BAHAN BAKU (FULL + PARTIAL)
      --    NOTE: sesuaikan nama mapping FULL BB kamu:
      --          (a) WashingProduksiInputBB  <-- rekomendasi
      --          (b) atau WashingProduksiInput (punya kolom NoBahanBaku/NoPallet/NoSak)
      ---------------------------------------------------------
      INSERT INTO @BBKeys (NoBahanBaku, NoPallet, NoSak)
      SELECT DISTINCT bb.NoBahanBaku, bb.NoPallet, bb.NoSak
      FROM dbo.BahanBaku_d AS bb
      WHERE EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputBB AS map
              WHERE map.NoProduksi   = @NoProduksi
                AND map.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak        = bb.NoSak
          )
         OR EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoProduksi   = @NoProduksi
                AND bp.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak        = bb.NoSak
          );

      DELETE bp
      FROM dbo.BahanBakuPartial AS bp
      JOIN dbo.WashingProduksiInputBBPartial AS mp
        ON mp.NoBBPartial = bp.NoBBPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.WashingProduksiInputBBPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.WashingProduksiInputBB
      WHERE NoProduksi = @NoProduksi;

      UPDATE bb
      SET bb.DateUsage = NULL,
          bb.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.BahanBakuPartial AS bp
              WHERE bp.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak = bb.NoSak
            ) THEN 1 ELSE 0 END
      FROM dbo.BahanBaku_d AS bb
      JOIN @BBKeys AS k
        ON k.NoBahanBaku = bb.NoBahanBaku
       AND ISNULL(k.NoPallet,'') = ISNULL(bb.NoPallet,'')
       AND k.NoSak = bb.NoSak;

      ---------------------------------------------------------
      -- 2. WASHING (FULL ONLY)
      ---------------------------------------------------------
      INSERT INTO @WashingKeys (NoWashing)
      SELECT DISTINCT map.NoWashing
      FROM dbo.WashingProduksiInputWashing AS map
      WHERE map.NoProduksi = @NoProduksi;

      UPDATE w
      SET w.DateUsage = NULL
      FROM dbo.Washing_d AS w
      JOIN @WashingKeys AS k
        ON k.NoWashing = w.NoWashing;

      DELETE FROM dbo.WashingProduksiInputWashing
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 3. GILINGAN (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT g.NoGilingan
      FROM dbo.Gilingan AS g
      WHERE EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
          )
         OR EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputGilinganPartial AS mp
              JOIN dbo.GilinganPartial AS gp
                ON gp.NoGilinganPartial = mp.NoGilinganPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND gp.NoGilingan = g.NoGilingan
          );

      DELETE gp
      FROM dbo.GilinganPartial AS gp
      JOIN dbo.WashingProduksiInputGilinganPartial AS mp
        ON mp.NoGilinganPartial = gp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.WashingProduksiInputGilinganPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.WashingProduksiInputGilingan
      WHERE NoProduksi = @NoProduksi;

      UPDATE g
      SET g.DateUsage = NULL,
          g.IsPartial = CASE
            WHEN EXISTS (SELECT 1 FROM dbo.GilinganPartial gp WHERE gp.NoGilingan = g.NoGilingan)
            THEN 1 ELSE 0 END
      FROM dbo.Gilingan AS g
      JOIN @GilinganKeys AS k ON k.NoGilingan = g.NoGilingan;

      ---------------------------------------------------------
      -- 4. TERAKHIR: HAPUS HEADER (OUTPUT INTO) + RETURN
      ---------------------------------------------------------
      DELETE FROM dbo.WashingProduksi_h
      OUTPUT
        DELETED.NoProduksi,
        DELETED.TglProduksi,
        DELETED.IdMesin,
        DELETED.IdOperator,
        DELETED.JamKerja,
        DELETED.Shift,
        DELETED.CreateBy,
        DELETED.CheckBy1,
        DELETED.CheckBy2,
        DELETED.ApproveBy,
        DELETED.JmlhAnggota,
        DELETED.Hadir,
        DELETED.HourMeter,
        DELETED.HourStart,
        DELETED.HourEnd
      INTO @outHeader
      WHERE NoProduksi = @NoProduksi;

      SELECT * FROM @outHeader;
    `;

    const delRes = await req.query(sqlDelete);
    const deletedHeader = delRes.recordset?.[0] || null;

    if (!deletedHeader) throw notFound(`NoProduksi tidak ditemukan: ${noProduksi}`);

    await tx.commit();
    return { success: true, header: deletedHeader };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}



/**
 * Ambil semua input untuk produksi Washing:
 * - Washing (full)
 * - Bahan Baku (full)
 * - Gilingan (full)
 * - Bahan Baku Partial
 * - Gilingan Partial
 */
async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noProduksi);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */
    SELECT 
      'washing' AS Src,
      iw.NoProduksi,
      iw.NoWashing AS Ref1,
      iw.NoSak     AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      wd.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      wh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.WashingProduksiInputWashing iw WITH (NOLOCK)
    LEFT JOIN dbo.Washing_d wd         WITH (NOLOCK)
      ON wd.NoWashing = iw.NoWashing AND wd.NoSak = iw.NoSak
    LEFT JOIN dbo.Washing_h wh         WITH (NOLOCK)
      ON wh.NoWashing = iw.NoWashing
    LEFT JOIN dbo.MstJenisPlastik jp   WITH (NOLOCK)
      ON jp.IdJenisPlastik = wh.IdJenisPlastik
    WHERE iw.NoProduksi = @no

    UNION ALL
    SELECT
      'bb' AS Src,
      ibb.NoProduksi,
      ibb.NoBahanBaku AS Ref1,
      ibb.NoPallet    AS Ref2,
      ibb.NoSak       AS Ref3,
      bb.Berat    AS Berat,
      bb.BeratAct AS BeratAct,
      bb.IsPartial AS IsPartial,
      bbh.IdJenisPlastik AS IdJenis,
      jpb.Jenis          AS NamaJenis
    FROM dbo.WashingProduksiInput ibb WITH (NOLOCK)
    LEFT JOIN dbo.BahanBaku_d bb            WITH (NOLOCK)
      ON bb.NoBahanBaku = ibb.NoBahanBaku
     AND bb.NoPallet    = ibb.NoPallet
     AND bb.NoSak       = ibb.NoSak
    LEFT JOIN dbo.BahanBakuPallet_h bbh     WITH (NOLOCK)
      ON bbh.NoBahanBaku = ibb.NoBahanBaku
     AND bbh.NoPallet    = ibb.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpb       WITH (NOLOCK)
      ON jpb.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE ibb.NoProduksi = @no

    UNION ALL
    SELECT
      'gilingan' AS Src,
      ig.NoProduksi,
      ig.NoGilingan AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      g.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      g.IsPartial AS IsPartial,
      g.IdGilingan    AS IdJenis,
      mg.NamaGilingan AS NamaJenis
    FROM dbo.WashingProduksiInputGilingan ig WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan g        WITH (NOLOCK)
      ON g.NoGilingan = ig.NoGilingan
    LEFT JOIN dbo.MstGilingan mg    WITH (NOLOCK)
      ON mg.IdGilingan = g.IdGilingan
    WHERE ig.NoProduksi = @no

    ORDER BY Ref1 DESC, Ref2 ASC;


    /* =========== [2] PARTIALS (BB & GILINGAN) =========== */

    /* BB partial → jenis plastik dari header pallet */
    SELECT
      pmap.NoBBPartial,
      pdet.NoBahanBaku,
      pdet.NoPallet,
      pdet.NoSak,
      pdet.Berat,
      bbh.IdJenisPlastik AS IdJenis,
      jpp.Jenis          AS NamaJenis
    FROM dbo.WashingProduksiInputBBPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BahanBakuPartial pdet WITH (NOLOCK)
      ON pdet.NoBBPartial = pmap.NoBBPartial
    LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
      ON bbh.NoBahanBaku = pdet.NoBahanBaku
     AND bbh.NoPallet    = pdet.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpp WITH (NOLOCK)
      ON jpp.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE pmap.NoProduksi = @no
    ORDER BY pmap.NoBBPartial DESC;

    /* Gilingan partial → jenis gilingan */
    SELECT
      gmap.NoGilinganPartial,
      gdet.NoGilingan,
      gdet.Berat,
      gh.IdGilingan    AS IdJenis,
      mg.NamaGilingan  AS NamaJenis
    FROM dbo.WashingProduksiInputGilinganPartial gmap WITH (NOLOCK)
    LEFT JOIN dbo.GilinganPartial gdet WITH (NOLOCK)
      ON gdet.NoGilinganPartial = gmap.NoGilinganPartial
    LEFT JOIN dbo.Gilingan gh      WITH (NOLOCK)
      ON gh.NoGilingan = gdet.NoGilingan
    LEFT JOIN dbo.MstGilingan mg   WITH (NOLOCK)
      ON mg.IdGilingan = gh.IdGilingan
    WHERE gmap.NoProduksi = @no
    ORDER BY gmap.NoGilinganPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bbPart   = rs.recordsets?.[1] || [];
  const gilPart  = rs.recordsets?.[2] || [];

  const out = {
    washing: [],
    bb: [],
    gilingan: [],
    summary: { washing: 0, bb: 0, gilingan: 0 },
  };

  // ===================== MAIN ROWS =====================
  for (const r of mainRows) {
    const base = {
      berat:     r.Berat ?? null,
      beratAct:  r.BeratAct ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis:   r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
    };

    switch (r.Src) {
      case 'washing':
        out.washing.push({
          noWashing: r.Ref1,
          noSak:     r.Ref2,
          ...base,
        });
        break;
      case 'bb':
        out.bb.push({
          noBahanBaku: r.Ref1,
          noPallet:    r.Ref2,
          noSak:       r.Ref3,
          ...base,
        });
        break;
      case 'gilingan':
        out.gilingan.push({
          noGilingan: r.Ref1,
          ...base,
        });
        break;
    }
  }

  // ===================== PARTIAL BB =====================
  for (const p of bbPart) {
    out.bb.push({
      noBBPartial: p.NoBBPartial,
      noBahanBaku: p.NoBahanBaku ?? null,
      noPallet:    p.NoPallet ?? null,
      noSak:       p.NoSak ?? null,
      berat:       p.Berat ?? null,
      idJenis:     p.IdJenis ?? null,
      namaJenis:   p.NamaJenis ?? null,
      // isPartial sengaja tidak diisi: identifikasi dari adanya noBBPartial
    });
  }

  // ===================== PARTIAL GILINGAN =====================
  for (const p of gilPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan:        p.NoGilingan ?? null,
      berat:             p.Berat ?? null,
      idJenis:           p.IdJenis ?? null,
      namaJenis:         p.NamaJenis ?? null,
      // sama: noGilinganPartial yang menandakan partial
    });
  }

  // ===================== SUMMARY =====================
  for (const k of Object.keys(out.summary)) {
    out.summary[k] = out[k].length;
  }

  return out;
}

/**
 * Validate label untuk Washing Production
 * Support prefix: A. (Bahan Baku), B. (Washing), V. (Gilingan)
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
    // B. Washing_d
    // =========================
    case 'B.':
      tableName = 'Washing_d';
      query = `
        SELECT
          d.NoWashing,
          d.NoSak,
          d.Berat,
          d.DateUsage,
          d.IdLokasi,
          h.IdJenisPlastik AS idJenis,
          jp.Jenis         AS namaJenis
        FROM dbo.Washing_d AS d WITH (NOLOCK)
        LEFT JOIN dbo.Washing_h AS h WITH (NOLOCK)
          ON h.NoWashing = d.NoWashing
        LEFT JOIN dbo.MstJenisPlastik AS jp WITH (NOLOCK)
          ON jp.IdJenisPlastik = h.IdJenisPlastik
        WHERE d.NoWashing = @labelCode
          AND d.DateUsage IS NULL
        ORDER BY d.NoWashing, d.NoSak;
      `;
      return await run(raw);

    // =========================
    // V. Gilingan
    // =========================
    case 'V.':
      tableName = 'Gilingan';
      query = `
        ;WITH PartialAgg AS (
          SELECT
            gp.NoGilingan,
            SUM(ISNULL(gp.Berat, 0)) AS PartialBerat
          FROM dbo.GilinganPartial AS gp WITH (NOLOCK)
          GROUP BY gp.NoGilingan
        )
        SELECT
          g.NoGilingan,
          g.DateCreate,
          g.IdGilingan      AS idJenis,
          mg.NamaGilingan   AS namaJenis,
          g.DateUsage,
          Berat       = CASE
                          WHEN g.Berat - ISNULL(pa.PartialBerat, 0) < 0 THEN 0
                          ELSE g.Berat - ISNULL(pa.PartialBerat, 0)
                        END,
          g.IsPartial

        FROM dbo.Gilingan AS g WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoGilingan = g.NoGilingan
        LEFT JOIN dbo.MstGilingan AS mg WITH (NOLOCK)
          ON mg.IdGilingan = g.IdGilingan
        WHERE g.NoGilingan = @labelCode
          AND g.DateUsage IS NULL
        ORDER BY g.NoGilingan;
      `;
      return await run(raw);

    default:
      throw new Error(`Invalid prefix: ${prefix}. Valid prefixes for Washing: A., B., V.`);
  }
}


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
  return sharedInputService.upsertInputsAndPartials('washingProduksi', no, body, {
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
  return sharedInputService.deleteInputsAndPartials('washingProduksi', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}


module.exports = { getProduksiByDate, getAllProduksi, createWashingProduksi, updateWashingProduksi, deleteWashingProduksi, fetchInputs, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials };   // ⬅️ pastikan ini ada

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




function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// 🔢 khusus WASHING: generate NoProduksi dengan prefix 'C.'
async function generateNextNoProduksiWashing(
  tx,
  { prefix = 'C.', width = 10 } = {}   // ⬅️ pakai 'C.' di sini
) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoProduksi
    FROM dbo.WashingProduksi_h AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE h.NoProduksi LIKE @prefix + '%'
    ORDER BY
      TRY_CONVERT(BIGINT, SUBSTRING(h.NoProduksi, LEN(@prefix) + 1, 50)) DESC,
      h.NoProduksi DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoProduksi;
    const numericPart = last.substring(prefix.length); // contoh: '0000002309'
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width); // contoh: 'C.' + '0000002310'
}


// sama persis dengan broker, tapi param namanya bebas
function parseJamToInt(jam) {
  if (jam == null) throw badReq('Format jam tidak valid');
  if (typeof jam === 'number') return Math.max(0, Math.round(jam)); // hours

  const s = String(jam).trim();
  const mRange = s.match(/^(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$/);
  if (mRange) {
    const sh = +mRange[1], sm = +mRange[2], eh = +mRange[3], em = +mRange[4];
    let mins = (eh * 60 + em) - (sh * 60 + sm);
    if (mins < 0) mins += 24 * 60; // cross-midnight
    return Math.max(0, Math.round(mins / 60));
  }
  const mTime = s.match(/^(\d{1,2}):(\d{2})$/);
  if (mTime) return Math.max(0, parseInt(mTime[1], 10));
  const mHour = s.match(/^(\d{1,2})$/);
  if (mHour) return Math.max(0, parseInt(mHour[1], 10));

  throw badReq('Format jam tidak valid. Gunakan angka (mis. 8) atau "HH:mm-HH:mm"');
}

// =========================
//  CREATE WashingProduksi_h
// =========================
async function createWashingProduksi(payload) {
  const must = [];
  if (!payload?.tglProduksi) must.push('tglProduksi');
  if (payload?.idMesin == null) must.push('idMesin');
  if (payload?.idOperator == null) must.push('idOperator');
  if (payload?.jamKerja == null) must.push('jamKerja');
  if (payload?.shift == null) must.push('shift');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) NORMALISASI TANGGAL CREATE -> docDateOnly (date-only)
    // -------------------------------------------------------
    const docDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (CREATE = WRITE, pakai lock)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,                 // IMPORTANT: pakai tx
      action: 'create WashingProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) GENERATE NoProduksi
    // -------------------------------------------------------
    const no1 = await generateNextNoProduksiWashing(tx, { prefix: 'C.', width: 10 });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.WashingProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksiWashing(tx, { prefix: 'C.', width: 10 })
      : no1;

    // -------------------------------------------------------
    // 3) INSERT HEADER (pakai docDateOnly)
    // -------------------------------------------------------
    const jamInt = parseJamToInt(payload.jamKerja);

    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),     noProduksi)
      .input('IdOperator',  sql.Int,            payload.idOperator)
      .input('IdMesin',     sql.Int,            payload.idMesin)
      .input('TglProduksi', sql.Date,           docDateOnly) // ✅ date-only
      .input('JamKerja',    sql.Int,            jamInt)
      .input('Shift',       sql.Int,            payload.shift)
      .input('CreateBy',    sql.VarChar(100),   payload.createBy)
      .input('CheckBy1',    sql.VarChar(100),   payload.checkBy1 ?? null)
      .input('CheckBy2',    sql.VarChar(100),   payload.checkBy2 ?? null)
      .input('ApproveBy',   sql.VarChar(100),   payload.approveBy ?? null)
      .input('JmlhAnggota', sql.Int,            payload.jmlhAnggota ?? null)
      .input('Hadir',       sql.Int,            payload.hadir ?? null)
      .input('HourMeter',   sql.Decimal(18, 2), payload.hourMeter ?? null)
      .input('HourStart',   sql.VarChar(20),    payload.hourStart ?? null)
      .input('HourEnd',     sql.VarChar(20),    payload.hourEnd ?? null);

    const insertSql = `
      INSERT INTO dbo.WashingProduksi_h (
        NoProduksi,
        IdOperator,
        IdMesin,
        TglProduksi,
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
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi,
        @IdOperator,
        @IdMesin,
        @TglProduksi,
        @JamKerja,
        @Shift,
        @CreateBy,
        @CheckBy1,
        @CheckBy2,
        @ApproveBy,
        @JmlhAnggota,
        @Hadir,
        @HourMeter,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL
             ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL
             ELSE CAST(@HourEnd AS time(7)) END
      );
    `;

    const insRes = await rqIns.query(insertSql);

    await tx.commit();
    return { header: insRes.recordset?.[0] || null };
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

async function updateWashingProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    menggantikan SELECT WashingProduksi_h manual
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'washingProduksi', // ⛳️ SESUAIKAN dengan config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // UPDATE = write
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) Jika user mengubah tanggal, hitung tanggal barunya (date-only)
    // -------------------------------------------------------
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq('tglProduksi tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // -------------------------------------------------------
    // 2) GUARD TUTUP TRANSAKSI
    //    - cek tanggal lama
    //    - kalau ganti tanggal, cek tanggal baru juga
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
    // 3) BUILD SET DINAMIS
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

    // hourStart / hourEnd (aman kalau null / kosong)
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

    if (sets.length === 0) throw badReq('No fields to update');

    rqUpd.input('NoProduksi', sql.VarChar(50), noProduksi);

    const updateSql = `
      UPDATE dbo.WashingProduksi_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.WashingProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // -------------------------------------------------------
    // 4) Jika TglProduksi berubah → sync DateUsage full + partial
    //    pakai tanggal hasil DB biar konsisten
    // -------------------------------------------------------
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.TglProduksi);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('TglProduksi', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BAHAN BAKU (FULL + PARTIAL)  [sesuaikan tabel mapping kamu]
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
        -- WASHING INPUT (FULL ONLY)
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
        -- GILINGAN (FULL + PARTIAL)
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
async function deleteWashingProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    GANTI SELECT WashingProduksi_h manual (tglProduksi)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'washingProduksi', // ⛳️ SESUAIKAN dengan config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // DELETE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (DELETE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete WashingProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) CEK DULU: SUDAH PUNYA OUTPUT ATAU BELUM
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
    // 3) LANJUT DELETE INPUT + PARTIAL + RESET DATEUSAGE
    //    (SQL BESAR kamu tetap)
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
      -- 1. BAHAN BAKU (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BBKeys (NoBahanBaku, NoPallet, NoSak)
      SELECT DISTINCT bb.NoBahanBaku, bb.NoPallet, bb.NoSak
      FROM dbo.BahanBaku_d AS bb
      WHERE EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInput AS map
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

      DELETE FROM dbo.WashingProduksiInput
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
      -- 4. TERAKHIR: HAPUS HEADER
      ---------------------------------------------------------
      DELETE FROM dbo.WashingProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    await req.query(sqlDelete);

    await tx.commit();
    return { success: true };
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

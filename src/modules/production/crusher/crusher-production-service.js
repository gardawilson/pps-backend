const { sql, poolPromise } = require('../../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
  loadDocDateOnlyFromConfig
} = require('../../../core/shared/tutup-transaksi-guard');


// Helper untuk error 400
function badReq(msg) {
  const err = new Error(msg);
  err.statusCode = 400;
  return err;
}



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
    ORDER BY h.Tanggal DESC, h.Jam ASC, h.NoCrusherProduksi DESC
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



async function createCrusherProduksi(payload) {
  // Validasi field wajib
  const must = [];
  if (!payload?.tanggal) must.push('tanggal');
  if (payload?.idMesin == null) must.push('idMesin');
  if (payload?.idOperator == null) must.push('idOperator');
  if (payload?.jam == null) must.push('jam');
  if (payload?.shift == null) must.push('shift');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =========================================================
    // 0) NORMALISASI TANGGAL (DATE-ONLY) + GUARD TUTUP TRANSAKSI
    // =========================================================
    const effectiveDate = resolveEffectiveDateForCreate(payload.tanggal);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create CrusherProduksi',
      useLock: true, // create = write action
    });

    // =========================================================
    // 1) Generate NoCrusherProduksi (inline)
    // Format: G.0000000420
    // =========================================================
    const prefix = 'G.';
    const width = 10;

    const rqGen = new sql.Request(tx);
    rqGen.input('Prefix', sql.VarChar(10), prefix);

    const genQry = `
      SELECT TOP 1 NoCrusherProduksi
      FROM dbo.CrusherProduksi_h WITH (NOLOCK)
      WHERE NoCrusherProduksi LIKE @Prefix + '%'
      ORDER BY NoCrusherProduksi DESC;
    `;

    const genRes = await rqGen.query(genQry);
    const last = genRes.recordset?.[0]?.NoCrusherProduksi;

    let no1;
    if (!last) {
      no1 = prefix + '1'.padStart(width, '0');
    } else {
      const numPart = String(last).replace(prefix, '');
      const nextNum = parseInt(numPart, 10) + 1;
      no1 = prefix + String(nextNum).padStart(width, '0');
    }

    // =========================================================
    // 2) Check duplicate (UPDLOCK untuk prevent race)
    // =========================================================
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoCrusherProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.CrusherProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoCrusherProduksi = @NoCrusherProduksi
      `);

    let noCrusherProduksi = no1;

    if (exist.recordset.length) {
      // collision => ambil last lagi, hitung lagi
      const rqGen2 = new sql.Request(tx);
      rqGen2.input('Prefix', sql.VarChar(10), prefix);

      const genRes2 = await rqGen2.query(genQry);
      const last2 = genRes2.recordset?.[0]?.NoCrusherProduksi;

      if (!last2) {
        noCrusherProduksi = prefix + '1'.padStart(width, '0');
      } else {
        const numPart2 = String(last2).replace(prefix, '');
        const nextNum2 = parseInt(numPart2, 10) + 1;
        noCrusherProduksi = prefix + String(nextNum2).padStart(width, '0');
      }
    }

    // =========================================================
    // 3) Insert header (pakai effectiveDate)
    // =========================================================
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi)
      .input('Tanggal',           sql.Date,        effectiveDate) // ✅ date-only
      .input('IdMesin',           sql.Int,         payload.idMesin)
      .input('IdOperator',        sql.Int,         payload.idOperator)
      .input('Jam',               sql.Int,         parseJamToInt(payload.jam)) // jika jam kamu format "HH:mm", kalau int biasa ganti payload.jam
      .input('Shift',             sql.Int,         payload.shift)
      .input('CreateBy',          sql.VarChar(100), payload.createBy)
      .input('CheckBy1',          sql.VarChar(100), payload.checkBy1 ?? null)
      .input('CheckBy2',          sql.VarChar(100), payload.checkBy2 ?? null)
      .input('ApproveBy',         sql.VarChar(100), payload.approveBy ?? null)
      .input('JmlhAnggota',       sql.Int,          payload.jmlhAnggota ?? null)
      .input('Hadir',             sql.Int,          payload.hadir ?? null)
      .input('HourMeter',         sql.Decimal(18, 2), payload.hourMeter ?? null)
      .input('HourStart',         sql.VarChar(20),   payload.hourStart ?? null)
      .input('HourEnd',           sql.VarChar(20),   payload.hourEnd ?? null);

    const insertSql = `
      INSERT INTO dbo.CrusherProduksi_h (
        NoCrusherProduksi, Tanggal, IdMesin, IdOperator, Jam, Shift,
        CreateBy, CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoCrusherProduksi, @Tanggal, @IdMesin, @IdOperator, @Jam, @Shift,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @JmlhAnggota, @Hadir, @HourMeter,
        CAST(@HourStart AS time(7)),
        CAST(@HourEnd   AS time(7))
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
 * Helper: Parse jam (tolerant)
 */
function parseJamToInt(val) {
  if (val === null || val === undefined) return null;
  const n = Number(val);
  return Number.isNaN(n) ? null : n;
}

/**
 * Helper: Bad request error
 */
function badReq(msg) {
  const e = new Error(msg);
  e.statusCode = 400;
  return e;
}

/**
 * UPDATE CRUSHER PRODUCTION HEADER
 * Supports partial updates of header fields
 * Automatically syncs DateUsage for all inputs when Tanggal is changed
 */
async function updateCrusherProduksi(noCrusherProduksi, payload) {
  if (!noCrusherProduksi) throw badReq('noCrusherProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    menggantikan SELECT header manual untuk ambil Tanggal
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'crusherProduksi',     // ✅ harus ada di config tutup-transaksi
      codeValue: noCrusherProduksi,
      runner: tx,
      useLock: true,                   // UPDATE = write
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) Jika user mengubah tanggal, hitung tanggal baru (date-only)
    // -------------------------------------------------------
    const isChangingDate = payload?.tanggal !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tanggal) throw badReq('tanggal tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tanggal);
    }

    // -------------------------------------------------------
    // 2) GUARD TUTUP TRANSAKSI
    //    - cek tanggal lama
    //    - kalau ganti tanggal, cek tanggal baru juga
    // -------------------------------------------------------
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

    // -------------------------------------------------------
    // 3) BUILD DYNAMIC SET
    // -------------------------------------------------------
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('Tanggal = @Tanggal');
      rqUpd.input('Tanggal', sql.Date, newDocDateOnly); // ✅ date-only
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

    // Jam (durasi)
    if (payload.jam !== undefined) {
      const jamInt = payload.jam === null ? null : parseJamToInt(payload.jam);
      sets.push('Jam = @Jam');
      rqUpd.input('Jam', sql.Int, jamInt);
    }

    // HourStart / HourEnd (lebih aman kalau null / kosong)
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

    const updateSql = `
      UPDATE dbo.CrusherProduksi_h
      SET ${sets.join(', ')}
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      SELECT *
      FROM dbo.CrusherProduksi_h
      WHERE NoCrusherProduksi = @NoCrusherProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // -------------------------------------------------------
    // 4) Jika Tanggal berubah → sync DateUsage (full + partial)
    //    pakai tanggal dari DB supaya konsisten
    // -------------------------------------------------------
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi)
        .input('Tanggal', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BAHAN BAKU (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE bb
        SET bb.DateUsage = @Tanggal
        FROM dbo.BahanBaku_d AS bb
        WHERE bb.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.CrusherProduksiInputBB AS map
              WHERE map.NoCrusherProduksi = @NoCrusherProduksi
                AND map.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak        = bb.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.CrusherProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoCrusherProduksi = @NoCrusherProduksi
                AND bp.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak       = bb.NoSak
            )
          );

        -------------------------------------------------------
        -- BONGGOLAN (FULL ONLY)
        -------------------------------------------------------
        UPDATE b
        SET b.DateUsage = @Tanggal
        FROM dbo.Bonggolan AS b
        WHERE b.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.CrusherProduksiInputBonggolan AS map
            WHERE map.NoCrusherProduksi = @NoCrusherProduksi
              AND map.NoBonggolan = b.NoBonggolan
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
 * DELETE CRUSHER PRODUCTION
 * Deletes header and all related inputs/partials
 * Validates that no outputs exist before deletion
 * Resets DateUsage and IsPartial flags for affected materials
 */
async function deleteCrusherProduksi(noCrusherProduksi) {
  if (!noCrusherProduksi) throw badReq('noCrusherProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    menggantikan SELECT CrusherProduksi_h manual
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'crusherProduksi',      // ✅ harus ada di config tutup-transaksi
      codeValue: noCrusherProduksi,
      runner: tx,
      useLock: true,                    // DELETE = write
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (DELETE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,                       // IMPORTANT: same tx
      action: 'delete CrusherProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) CEK OUTPUT DULU (kalau sudah ada output -> tolak delete)
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const outCheck = await rqCheck
      .input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi)
      .query(`
        SELECT COUNT(*) AS CntOutput
        FROM dbo.CrusherProduksiOutput
        WHERE NoCrusherProduksi = @NoCrusherProduksi;
      `);

    const row = outCheck.recordset?.[0] || { CntOutput: 0 };
    const hasOutput = (row.CntOutput || 0) > 0;

    if (hasOutput) {
      throw badReq('Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.');
    }

    // -------------------------------------------------------
    // 3) DELETE INPUTS + PARTIALS + RESET DATEUSAGE + DELETE HEADER
    //    (SQL besar kamu tetap)
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoCrusherProduksi', sql.VarChar(50), noCrusherProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- TABLE VARIABLES TO STORE AFFECTED KEYS
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

      -- Delete partial detail records linked to this production
      DELETE bp
      FROM dbo.BahanBakuPartial AS bp
      JOIN dbo.CrusherProduksiInputBBPartial AS mp
        ON mp.NoBBPartial = bp.NoBBPartial
      WHERE mp.NoCrusherProduksi = @NoCrusherProduksi;

      -- Delete partial mapping
      DELETE FROM dbo.CrusherProduksiInputBBPartial
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      -- Delete full mapping
      DELETE FROM dbo.CrusherProduksiInputBB
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      -- Reset DateUsage & IsPartial in BahanBaku_d for affected keys
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
      -- 2. BONGGOLAN (NO PARTIAL SUPPORT)
      ---------------------------------------------------------
      INSERT INTO @BonggolanKeys (NoBonggolan)
      SELECT DISTINCT b.NoBonggolan
      FROM dbo.Bonggolan AS b
      WHERE EXISTS (
        SELECT 1
        FROM dbo.CrusherProduksiInputBonggolan AS map
        WHERE map.NoCrusherProduksi = @NoCrusherProduksi
          AND map.NoBonggolan = b.NoBonggolan
      );

      -- Delete full mapping
      DELETE FROM dbo.CrusherProduksiInputBonggolan
      WHERE NoCrusherProduksi = @NoCrusherProduksi;

      -- Reset DateUsage in Bonggolan for affected keys
      UPDATE b
      SET b.DateUsage = NULL
      FROM dbo.Bonggolan AS b
      JOIN @BonggolanKeys AS k
        ON k.NoBonggolan = b.NoBonggolan;

      ---------------------------------------------------------
      -- 3. FINALLY: DELETE HEADER
      ---------------------------------------------------------
      DELETE FROM dbo.CrusherProduksi_h
      WHERE NoCrusherProduksi = @NoCrusherProduksi;
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


/**
 * UPSERT INPUTS & PARTIALS for Crusher Production
 * Payload shape:
 * {
 *   bb: [{ noBahanBaku, noPallet, noSak }],
 *   bonggolan: [{ noBonggolan }],
 *   bbPartialNew: [{ noBahanBaku, noPallet, noSak, berat }]
 * }
 */
async function upsertInputsAndPartials(noCrusherProduksi, payload) {
  if (!noCrusherProduksi) throw badReq('noCrusherProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    bb: norm(payload?.bb),
    bonggolan: norm(payload?.bonggolan),
    bbPartialNew: norm(payload?.bbPartialNew),
  };

  try {
    // IMPORTANT: serializable biar konsisten + cegah race
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    Ini menggantikan SELECT CrusherProduksi_h manual
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'crusherProduksi',      // ✅ harus ada di config tutup-transaksi
      codeValue: noCrusherProduksi,
      runner: tx,
      useLock: true,                    // UPSERT = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (UPSERT INPUT/PARTIAL = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,                       // WAJIB tx
      action: 'upsert CrusherProduksi inputs/partials',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) Create BB partials + map them to produksi
    // -------------------------------------------------------
    const partials = await _insertPartialsWithTx(tx, noCrusherProduksi, {
      bbPartialNew: body.bbPartialNew,
    });

    // -------------------------------------------------------
    // 3) Attach existing inputs (idempotent)
    // -------------------------------------------------------
    const attachments = await _insertInputsWithTx(tx, noCrusherProduksi, {
      bb: body.bb,
      bonggolan: body.bonggolan,
    });

    await tx.commit();

    // ===== response kamu tetap =====
    const totalInserted = Object.values(attachments).reduce((sum, item) => sum + (item.inserted || 0), 0);
    const totalSkipped  = Object.values(attachments).reduce((sum, item) => sum + (item.skipped  || 0), 0);
    const totalInvalid  = Object.values(attachments).reduce((sum, item) => sum + (item.invalid  || 0), 0);

    const totalPartialsCreated = Object.values(partials.summary || {}).reduce(
      (sum, item) => sum + (item.created || 0),
      0
    );

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = totalInserted === 0 && totalPartialsCreated === 0;

    const response = {
      noCrusherProduksi,
      summary: {
        totalInserted,
        totalSkipped,
        totalInvalid,
        totalPartialsCreated,
      },
      details: {
        inputs: _buildInputDetails(attachments, body),
        partials: _buildPartialDetails(partials, body),
      },
      createdPartials: partials.createdLists,
    };

    return {
      success: !hasInvalid && !hasNoSuccess,
      hasWarnings: totalSkipped > 0,
      data: response,
    };
  } catch (err) {
    try { await tx.rollback(); } catch {}
    throw err;
  }
}


// Helper function to build detailed input information
function _buildInputDetails(attachments, requestBody) {
  const details = [];

  const sections = [
    { key: 'bb', label: 'Bahan Baku' },
    { key: 'bonggolan', label: 'Bonggolan' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = attachments[section.key] || { inserted: 0, skipped: 0, invalid: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      inserted: result.inserted,
      skipped: result.skipped,
      invalid: result.invalid,
      status: result.invalid > 0 ? 'error' : result.skipped > 0 ? 'warning' : 'success',
      message: _buildSectionMessage(section.label, result, requestedCount),
    });
  }

  return details;
}

// Helper function to build detailed partial information
function _buildPartialDetails(partials, requestBody) {
  const details = [];

  const sections = [
    { key: 'bbPartialNew', label: 'Bahan Baku Partial' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const created = partials.summary[section.key]?.created || 0;

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      created: created,
      status: created === requestedCount ? 'success' : 'error',
      message: `${created} dari ${requestedCount} ${section.label} berhasil dibuat`,
      codes: partials.createdLists[section.key] || [],
    });
  }

  return details;
}

// Helper function to build section message
function _buildSectionMessage(label, result, requested) {
  const parts = [];

  if (result.inserted > 0) {
    parts.push(`${result.inserted} berhasil ditambahkan`);
  }
  if (result.skipped > 0) {
    parts.push(`${result.skipped} sudah ada (dilewati)`);
  }
  if (result.invalid > 0) {
    parts.push(`${result.invalid} tidak valid (tidak ditemukan)`);
  }

  if (parts.length === 0) {
    return `Tidak ada ${label} yang diproses`;
  }

  return `${label}: ${parts.join(', ')}`;
}

/* --------------------------
   SQL batches (set-based)
-------------------------- */

async function _insertPartialsWithTx(tx, noCrusherProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noCrusherProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_PARTIALS = `
  SET NOCOUNT ON;

  -- Get Tanggal from crusher header
  DECLARE @tanggal datetime;
  SELECT @tanggal = Tanggal 
  FROM dbo.CrusherProduksi_h WITH (NOLOCK)
  WHERE NoCrusherProduksi = @no;

  -- Global lock for sequence generation (10s timeout)
  DECLARE @lockResult int;
  EXEC @lockResult = sp_getapplock
    @Resource = 'SEQ_PARTIALS_CRUSHER',
    @LockMode = 'Exclusive',
    @LockTimeout = 10000,
    @DbPrincipal = 'public';

  IF (@lockResult < 0)
  BEGIN
    RAISERROR('Failed to acquire SEQ_PARTIALS_CRUSHER lock', 16, 1);
  END;

  -- Capture generated codes for response
  DECLARE @bbNew TABLE(NoBBPartial varchar(50));

  /* =========================
     BB PARTIAL (P.##########)
     ========================= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.bbPartialNew'))
  BEGIN
    DECLARE @nextBB int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoBBPartial,10) AS int))
      FROM dbo.BahanBakuPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBBPartial LIKE 'P.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noBahanBaku,
        noPallet,
        noSak,
        berat,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.bbPartialNew')
      WITH (
        noBahanBaku varchar(50) '$.noBahanBaku',
        noPallet    int         '$.noPallet',
        noSak       int         '$.noSak',
        berat       decimal(18,3) '$.berat'
      )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('P.', RIGHT(REPLICATE('0',10) + CAST(@nextBB + rn AS varchar(10)), 10)),
        noBahanBaku, noPallet, noSak, berat
      FROM src
    )
    INSERT INTO dbo.BahanBakuPartial (NoBBPartial, NoBahanBaku, NoPallet, NoSak, Berat)
    OUTPUT INSERTED.NoBBPartial INTO @bbNew(NoBBPartial)
    SELECT NewNo, noBahanBaku, noPallet, noSak, berat
    FROM numbered;

    -- Map to crusher produksi
    INSERT INTO dbo.CrusherProduksiInputBBPartial (NoCrusherProduksi, NoBBPartial)
    SELECT @no, n.NoBBPartial
    FROM @bbNew n;

    -- Update IsPartial & DateUsage for BahanBaku_d
    ;WITH existingPartials AS (
      SELECT 
        bp.NoBahanBaku,
        bp.NoPallet,
        bp.NoSak,
        SUM(ISNULL(bp.Berat, 0)) AS TotalBeratPartialExisting
      FROM dbo.BahanBakuPartial bp WITH (NOLOCK)
      WHERE bp.NoBBPartial NOT IN (SELECT NoBBPartial FROM @bbNew)
      GROUP BY bp.NoBahanBaku, bp.NoPallet, bp.NoSak
    ),
    newPartials AS (
      SELECT 
        noBahanBaku,
        noPallet,
        noSak,
        SUM(berat) AS TotalBeratPartialNew
      FROM OPENJSON(@jsPartials, '$.bbPartialNew')
      WITH (
        noBahanBaku varchar(50) '$.noBahanBaku',
        noPallet    int         '$.noPallet',
        noSak       int         '$.noSak',
        berat       decimal(18,3) '$.berat'
      )
      GROUP BY noBahanBaku, noPallet, noSak
    )
    UPDATE bb
    SET 
      bb.IsPartial = 1,
      -- ⬇️ FIX: Add tolerance of 0.001 kg (1 gram) for floating point comparison
      bb.DateUsage = CASE 
        WHEN (ISNULL(NULLIF(bb.BeratAct, 0), bb.Berat) - ISNULL(ep.TotalBeratPartialExisting, 0) - ISNULL(np.TotalBeratPartialNew, 0)) <= 0.001
        THEN @tanggal 
        ELSE bb.DateUsage 
      END
    FROM dbo.BahanBaku_d bb
    LEFT JOIN existingPartials ep 
      ON ep.NoBahanBaku = bb.NoBahanBaku 
      AND ep.NoPallet = bb.NoPallet 
      AND ep.NoSak = bb.NoSak
    INNER JOIN newPartials np 
      ON np.noBahanBaku = bb.NoBahanBaku 
      AND np.noPallet = bb.NoPallet 
      AND np.noSak = bb.NoSak;
  END;

  -- Release the applock
  EXEC sp_releaseapplock @Resource = 'SEQ_PARTIALS_CRUSHER', @DbPrincipal = 'public';

  -- Summary
  SELECT 'bbPartialNew' AS Section, COUNT(*) AS Created FROM @bbNew;

  -- Return generated codes
  SELECT NoBBPartial FROM @bbNew;
  `;

  const rs = await req.query(SQL_PARTIALS);

  // Recordset[0]: summary rows
  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  // Recordset[1]: BB partial codes
  const createdLists = {
    bbPartialNew: (rs.recordsets?.[1] || []).map((r) => r.NoBBPartial),
  };

  return { summary, createdLists };
}

async function _insertInputsWithTx(tx, noCrusherProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noCrusherProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_ATTACH = `
  SET NOCOUNT ON;

  -- Get Tanggal from crusher header
  DECLARE @tanggal datetime;
  SELECT @tanggal = Tanggal 
  FROM dbo.CrusherProduksi_h WITH (NOLOCK)
  WHERE NoCrusherProduksi = @no;

  DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);

  -- BB
  DECLARE @bbInserted int = 0;
  DECLARE @bbSkipped int = 0;
  DECLARE @bbInvalid int = 0;

  ;WITH j AS (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) 
                  WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak)
  )
  INSERT INTO dbo.CrusherProduksiInputBB (NoCrusherProduksi, NoBahanBaku, NoPallet, NoSak)
  SELECT @no, v.noBahanBaku, v.noPallet, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.CrusherProduksiInputBB x 
    WHERE x.NoCrusherProduksi=@no AND x.NoBahanBaku=v.noBahanBaku 
      AND x.NoPallet=v.noPallet AND x.NoSak=v.noSak
  );

  SET @bbInserted = @@ROWCOUNT;

  -- Update DateUsage for BahanBaku_d
  IF @bbInserted > 0
  BEGIN
    UPDATE bb
    SET bb.DateUsage = @tanggal
    FROM dbo.BahanBaku_d bb
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.bb')
      WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' ) src
      WHERE bb.NoBahanBaku = src.noBahanBaku AND bb.NoPallet = src.noPallet AND bb.NoSak = src.noSak
    );
  END;

  SELECT @bbSkipped = COUNT(*) FROM (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) 
                WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak)
    AND EXISTS (SELECT 1 FROM dbo.CrusherProduksiInputBB x 
                WHERE x.NoCrusherProduksi=@no AND x.NoBahanBaku=j.noBahanBaku 
                  AND x.NoPallet=j.noPallet AND x.NoSak=j.noSak);

  SELECT @bbInvalid = COUNT(*) FROM (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) 
                    WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'bb', @bbInserted, @bbSkipped, @bbInvalid;

  -- BONGGOLAN
  DECLARE @bonggolInserted int = 0;
  DECLARE @bonggolSkipped int = 0;
  DECLARE @bonggolInvalid int = 0;

  ;WITH j AS (
    SELECT noBonggolan
    FROM OPENJSON(@jsInputs, '$.bonggolan') WITH ( noBonggolan varchar(50) '$.noBonggolan' )
  ),
  v AS (
    SELECT j.* FROM j WHERE EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan)
  )
  INSERT INTO dbo.CrusherProduksiInputBonggolan (NoCrusherProduksi, NoBonggolan)
  SELECT @no, v.noBonggolan
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.CrusherProduksiInputBonggolan x 
    WHERE x.NoCrusherProduksi=@no AND x.NoBonggolan=v.noBonggolan
  );

  SET @bonggolInserted = @@ROWCOUNT;

  -- Update DateUsage for Bonggolan
  IF @bonggolInserted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tanggal
    FROM dbo.Bonggolan b
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.bonggolan')
      WITH ( noBonggolan varchar(50) '$.noBonggolan' ) src
      WHERE b.NoBonggolan = src.noBonggolan
    );
  END;

  SELECT @bonggolSkipped = COUNT(*) FROM (
    SELECT noBonggolan
    FROM OPENJSON(@jsInputs, '$.bonggolan') WITH ( noBonggolan varchar(50) '$.noBonggolan' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan)
    AND EXISTS (SELECT 1 FROM dbo.CrusherProduksiInputBonggolan x 
                WHERE x.NoCrusherProduksi=@no AND x.NoBonggolan=j.noBonggolan);

  SELECT @bonggolInvalid = COUNT(*) FROM (
    SELECT noBonggolan
    FROM OPENJSON(@jsInputs, '$.bonggolan') WITH ( noBonggolan varchar(50) '$.noBonggolan' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan);

  INSERT INTO @out SELECT 'bonggolan', @bonggolInserted, @bonggolSkipped, @bonggolInvalid;

  SELECT Section, Inserted, Skipped, Invalid FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_ATTACH);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = {
      inserted: row.Inserted,
      skipped: row.Skipped,
      invalid: row.Invalid,
    };
  }
  return out;
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
 * DELETE INPUTS AND PARTIALS for Crusher Production
 * Only supports: bb, bonggolan, bbPartial
 */
async function deleteInputsAndPartials(noCrusherProduksi, payload) {
  if (!noCrusherProduksi) throw badReq('noCrusherProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    bb: norm(payload?.bb),
    bonggolan: norm(payload?.bonggolan),
    bbPartial: norm(payload?.bbPartial),
  };

  try {
    // IMPORTANT: serializable biar konsisten + cegah race
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    Ini menggantikan SELECT CrusherProduksi_h manual
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'crusherProduksi',      // ✅ harus ada di config tutup-transaksi
      codeValue: noCrusherProduksi,
      runner: tx,
      useLock: true,                    // DELETE INPUT/PARTIAL = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (DELETE INPUT/PARTIAL = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,                       // WAJIB tx
      action: 'delete CrusherProduksi inputs/partials',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) Delete partials mappings (+ kalau logic kamu delete row partial juga)
    // -------------------------------------------------------
    const partialsResult = await _deletePartialsWithTx(tx, noCrusherProduksi, {
      bbPartial: body.bbPartial,
    });

    // -------------------------------------------------------
    // 3) Delete inputs mappings
    // -------------------------------------------------------
    const inputsResult = await _deleteInputsWithTx(tx, noCrusherProduksi, {
      bb: body.bb,
      bonggolan: body.bonggolan,
    });

    await tx.commit();

    // ===== response kamu tetap =====
    const totalDeleted = Object.values(inputsResult).reduce((sum, item) => sum + (item.deleted || 0), 0);
    const totalNotFound = Object.values(inputsResult).reduce((sum, item) => sum + (item.notFound || 0), 0);

    const totalPartialsDeleted = Object.values(partialsResult.summary || {}).reduce(
      (sum, item) => sum + (item.deleted || 0),
      0
    );
    const totalPartialsNotFound = Object.values(partialsResult.summary || {}).reduce(
      (sum, item) => sum + (item.notFound || 0),
      0
    );

    const hasNotFound = totalNotFound > 0 || totalPartialsNotFound > 0;
    const hasNoSuccess = totalDeleted === 0 && totalPartialsDeleted === 0;

    const response = {
      noCrusherProduksi,
      summary: {
        totalDeleted,
        totalNotFound,
        totalPartialsDeleted,
        totalPartialsNotFound,
      },
      details: {
        inputs: _buildDeleteInputDetails(inputsResult, body),
        partials: _buildDeletePartialDetails(partialsResult, body),
      },
    };

    return {
      success: !hasNoSuccess,
      hasWarnings: hasNotFound,
      data: response,
    };
  } catch (err) {
    try { await tx.rollback(); } catch {}
    throw err;
  }
}


/**
 * Helper to build delete input details
 */
function _buildDeleteInputDetails(results, requestBody) {
  const details = [];
  const sections = [
    { key: 'bb', label: 'Bahan Baku' },
    { key: 'bonggolan', label: 'Bonggolan' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = results[section.key] || { deleted: 0, notFound: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      deleted: result.deleted,
      notFound: result.notFound,
      status: result.notFound > 0 ? 'warning' : 'success',
      message: `${section.label}: ${result.deleted} berhasil dihapus${result.notFound > 0 ? `, ${result.notFound} tidak ditemukan` : ''}`,
    });
  }

  return details;
}

/**
 * Helper to build delete partial details
 */
function _buildDeletePartialDetails(partialsResult, requestBody) {
  const details = [];
  const sections = [
    { key: 'bbPartial', label: 'Bahan Baku Partial' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = partialsResult.summary[section.key] || { deleted: 0, notFound: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      deleted: result.deleted,
      notFound: result.notFound,
      status: result.notFound > 0 ? 'warning' : 'success',
      message: `${section.label}: ${result.deleted} berhasil dihapus${result.notFound > 0 ? `, ${result.notFound} tidak ditemukan` : ''}`,
    });
  }

  return details;
}

/**
 * Delete partials with transaction (only BB partial for crusher)
 */
async function _deletePartialsWithTx(tx, noCrusherProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noCrusherProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE_PARTIALS = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  -- BB PARTIAL
  DECLARE @bbDeleted int = 0, @bbNotFound int = 0;
  
  SELECT @bbDeleted = COUNT(*)
  FROM dbo.CrusherProduksiInputBBPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON map.NoBBPartial = j.noBBPartial
  WHERE map.NoCrusherProduksi = @no;
  
  DECLARE @deletedBBPartials TABLE (
    NoBahanBaku varchar(50),
    NoPallet int,
    NoSak int
  );
  
  INSERT INTO @deletedBBPartials (NoBahanBaku, NoPallet, NoSak)
  SELECT DISTINCT bp.NoBahanBaku, bp.NoPallet, bp.NoSak
  FROM dbo.BahanBakuPartial bp
  INNER JOIN dbo.CrusherProduksiInputBBPartial map ON bp.NoBBPartial = map.NoBBPartial
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON map.NoBBPartial = j.noBBPartial
  WHERE map.NoCrusherProduksi = @no;
  
  DELETE map
  FROM dbo.CrusherProduksiInputBBPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON map.NoBBPartial = j.noBBPartial
  WHERE map.NoCrusherProduksi = @no;
  
  DELETE bp
  FROM dbo.BahanBakuPartial bp
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON bp.NoBBPartial = j.noBBPartial;
  
  IF @bbDeleted > 0
  BEGIN
    UPDATE d
    SET 
      d.DateUsage = NULL,
      d.IsPartial = 1
    FROM dbo.BahanBaku_d d
    INNER JOIN @deletedBBPartials del 
      ON d.NoBahanBaku = del.NoBahanBaku 
      AND d.NoPallet = del.NoPallet 
      AND d.NoSak = del.NoSak
    WHERE EXISTS (
      SELECT 1 
      FROM dbo.BahanBakuPartial bp 
      WHERE bp.NoBahanBaku = d.NoBahanBaku 
        AND bp.NoPallet = d.NoPallet 
        AND bp.NoSak = d.NoSak
    );
    
    UPDATE d
    SET 
      d.DateUsage = NULL,
      d.IsPartial = 0
    FROM dbo.BahanBaku_d d
    INNER JOIN @deletedBBPartials del 
      ON d.NoBahanBaku = del.NoBahanBaku 
      AND d.NoPallet = del.NoPallet 
      AND d.NoSak = del.NoSak
    WHERE NOT EXISTS (
      SELECT 1 
      FROM dbo.BahanBakuPartial bp 
      WHERE bp.NoBahanBaku = d.NoBahanBaku 
        AND bp.NoPallet = d.NoPallet 
        AND bp.NoSak = d.NoSak
    );
  END;
  
  DECLARE @bbRequested int;
  SELECT @bbRequested = COUNT(*)
  FROM OPENJSON(@jsPartials, '$.bbPartial');
  
  SET @bbNotFound = @bbRequested - @bbDeleted;
  
  INSERT INTO @out SELECT 'bbPartial', @bbDeleted, @bbNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_DELETE_PARTIALS);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = {
      deleted: row.Deleted,
      notFound: row.NotFound,
    };
  }

  return { summary };
}

/**
 * Delete inputs with transaction
 */
async function _deleteInputsWithTx(tx, noCrusherProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noCrusherProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE_INPUTS = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  -- BB
  DECLARE @bbDeleted int = 0, @bbNotFound int = 0;
  
  SELECT @bbDeleted = COUNT(*)
  FROM dbo.CrusherProduksiInputBB map
  INNER JOIN OPENJSON(@jsInputs, '$.bb') 
  WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
  ON map.NoBahanBaku = j.noBahanBaku AND map.NoPallet = j.noPallet AND map.NoSak = j.noSak
  WHERE map.NoCrusherProduksi = @no;
  
  -- Reset DateUsage sebelum DELETE
  IF @bbDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage = NULL
    FROM dbo.BahanBaku_d d
    INNER JOIN dbo.CrusherProduksiInputBB map 
      ON d.NoBahanBaku = map.NoBahanBaku AND d.NoPallet = map.NoPallet AND d.NoSak = map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.bb') 
    WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
    ON map.NoBahanBaku = j.noBahanBaku AND map.NoPallet = j.noPallet AND map.NoSak = j.noSak
    WHERE map.NoCrusherProduksi = @no;
  END;
  
  DELETE map
  FROM dbo.CrusherProduksiInputBB map
  INNER JOIN OPENJSON(@jsInputs, '$.bb') 
  WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
  ON map.NoBahanBaku = j.noBahanBaku AND map.NoPallet = j.noPallet AND map.NoSak = j.noSak
  WHERE map.NoCrusherProduksi = @no;
  
  DECLARE @bbRequested int;
  SELECT @bbRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.bb');
  
  SET @bbNotFound = @bbRequested - @bbDeleted;
  
  INSERT INTO @out SELECT 'bb', @bbDeleted, @bbNotFound;

  -- BONGGOLAN
  DECLARE @bonggolanDeleted int = 0, @bonggolanNotFound int = 0;
  
  SELECT @bonggolanDeleted = COUNT(*)
  FROM dbo.CrusherProduksiInputBonggolan map
  INNER JOIN OPENJSON(@jsInputs, '$.bonggolan') 
  WITH (noBonggolan varchar(50) '$.noBonggolan') j
  ON map.NoBonggolan = j.noBonggolan
  WHERE map.NoCrusherProduksi = @no;
  
  -- Reset DateUsage sebelum DELETE
  IF @bonggolanDeleted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = NULL
    FROM dbo.Bonggolan b
    INNER JOIN dbo.CrusherProduksiInputBonggolan map ON b.NoBonggolan = map.NoBonggolan
    INNER JOIN OPENJSON(@jsInputs, '$.bonggolan') 
    WITH (noBonggolan varchar(50) '$.noBonggolan') j
    ON map.NoBonggolan = j.noBonggolan
    WHERE map.NoCrusherProduksi = @no;
  END;
  
  DELETE map
  FROM dbo.CrusherProduksiInputBonggolan map
  INNER JOIN OPENJSON(@jsInputs, '$.bonggolan') 
  WITH (noBonggolan varchar(50) '$.noBonggolan') j
  ON map.NoBonggolan = j.noBonggolan
  WHERE map.NoCrusherProduksi = @no;
  
  DECLARE @bonggolanRequested int;
  SELECT @bonggolanRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.bonggolan');
  
  SET @bonggolanNotFound = @bonggolanRequested - @bonggolanDeleted;
  
  INSERT INTO @out SELECT 'bonggolan', @bonggolanDeleted, @bonggolanNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_DELETE_INPUTS);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = {
      deleted: row.Deleted,
      notFound: row.NotFound,
    };
  }
  return out;
}


module.exports = { getAllProduksi, getProduksiByDate, getCrusherMasters, createCrusherProduksi, updateCrusherProduksi, deleteCrusherProduksi, fetchInputs, upsertInputsAndPartials, validateLabel, deleteInputsAndPartials };

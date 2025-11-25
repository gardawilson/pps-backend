// services/production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const offset = (page - 1) * pageSize;
  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
  `;

  // 1) Total baris
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

  // 2) Data halaman - KONVERSI TIME KE STRING
  const dataQry = `
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
      CONVERT(VARCHAR(8), h.HourEnd, 108) AS HourEnd
    FROM dbo.WashingProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator
    ${whereClause}
    ORDER BY h.TglProduksi DESC, h.JamKerja ASC, h.NoProduksi ASC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, pageSize);

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




// CREATE WASHING PRODUCTION HEADER
function badReq(msg) {
  const e = new Error(msg);
  e.statusCode = 400;
  return e;
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
    // Prefix 'C.' untuk washing
    const no1 = await generateNextNoProduksiWashing(tx, { prefix: 'C.', width: 10 });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar, no1)
      .query(`
        SELECT 1
        FROM dbo.WashingProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksiWashing(tx, { prefix: 'C.', width: 10 })
      : no1;

    const jamInt = parseJamToInt(payload.jamKerja);

    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),    noProduksi)
      .input('IdOperator',  sql.Int,           payload.idOperator)
      .input('IdMesin',     sql.Int,           payload.idMesin)
      .input('TglProduksi', sql.Date,          payload.tglProduksi)
      .input('JamKerja',    sql.Int,           jamInt)
      .input('Shift',       sql.Int,           payload.shift)
      .input('CreateBy',    sql.VarChar(100),  payload.createBy)
      .input('CheckBy1',    sql.VarChar(100),  payload.checkBy1 ?? null)
      .input('CheckBy2',    sql.VarChar(100),  payload.checkBy2 ?? null)
      .input('ApproveBy',   sql.VarChar(100),  payload.approveBy ?? null)
      .input('JmlhAnggota', sql.Int,           payload.jmlhAnggota ?? null)
      .input('Hadir',       sql.Int,           payload.hadir ?? null)
      .input('HourMeter',   sql.Decimal(18, 2), payload.hourMeter ?? null)
      .input('HourStart',   sql.VarChar(20),   payload.hourStart ?? null)
      .input('HourEnd',     sql.VarChar(20),   payload.hourEnd ?? null);

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
    // ===================================================================
    // 1. CEK DATA EXISTS + LOCK ROW
    // ===================================================================
    const rqGet = new sql.Request(tx);
    const current = await rqGet
      .input('NoProduksi', sql.VarChar, noProduksi)
      .query(`
        SELECT *
        FROM dbo.WashingProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (current.recordset.length === 0) {
      throw badReq('Data not found');
    }

    // ===================================================================
    // 2. BUILD DYNAMIC SET CLAUSE
    // ===================================================================
    const sets = [];
    const rqUpd = new sql.Request(tx);

    // TglProduksi
    if (payload.tglProduksi !== undefined) {
      sets.push('TglProduksi = @TglProduksi');
      rqUpd.input('TglProduksi', sql.Date, payload.tglProduksi);
    }

    // IdMesin
    if (payload.idMesin !== undefined) {
      sets.push('IdMesin = @IdMesin');
      rqUpd.input('IdMesin', sql.Int, payload.idMesin);
    }

    // IdOperator
    if (payload.idOperator !== undefined) {
      sets.push('IdOperator = @IdOperator');
      rqUpd.input('IdOperator', sql.Int, payload.idOperator);
    }

    // Shift
    if (payload.shift !== undefined) {
      sets.push('Shift = @Shift');
      rqUpd.input('Shift', sql.Int, payload.shift);
    }

    // ⚠️ PERBEDAAN: JamKerja (bukan Jam seperti di Broker)
    if (payload.jamKerja !== undefined) {
      const jamInt = payload.jamKerja === null ? null : parseJamToInt(payload.jamKerja);
      sets.push('JamKerja = @JamKerja');
      rqUpd.input('JamKerja', sql.Int, jamInt);
    }

    // HourStart
    if (payload.hourStart !== undefined) {
      sets.push('HourStart = CAST(@HourStart AS time(7))');
      rqUpd.input('HourStart', sql.VarChar(20), payload.hourStart);
    }

    // HourEnd
    if (payload.hourEnd !== undefined) {
      sets.push('HourEnd = CAST(@HourEnd AS time(7))');
      rqUpd.input('HourEnd', sql.VarChar(20), payload.hourEnd);
    }

    // CheckBy1
    if (payload.checkBy1 !== undefined) {
      sets.push('CheckBy1 = @CheckBy1');
      rqUpd.input('CheckBy1', sql.VarChar(100), payload.checkBy1 ?? null);
    }

    // CheckBy2
    if (payload.checkBy2 !== undefined) {
      sets.push('CheckBy2 = @CheckBy2');
      rqUpd.input('CheckBy2', sql.VarChar(100), payload.checkBy2 ?? null);
    }

    // ApproveBy
    if (payload.approveBy !== undefined) {
      sets.push('ApproveBy = @ApproveBy');
      rqUpd.input('ApproveBy', sql.VarChar(100), payload.approveBy ?? null);
    }

    // JmlhAnggota
    if (payload.jmlhAnggota !== undefined) {
      sets.push('JmlhAnggota = @JmlhAnggota');
      rqUpd.input('JmlhAnggota', sql.Int, payload.jmlhAnggota ?? null);
    }

    // Hadir
    if (payload.hadir !== undefined) {
      sets.push('Hadir = @Hadir');
      rqUpd.input('Hadir', sql.Int, payload.hadir ?? null);
    }

    // HourMeter
    if (payload.hourMeter !== undefined) {
      sets.push('HourMeter = @HourMeter');
      rqUpd.input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    // Validasi: harus ada minimal 1 field untuk di-update
    if (sets.length === 0) {
      await tx.rollback();
      throw badReq('No fields to update');
    }

    rqUpd.input('NoProduksi', sql.VarChar, noProduksi);

    // ===================================================================
    // 3. EXECUTE UPDATE + SELECT UPDATED ROW
    // ===================================================================
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

    // ===================================================================
    // 4. SYNC DateUsage KE SEMUA INPUT LABELS (jika TglProduksi berubah)
    // ===================================================================
    if (payload.tglProduksi !== undefined && updatedHeader) {
      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar, noProduksi)
        .input('TglProduksi', sql.Date, updatedHeader.TglProduksi);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BAHAN BAKU (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE bb
        SET bb.DateUsage = @TglProduksi
        FROM dbo.BahanBaku_d AS bb
        WHERE bb.DateUsage IS NOT NULL
          AND (
            -- full
            EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInput AS map
              WHERE map.NoProduksi   = @NoProduksi
                AND map.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak        = bb.NoSak
            )
            OR
            -- partial
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
        -- WASHING (FULL ONLY - TIDAK ADA PARTIAL)
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
            -- full
            EXISTS (
              SELECT 1
              FROM dbo.WashingProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
            )
            OR
            -- partial
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

    // ===================================================================
    // 5. COMMIT TRANSACTION
    // ===================================================================
    await tx.commit();

    return { header: updatedHeader };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
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
    // 0. CEK DULU: SUDAH PUNYA OUTPUT ATAU BELUM
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const outCheck = await rqCheck
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT COUNT(*) AS CntOutput
        FROM dbo.WashingProduksiOutput
        WHERE NoProduksi = @NoProduksi;
      `);

    const row = outCheck.recordset[0] || { CntOutput: 0 };
    const hasOutput = (row.CntOutput || 0) > 0;

    if (hasOutput) {
      // Sudah ada data output → tolak delete
      await tx.rollback();
      throw badReq('Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.');
    }

    // -------------------------------------------------------
    // 1. LANJUT DELETE INPUT + PARTIAL + RESET DATEUSAGE
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

    DECLARE @WashingKeys TABLE (
      NoWashing varchar(50)
    );

    DECLARE @GilinganKeys TABLE (
      NoGilingan varchar(50)
    );

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

    -- Hapus baris partial detail yang terhubung NoProduksi ini
    DELETE bp
    FROM dbo.BahanBakuPartial AS bp
    JOIN dbo.WashingProduksiInputBBPartial AS mp
      ON mp.NoBBPartial = bp.NoBBPartial
    WHERE mp.NoProduksi = @NoProduksi;

    -- Hapus mapping partial
    DELETE FROM dbo.WashingProduksiInputBBPartial
    WHERE NoProduksi = @NoProduksi;

    -- Hapus mapping full
    DELETE FROM dbo.WashingProduksiInput
    WHERE NoProduksi = @NoProduksi;

    -- Reset DateUsage & IsPartial di BahanBaku_d untuk key yang terdampak
    UPDATE bb
    SET bb.DateUsage = NULL,
        bb.IsPartial = CASE 
          WHEN EXISTS (
            SELECT 1
            FROM dbo.BahanBakuPartial AS bp
            WHERE bp.NoBahanBaku = bb.NoBahanBaku
              AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
              AND bp.NoSak       = bb.NoSak
          ) THEN 1 ELSE 0 END
    FROM dbo.BahanBaku_d AS bb
    JOIN @BBKeys AS k
      ON k.NoBahanBaku = bb.NoBahanBaku
     AND ISNULL(k.NoPallet,'') = ISNULL(bb.NoPallet,'')
     AND k.NoSak       = bb.NoSak;

    ---------------------------------------------------------
    -- 2. WASHING (TIDAK ADA PARTIAL)
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
    -- 3. GILINGAN (ADA PARTIAL)
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

    -- Hapus detail partial
    DELETE gp
    FROM dbo.GilinganPartial AS gp
    JOIN dbo.WashingProduksiInputGilinganPartial AS mp
      ON mp.NoGilinganPartial = gp.NoGilinganPartial
    WHERE mp.NoProduksi = @NoProduksi;

    -- Hapus mapping partial
    DELETE FROM dbo.WashingProduksiInputGilinganPartial
    WHERE NoProduksi = @NoProduksi;

    -- Hapus mapping full
    DELETE FROM dbo.WashingProduksiInputGilingan
    WHERE NoProduksi = @NoProduksi;

    -- Reset DateUsage & IsPartial di Gilingan
    UPDATE g
    SET g.DateUsage = NULL,
        g.IsPartial = CASE 
          WHEN EXISTS (
            SELECT 1 FROM dbo.GilinganPartial AS gp
            WHERE gp.NoGilingan = g.NoGilingan
          ) THEN 1 ELSE 0 END
    FROM dbo.Gilingan AS g
    JOIN @GilinganKeys AS k
      ON k.NoGilingan = g.NoGilingan;

    ---------------------------------------------------------
    -- 4. TERAKHIR: HAPUS HEADER WASHINGPRODUKSI_H
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


async function upsertInputsAndPartials(noProduksi, payload) {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    bb: norm(payload.bb),
    washing: norm(payload.washing),
    gilingan: norm(payload.gilingan),

    bbPartialNew: norm(payload.bbPartialNew),
    gilinganPartialNew: norm(payload.gilinganPartialNew),
  };

  try {
    await tx.begin();

    // 1) Create partials + map them to produksi
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      bbPartialNew: body.bbPartialNew,
      gilinganPartialNew: body.gilinganPartialNew,
    });

    // 2) Attach existing inputs (idempotent)
    const attachments = await _insertInputsWithTx(tx, noProduksi, {
      bb: body.bb,
      washing: body.washing,
      gilingan: body.gilingan,
    });

    await tx.commit();

    // Calculate totals
    const totalInserted = Object.values(attachments).reduce((sum, item) => sum + (item.inserted || 0), 0);
    const totalSkipped = Object.values(attachments).reduce((sum, item) => sum + (item.skipped || 0), 0);
    const totalInvalid = Object.values(attachments).reduce((sum, item) => sum + (item.invalid || 0), 0);
    const totalPartialsCreated = Object.values(partials.summary).reduce((sum, item) => sum + (item.created || 0), 0);

    // Determine if there are any issues
    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = totalInserted === 0 && totalPartialsCreated === 0;

    // Build detailed response
    const response = {
      noProduksi,
      summary: {
        totalInserted,
        totalSkipped,
        totalInvalid,
        totalPartialsCreated
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
    try {
      await tx.rollback();
    } catch {}
    throw err;
  }
}

// Helper function to build detailed input information
function _buildInputDetails(attachments, requestBody) {
  const details = [];

  const sections = [
    { key: 'bb', label: 'Bahan Baku' },
    { key: 'washing', label: 'Washing' },
    { key: 'gilingan', label: 'Gilingan' },
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
    { key: 'gilinganPartialNew', label: 'Gilingan Partial' },
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

async function _insertPartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_PARTIALS = `
  SET NOCOUNT ON;

  -- Get TglProduksi from header
  DECLARE @tglProduksi datetime;
  SELECT @tglProduksi = TglProduksi 
  FROM dbo.WashingProduksi_h WITH (NOLOCK)
  WHERE NoProduksi = @no;

  -- Global lock for sequence generation (10s timeout)
  DECLARE @lockResult int;
  EXEC @lockResult = sp_getapplock
    @Resource = 'SEQ_WASHING_PARTIALS',
    @LockMode = 'Exclusive',
    @LockTimeout = 10000,
    @DbPrincipal = 'public';

  IF (@lockResult < 0)
  BEGIN
    RAISERROR('Failed to acquire SEQ_WASHING_PARTIALS lock', 16, 1);
  END;

  -- Capture generated codes for response
  DECLARE @bbNew TABLE(NoBBPartial varchar(50));
  DECLARE @gilNew TABLE(NoGilinganPartial varchar(50));

  /* =========================
     BB PARTIAL (WP.##########)
     ========================= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.bbPartialNew'))
  BEGIN
    DECLARE @nextBB int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoBBPartial,10) AS int))
      FROM dbo.BahanBakuPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBBPartial LIKE 'WP.%'
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
        NewNo = CONCAT('WP.', RIGHT(REPLICATE('0',10) + CAST(@nextBB + rn AS varchar(10)), 10)),
        noBahanBaku, noPallet, noSak, berat
      FROM src
    )
    INSERT INTO dbo.BahanBakuPartial (NoBBPartial, NoBahanBaku, NoPallet, NoSak, Berat)
    OUTPUT INSERTED.NoBBPartial INTO @bbNew(NoBBPartial)
    SELECT NewNo, noBahanBaku, noPallet, noSak, berat
    FROM numbered;

    -- Map to produksi
    INSERT INTO dbo.WashingProduksiInputBBPartial (NoProduksi, NoBBPartial)
    SELECT @no, n.NoBBPartial
    FROM @bbNew n;

    -- Update IsPartial and DateUsage
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
      bb.DateUsage = CASE 
        WHEN (bb.BeratAct - ISNULL(ep.TotalBeratPartialExisting, 0) - ISNULL(np.TotalBeratPartialNew, 0)) <= 0 
        THEN @tglProduksi 
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

  /* ==============================
     GILINGAN PARTIAL (WY.##########)
     ============================== */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.gilinganPartialNew'))
  BEGIN
    DECLARE @nextG int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoGilinganPartial,10) AS int))
      FROM dbo.GilinganPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoGilinganPartial LIKE 'WY.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noGilingan,
        berat,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.gilinganPartialNew')
      WITH (
        noGilingan varchar(50) '$.noGilingan',
        berat      decimal(18,3) '$.berat'
      )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('WY.', RIGHT(REPLICATE('0',10) + CAST(@nextG + rn AS varchar(10)), 10)),
        noGilingan, berat
      FROM src
    )
    INSERT INTO dbo.GilinganPartial (NoGilinganPartial, NoGilingan, Berat)
    OUTPUT INSERTED.NoGilinganPartial INTO @gilNew(NoGilinganPartial)
    SELECT NewNo, noGilingan, berat
    FROM numbered;

    INSERT INTO dbo.WashingProduksiInputGilinganPartial (NoProduksi, NoGilinganPartial)
    SELECT @no, n.NoGilinganPartial
    FROM @gilNew n;

    -- Update IsPartial and DateUsage
    ;WITH existingPartials AS (
      SELECT 
        gp.NoGilingan,
        SUM(ISNULL(gp.Berat, 0)) AS TotalBeratPartialExisting
      FROM dbo.GilinganPartial gp WITH (NOLOCK)
      WHERE gp.NoGilinganPartial NOT IN (SELECT NoGilinganPartial FROM @gilNew)
      GROUP BY gp.NoGilingan
    ),
    newPartials AS (
      SELECT 
        noGilingan,
        SUM(berat) AS TotalBeratPartialNew
      FROM OPENJSON(@jsPartials, '$.gilinganPartialNew')
      WITH (
        noGilingan varchar(50) '$.noGilingan',
        berat      decimal(18,3) '$.berat'
      )
      GROUP BY noGilingan
    )
    UPDATE g
    SET 
      g.IsPartial = 1,
      g.DateUsage = CASE 
        WHEN (g.Berat - ISNULL(ep.TotalBeratPartialExisting, 0) - ISNULL(np.TotalBeratPartialNew, 0)) <= 0 
        THEN @tglProduksi 
        ELSE g.DateUsage 
      END
    FROM dbo.Gilingan g
    LEFT JOIN existingPartials ep ON ep.NoGilingan = g.NoGilingan
    INNER JOIN newPartials np ON np.noGilingan = g.NoGilingan;
  END;

  -- Release the applock
  EXEC sp_releaseapplock @Resource = 'SEQ_WASHING_PARTIALS', @DbPrincipal = 'public';

  -- Summaries
  SELECT 'bbPartialNew'       AS Section, COUNT(*) AS Created FROM @bbNew
  UNION ALL
  SELECT 'gilinganPartialNew' AS Section, COUNT(*) FROM @gilNew;

  -- Return generated codes as separate recordsets (for UI)
  SELECT NoBBPartial        FROM @bbNew;        -- recordsets[1]
  SELECT NoGilinganPartial  FROM @gilNew;       -- recordsets[2]
  `;

  const rs = await req.query(SQL_PARTIALS);

  // Recordset[0]: summary rows
  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  // Recordsets[1..2]: codes
  const createdLists = {
    bbPartialNew:       (rs.recordsets?.[1] || []).map((r) => r.NoBBPartial),
    gilinganPartialNew: (rs.recordsets?.[2] || []).map((r) => r.NoGilinganPartial),
  };

  return { summary, createdLists };
}

async function _insertInputsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_ATTACH = `
  SET NOCOUNT ON;

  -- Get TglProduksi from header
  DECLARE @tglProduksi datetime;
  SELECT @tglProduksi = TglProduksi 
  FROM dbo.WashingProduksi_h WITH (NOLOCK)
  WHERE NoProduksi = @no;

  DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);

  -- BAHAN BAKU
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
    WHERE EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak)
  )
  INSERT INTO dbo.WashingProduksiInput (NoProduksi, NoBahanBaku, NoPallet, NoSak)
  SELECT @no, v.noBahanBaku, v.noPallet, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.WashingProduksiInput x 
    WHERE x.NoProduksi=@no AND x.NoBahanBaku=v.noBahanBaku AND x.NoPallet=v.noPallet AND x.NoSak=v.noSak
  );

  SET @bbInserted = @@ROWCOUNT;

  -- Update DateUsage for BahanBaku_d
  IF @bbInserted > 0
  BEGIN
    UPDATE bb
    SET bb.DateUsage = @tglProduksi
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
  WHERE EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak)
    AND EXISTS (SELECT 1 FROM dbo.WashingProduksiInput x WHERE x.NoProduksi=@no AND x.NoBahanBaku=j.noBahanBaku AND x.NoPallet=j.noPallet AND x.NoSak=j.noSak);

  SELECT @bbInvalid = COUNT(*) FROM (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'bb', @bbInserted, @bbSkipped, @bbInvalid;

  -- WASHING
  DECLARE @washingInserted int = 0;
  DECLARE @washingSkipped int = 0;
  DECLARE @washingInvalid int = 0;

  ;WITH j AS (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK) WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak)
  )
  INSERT INTO dbo.WashingProduksiInputWashing (NoProduksi, NoWashing, NoSak)
  SELECT @no, v.noWashing, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.WashingProduksiInputWashing x 
    WHERE x.NoProduksi=@no AND x.NoWashing=v.noWashing AND x.NoSak=v.noSak
  );

  SET @washingInserted = @@ROWCOUNT;

  -- Update DateUsage for Washing_d
  IF @washingInserted > 0
  BEGIN
    UPDATE w
    SET w.DateUsage = @tglProduksi
    FROM dbo.Washing_d w
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.washing')
      WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' ) src
      WHERE w.NoWashing = src.noWashing AND w.NoSak = src.noSak
    );
  END;

  SELECT @washingSkipped = COUNT(*) FROM (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK) WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak)
    AND EXISTS (SELECT 1 FROM dbo.WashingProduksiInputWashing x WHERE x.NoProduksi=@no AND x.NoWashing=j.noWashing AND x.NoSak=j.noSak);

  SELECT @washingInvalid = COUNT(*) FROM (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK) WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'washing', @washingInserted, @washingSkipped, @washingInvalid;

  -- GILINGAN
  DECLARE @gilinganInserted int = 0;
  DECLARE @gilinganSkipped int = 0;
  DECLARE @gilinganInvalid int = 0;

  ;WITH j AS (
    SELECT noGilingan
    FROM OPENJSON(@jsInputs, '$.gilingan') WITH ( noGilingan varchar(50) '$.noGilingan' )
  ),
  v AS (
    SELECT j.* FROM j WHERE EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan)
  )
  INSERT INTO dbo.WashingProduksiInputGilingan (NoProduksi, NoGilingan)
  SELECT @no, v.noGilingan
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.WashingProduksiInputGilingan x WHERE x.NoProduksi=@no AND x.NoGilingan=v.noGilingan
  );

  SET @gilinganInserted = @@ROWCOUNT;

  -- Update DateUsage for Gilingan
  IF @gilinganInserted > 0
  BEGIN
    UPDATE g
    SET g.DateUsage = @tglProduksi
    FROM dbo.Gilingan g
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.gilingan')
      WITH ( noGilingan varchar(50) '$.noGilingan' ) src
      WHERE g.NoGilingan = src.noGilingan
    );
  END;

  SELECT @gilinganSkipped = COUNT(*) FROM (
    SELECT noGilingan
    FROM OPENJSON(@jsInputs, '$.gilingan') WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan)
    AND EXISTS (SELECT 1 FROM dbo.WashingProduksiInputGilingan x WHERE x.NoProduksi=@no AND x.NoGilingan=j.noGilingan);

  SELECT @gilinganInvalid = COUNT(*) FROM (
    SELECT noGilingan
    FROM OPENJSON(@jsInputs, '$.gilingan') WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan);

  INSERT INTO @out SELECT 'gilingan', @gilinganInserted, @gilinganSkipped, @gilinganInvalid;

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

async function deleteInputsAndPartials(noProduksi, payload) {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    bb: norm(payload.bb),
    washing: norm(payload.washing),
    gilingan: norm(payload.gilingan),

    bbPartial: norm(payload.bbPartial),
    gilinganPartial: norm(payload.gilinganPartial),
  };

  try {
    await tx.begin();

    // 1) Delete partials mappings
    const partialsResult = await _deletePartialsWithTx(tx, noProduksi, {
      bbPartial: body.bbPartial,
      gilinganPartial: body.gilinganPartial,
    });

    // 2) Delete inputs mappings
    const inputsResult = await _deleteInputsWithTx(tx, noProduksi, {
      bb: body.bb,
      washing: body.washing,
      gilingan: body.gilingan,
    });

    await tx.commit();

    // Calculate totals
    const totalDeleted = Object.values(inputsResult).reduce((sum, item) => sum + (item.deleted || 0), 0);
    const totalNotFound = Object.values(inputsResult).reduce((sum, item) => sum + (item.notFound || 0), 0);
    const totalPartialsDeleted = Object.values(partialsResult.summary).reduce((sum, item) => sum + (item.deleted || 0), 0);
    const totalPartialsNotFound = Object.values(partialsResult.summary).reduce((sum, item) => sum + (item.notFound || 0), 0);

    const hasNotFound = totalNotFound > 0 || totalPartialsNotFound > 0;
    const hasNoSuccess = totalDeleted === 0 && totalPartialsDeleted === 0;

    const response = {
      noProduksi,
      summary: {
        totalDeleted,
        totalNotFound,
        totalPartialsDeleted,
        totalPartialsNotFound
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
    try {
      await tx.rollback();
    } catch {}
    throw err;
  }
}

// Helper to build delete input details
function _buildDeleteInputDetails(results, requestBody) {
  const details = [];
  const sections = [
    { key: 'bb', label: 'Bahan Baku' },
    { key: 'washing', label: 'Washing' },
    { key: 'gilingan', label: 'Gilingan' },
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

// Helper to build delete partial details
function _buildDeletePartialDetails(partialsResult, requestBody) {
  const details = [];
  const sections = [
    { key: 'bbPartial', label: 'Bahan Baku Partial' },
    { key: 'gilinganPartial', label: 'Gilingan Partial' },
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

// Delete partials with transaction
async function _deletePartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE_PARTIALS = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  -- BB PARTIAL
  DECLARE @bbDeleted int = 0, @bbNotFound int = 0;
  
  SELECT @bbDeleted = COUNT(*)
  FROM dbo.WashingProduksiInputBBPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON map.NoBBPartial = j.noBBPartial
  WHERE map.NoProduksi = @no;
  
  DECLARE @deletedBBPartials TABLE (
    NoBahanBaku varchar(50),
    NoPallet int,
    NoSak int
  );
  
  INSERT INTO @deletedBBPartials (NoBahanBaku, NoPallet, NoSak)
  SELECT DISTINCT bp.NoBahanBaku, bp.NoPallet, bp.NoSak
  FROM dbo.BahanBakuPartial bp
  INNER JOIN dbo.WashingProduksiInputBBPartial map ON bp.NoBBPartial = map.NoBBPartial
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON map.NoBBPartial = j.noBBPartial
  WHERE map.NoProduksi = @no;
  
  DELETE map
  FROM dbo.WashingProduksiInputBBPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.bbPartial') 
  WITH (noBBPartial varchar(50) '$.noBBPartial') j
  ON map.NoBBPartial = j.noBBPartial
  WHERE map.NoProduksi = @no;
  
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

  -- GILINGAN PARTIAL
  DECLARE @gilinganDeleted int = 0, @gilinganNotFound int = 0;
  
  SELECT @gilinganDeleted = COUNT(*)
  FROM dbo.WashingProduksiInputGilinganPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.gilinganPartial') 
  WITH (noGilinganPartial varchar(50) '$.noGilinganPartial') j
  ON map.NoGilinganPartial = j.noGilinganPartial
  WHERE map.NoProduksi = @no;
  
  DECLARE @deletedGilinganPartials TABLE (
    NoGilingan varchar(50)
  );
  
  INSERT INTO @deletedGilinganPartials (NoGilingan)
  SELECT DISTINCT gp.NoGilingan
  FROM dbo.GilinganPartial gp
  INNER JOIN dbo.WashingProduksiInputGilinganPartial map ON gp.NoGilinganPartial = map.NoGilinganPartial
  INNER JOIN OPENJSON(@jsPartials, '$.gilinganPartial') 
  WITH (noGilinganPartial varchar(50) '$.noGilinganPartial') j
  ON map.NoGilinganPartial = j.noGilinganPartial
  WHERE map.NoProduksi = @no;
  
  DELETE map
  FROM dbo.WashingProduksiInputGilinganPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.gilinganPartial') 
  WITH (noGilinganPartial varchar(50) '$.noGilinganPartial') j
  ON map.NoGilinganPartial = j.noGilinganPartial
  WHERE map.NoProduksi = @no;
  
  DELETE gp
  FROM dbo.GilinganPartial gp
  INNER JOIN OPENJSON(@jsPartials, '$.gilinganPartial') 
  WITH (noGilinganPartial varchar(50) '$.noGilinganPartial') j
  ON gp.NoGilinganPartial = j.noGilinganPartial;
  
  IF @gilinganDeleted > 0
  BEGIN
    UPDATE g
    SET 
      g.DateUsage = NULL,
      g.IsPartial = 1
    FROM dbo.Gilingan g
    INNER JOIN @deletedGilinganPartials del ON g.NoGilingan = del.NoGilingan
    WHERE EXISTS (
      SELECT 1 
      FROM dbo.GilinganPartial gp 
      WHERE gp.NoGilingan = g.NoGilingan
    );
    
    UPDATE g
    SET 
      g.DateUsage = NULL,
      g.IsPartial = 0
    FROM dbo.Gilingan g
    INNER JOIN @deletedGilinganPartials del ON g.NoGilingan = del.NoGilingan
    WHERE NOT EXISTS (
      SELECT 1 
      FROM dbo.GilinganPartial gp 
      WHERE gp.NoGilingan = g.NoGilingan
    );
  END;
  
  DECLARE @gilinganRequested int;
  SELECT @gilinganRequested = COUNT(*)
  FROM OPENJSON(@jsPartials, '$.gilinganPartial');
  
  SET @gilinganNotFound = @gilinganRequested - @gilinganDeleted;
  
  INSERT INTO @out SELECT 'gilinganPartial', @gilinganDeleted, @gilinganNotFound;

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

// Delete inputs with transaction
async function _deleteInputsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE_INPUTS = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  -- BAHAN BAKU (BB)
  DECLARE @bbDeleted int = 0, @bbNotFound int = 0;
  
  SELECT @bbDeleted = COUNT(*)
  FROM dbo.WashingProduksiInput map
  INNER JOIN OPENJSON(@jsInputs, '$.bb') 
  WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
  ON map.NoBahanBaku = j.noBahanBaku AND map.NoPallet = j.noPallet AND map.NoSak = j.noSak
  WHERE map.NoProduksi = @no;
  
  -- Reset DateUsage sebelum DELETE
  IF @bbDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage = NULL
    FROM dbo.BahanBaku_d d
    INNER JOIN dbo.WashingProduksiInput map 
      ON d.NoBahanBaku = map.NoBahanBaku AND d.NoPallet = map.NoPallet AND d.NoSak = map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.bb') 
    WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
    ON map.NoBahanBaku = j.noBahanBaku AND map.NoPallet = j.noPallet AND map.NoSak = j.noSak
    WHERE map.NoProduksi = @no;
  END;
  
  DELETE map
  FROM dbo.WashingProduksiInput map
  INNER JOIN OPENJSON(@jsInputs, '$.bb') 
  WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
  ON map.NoBahanBaku = j.noBahanBaku AND map.NoPallet = j.noPallet AND map.NoSak = j.noSak
  WHERE map.NoProduksi = @no;
  
  DECLARE @bbRequested int;
  SELECT @bbRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.bb');
  
  SET @bbNotFound = @bbRequested - @bbDeleted;
  
  INSERT INTO @out SELECT 'bb', @bbDeleted, @bbNotFound;

  -- WASHING
  DECLARE @washingDeleted int = 0, @washingNotFound int = 0;
  
  SELECT @washingDeleted = COUNT(*)
  FROM dbo.WashingProduksiInputWashing map
  INNER JOIN OPENJSON(@jsInputs, '$.washing') 
  WITH (noWashing varchar(50) '$.noWashing', noSak int '$.noSak') j
  ON map.NoWashing = j.noWashing AND map.NoSak = j.noSak
  WHERE map.NoProduksi = @no;
  
  -- Reset DateUsage sebelum DELETE
  IF @washingDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage = NULL
    FROM dbo.Washing_d d
    INNER JOIN dbo.WashingProduksiInputWashing map ON d.NoWashing = map.NoWashing AND d.NoSak = map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.washing') 
    WITH (noWashing varchar(50) '$.noWashing', noSak int '$.noSak') j
    ON map.NoWashing = j.noWashing AND map.NoSak = j.noSak
    WHERE map.NoProduksi = @no;
  END;
  
  DELETE map
  FROM dbo.WashingProduksiInputWashing map
  INNER JOIN OPENJSON(@jsInputs, '$.washing') 
  WITH (noWashing varchar(50) '$.noWashing', noSak int '$.noSak') j
  ON map.NoWashing = j.noWashing AND map.NoSak = j.noSak
  WHERE map.NoProduksi = @no;
  
  DECLARE @washingRequested int;
  SELECT @washingRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.washing');
  
  SET @washingNotFound = @washingRequested - @washingDeleted;
  
  INSERT INTO @out SELECT 'washing', @washingDeleted, @washingNotFound;

  -- GILINGAN
  DECLARE @gilinganDeleted int = 0, @gilinganNotFound int = 0;
  
  SELECT @gilinganDeleted = COUNT(*)
  FROM dbo.WashingProduksiInputGilingan map
  INNER JOIN OPENJSON(@jsInputs, '$.gilingan') 
  WITH (noGilingan varchar(50) '$.noGilingan') j
  ON map.NoGilingan = j.noGilingan
  WHERE map.NoProduksi = @no;
  
  -- Reset DateUsage sebelum DELETE
  IF @gilinganDeleted > 0
  BEGIN
    UPDATE g
    SET g.DateUsage = NULL
    FROM dbo.Gilingan g
    INNER JOIN dbo.WashingProduksiInputGilingan map ON g.NoGilingan = map.NoGilingan
    INNER JOIN OPENJSON(@jsInputs, '$.gilingan') 
    WITH (noGilingan varchar(50) '$.noGilingan') j
    ON map.NoGilingan = j.noGilingan
    WHERE map.NoProduksi = @no;
  END;
  
  DELETE map
  FROM dbo.WashingProduksiInputGilingan map
  INNER JOIN OPENJSON(@jsInputs, '$.gilingan') 
  WITH (noGilingan varchar(50) '$.noGilingan') j
  ON map.NoGilingan = j.noGilingan
  WHERE map.NoProduksi = @no;
  
  DECLARE @gilinganRequested int;
  SELECT @gilinganRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.gilingan');
  
  SET @gilinganNotFound = @gilinganRequested - @gilinganDeleted;
  
  INSERT INTO @out SELECT 'gilingan', @gilinganDeleted, @gilinganNotFound;

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


module.exports = { getProduksiByDate, getAllProduksi, createWashingProduksi, updateWashingProduksi, deleteWashingProduksi, fetchInputs, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials };   // ⬅️ pastikan ini ada

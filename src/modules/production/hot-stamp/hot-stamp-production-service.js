// services/hotstamping-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
  loadDocDateOnlyFromConfig
} = require('../../../core/shared/tutup-transaksi-guard');

async function getProduksiByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      h.NoProduksi,
      h.Tanggal,
      h.IdMesin,
      m.NamaMesin,
      h.IdOperator,
      o.NamaOperator,       -- sesuaikan kalau nama kolom beda
      h.Shift,
      h.JamKerja,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter
    FROM [dbo].[HotStamping_h] h
    LEFT JOIN [dbo].[MstMesin] m
      ON h.IdMesin = m.IdMesin
    LEFT JOIN [dbo].[MstOperator] o
      ON h.IdOperator = o.IdOperator
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.JamKerja ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}


// ✅ GET ALL (paged + search + lastClosed + isLocked)
async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
  `;

  // 1) Count
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.HotStamping_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);

  const countRes = await countReq.query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;

  if (total === 0) return { data: [], total: 0 };

  // 2) Data + LastClosedDate + IsLocked
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
      h.Tanggal      AS TglProduksi,
      h.IdMesin,
      ms.NamaMesin,
      h.IdOperator,
      op.NamaOperator,
      h.JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd,

      lc.LastClosedDate AS LastClosedDate,

      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.HotStamping_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    ORDER BY h.Tanggal DESC, h.JamKerja ASC, h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

function badReq(msg) {
  const e = new Error(msg);
  e.statusCode = 400;
  return e;
}

/**
 * Generate next NoProduksi HotStamping
 * Contoh: BH.0000000010  (10 digit angka)
 */
async function generateNextNoProduksi(tx, { prefix = 'BH.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoProduksi
    FROM dbo.HotStamping_h AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE h.NoProduksi LIKE @prefix + '%'
    ORDER BY
      TRY_CONVERT(BIGINT, SUBSTRING(h.NoProduksi, LEN(@prefix) + 1, 50)) DESC,
      h.NoProduksi DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoProduksi;
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }

  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

/**
 * Sama konsep dengan gilingan:
 * jamKerja bisa:
 *  - number (8)
 *  - "HH:mm-HH:mm" => selisih jam
 *  - "HH:mm" => ambil jam-nya
 */
function parseJamToInt(jam) {
  if (jam == null) throw badReq('Format jamKerja tidak valid');
  if (typeof jam === 'number') return Math.max(0, Math.round(jam));

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

  throw badReq('Format jamKerja tidak valid. Gunakan angka (mis. 8) atau "HH:mm-HH:mm"');
}

// optional: kalau jamKerja kosong, hitung dari hourStart-hourEnd (lebih konsisten)
function calcJamKerjaFromStartEnd(hourStart, hourEnd) {
  if (!hourStart || !hourEnd) return null;

  // terima 'HH:mm' atau 'HH:mm:ss'
  const norm = (s) => {
    const t = String(s).trim();
    if (/^\d{1,2}:\d{2}$/.test(t)) return `${t}:00`;
    return t;
  };

  const hs = norm(hourStart);
  const he = norm(hourEnd);

  const parse = (t) => {
    const m = String(t).match(/^(\d{1,2}):(\d{2}):(\d{2})$/);
    if (!m) return null;
    const h = +m[1], min = +m[2], sec = +m[3];
    return h * 3600 + min * 60 + sec;
  };

  const s1 = parse(hs);
  const s2 = parse(he);
  if (s1 == null || s2 == null) return null;

  let diff = s2 - s1;
  if (diff < 0) diff += 24 * 3600; // cross-midnight
  const hours = diff / 3600;
  return Math.max(0, Math.round(hours));
}

/**
 * CREATE header HotStamping_h
 * payload:
 *  tglProduksi, idMesin, idOperator, shift,
 *  jamKerja?, hourStart, hourEnd, hourMeter, createBy, checkBy*, approveBy*
 */
async function createHotStampingProduksi(payload) {
  const must = [];
  if (!payload?.tglProduksi) must.push('tglProduksi');
  if (payload?.idMesin == null) must.push('idMesin');
  if (payload?.idOperator == null) must.push('idOperator');
  if (payload?.shift == null) must.push('shift');
  if (!payload?.hourStart) must.push('hourStart');
  if (!payload?.hourEnd) must.push('hourEnd');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) NORMALIZE DATE + TUTUP TRANSAKSI GUARD
    // -------------------------------------------------------
    const effectiveDate = resolveEffectiveDateForCreate(payload.tglProduksi);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create HotStamping',
      useLock: true,
    });

    // -------------------------------------------------------
    // 1) GENERATE NO PRODUKSI (BH.) + ANTI RACE
    // -------------------------------------------------------
    const no1 = await generateNextNoProduksi(tx, { prefix: 'BH.', width: 10 });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.HotStamping_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksi(tx, { prefix: 'BH.', width: 10 })
      : no1;

    // -------------------------------------------------------
    // 2) JAM KERJA
    //    - kalau payload.jamKerja diisi, parse
    //    - kalau kosong, hitung dari hourStart-hourEnd
    // -------------------------------------------------------
    let jamKerjaInt = null;
    if (payload.jamKerja !== null && payload.jamKerja !== undefined && payload.jamKerja !== '') {
      jamKerjaInt = parseJamToInt(payload.jamKerja);
    } else {
      jamKerjaInt = calcJamKerjaFromStartEnd(payload.hourStart, payload.hourEnd);
    }

    // -------------------------------------------------------
    // 3) INSERT HEADER
    // -------------------------------------------------------
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .input('Tanggal', sql.Date, effectiveDate)
      .input('IdMesin', sql.Int, payload.idMesin)
      .input('IdOperator', sql.Int, payload.idOperator)
      .input('Shift', sql.Int, payload.shift)
      .input('JamKerja', sql.Int, jamKerjaInt)
      .input('CreateBy', sql.VarChar(100), payload.createBy)
      .input('CheckBy1', sql.VarChar(100), payload.checkBy1 ?? null)
      .input('CheckBy2', sql.VarChar(100), payload.checkBy2 ?? null)
      .input('ApproveBy', sql.VarChar(100), payload.approveBy ?? null)
      .input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null)
      .input('HourStart', sql.VarChar(20), payload.hourStart ?? null)
      .input('HourEnd', sql.VarChar(20), payload.hourEnd ?? null);

    const insertSql = `
      INSERT INTO dbo.HotStamping_h (
        NoProduksi, Tanggal, IdMesin, IdOperator, Shift, JamKerja,
        CreateBy, CheckBy1, CheckBy2, ApproveBy, HourMeter,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi, @Tanggal, @IdMesin, @IdOperator, @Shift,
        @JamKerja,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @HourMeter,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd   IS NULL OR LTRIM(RTRIM(@HourEnd))   = '' THEN NULL ELSE CAST(@HourEnd   AS time(7)) END
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
 * UPDATE header HotStamping_h
 * - Wajib ada noProduksi
 * - Field update bersifat dinamis (yang dikirim saja)
 * - Jika Tanggal berubah -> sync DateUsage furniture wip input
 */
async function updateHotStampingProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'hotStampingProduksi', // pastikan sesuai config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // UPDATE = write action
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
      action: 'update HotStamping (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update HotStamping (new date)',
        useLock: true,
      });
    }

    // -------------------------------------------------------
    // 3) BUILD SET DINAMIS
    // -------------------------------------------------------
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

    // HotStamping pakai JamKerja
    if (payload.jamKerja !== undefined) {
      const jamKerjaInt = payload.jamKerja === null ? null : parseJamToInt(payload.jamKerja);
      sets.push('JamKerja = @JamKerja');
      rqUpd.input('JamKerja', sql.Int, jamKerjaInt);
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

    const updateSql = `
      UPDATE dbo.HotStamping_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.HotStamping_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // -------------------------------------------------------
    // 4) Jika Tanggal berubah → sinkron DateUsage full + partial
    //    (pakai tanggal hasil DB agar konsisten)
    // -------------------------------------------------------
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('Tanggal', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- FURNITURE WIP (FULL)
        -------------------------------------------------------
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.HotStampingInputLabelFWIP AS map
            WHERE map.NoProduksi     = @NoProduksi
              AND map.NoFurnitureWIP = fw.NoFurnitureWIP
          );

        -------------------------------------------------------
        -- FURNITURE WIP (PARTIAL)
        -- FurnitureWIPPartial tidak punya DateUsage, jadi update ke FurnitureWIP via join
        -------------------------------------------------------
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.HotStampingInputLabelFWIPPartial AS mp
            JOIN dbo.FurnitureWIPPartial AS fwp
              ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
            WHERE mp.NoProduksi = @NoProduksi
              AND fwp.NoFurnitureWIP = fw.NoFurnitureWIP
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


async function deleteHotStampingProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'hotStampingProduksi', // pastikan ada di config tutup-transaksi
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
      action: 'delete HotStamping',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) CEK OUTPUT (FWIP / REJECT). Jika ada → tolak delete
    // -------------------------------------------------------
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT
          SUM(CASE WHEN Src = 'FWIP'   THEN Cnt ELSE 0 END) AS CntOutputFWIP,
          SUM(CASE WHEN Src = 'REJECT' THEN Cnt ELSE 0 END) AS CntOutputReject
        FROM (
          SELECT 'FWIP' AS Src, COUNT(1) AS Cnt
          FROM dbo.HotStampingOutputLabelFWIP WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL

          SELECT 'REJECT' AS Src, COUNT(1) AS Cnt
          FROM dbo.HotStampingOutputRejectV2 WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
        ) X;
      `);

    const row = outRes.recordset?.[0] || { CntOutputFWIP: 0, CntOutputReject: 0 };
    const hasOutputFWIP = (row.CntOutputFWIP || 0) > 0;
    const hasOutputReject = (row.CntOutputReject || 0) > 0;

    if (hasOutputFWIP || hasOutputReject) {
      throw badReq(
        'Tidak dapat menghapus Nomor Produksi ini karena sudah memiliki data output.'
      );
    }

    // -------------------------------------------------------
    // 3) LANJUT DELETE INPUT (LABEL + MATERIAL) + RESET DATEUSAGE
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- SIMPAN KEY FURNITURE WIP YANG TERDAMPAK (FULL/PARTIAL)
      ---------------------------------------------------------
      DECLARE @FWIPKeys TABLE (
        NoFurnitureWIP varchar(50) PRIMARY KEY
      );

      ---------------------------------------------------------
      -- A) KUMPULKAN KEY dari FULL mapping
      ---------------------------------------------------------
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT map.NoFurnitureWIP
      FROM dbo.HotStampingInputLabelFWIP AS map
      WHERE map.NoProduksi = @NoProduksi
        AND map.NoFurnitureWIP IS NOT NULL;

      ---------------------------------------------------------
      -- B) KUMPULKAN KEY dari PARTIAL mapping
      --    HotStampingInputLabelFWIPPartial -> FurnitureWIPPartial -> NoFurnitureWIP
      ---------------------------------------------------------
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.HotStampingInputLabelFWIPPartial AS mp
      JOIN dbo.FurnitureWIPPartial AS fwp
        ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND fwp.NoFurnitureWIP IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM @FWIPKeys k WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP
        );

      ---------------------------------------------------------
      -- C) HAPUS INPUT MATERIAL
      ---------------------------------------------------------
      DELETE FROM dbo.HotStampingInputMaterial
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- D) HAPUS ROW PARTIAL yang dipakai oleh produksi ini
      ---------------------------------------------------------
      DELETE fwp
      FROM dbo.FurnitureWIPPartial AS fwp
      JOIN dbo.HotStampingInputLabelFWIPPartial AS mp
        ON mp.NoFurnitureWIPPartial = fwp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- E) HAPUS MAPPING PARTIAL & FULL
      ---------------------------------------------------------
      DELETE FROM dbo.HotStampingInputLabelFWIPPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.HotStampingInputLabelFWIP
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- F) RESET DATEUSAGE + RECALC IsPartial di FurnitureWIP
      ---------------------------------------------------------
      UPDATE fw
      SET fw.DateUsage = NULL,
          fw.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.FurnitureWIPPartial p
              WHERE p.NoFurnitureWIP = fw.NoFurnitureWIP
            )
            THEN 1 ELSE 0 END
      FROM dbo.FurnitureWIP AS fw
      JOIN @FWIPKeys AS k
        ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      ---------------------------------------------------------
      -- G) TERAKHIR: HAPUS HEADER
      ---------------------------------------------------------
      DELETE FROM dbo.HotStamping_h
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


async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noProduksi);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */
    
    -- Full FurnitureWIP
    SELECT
      'fwip' AS Src,
      map.NoProduksi,
      map.NoFurnitureWIP       AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      
      fw.Berat,
      fw.Pcs,
      fw.IsPartial,
      fw.IDFurnitureWIP        AS IdJenis,
      CAST(NULL AS varchar(255)) AS NamaJenis,
      CAST(NULL AS varchar(50)) AS NamaUOM
    FROM dbo.HotStampingInputLabelFWIP map WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = map.NoFurnitureWIP
    WHERE map.NoProduksi = @no

    UNION ALL

    -- Material
    SELECT
      'material' AS Src,
      im.NoProduksi,
      CAST(im.IdCabinetMaterial AS varchar(50)) AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      
      CAST(NULL AS decimal(18,3)) AS Berat,
      CAST(im.Jumlah AS int) AS Pcs,
      CAST(NULL AS bit) AS IsPartial,
      CAST(NULL AS int) AS IdJenis,
      mm.Nama AS NamaJenis,
      uom.NamaUOM
    FROM dbo.HotStampingInputMaterial im WITH (NOLOCK)
    LEFT JOIN dbo.MstCabinetMaterial mm WITH (NOLOCK)
      ON mm.IdCabinetMaterial = im.IdCabinetMaterial
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mm.IdUOM
    WHERE im.NoProduksi = @no

    ORDER BY Ref1 DESC, Ref2 ASC;


    /* ===================== [2] PARTIALS ===================== */
    
    -- FurnitureWIP Partial
    SELECT
      mp.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs,
      fw.Berat,
      fw.IDFurnitureWIP AS IdJenis,
      CAST(NULL AS varchar(255)) AS NamaJenis
    FROM dbo.HotStampingInputLabelFWIPPartial mp WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
      ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP
    WHERE mp.NoProduksi = @no
    ORDER BY mp.NoFurnitureWIPPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const fwipPart = rs.recordsets?.[1] || [];

  const out = {
    furnitureWip: [],
    cabinetMaterial: [],
    summary: {
      furnitureWip: 0,
      cabinetMaterial: 0,
    },
  };

  // ========== MAIN ROWS ==========
  for (const r of mainRows) {
    switch (r.Src) {
      case 'fwip':
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          berat: r.Berat ?? null,
          pcs: r.Pcs ?? null,
          isPartial: r.IsPartial ?? null,
          idJenis: r.IdJenis ?? null,
          namaJenis: r.NamaJenis ?? null,
        });
        break;

      case 'material':
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1,
          pcs: r.Pcs ?? null,
          namaJenis: r.NamaJenis ?? null, // nama material
          namaUom: r.NamaUOM ?? null,      // nama UOM untuk display
        });
        break;
    }
  }

  // ========== PARTIAL ROWS ==========
  for (const p of fwipPart) {
    out.furnitureWip.push({
      noFurnitureWipPartial: p.NoFurnitureWIPPartial,
      noFurnitureWip: p.NoFurnitureWIP ?? null,
      pcs: p.Pcs ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // ========== SUMMARY ==========
  out.summary.furnitureWip = out.furnitureWip.length;
  out.summary.cabinetMaterial = out.cabinetMaterial.length;

  return out;
}


async function validateFwipLabel(labelCode) {
  const pool = await poolPromise;
  const raw = String(labelCode || '').trim();
  if (!raw) throw new Error('Label code is required');

  /* =========================================================
   * 1) FULL : FurnitureWIP.NoFurnitureWIP
   *    - valid jika DateUsage IS NULL
   * ========================================================= */
  {
    const req = pool.request();
    req.input('code', sql.VarChar(50), raw);

    const q = `
      SELECT
        fw.NoFurnitureWIP,
        fw.DateCreate,
        fw.Jam,
        fw.Pcs,
        fw.IDFurnitureWIP,
        fw.Berat,
        fw.IsPartial,
        fw.DateUsage,
        fw.IdWarehouse,
        fw.IdWarna,
        fw.CreateBy,
        fw.DateTimeCreate,
        fw.Blok,
        fw.IdLokasi
      FROM dbo.FurnitureWIP fw WITH (NOLOCK)
      WHERE fw.NoFurnitureWIP = @code
        AND fw.DateUsage IS NULL;
    `;

    const rs = await req.query(q);
    const rows = rs.recordset || [];

    if (rows.length > 0) {
      return {
        found: true,
        count: rows.length,
        tableName: 'FurnitureWIP',
        data: rows,
      };
    }
  }

  /* =========================================================
   * 2) PARTIAL : FurnitureWIPPartial.NoFurnitureWIPPartial
   *    - header FurnitureWIP HARUS belum dipakai
   * ========================================================= */
  {
    const req = pool.request();
    req.input('code', sql.VarChar(50), raw);

    const q = `
      SELECT
        fwp.NoFurnitureWIPPartial,
        fwp.NoFurnitureWIP,
        fwp.Pcs                AS PcsPartial,

        fw.DateCreate,
        fw.Jam,
        fw.Pcs                AS PcsHeader,
        fw.IDFurnitureWIP,
        fw.Berat,
        fw.IsPartial,
        fw.DateUsage,
        fw.IdWarehouse,
        fw.IdWarna,
        fw.CreateBy,
        fw.DateTimeCreate,
        fw.Blok,
        fw.IdLokasi
      FROM dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
      JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
        ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP
      WHERE fwp.NoFurnitureWIPPartial = @code
        AND fw.DateUsage IS NULL;
    `;

    const rs = await req.query(q);
    const rows = rs.recordset || [];

    if (rows.length > 0) {
      return {
        found: true,
        count: rows.length,
        tableName: 'FurnitureWIPPartial',
        data: rows,
      };
    }
  }

  return {
    found: false,
    count: 0,
    tableName: '',
    data: [],
  };
}


async function validateCabinetMaterialStock({ id = null, itemCode = null, idWarehouse }) {
  const pool = await poolPromise;
  const req = pool.request();

  req.input('id', sql.Int, id);
  req.input('itemCode', sql.VarChar(50), itemCode);
  req.input('IdWarehouse', sql.Int, idWarehouse);

  const q = `
    DECLARE @TglAkhir date = CAST(GETDATE() AS date);

    ;WITH A AS (
      SELECT TOP 1
        m.IdCabinetMaterial,
        m.Nama,
        u.NamaUOM,
        m.TglSaldoAwal,
        m.IdUOM,
        m.Enable,
        m.ItemCode
      FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      INNER JOIN dbo.MstUOM u WITH (NOLOCK) ON u.IdUOM = m.IdUOM
      WHERE m.Enable = 1
        AND (
          (@id IS NOT NULL AND m.IdCabinetMaterial = @id)
          OR (@id IS NULL AND @itemCode IS NOT NULL AND m.ItemCode = @itemCode)
        )
    ),
    W AS (
      SELECT w.IdWarehouse, w.NamaWarehouse
      FROM dbo.MstWarehouse w WITH (NOLOCK)
      WHERE w.IdWarehouse = @IdWarehouse
    ),
    K AS (
      SELECT
        a.IdCabinetMaterial,
        w.IdWarehouse,
        w.NamaWarehouse,
        a.TglSaldoAwal,
        SUM(ISNULL(sa.SaldoAwal,0)) AS SaldoAwal
      FROM A a
      CROSS JOIN W w
      LEFT JOIN dbo.MstCabinetMaterialSaldoAwal sa WITH (NOLOCK)
        ON sa.IdCabinetMaterial = a.IdCabinetMaterial
       AND sa.IdWarehouse       = w.IdWarehouse
      GROUP BY a.IdCabinetMaterial, w.IdWarehouse, w.NamaWarehouse, a.TglSaldoAwal
    ),

    B AS (
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS PenrmnMaterl
      FROM dbo.CabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.CabinetMaterial_h h WITH (NOLOCK)
        ON h.NoCabinetMaterial = d.NoCabinetMaterial
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),

    C AS (
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS BJualMaterl
      FROM dbo.BJJualCabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.BJJual_h h WITH (NOLOCK)
        ON h.NoBJJual = d.NoBJJual
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),

    D AS (
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS ReturMaterl
      FROM dbo.BJReturCabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.BJRetur_h h WITH (NOLOCK)
        ON h.NoRetur = d.NoRetur
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),

    E AS (
      SELECT Z.IdCabinetMaterial, K.IdWarehouse, SUM(Z.CabAssblMaterl) AS CabAssblMaterl
      FROM K
      INNER JOIN (
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah,0)) AS CabAssblMaterl
        FROM dbo.HotStampingInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.HotStamping_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah,0))
        FROM dbo.PackingProduksiInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.PackingProduksi_h b WITH (NOLOCK) ON b.NoPacking = a.NoPacking
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah,0))
        FROM dbo.PasangKunciInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.PasangKunci_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah,0))
        FROM dbo.SpannerInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.Spanner_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal
      ) Z ON Z.IdCabinetMaterial = K.IdCabinetMaterial
      WHERE Z.Tanggal >= K.TglSaldoAwal AND Z.Tanggal <= @TglAkhir
      GROUP BY Z.IdCabinetMaterial, K.IdWarehouse
    ),

    F AS (
      SELECT d.IdCabinetMaterial, h.IdWhTujuan AS IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS GoodTrfIn
      FROM dbo.GoodsTransfer_d_CabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.GoodsTransfer_h h WITH (NOLOCK)
        ON h.NoGT = d.NoGT
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = h.IdWhTujuan
      WHERE h.DateCreate >= K.TglSaldoAwal AND h.DateCreate <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWhTujuan
    ),

    G AS (
      SELECT d.IdCabinetMaterial, h.IdWhAsal AS IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS GoodTrfOut
      FROM dbo.GoodsTransfer_d_CabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.GoodsTransfer_h h WITH (NOLOCK)
        ON h.NoGT = d.NoGT
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = h.IdWhAsal
      WHERE h.DateCreate >= K.TglSaldoAwal AND h.DateCreate <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWhAsal
    ),

    H AS (
      SELECT d.IdCabinetMaterial, K.IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS InjectProdMaterl
      FROM dbo.InjectProduksiInputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.InjectProduksi_h h WITH (NOLOCK)
        ON h.NoProduksi = d.NoProduksi
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
      WHERE h.TglProduksi >= K.TglSaldoAwal AND h.TglProduksi <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, K.IdWarehouse
    ),

    I AS (
      SELECT d.IdCabinetMaterial, d.IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS AdjInput
      FROM dbo.AdjustmentInputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.Adjustment_h h WITH (NOLOCK)
        ON h.NoAdjustment = d.NoAdjustment
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = d.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, d.IdWarehouse
    ),

    J AS (
      SELECT d.IdCabinetMaterial, d.IdWarehouse, SUM(ISNULL(d.Pcs,0)) AS AdjOutput
      FROM dbo.AdjustmentOutputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.Adjustment_h h WITH (NOLOCK)
        ON h.NoAdjustment = d.NoAdjustment
      INNER JOIN K
        ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse       = d.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, d.IdWarehouse
    )

    SELECT
      a.IdCabinetMaterial,
      a.Nama,
      a.NamaUOM,
      K.IdWarehouse,
      K.NamaWarehouse,
      K.TglSaldoAwal,
      K.SaldoAwal,
      B.PenrmnMaterl,
      C.BJualMaterl,
      D.ReturMaterl,
      E.CabAssblMaterl,
      F.GoodTrfIn,
      G.GoodTrfOut,
      H.InjectProdMaterl,
      I.AdjInput,
      J.AdjOutput,
      (
          ISNULL(K.SaldoAwal,0)
        + ISNULL(B.PenrmnMaterl,0)
        - ISNULL(C.BJualMaterl,0)
        + ISNULL(D.ReturMaterl,0)
        - ISNULL(E.CabAssblMaterl,0)
        + ISNULL(F.GoodTrfIn,0)
        - ISNULL(G.GoodTrfOut,0)
        - ISNULL(H.InjectProdMaterl,0)
        - ISNULL(I.AdjInput,0)
        + ISNULL(J.AdjOutput,0)
      ) AS SaldoAkhir
    FROM A a
    INNER JOIN K ON K.IdCabinetMaterial = a.IdCabinetMaterial
    LEFT JOIN B ON B.IdCabinetMaterial = a.IdCabinetMaterial AND B.IdWarehouse = K.IdWarehouse
    LEFT JOIN C ON C.IdCabinetMaterial = a.IdCabinetMaterial AND C.IdWarehouse = K.IdWarehouse
    LEFT JOIN D ON D.IdCabinetMaterial = a.IdCabinetMaterial AND D.IdWarehouse = K.IdWarehouse
    LEFT JOIN E ON E.IdCabinetMaterial = a.IdCabinetMaterial AND E.IdWarehouse = K.IdWarehouse
    LEFT JOIN F ON F.IdCabinetMaterial = a.IdCabinetMaterial AND F.IdWarehouse = K.IdWarehouse
    LEFT JOIN G ON G.IdCabinetMaterial = a.IdCabinetMaterial AND G.IdWarehouse = K.IdWarehouse
    LEFT JOIN H ON H.IdCabinetMaterial = a.IdCabinetMaterial AND H.IdWarehouse = K.IdWarehouse
    LEFT JOIN I ON I.IdCabinetMaterial = a.IdCabinetMaterial AND I.IdWarehouse = K.IdWarehouse
    LEFT JOIN J ON J.IdCabinetMaterial = a.IdCabinetMaterial AND J.IdWarehouse = K.IdWarehouse;
  `;

  const rs = await req.query(q);
  const rows = rs.recordset || [];

  if (!rows.length) return { found: false, count: 0, tableName: 'CabinetMaterial', data: [] };
  return { found: true, count: rows.length, tableName: 'CabinetMaterial', data: rows };
}


/**
 * Get all master cabinet materials with stock info
 * @param {number} idWarehouse - Warehouse ID for stock calculation
 * @returns {Promise<{found: boolean, count: number, data: Array}>}
 */
async function getMasterCabinetMaterials({ idWarehouse }) {
  const pool = await poolPromise;
  const req = pool.request();

  req.input('IdWarehouse', sql.Int, idWarehouse);

  const query = `
    DECLARE @TglAkhir date = CAST(GETDATE() AS date);

    ;WITH A AS (
      -- Master Cabinet Material
      SELECT 
        m.IdCabinetMaterial,
        m.Nama,
        m.ItemCode,
        m.TglSaldoAwal,
        m.IdUOM,
        m.Enable,
        u.NamaUOM
      FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      INNER JOIN dbo.MstUOM u WITH (NOLOCK) ON u.IdUOM = m.IdUOM
      WHERE m.Enable = 1
    ),
    W AS (
      -- Warehouse info
      SELECT w.IdWarehouse, w.NamaWarehouse
      FROM dbo.MstWarehouse w WITH (NOLOCK)
      WHERE w.IdWarehouse = @IdWarehouse
    ),
    K AS (
      -- Saldo Awal per material
      SELECT
        a.IdCabinetMaterial,
        w.IdWarehouse,
        w.NamaWarehouse,
        a.TglSaldoAwal,
        SUM(ISNULL(sa.SaldoAwal, 0)) AS SaldoAwal
      FROM A a
      CROSS JOIN W w
      LEFT JOIN dbo.MstCabinetMaterialSaldoAwal sa WITH (NOLOCK)
        ON sa.IdCabinetMaterial = a.IdCabinetMaterial
       AND sa.IdWarehouse = w.IdWarehouse
      GROUP BY a.IdCabinetMaterial, w.IdWarehouse, w.NamaWarehouse, a.TglSaldoAwal
    ),
    B AS (
      -- Penerimaan Material
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS PenrmnMaterl
      FROM dbo.CabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.CabinetMaterial_h h WITH (NOLOCK)
        ON h.NoCabinetMaterial = d.NoCabinetMaterial
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),
    C AS (
      -- Barang Jual Material
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS BJualMaterl
      FROM dbo.BJJualCabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.BJJual_h h WITH (NOLOCK)
        ON h.NoBJJual = d.NoBJJual
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),
    D AS (
      -- Retur Material
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS ReturMaterl
      FROM dbo.BJReturCabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.BJRetur_h h WITH (NOLOCK)
        ON h.NoRetur = d.NoRetur
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),
    E AS (
      -- Cabinet Assembly Material (HotStamp, Packing, PasangKunci, Spanner)
      SELECT Z.IdCabinetMaterial, K.IdWarehouse, SUM(Z.CabAssblMaterl) AS CabAssblMaterl
      FROM K
      INNER JOIN (
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0)) AS CabAssblMaterl
        FROM dbo.HotStampingInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.HotStamping_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0))
        FROM dbo.PackingProduksiInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.PackingProduksi_h b WITH (NOLOCK) ON b.NoPacking = a.NoPacking
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0))
        FROM dbo.PasangKunciInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.PasangKunci_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0))
        FROM dbo.SpannerInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.Spanner_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal
      ) Z ON Z.IdCabinetMaterial = K.IdCabinetMaterial
      WHERE Z.Tanggal >= K.TglSaldoAwal AND Z.Tanggal <= @TglAkhir
      GROUP BY Z.IdCabinetMaterial, K.IdWarehouse
    ),
    F AS (
      -- Goods Transfer In
      SELECT d.IdCabinetMaterial, h.IdWhTujuan AS IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS GoodTrfIn
      FROM dbo.GoodsTransfer_d_CabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.GoodsTransfer_h h WITH (NOLOCK)
        ON h.NoGT = d.NoGT
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWhTujuan
      WHERE h.DateCreate >= K.TglSaldoAwal AND h.DateCreate <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWhTujuan
    ),
    G AS (
      -- Goods Transfer Out
      SELECT d.IdCabinetMaterial, h.IdWhAsal AS IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS GoodTrfOut
      FROM dbo.GoodsTransfer_d_CabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.GoodsTransfer_h h WITH (NOLOCK)
        ON h.NoGT = d.NoGT
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWhAsal
      WHERE h.DateCreate >= K.TglSaldoAwal AND h.DateCreate <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWhAsal
    ),
    H AS (
      -- Inject Produksi Material
      SELECT d.IdCabinetMaterial, K.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS InjectProdMaterl
      FROM dbo.InjectProduksiInputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.InjectProduksi_h h WITH (NOLOCK)
        ON h.NoProduksi = d.NoProduksi
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
      WHERE h.TglProduksi >= K.TglSaldoAwal AND h.TglProduksi <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, K.IdWarehouse
    ),
    I AS (
      -- Adjustment Input
      SELECT d.IdCabinetMaterial, d.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS AdjInput
      FROM dbo.AdjustmentInputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.Adjustment_h h WITH (NOLOCK)
        ON h.NoAdjustment = d.NoAdjustment
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = d.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, d.IdWarehouse
    ),
    J AS (
      -- Adjustment Output
      SELECT d.IdCabinetMaterial, d.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS AdjOutput
      FROM dbo.AdjustmentOutputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.Adjustment_h h WITH (NOLOCK)
        ON h.NoAdjustment = d.NoAdjustment
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = d.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, d.IdWarehouse
    )

    SELECT
      a.IdCabinetMaterial,
      a.Nama,
      a.ItemCode,
      a.NamaUOM,
      K.IdWarehouse,
      K.NamaWarehouse,
      K.TglSaldoAwal,
      ISNULL(K.SaldoAwal, 0) AS SaldoAwal,
      ISNULL(B.PenrmnMaterl, 0) AS PenrmnMaterl,
      ISNULL(C.BJualMaterl, 0) AS BJualMaterl,
      ISNULL(D.ReturMaterl, 0) AS ReturMaterl,
      ISNULL(E.CabAssblMaterl, 0) AS CabAssblMaterl,
      ISNULL(F.GoodTrfIn, 0) AS GoodTrfIn,
      ISNULL(G.GoodTrfOut, 0) AS GoodTrfOut,
      ISNULL(H.InjectProdMaterl, 0) AS InjectProdMaterl,
      ISNULL(I.AdjInput, 0) AS AdjInput,
      ISNULL(J.AdjOutput, 0) AS AdjOutput,
      (
          ISNULL(K.SaldoAwal, 0)
        + ISNULL(B.PenrmnMaterl, 0)
        - ISNULL(C.BJualMaterl, 0)
        + ISNULL(D.ReturMaterl, 0)
        - ISNULL(E.CabAssblMaterl, 0)
        + ISNULL(F.GoodTrfIn, 0)
        - ISNULL(G.GoodTrfOut, 0)
        - ISNULL(H.InjectProdMaterl, 0)
        - ISNULL(I.AdjInput, 0)
        + ISNULL(J.AdjOutput, 0)
      ) AS SaldoAkhir
    FROM A a
    INNER JOIN K ON K.IdCabinetMaterial = a.IdCabinetMaterial
    LEFT JOIN B ON B.IdCabinetMaterial = a.IdCabinetMaterial AND B.IdWarehouse = K.IdWarehouse
    LEFT JOIN C ON C.IdCabinetMaterial = a.IdCabinetMaterial AND C.IdWarehouse = K.IdWarehouse
    LEFT JOIN D ON D.IdCabinetMaterial = a.IdCabinetMaterial AND D.IdWarehouse = K.IdWarehouse
    LEFT JOIN E ON E.IdCabinetMaterial = a.IdCabinetMaterial AND E.IdWarehouse = K.IdWarehouse
    LEFT JOIN F ON F.IdCabinetMaterial = a.IdCabinetMaterial AND F.IdWarehouse = K.IdWarehouse
    LEFT JOIN G ON G.IdCabinetMaterial = a.IdCabinetMaterial AND G.IdWarehouse = K.IdWarehouse
    LEFT JOIN H ON H.IdCabinetMaterial = a.IdCabinetMaterial AND H.IdWarehouse = K.IdWarehouse
    LEFT JOIN I ON I.IdCabinetMaterial = a.IdCabinetMaterial AND I.IdWarehouse = K.IdWarehouse
    LEFT JOIN J ON J.IdCabinetMaterial = a.IdCabinetMaterial AND J.IdWarehouse = K.IdWarehouse
    ORDER BY a.Nama;
  `;

  const result = await req.query(query);
  const rows = result.recordset || [];

  return {
    found: rows.length > 0,
    count: rows.length,
    data: rows,
  };
}



/**
 * Single entry: create NEW partials + link them, and attach EXISTING inputs.
 * All in one transaction.
 *
 * Payload shape (arrays optional):
 * {
 *   // existing inputs to attach
 *   furnitureWip:           [{ noFurnitureWip }],
 *   cabinetMaterial:        [{ idCabinetMaterial, jumlah }],
 *
 *   // NEW partials to create + map
 *   furnitureWipPartialNew: [{ noFurnitureWip, pcs }]
 * }
 */
async function upsertInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    furnitureWipPartialNew: norm(payload?.furnitureWipPartialNew),
  };

  try {
    // IMPORTANT: gunakan serializable biar konsisten + cegah race
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'hotStamping',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // UPSERT = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (UPSERT INPUT = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx, // WAJIB tx
      action: 'upsert HotStamping inputs/partials',
      useLock: true, // write action
    });

    // -------------------------------------------------------
    // 2) Create partials + map them to produksi
    // -------------------------------------------------------
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      furnitureWipPartialNew: body.furnitureWipPartialNew,
    });

    // -------------------------------------------------------
    // 3) Attach existing inputs (split methods for different logic)
    // -------------------------------------------------------
    const fwipAttach = await _insertFurnitureWipWithTx(tx, noProduksi, {
      furnitureWip: body.furnitureWip,
    });

    const matAttach = await _insertCabinetMaterialWithTx(tx, noProduksi, {
      cabinetMaterial: body.cabinetMaterial,
    });

    const attachments = {
      furnitureWip: fwipAttach.furnitureWip,
      cabinetMaterial: matAttach.cabinetMaterial,
    };

    await tx.commit();

    // ===== response structure (following broker pattern) =====
    const totalInserted = Object.values(attachments).reduce((sum, item) => sum + (item.inserted || 0), 0);
    const totalUpdated = Object.values(attachments).reduce((sum, item) => sum + (item.updated || 0), 0);
    const totalSkipped = Object.values(attachments).reduce((sum, item) => sum + (item.skipped || 0), 0);
    const totalInvalid = Object.values(attachments).reduce((sum, item) => sum + (item.invalid || 0), 0);

    const totalPartialsCreated = Object.values(partials.summary || {}).reduce(
      (sum, item) => sum + (item.created || 0),
      0
    );

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = (totalInserted + totalUpdated) === 0 && totalPartialsCreated === 0;

    const response = {
      noProduksi,
      summary: {
        totalInserted,
        totalUpdated,
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
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = attachments[section.key] || { inserted: 0, updated: 0, skipped: 0, invalid: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      inserted: result.inserted,
      updated: result.updated,
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
    { key: 'furnitureWipPartialNew', label: 'Furniture WIP Partial' },
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
  if (result.updated > 0) {
    parts.push(`${result.updated} berhasil diperbarui`);
  }
  if (result.skipped > 0) {
    parts.push(`${result.skipped} sudah ada (dilewati)`);
  }
  if (result.invalid > 0) {
    parts.push(`${result.invalid} tidak valid`);
  }

  if (parts.length === 0) {
    return `Tidak ada ${label} yang diproses`;
  }

  return `${label}: ${parts.join(', ')}`;
}

/* --------------------------
   SQL batches (set-based)
-------------------------- */

/**
 * Create NEW partial labels (BC.0000000001) from parent furnitureWip.
 * Uses MAX + ROW_NUMBER pattern (NO SQL SEQUENCE required)
 */
async function _insertPartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.HotStamping_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header HotStamping_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Created int);
  DECLARE @createdFWP TABLE(NoFurnitureWIPPartial varchar(50));

  /* ========= FURNITURE WIP PARTIAL NEW (create BC.*) ========= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew'))
  BEGIN
    -- ✅ FIX: Gunakan MAX + UPDLOCK pattern seperti Broker
    DECLARE @nextFWP int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoFurnitureWIPPartial, 10) AS int))
      FROM dbo.FurnitureWIPPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoFurnitureWIPPartial LIKE 'BC.%'
    ), 0);

    -- Validasi request + generate nomor menggunakan ROW_NUMBER
    ;WITH src AS (
      SELECT
        noFurnitureWip,
        pcs,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew')
      WITH (
        noFurnitureWip varchar(50) '$.noFurnitureWip',
        pcs int '$.pcs'
      )
      WHERE NULLIF(noFurnitureWip, '') IS NOT NULL
        AND ISNULL(pcs, 0) > 0
        AND EXISTS (
          SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK)
          WHERE f.NoFurnitureWIP = noFurnitureWip
            AND f.DateUsage IS NULL
        )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('BC.', RIGHT(REPLICATE('0',10) + CAST(@nextFWP + rn AS varchar(10)), 10)),
        noFurnitureWip,
        pcs
      FROM src
    )
    INSERT INTO dbo.FurnitureWIPPartial (NoFurnitureWIPPartial, NoFurnitureWIP, Pcs)
    OUTPUT INSERTED.NoFurnitureWIPPartial INTO @createdFWP(NoFurnitureWIPPartial)
    SELECT NewNo, noFurnitureWip, pcs
    FROM numbered;

    -- Attach mapping ke HotStamping
    INSERT INTO dbo.HotStampingInputLabelFWIPPartial (NoProduksi, NoFurnitureWIPPartial)
    SELECT @no, c.NoFurnitureWIPPartial
    FROM @createdFWP c
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.HotStampingInputLabelFWIPPartial x WITH (NOLOCK)
      WHERE x.NoProduksi=@no AND x.NoFurnitureWIPPartial=c.NoFurnitureWIPPartial
    );

    -- Update parent: set IsPartial=1 and DateUsage
    -- ✅ FIX: Cek apakah total Pcs partial sudah habis
    DECLARE @ins int = @@ROWCOUNT;

    IF @ins > 0
    BEGIN
      ;WITH existingPartials AS (
        SELECT 
          fp.NoFurnitureWIP,
          SUM(ISNULL(fp.Pcs, 0)) AS TotalPcsPartialExisting
        FROM dbo.FurnitureWIPPartial fp WITH (NOLOCK)
        WHERE fp.NoFurnitureWIPPartial NOT IN (SELECT NoFurnitureWIPPartial FROM @createdFWP)
        GROUP BY fp.NoFurnitureWIP
      ),
      newPartials AS (
        SELECT 
          noFurnitureWip,
          SUM(pcs) AS TotalPcsPartialNew
        FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew')
        WITH (
          noFurnitureWip varchar(50) '$.noFurnitureWip',
          pcs int '$.pcs'
        )
        GROUP BY noFurnitureWip
      )
      UPDATE f
      SET 
        f.IsPartial = 1,
        -- ✅ Set DateUsage jika Pcs sudah habis (tolerance 0 karena integer)
        f.DateUsage = CASE 
          WHEN (f.Pcs - ISNULL(ep.TotalPcsPartialExisting, 0) - ISNULL(np.TotalPcsPartialNew, 0)) <= 0
          THEN @tgl 
          ELSE f.DateUsage 
        END
      FROM dbo.FurnitureWIP f
      LEFT JOIN existingPartials ep ON ep.NoFurnitureWIP = f.NoFurnitureWIP
      INNER JOIN newPartials np ON np.noFurnitureWip = f.NoFurnitureWIP;
    END

    INSERT INTO @out SELECT 'furnitureWipPartialNew', @ins;
  END

  -- Return summary + generated codes
  SELECT Section, Created FROM @out;
  SELECT NoFurnitureWIPPartial FROM @createdFWP;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  const createdLists = {
    furnitureWipPartialNew: (rs.recordsets?.[1] || []).map(r => r.NoFurnitureWIPPartial),
  };

  return { summary, createdLists };
}
async function _insertFurnitureWipWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.HotStamping_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header HotStamping_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @fwIns int=0, @fwSkp int=0, @fwInv int=0;
  
  -- ✅ FIX: Gunakan table variables untuk tracking
  DECLARE @reqFW TABLE(NoFurnitureWip varchar(50));
  DECLARE @alreadyMapped TABLE(NoFurnitureWip varchar(50));
  DECLARE @eligibleNotMapped TABLE(NoFurnitureWip varchar(50));
  DECLARE @invalid TABLE(NoFurnitureWip varchar(50));
  DECLARE @insFW TABLE(NoFurnitureWIP varchar(50));

  -- 1) Populate request list
  INSERT INTO @reqFW(NoFurnitureWip)
  SELECT DISTINCT noFurnitureWip
  FROM OPENJSON(@jsInputs, '$.furnitureWip')
  WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  WHERE NULLIF(noFurnitureWip,'') IS NOT NULL;

  -- 2) Find already mapped
  INSERT INTO @alreadyMapped(NoFurnitureWip)
  SELECT r.NoFurnitureWip
  FROM @reqFW r
  WHERE EXISTS (
    SELECT 1 FROM dbo.HotStampingInputLabelFWIP x WITH (NOLOCK)
    WHERE x.NoProduksi=@no AND x.NoFurnitureWIP=r.NoFurnitureWip
  );

  -- 3) Find eligible (not mapped + available)
  INSERT INTO @eligibleNotMapped(NoFurnitureWip)
  SELECT r.NoFurnitureWip
  FROM @reqFW r
  WHERE NOT EXISTS (SELECT 1 FROM @alreadyMapped a WHERE a.NoFurnitureWip=r.NoFurnitureWip)
    AND EXISTS (
      SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK)
      WHERE f.NoFurnitureWIP=r.NoFurnitureWip
        AND f.DateUsage IS NULL
    );

  -- 4) Find invalid (not mapped + not eligible)
  INSERT INTO @invalid(NoFurnitureWip)
  SELECT r.NoFurnitureWip
  FROM @reqFW r
  WHERE NOT EXISTS (SELECT 1 FROM @alreadyMapped a WHERE a.NoFurnitureWip=r.NoFurnitureWip)
    AND NOT EXISTS (SELECT 1 FROM @eligibleNotMapped e WHERE e.NoFurnitureWip=r.NoFurnitureWip);

  -- 5) Insert eligible ones
  INSERT INTO dbo.HotStampingInputLabelFWIP (NoProduksi, NoFurnitureWIP)
  OUTPUT INSERTED.NoFurnitureWIP INTO @insFW(NoFurnitureWIP)
  SELECT @no, e.NoFurnitureWip
  FROM @eligibleNotMapped e;

  SET @fwIns = @@ROWCOUNT;

  -- 6) Update DateUsage for inserted items
  IF @fwIns > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = @tgl
    FROM dbo.FurnitureWIP f
    JOIN @insFW i ON i.NoFurnitureWIP=f.NoFurnitureWIP;
  END

  -- 7) Count skipped and invalid
  SELECT @fwSkp = COUNT(*) FROM @alreadyMapped;
  SELECT @fwInv = COUNT(*) FROM @invalid;

  -- 8) Return stats
  SELECT
    @fwIns AS Inserted,
    0 AS Updated,
    @fwSkp AS Skipped,
    @fwInv AS Invalid;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    furnitureWip: {
      inserted: row.Inserted || 0,
      updated: row.Updated || 0,
      skipped: row.Skipped || 0,
      invalid: row.Invalid || 0,
    }
  };
}
async function _insertCabinetMaterialWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.HotStamping_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header HotStamping_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @mIns int=0, @mUpd int=0, @mInv int=0;

  DECLARE @MatSrc TABLE(IdCabinetMaterial int, Jumlah int);

  INSERT INTO @MatSrc(IdCabinetMaterial, Jumlah)
  SELECT IdCabinetMaterial, SUM(ISNULL(Jumlah,0)) AS Jumlah
  FROM OPENJSON(@jsInputs, '$.cabinetMaterial')
  WITH (
    IdCabinetMaterial int '$.idCabinetMaterial',
    Jumlah int '$.jumlah'
  )
  WHERE IdCabinetMaterial IS NOT NULL
  GROUP BY IdCabinetMaterial;

  -- invalid jika jumlah <=0 atau material tidak enable
  SELECT @mInv = COUNT(*)
  FROM @MatSrc s
  WHERE s.Jumlah <= 0
     OR NOT EXISTS (
        SELECT 1
        FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
        WHERE m.IdCabinetMaterial=s.IdCabinetMaterial AND m.Enable=1
     );

  -- UPDATE existing rows
  UPDATE tgt
  SET tgt.Jumlah = src.Jumlah
  FROM dbo.HotStampingInputMaterial tgt
  JOIN @MatSrc src ON src.IdCabinetMaterial=tgt.IdCabinetMaterial
  WHERE tgt.NoProduksi=@no
    AND src.Jumlah > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    );

  SET @mUpd = @@ROWCOUNT;

  -- INSERT missing rows
  INSERT INTO dbo.HotStampingInputMaterial (NoProduksi, IdCabinetMaterial, Jumlah)
  SELECT @no, src.IdCabinetMaterial, src.Jumlah
  FROM @MatSrc src
  WHERE src.Jumlah > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    )
    AND NOT EXISTS (
      SELECT 1 FROM dbo.HotStampingInputMaterial x WITH (NOLOCK)
      WHERE x.NoProduksi=@no AND x.IdCabinetMaterial=src.IdCabinetMaterial
    );

  SET @mIns = @@ROWCOUNT;

  SELECT
    @mIns AS Inserted,
    @mUpd AS Updated,
    0 AS Skipped,
    @mInv AS Invalid;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    cabinetMaterial: {
      inserted: row.Inserted || 0,
      updated: row.Updated || 0,
      skipped: row.Skipped || 0,
      invalid: row.Invalid || 0,
    }
  };
}


/**
 * Delete inputs and partials from HotStamping production
 * 
 * Payload shape:
 * {
 *   furnitureWip: [{ noFurnitureWip }],
 *   cabinetMaterial: [{ idCabinetMaterial }],
 *   furnitureWipPartial: [{ noFurnitureWipPartial }]
 * }
 */
async function deleteInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    furnitureWipPartial: norm(payload?.furnitureWipPartial),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // -------------------------------------------------------
    // 0) AMBIL HEADER + LOCK ROW
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'hotStamping',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (DELETE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete HotStamping inputs/partials',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) Delete partials
    // -------------------------------------------------------
    const partialsResult = await _deletePartialsWithTx(tx, noProduksi, {
      furnitureWipPartial: body.furnitureWipPartial,
    });

    // -------------------------------------------------------
    // 3) Delete inputs (split methods for different logic)
    // -------------------------------------------------------
    const fwipResult = await _deleteFurnitureWipWithTx(tx, noProduksi, {
      furnitureWip: body.furnitureWip,
    });

    const matResult = await _deleteCabinetMaterialWithTx(tx, noProduksi, {
      cabinetMaterial: body.cabinetMaterial,
    });

    const inputsResult = {
      furnitureWip: fwipResult.furnitureWip,
      cabinetMaterial: matResult.cabinetMaterial,
    };

    await tx.commit();

    // ===== response structure =====
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

// Helper to build delete input details
function _buildDeleteInputDetails(results, requestBody) {
  const details = [];
  const sections = [
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },
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
    { key: 'furnitureWipPartial', label: 'Furniture WIP Partial' },
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

/* --------------------------
   SQL DELETE functions
-------------------------- */

async function _deletePartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.HotStamping_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header HotStamping_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  /* ========= FURNITURE WIP PARTIAL ========= */
  DECLARE @fwpDeleted int = 0, @fwpNotFound int = 0;
  
  -- Count yang akan dihapus
  SELECT @fwpDeleted = COUNT(*)
  FROM dbo.HotStampingInputLabelFWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial') 
  WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
  ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;
  
  -- Simpan NoFurnitureWIP dari partial yang akan dihapus
  DECLARE @deletedFWPPartials TABLE (
    NoFurnitureWIP varchar(50)
  );
  
  INSERT INTO @deletedFWPPartials (NoFurnitureWIP)
  SELECT DISTINCT fp.NoFurnitureWIP
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN dbo.HotStampingInputLabelFWIPPartial map ON fp.NoFurnitureWIPPartial = map.NoFurnitureWIPPartial
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial') 
  WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
  ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;
  
  -- Delete dari mapping table
  DELETE map
  FROM dbo.HotStampingInputLabelFWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial') 
  WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
  ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;
  
  -- Delete dari FurnitureWIPPartial table
  DELETE fp
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial') 
  WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
  ON fp.NoFurnitureWIPPartial = j.noFurnitureWipPartial;
  
  -- Update FurnitureWIP parent
  IF @fwpDeleted > 0
  BEGIN
    -- Update untuk yang MASIH ADA partial lainnya
    UPDATE f
    SET 
      f.DateUsage = NULL,
      f.IsPartial = 1
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedFWPPartials del ON f.NoFurnitureWIP = del.NoFurnitureWIP
    WHERE EXISTS (
      SELECT 1 
      FROM dbo.FurnitureWIPPartial fp 
      WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP
    );
    
    -- Update untuk yang TIDAK ADA lagi partial nya
    UPDATE f
    SET 
      f.DateUsage = NULL,
      f.IsPartial = 0
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedFWPPartials del ON f.NoFurnitureWIP = del.NoFurnitureWIP
    WHERE NOT EXISTS (
      SELECT 1 
      FROM dbo.FurnitureWIPPartial fp 
      WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP
    );
  END;
  
  DECLARE @fwpRequested int;
  SELECT @fwpRequested = COUNT(*)
  FROM OPENJSON(@jsPartials, '$.furnitureWipPartial');
  
  SET @fwpNotFound = @fwpRequested - @fwpDeleted;
  
  INSERT INTO @out SELECT 'furnitureWipPartial', @fwpDeleted, @fwpNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = {
      deleted: row.Deleted,
      notFound: row.NotFound,
    };
  }

  return { summary };
}

async function _deleteFurnitureWipWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.HotStamping_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header HotStamping_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @fwDeleted int = 0, @fwNotFound int = 0;
  
  -- Count yang akan dihapus
  SELECT @fwDeleted = COUNT(*)
  FROM dbo.HotStampingInputLabelFWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip') 
  WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
  ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoProduksi = @no;
  
  -- Reset DateUsage sebelum DELETE
  IF @fwDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = NULL
    FROM dbo.FurnitureWIP f
    INNER JOIN dbo.HotStampingInputLabelFWIP map ON f.NoFurnitureWIP = map.NoFurnitureWIP
    INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip') 
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
    WHERE map.NoProduksi = @no;
  END;
  
  -- DELETE
  DELETE map
  FROM dbo.HotStampingInputLabelFWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip') 
  WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
  ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoProduksi = @no;
  
  DECLARE @fwRequested int;
  SELECT @fwRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.furnitureWip');
  
  SET @fwNotFound = @fwRequested - @fwDeleted;
  
  SELECT @fwDeleted AS Deleted, @fwNotFound AS NotFound;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    furnitureWip: {
      deleted: row.Deleted || 0,
      notFound: row.NotFound || 0,
    }
  };
}

async function _deleteCabinetMaterialWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.HotStamping_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header HotStamping_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @matDeleted int = 0, @matNotFound int = 0;
  
  -- Count yang akan dihapus
  SELECT @matDeleted = COUNT(*)
  FROM dbo.HotStampingInputMaterial map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial') 
  WITH (idCabinetMaterial int '$.idCabinetMaterial') j
  ON map.IdCabinetMaterial = j.idCabinetMaterial
  WHERE map.NoProduksi = @no;
  
  -- DELETE (no DateUsage to reset for materials)
  DELETE map
  FROM dbo.HotStampingInputMaterial map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial') 
  WITH (idCabinetMaterial int '$.idCabinetMaterial') j
  ON map.IdCabinetMaterial = j.idCabinetMaterial
  WHERE map.NoProduksi = @no;
  
  DECLARE @matRequested int;
  SELECT @matRequested = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.cabinetMaterial');
  
  SET @matNotFound = @matRequested - @matDeleted;
  
  SELECT @matDeleted AS Deleted, @matNotFound AS NotFound;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    cabinetMaterial: {
      deleted: row.Deleted || 0,
      notFound: row.NotFound || 0,
    }
  };
}

module.exports = { getProduksiByDate, getAllProduksi, createHotStampingProduksi, updateHotStampingProduksi, deleteHotStampingProduksi, fetchInputs, validateFwipLabel, validateCabinetMaterialStock, getMasterCabinetMaterials, upsertInputsAndPartials, deleteInputsAndPartials };

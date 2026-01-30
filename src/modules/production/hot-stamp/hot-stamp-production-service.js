// services/hotstamping-production-service.js
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
  map.NoFurnitureWIP        AS Ref1,
  CAST(NULL AS varchar(50)) AS Ref2,

  fw.Berat,
  fw.Pcs,
  fw.IsPartial,
  fw.IDFurnitureWIP         AS IdJenis,

  mw.Nama                   AS NamaJenis,
  uom.NamaUOM               AS NamaUOM
FROM dbo.HotStampingInputLabelFWIP map WITH (NOLOCK)
LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
  ON fw.NoFurnitureWIP = map.NoFurnitureWIP

LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
  ON mw.IdCabinetWIP = fw.IDFurnitureWIP

LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
  ON uom.IdUOM = mw.IdUOM

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

  mw.Nama           AS NamaJenis,
  uom.NamaUOM       AS NamaUOM
FROM dbo.HotStampingInputLabelFWIPPartial mp WITH (NOLOCK)
LEFT JOIN dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
  ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
  ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP

LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
  ON mw.IdCabinetWIP = fw.IDFurnitureWIP

LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
  ON uom.IdUOM = mw.IdUOM

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
       ;WITH PartialSum AS (
        SELECT
          fwp.NoFurnitureWIP,
          SUM(ISNULL(fwp.Pcs, 0)) AS PcsPartial
        FROM dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
        GROUP BY fwp.NoFurnitureWIP
      )
      SELECT
        fw.NoFurnitureWIP,
        fw.DateCreate,
        fw.Jam,
        CAST(fw.Pcs - ISNULL(ps.PcsPartial, 0) AS int) AS Pcs,  -- ✅ sisa pcs
        fw.IDFurnitureWIP,
        fw.Berat,
        CASE WHEN ISNULL(ps.PcsPartial, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS IsPartial,
        fw.DateUsage,
        fw.IdWarehouse,
        fw.IdWarna,
        fw.CreateBy,
        fw.DateTimeCreate,
        fw.Blok,
        fw.IdLokasi,
        ISNULL(ps.PcsPartial, 0) AS PcsPartial  -- opsional debug
      FROM dbo.FurnitureWIP fw WITH (NOLOCK)
      LEFT JOIN PartialSum ps
        ON ps.NoFurnitureWIP = fw.NoFurnitureWIP
      WHERE fw.NoFurnitureWIP = @code
        AND fw.DateUsage IS NULL
        AND (fw.Pcs - ISNULL(ps.PcsPartial, 0)) > 0;
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

  return {
    found: false,
    count: 0,
    tableName: '',
    data: [],
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

  // ✅ forward ctx yang sudah dinormalisasi ke shared service
  return sharedInputService.upsertInputsAndPartials('hotStamping', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

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
  return sharedInputService.deleteInputsAndPartials('hotStamping', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}


module.exports = { getProduksiByDate, getAllProduksi, createHotStampingProduksi, updateHotStampingProduksi, deleteHotStampingProduksi, fetchInputs, validateFwipLabel, upsertInputsAndPartials, deleteInputsAndPartials };

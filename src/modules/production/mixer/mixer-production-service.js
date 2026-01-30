// services/mixer-production-service.js
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
      h.TglProduksi,
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
      h.HourMeter
    FROM [dbo].[MixerProduksi_h] h
    LEFT JOIN dbo.MstMesin m ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @date
    ORDER BY h.Jam ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}


/**
 * Paginated fetch for dbo.MixerProduksi_h
 * Join MstMesin untuk ambil NamaMesin.
 * (Opsional) Join MstOperator untuk NamaOperator (biar sama kayak Broker).
 */
async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
  `;

  // 1) Count (tanpa join, ringan)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.MixerProduksi_h h WITH (NOLOCK)
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
      h.TglProduksi,
      h.IdMesin,
      ms.NamaMesin,
      h.IdOperator,
      op.NamaOperator,
      h.Jam AS JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd, 108)   AS HourEnd,

      -- (opsional utk FE)
      lc.LastClosedDate AS LastClosedDate,

      -- ✅ flag tutup transaksi
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.TglProduksi) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.MixerProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin ms WITH (NOLOCK)
      ON ms.IdMesin = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK)
      ON op.IdOperator = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    -- rekomendasi urut konsisten
    ORDER BY h.TglProduksi DESC, h.Jam ASC, h.NoProduksi DESC
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
// generator khusus Mixer (beda tabel saja)
// =========================
async function generateNextNoProduksiMixer(tx, { prefix = 'I.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoProduksi
    FROM dbo.MixerProduksi_h AS h WITH (UPDLOCK, HOLDLOCK)
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

// =========================
// create MixerProduksi_h (mirip Broker)
// =========================
async function createMixerProduksi(payload) {
  const must = [];
  if (!payload?.tglProduksi) must.push('tglProduksi');
  if (payload?.idMesin == null) must.push('idMesin');
  if (payload?.idOperator == null) must.push('idOperator');
  if (payload?.jam == null) must.push('jam');
  if (payload?.shift == null) must.push('shift');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) NORMALISASI TANGGAL (DATE-ONLY UTC)
    // -------------------------------------------------------
    const effectiveDate = resolveEffectiveDateForCreate(payload.tglProduksi);

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (CREATE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: effectiveDate,
      runner: tx,                 // IMPORTANT: pakai tx
      action: 'create MixerProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) generate noProduksi (prefix mixer)
    // -------------------------------------------------------
    const no1 = await generateNextNoProduksiMixer(tx, { prefix: 'I.', width: 10 });

    // -------------------------------------------------------
    // 3) double-check exist + lock (pola sama)
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.MixerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksiMixer(tx, { prefix: 'I.', width: 10 })
      : no1;

    // -------------------------------------------------------
    // 4) parse jam -> int
    // -------------------------------------------------------
    const jamInt = parseJamToInt(payload.jam);

    // -------------------------------------------------------
    // 5) insert (pakai effectiveDate)
    // -------------------------------------------------------
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),    noProduksi)
      .input('IdOperator',  sql.Int,            payload.idOperator)
      .input('IdMesin',     sql.Int,            payload.idMesin)
      .input('TglProduksi', sql.Date,           effectiveDate) // ✅
      .input('Jam',         sql.Int,            jamInt)
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
      INSERT INTO dbo.MixerProduksi_h (
        NoProduksi,
        IdOperator,
        IdMesin,
        TglProduksi,
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
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi,
        @IdOperator,
        @IdMesin,
        @TglProduksi,
        @Jam,
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



async function updateMixerProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    GANTI SELECT MixerProduksi_h manual untuk tanggal
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'mixerProduksi',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,          // UPDATE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) Jika user mengubah tanggal, hitung tanggal barunya
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
      action: 'update MixerProduksi (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update MixerProduksi (new date)',
        useLock: true,
      });
    }

    // -------------------------------------------------------
    // 3) (Opsional tapi bagus) LOCK HEADER ROW untuk memastikan exists
    //    Karena loadDocDateOnlyFromConfig sudah throwIfNotFound,
    //    ini sebenarnya tidak wajib. Tapi aman kalau mau ambil row juga.
    // -------------------------------------------------------
    // const rqLock = new sql.Request(tx);
    // await rqLock.input('NoProduksi', sql.VarChar(50), noProduksi).query(`
    //   SELECT 1 FROM dbo.MixerProduksi_h WITH (UPDLOCK, HOLDLOCK)
    //   WHERE NoProduksi = @NoProduksi
    // `);

    // -------------------------------------------------------
    // 4) BUILD SET DINAMIS
    // -------------------------------------------------------
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('TglProduksi = @TglProduksi');
      rqUpd.input('TglProduksi', sql.Date, newDocDateOnly); // ✅ date-only UTC
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
      UPDATE dbo.MixerProduksi_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.MixerProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // -------------------------------------------------------
    // 5) Jika TglProduksi berubah → sync DateUsage full + partial
    //    Pakai tanggal hasil DB agar konsisten
    // -------------------------------------------------------
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.TglProduksi);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('TglProduksi', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BAHAN BAKU (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE bb
        SET bb.DateUsage = @TglProduksi
        FROM dbo.BahanBaku_d AS bb
        WHERE bb.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBB AS map
              WHERE map.NoProduksi  = @NoProduksi
                AND map.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak       = bb.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoProduksi  = @NoProduksi
                AND bp.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak       = bb.NoSak
            )
          );

        -------------------------------------------------------
        -- BROKER (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE br
        SET br.DateUsage = @TglProduksi
        FROM dbo.Broker_d AS br
        WHERE br.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBroker AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoBroker   = br.NoBroker
                AND map.NoSak      = br.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBrokerPartial AS mp
              JOIN dbo.BrokerPartial AS bp
                ON bp.NoBrokerPartial = mp.NoBrokerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND bp.NoBroker   = br.NoBroker
                AND bp.NoSak      = br.NoSak
            )
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
              FROM dbo.MixerProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputGilinganPartial AS mp
              JOIN dbo.GilinganPartial AS gp
                ON gp.NoGilinganPartial = mp.NoGilinganPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND gp.NoGilingan = g.NoGilingan
            )
          );

        -------------------------------------------------------
        -- MIXER (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE m
        SET m.DateUsage = @TglProduksi
        FROM dbo.Mixer_d AS m
        WHERE m.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputMixer AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoMixer    = m.NoMixer
                AND map.NoSak      = m.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputMixerPartial AS mp
              JOIN dbo.MixerPartial AS mpd
                ON mpd.NoMixerPartial = mp.NoMixerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND mpd.NoMixer   = m.NoMixer
                AND mpd.NoSak     = m.NoSak
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



async function deleteMixerProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    GANTI SELECT MixerProduksi_h manual
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'mixerProduksi',     // pastikan ada di config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,                  // DELETE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (DELETE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete MixerProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) CEK OUTPUT: MixerProduksiOutput (kalau ada → tolak delete)
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const outCheck = await rqCheck
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT COUNT(1) AS CntOutput
        FROM dbo.MixerProduksiOutput WITH (NOLOCK)
        WHERE NoProduksi = @NoProduksi;
      `);

    const row = outCheck.recordset?.[0] || { CntOutput: 0 };
    const hasOutput = (row.CntOutput || 0) > 0;

    if (hasOutput) {
      throw badReq('Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.');
    }

    // -------------------------------------------------------
    // 3) DELETE INPUT + PARTIAL + RESET DATEUSAGE & ISPARTIAL
    //    (SQL BESAR kamu tetap)
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK MENYIMPAN KEY YANG TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BBKeys TABLE (NoBahanBaku varchar(50), NoPallet varchar(50), NoSak varchar(50));
      DECLARE @BrokerKeys TABLE (NoBroker varchar(50), NoSak varchar(50));
      DECLARE @GilinganKeys TABLE (NoGilingan varchar(50));
      DECLARE @MixerKeys TABLE (NoMixer varchar(50), NoSak varchar(50));

      ---------------------------------------------------------
      -- 1) BAHAN BAKU (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BBKeys (NoBahanBaku, NoPallet, NoSak)
      SELECT DISTINCT bb.NoBahanBaku, bb.NoPallet, bb.NoSak
      FROM dbo.BahanBaku_d AS bb
      WHERE EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBB AS map
              WHERE map.NoProduksi  = @NoProduksi
                AND map.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak       = bb.NoSak
            )
         OR EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoProduksi  = @NoProduksi
                AND bp.NoBahanBaku = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak       = bb.NoSak
            );

      DELETE bp
      FROM dbo.BahanBakuPartial AS bp
      JOIN dbo.MixerProduksiInputBBPartial AS mp
        ON mp.NoBBPartial = bp.NoBBPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.MixerProduksiInputBBPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.MixerProduksiInputBB        WHERE NoProduksi = @NoProduksi;

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
      -- 2) BROKER (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BrokerKeys (NoBroker, NoSak)
      SELECT DISTINCT b.NoBroker, b.NoSak
      FROM dbo.Broker_d AS b
      WHERE EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBroker AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoBroker   = b.NoBroker
                AND map.NoSak      = b.NoSak
          )
         OR EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputBrokerPartial AS mp
              JOIN dbo.BrokerPartial AS bp
                ON bp.NoBrokerPartial = mp.NoBrokerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND bp.NoBroker   = b.NoBroker
                AND bp.NoSak      = b.NoSak
          );

      DELETE bp
      FROM dbo.BrokerPartial AS bp
      JOIN dbo.MixerProduksiInputBrokerPartial AS mp
        ON mp.NoBrokerPartial = bp.NoBrokerPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.MixerProduksiInputBrokerPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.MixerProduksiInputBroker        WHERE NoProduksi = @NoProduksi;

      UPDATE b
      SET b.DateUsage = NULL,
          b.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.BrokerPartial AS bp
              WHERE bp.NoBroker = b.NoBroker
                AND bp.NoSak    = b.NoSak
            ) THEN 1 ELSE 0 END
      FROM dbo.Broker_d AS b
      JOIN @BrokerKeys AS k
        ON k.NoBroker = b.NoBroker
       AND k.NoSak    = b.NoSak;

      ---------------------------------------------------------
      -- 3) GILINGAN (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT g.NoGilingan
      FROM dbo.Gilingan AS g
      WHERE EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
          )
         OR EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputGilinganPartial AS mp
              JOIN dbo.GilinganPartial AS gp
                ON gp.NoGilinganPartial = mp.NoGilinganPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND gp.NoGilingan = g.NoGilingan
          );

      DELETE gp
      FROM dbo.GilinganPartial AS gp
      JOIN dbo.MixerProduksiInputGilinganPartial AS mp
        ON mp.NoGilinganPartial = gp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.MixerProduksiInputGilinganPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.MixerProduksiInputGilingan        WHERE NoProduksi = @NoProduksi;

      UPDATE g
      SET g.DateUsage = NULL,
          g.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.GilinganPartial AS gp
              WHERE gp.NoGilingan = g.NoGilingan
            ) THEN 1 ELSE 0 END
      FROM dbo.Gilingan AS g
      JOIN @GilinganKeys AS k
        ON k.NoGilingan = g.NoGilingan;

      ---------------------------------------------------------
      -- 4) MIXER (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @MixerKeys (NoMixer, NoSak)
      SELECT DISTINCT m.NoMixer, m.NoSak
      FROM dbo.Mixer_d AS m
      WHERE EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputMixer AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoMixer    = m.NoMixer
                AND map.NoSak      = m.NoSak
          )
         OR EXISTS (
              SELECT 1
              FROM dbo.MixerProduksiInputMixerPartial AS mp
              JOIN dbo.MixerPartial AS mpd
                ON mpd.NoMixerPartial = mp.NoMixerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND mpd.NoMixer   = m.NoMixer
                AND mpd.NoSak     = m.NoSak
          );

      DELETE mpd
      FROM dbo.MixerPartial AS mpd
      JOIN dbo.MixerProduksiInputMixerPartial AS mp
        ON mp.NoMixerPartial = mpd.NoMixerPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.MixerProduksiInputMixerPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.MixerProduksiInputMixer        WHERE NoProduksi = @NoProduksi;

      UPDATE m
      SET m.DateUsage = NULL,
          m.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.MixerPartial AS mpd
              WHERE mpd.NoMixer = m.NoMixer
                AND mpd.NoSak   = m.NoSak
            ) THEN 1 ELSE 0 END
      FROM dbo.Mixer_d AS m
      JOIN @MixerKeys AS k
        ON k.NoMixer = m.NoMixer
       AND k.NoSak   = m.NoSak;

      ---------------------------------------------------------
      -- 5) TERAKHIR: HAPUS HEADER MIXERPRODUKSI_H
      ---------------------------------------------------------
      DELETE FROM dbo.MixerProduksi_h
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

    -- BROKER (FULL)
    SELECT
      'broker' AS Src,
      ib.NoProduksi,
      ib.NoBroker AS Ref1,
      ib.NoSak    AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      br.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      br.IsPartial AS IsPartial,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.MixerProduksiInputBroker ib WITH (NOLOCK)
    LEFT JOIN dbo.Broker_d br        WITH (NOLOCK) ON br.NoBroker = ib.NoBroker AND br.NoSak = ib.NoSak
    LEFT JOIN dbo.Broker_h bh        WITH (NOLOCK) ON bh.NoBroker = ib.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK) ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE ib.NoProduksi=@no

    UNION ALL

    -- BAHAN BAKU (FULL)
    SELECT
      'bb' AS Src,
      ibb.NoProduksi,
      ibb.NoBahanBaku AS Ref1,
      ibb.NoPallet    AS Ref2,
      ibb.NoSak       AS Ref3,
      bb.Berat AS Berat,
      bb.BeratAct AS BeratAct,
      bb.IsPartial AS IsPartial,
      bbh.IdJenisPlastik AS IdJenis,
      jpb.Jenis          AS NamaJenis
    FROM dbo.MixerProduksiInputBB ibb WITH (NOLOCK)
    LEFT JOIN dbo.BahanBaku_d bb            WITH (NOLOCK)
      ON bb.NoBahanBaku = ibb.NoBahanBaku AND bb.NoPallet = ibb.NoPallet AND bb.NoSak = ibb.NoSak
    LEFT JOIN dbo.BahanBakuPallet_h bbh     WITH (NOLOCK)
      ON bbh.NoBahanBaku = ibb.NoBahanBaku AND bbh.NoPallet = ibb.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpb       WITH (NOLOCK)
      ON jpb.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE ibb.NoProduksi=@no

    UNION ALL

    -- GILINGAN (FULL)
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
    FROM dbo.MixerProduksiInputGilingan ig WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan g        WITH (NOLOCK) ON g.NoGilingan = ig.NoGilingan
    LEFT JOIN dbo.MstGilingan mg    WITH (NOLOCK) ON mg.IdGilingan = g.IdGilingan
    WHERE ig.NoProduksi=@no

    UNION ALL

    -- MIXER (FULL)  (input mixer produksi refer ke Mixer_d)
    SELECT
      'mixer' AS Src,
      im.NoProduksi,
      im.NoMixer AS Ref1,
      im.NoSak   AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      md.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      md.IsPartial AS IsPartial,
      mh.IdMixer  AS IdJenis,
      mm.Jenis    AS NamaJenis
    FROM dbo.MixerProduksiInputMixer im WITH (NOLOCK)
    LEFT JOIN dbo.Mixer_d md  WITH (NOLOCK)
      ON md.NoMixer = im.NoMixer AND md.NoSak = im.NoSak
    LEFT JOIN dbo.Mixer_h mh  WITH (NOLOCK)
      ON mh.NoMixer = im.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE im.NoProduksi=@no

    ORDER BY Ref1 DESC, Ref2 ASC;


    /* ===================== [2] PARTIALS ===================== */

    -- BB partial → jenis plastik dari header pallet
    SELECT
      pmap.NoBBPartial,
      pdet.NoBahanBaku,
      pdet.NoPallet,
      pdet.NoSak,
      pdet.Berat,
      bbh.IdJenisPlastik AS IdJenis,
      jpp.Jenis          AS NamaJenis
    FROM dbo.MixerProduksiInputBBPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BahanBakuPartial pdet WITH (NOLOCK)
      ON pdet.NoBBPartial = pmap.NoBBPartial
    LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
      ON bbh.NoBahanBaku = pdet.NoBahanBaku AND bbh.NoPallet = pdet.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpp WITH (NOLOCK)
      ON jpp.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE pmap.NoProduksi = @no
    ORDER BY pmap.NoBBPartial DESC;

    -- Gilingan partial → jenis gilingan
    SELECT
      gmap.NoGilinganPartial,
      gdet.NoGilingan,
      gdet.Berat,
      gh.IdGilingan    AS IdJenis,
      mg.NamaGilingan  AS NamaJenis
    FROM dbo.MixerProduksiInputGilinganPartial gmap WITH (NOLOCK)
    LEFT JOIN dbo.GilinganPartial gdet WITH (NOLOCK)
      ON gdet.NoGilinganPartial = gmap.NoGilinganPartial
    LEFT JOIN dbo.Gilingan gh      WITH (NOLOCK) ON gh.NoGilingan = gdet.NoGilingan
    LEFT JOIN dbo.MstGilingan mg   WITH (NOLOCK) ON mg.IdGilingan = gh.IdGilingan
    WHERE gmap.NoProduksi = @no
    ORDER BY gmap.NoGilinganPartial DESC;

    -- Mixer partial → jenis mixer
    SELECT
      mmap.NoMixerPartial,
      mdet.NoMixer,
      mdet.NoSak,
      mdet.Berat,
      mh.IdMixer  AS IdJenis,
      mm.Jenis    AS NamaJenis
    FROM dbo.MixerProduksiInputMixerPartial mmap WITH (NOLOCK)
    LEFT JOIN dbo.MixerPartial mdet WITH (NOLOCK)
      ON mdet.NoMixerPartial = mmap.NoMixerPartial
    LEFT JOIN dbo.Mixer_h mh  WITH (NOLOCK)
      ON mh.NoMixer = mdet.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE mmap.NoProduksi = @no
    ORDER BY mmap.NoMixerPartial DESC;

    -- Broker partial → jenis plastik dari header broker
    SELECT
      bmap.NoBrokerPartial,
      bdet.NoBroker,
      bdet.NoSak,
      bdet.Berat,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.MixerProduksiInputBrokerPartial bmap WITH (NOLOCK)
    LEFT JOIN dbo.BrokerPartial bdet WITH (NOLOCK)
      ON bdet.NoBrokerPartial = bmap.NoBrokerPartial
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = bdet.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE bmap.NoProduksi = @no
    ORDER BY bmap.NoBrokerPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bbPart   = rs.recordsets?.[1] || [];
  const gilPart  = rs.recordsets?.[2] || [];
  const mixPart  = rs.recordsets?.[3] || [];
  const brkPart  = rs.recordsets?.[4] || [];

  const out = {
    broker: [],
    bb: [],
    gilingan: [],
    mixer: [],
    summary: { broker: 0, bb: 0, gilingan: 0, mixer: 0 },
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
      case 'broker':
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'bb':
        out.bb.push({ noBahanBaku: r.Ref1, noPallet: r.Ref2, noSak: r.Ref3, ...base });
        break;
      case 'gilingan':
        out.gilingan.push({ noGilingan: r.Ref1, ...base });
        break;
      case 'mixer':
        out.mixer.push({ noMixer: r.Ref1, noSak: r.Ref2, ...base });
        break;
    }
  }

  // PARTIAL rows
  for (const p of bbPart) {
    out.bb.push({
      noBBPartial: p.NoBBPartial,
      noBahanBaku: p.NoBahanBaku ?? null,
      noPallet: p.NoPallet ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  for (const p of gilPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan: p.NoGilingan ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  for (const p of mixPart) {
    out.mixer.push({
      noMixerPartial: p.NoMixerPartial,
      noMixer: p.NoMixer ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  for (const p of brkPart) {
    out.broker.push({
      noBrokerPartial: p.NoBrokerPartial,
      noBroker: p.NoBroker ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // Summary
  for (const k of Object.keys(out.summary)) out.summary[k] = out[k].length;

  return out;
}



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

  let prefix = '';
  if (raw.substring(0, 3).toUpperCase() === 'BF.') {
    prefix = 'BF.';
  } else {
    prefix = raw.substring(0, 2).toUpperCase();
  }

  let query = '';
  let tableName = '';

  // Helper eksekusi single-query (untuk semua prefix selain A. yang butuh dua input)
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
    // D. Broker_d
    // =========================
    case 'D.':
      tableName = 'Broker_d';
      query = `
      ;WITH PartialSum AS (
        SELECT
            bp.NoBroker,
            bp.NoSak,
            SUM(ISNULL(bp.Berat, 0)) AS BeratPartial
        FROM dbo.BrokerPartial AS bp WITH (NOLOCK)
        GROUP BY bp.NoBroker, bp.NoSak
      )
      SELECT
          d.NoBroker                    AS noBroker,
          d.NoSak                       AS noSak,
          CAST(d.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          d.DateUsage                   AS dateUsage,
          CASE 
            WHEN ISNULL(ps.BeratPartial, 0) > 0 
              THEN CAST(1 AS bit) 
            ELSE CAST(0 AS bit) 
          END                           AS isPartial,
          h.IdJenisPlastik              AS idJenis,
          jp.Jenis                      AS namaJenis
      FROM dbo.Broker_d AS d WITH (NOLOCK)
      LEFT JOIN PartialSum AS ps
        ON ps.NoBroker = d.NoBroker
       AND ps.NoSak    = d.NoSak
      LEFT JOIN dbo.Broker_h AS h WITH (NOLOCK)
        ON h.NoBroker = d.NoBroker
      LEFT JOIN dbo.MstJenisPlastik AS jp WITH (NOLOCK)
        ON jp.IdJenisPlastik = h.IdJenisPlastik
      WHERE d.NoBroker = @labelCode
        AND d.DateUsage IS NULL
        AND (d.Berat - ISNULL(ps.BeratPartial, 0)) > 0
      ORDER BY d.NoBroker, d.NoSak;
      `;
      return await run(raw);

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

    // =========================
    // F. Crusher
    // =========================
    case 'F.':
      tableName = 'Crusher';
      query = `
        SELECT
          c.NoCrusher,
          c.DateCreate,
          c.IdCrusher      AS idJenis,
          mc.NamaCrusher   AS namaJenis,
          c.IdWarehouse,
          c.DateUsage,
          c.Berat,
          c.IdStatus,
          c.Blok,
          c.IdLokasi,
          c.CreateBy,
          c.DateTimeCreate
        FROM dbo.Crusher AS c WITH (NOLOCK)
        LEFT JOIN dbo.MstCrusher AS mc WITH (NOLOCK)
          ON mc.IdCrusher = c.IdCrusher
        WHERE c.NoCrusher = @labelCode
          AND c.DateUsage IS NULL
        ORDER BY c.NoCrusher;
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

    // =========================
    // H. Mixer_d
    // =========================
    case 'H.':
      tableName = 'Mixer_d';
      query = `
      ;WITH PartialSum AS (
        SELECT
            mp.NoMixer,
            mp.NoSak,
            SUM(ISNULL(mp.Berat, 0)) AS BeratPartial
        FROM dbo.MixerPartial AS mp WITH (NOLOCK)
        GROUP BY mp.NoMixer, mp.NoSak
      )
      SELECT
          d.NoMixer                       AS noMixer,
          d.NoSak                         AS noSak,
          CAST(d.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          d.DateUsage                     AS dateUsage,
          CASE WHEN ISNULL(ps.BeratPartial, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS isPartial,
          d.IdLokasi                      AS idLokasi,
          h.IdMixer                       AS idJenis,
          mm.Jenis                        AS namaJenis
      FROM dbo.Mixer_d AS d WITH (NOLOCK)
      LEFT JOIN PartialSum AS ps
        ON ps.NoMixer = d.NoMixer
      AND ps.NoSak   = d.NoSak
      LEFT JOIN dbo.Mixer_h AS h WITH (NOLOCK)
        ON h.NoMixer = d.NoMixer
      LEFT JOIN dbo.MstMixer AS mm WITH (NOLOCK)
        ON mm.IdMixer = h.IdMixer
      WHERE d.NoMixer = @labelCode
        AND d.DateUsage IS NULL
        AND (d.Berat - ISNULL(ps.BeratPartial, 0)) > 0
      ORDER BY d.NoMixer, d.NoSak;
      `;
      return await run(raw);

    // =========================
    // BF. RejectV2
    // =========================
    case 'BF.':
      tableName = 'RejectV2';
      query = `
      ;WITH PartialSum AS (
        SELECT
            rp.NoReject,
            SUM(ISNULL(rp.Berat, 0)) AS BeratPartial
        FROM dbo.RejectV2Partial AS rp WITH (NOLOCK)
        WHERE rp.NoReject = @labelCode
        GROUP BY rp.NoReject
      )
      SELECT
          r.NoReject,
          r.IdReject       AS idJenis,
          mr.NamaReject    AS namaJenis,
          r.DateCreate,
          r.DateUsage,
          r.IdWarehouse,
          CAST(r.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          r.Jam,
          r.CreateBy,
          r.DateTimeCreate,
          r.Blok,
          r.IdLokasi,
          CASE 
            WHEN ISNULL(ps.BeratPartial, 0) > 0 
              THEN CAST(1 AS bit) 
            ELSE CAST(0 AS bit) 
          END              AS isPartial
      FROM dbo.RejectV2 AS r WITH (NOLOCK)
      LEFT JOIN PartialSum AS ps
        ON ps.NoReject = r.NoReject
      LEFT JOIN dbo.MstReject AS mr WITH (NOLOCK)
        ON mr.IdReject = r.IdReject
      WHERE r.NoReject = @labelCode
        AND r.DateUsage IS NULL
        AND (r.Berat - ISNULL(ps.BeratPartial, 0)) > 0   -- hanya yang masih ada sisa berat
      ORDER BY r.NoReject;
      `;
      return await run(raw);


    default:
      throw new Error(`Invalid prefix: ${prefix}. Valid prefixes: A., B., D., M., F., V., H., BF.`);
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
  return sharedInputService.upsertInputsAndPartials('mixerProduksi', no, body, {
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
  return sharedInputService.deleteInputsAndPartials('mixerProduksi', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

module.exports = { getProduksiByDate, getAllProduksi, createMixerProduksi, updateMixerProduksi, deleteMixerProduksi, fetchInputs, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials };

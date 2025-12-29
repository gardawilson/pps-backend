// services/gilingan-production-service.js
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
      h.Jam,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter
    FROM [dbo].[GilinganProduksi_h] AS h
    LEFT JOIN [dbo].[MstMesin] AS m
      ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.Jam ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}


/**
 * Paginated fetch for dbo.GilinganProduksi_h
 * Kolom yang tersedia:
 *  NoProduksi, Tanggal, IdMesin, IdOperator, Jam, Shift, CreateBy,
 *  CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter,
 *  HourStart, HourEnd
 *
 * Kita LEFT JOIN ke masters dan ALIAS Jam -> JamKerja untuk kompatibilitas UI.
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

  // 1) Count
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.GilinganProduksi_h h WITH (NOLOCK)
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
      h.Jam          AS JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd,

      -- (opsional utk FE)
      lc.LastClosedDate AS LastClosedDate,

      -- ✅ flag tutup transaksi (pakai kolom asli: h.Tanggal)
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.GilinganProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    -- rekomendasi: urut by tanggal + jam + no
    ORDER BY h.Tanggal DESC, h.Jam ASC, h.NoProduksi DESC
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
 * Generate next NoProduksi untuk Gilingan
 * Contoh: W.0000000123
 */
async function generateNextNoProduksi(tx, { prefix = 'W.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoProduksi
    FROM dbo.GilinganProduksi_h AS h WITH (UPDLOCK, HOLDLOCK)
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
 * Sama seperti broker
 * jam bisa:
 *  - number => jam langsung (8)
 *  - "HH:mm-HH:mm" => dihitung selisih jam
 *  - "HH:mm" => ambil jam-nya saja
 */
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

/**
 * CREATE header GilinganProduksi_h
 * payload field-nya sama dengan broker:
 *  tglProduksi, idMesin, idOperator, shift, ...
 */
async function createGilinganProduksi(payload) {
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
    // 0) NORMALIZE DATE (DATE-ONLY) + TUTUP TRANSAKSI GUARD
    // -------------------------------------------------------
    const effectiveDate = resolveEffectiveDateForCreate(payload.tglProduksi);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create GilinganProduksi',
      useLock: true, // create = write
    });

    // -------------------------------------------------------
    // 1) GENERATE NO PRODUKSI (W.) + ANTI RACE
    // -------------------------------------------------------
    const no1 = await generateNextNoProduksi(tx, { prefix: 'W.', width: 10 });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.GilinganProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksi(tx, { prefix: 'W.', width: 10 })
      : no1;

    // -------------------------------------------------------
    // 2) INSERT HEADER (PAKAI effectiveDate)
    // -------------------------------------------------------
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),    noProduksi)
      .input('Tanggal',     sql.Date,           effectiveDate) // ✅ date-only
      .input('IdMesin',     sql.Int,            payload.idMesin)
      .input('IdOperator',  sql.Int,            payload.idOperator)
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
      INSERT INTO dbo.GilinganProduksi_h (
        NoProduksi, Tanggal, IdMesin, IdOperator, Shift,
        CreateBy, CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi, @Tanggal, @IdMesin, @IdOperator, @Shift,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @JmlhAnggota, @Hadir, @HourMeter,
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
 * UPDATE header GilinganProduksi_h
 * - Tanpa kolom Jam
 * - Wajib kirim field utama, sama seperti create
 */
async function updateGilinganProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'gilinganProduksi', // ✅ harus ada di config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // UPDATE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) Jika user ubah tanggal -> hitung date-only barunya
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
      action: 'update GilinganProduksi (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update GilinganProduksi (new date)',
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

    rqUpd.input('NoProduksi', sql.VarChar(50), noProduksi);

    const updateSql = `
      UPDATE dbo.GilinganProduksi_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.GilinganProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // -------------------------------------------------------
    // 4) Kalau tanggal berubah -> sync DateUsage input
    //    pakai tanggal hasil DB (stabil)
    // -------------------------------------------------------
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('Tanggal', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BROKER (FULL + PARTIAL) sebagai input GILINGAN
        -------------------------------------------------------
        UPDATE br
        SET br.DateUsage = @Tanggal
        FROM dbo.Broker_d AS br
        WHERE br.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputBroker AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoBroker   = br.NoBroker
                AND map.NoSak      = br.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputBrokerPartial AS mp
              JOIN dbo.BrokerPartial AS bp
                ON bp.NoBrokerPartial = mp.NoBrokerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND bp.NoBroker   = br.NoBroker
                AND bp.NoSak      = br.NoSak
            )
          );

        -------------------------------------------------------
        -- BONGGOLAN (FULL ONLY) sebagai input GILINGAN
        -------------------------------------------------------
        UPDATE b
        SET b.DateUsage = @Tanggal
        FROM dbo.Bonggolan AS b
        WHERE b.DateUsage IS NULL -- Biasanya diupdate jika belum digunakan
          AND EXISTS (
            SELECT 1
            順 FROM dbo.GilinganProduksiInputBonggolan AS map
            WHERE map.NoProduksi  = @NoProduksi
              AND map.NoBonggolan = b.NoBonggolan
          );

        -------------------------------------------------------
        -- CRUSHER (FULL ONLY) sebagai input GILINGAN
        -------------------------------------------------------
        UPDATE c
        SET c.DateUsage = @Tanggal
        FROM dbo.Crusher AS c
        WHERE c.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputCrusher AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoCrusher  = c.NoCrusher
          );

        -------------------------------------------------------
        -- REJECT (FULL + PARTIAL) sebagai input GILINGAN
        -------------------------------------------------------
        UPDATE r
        SET r.DateUsage = @Tanggal
        FROM dbo.RejectV2 AS r
        WHERE r.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputRejectV2 AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoReject   = r.NoReject
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputRejectV2Partial AS mp
              JOIN dbo.RejectV2Partial AS rp
                ON rp.NoRejectPartial = mp.NoRejectPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND rp.NoReject   = r.NoReject
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



async function deleteGilinganProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    Ini sekaligus memastikan header ada (throwIfNotFound)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'gilinganProduksi', // ✅ harus ada di config tutup-transaksi
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
      action: 'delete GilinganProduksi',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) (OPSIONAL) CEK OUTPUT kalau ada tabel output gilingan
    //    Kalau belum ada tabel output, boleh hapus blok ini.
    // -------------------------------------------------------
    /*
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT COUNT(*) AS Cnt
        FROM dbo.GilinganProduksiOutput
        WHERE NoProduksi = @NoProduksi
      `);

    const hasOutput = (outRes.recordset?.[0]?.Cnt || 0) > 0;
    if (hasOutput) throw badReq('Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.');
    */

    // -------------------------------------------------------
    // 3) LANJUT DELETE INPUT + PARTIAL + RESET DATEUSAGE
    //    (SQL BESAR kamu tetap)
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
      SET NOCOUNT ON;

      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK MENYIMPAN KEY YANG TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BrokerKeys TABLE (NoBroker varchar(50), NoSak int);
      DECLARE @BrokerPartialKeys TABLE (NoBrokerPartial varchar(50));

      DECLARE @RejectKeys TABLE (NoReject varchar(50));
      DECLARE @RejectPartialKeys TABLE (NoRejectPartial varchar(50));

      ---------------------------------------------------------
      -- 1. BONGGOLAN (hapus mapping)
      ---------------------------------------------------------
      DELETE FROM dbo.GilinganProduksiInputBonggolan
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 2. BROKER (FULL + PARTIAL) sebagai input GILINGAN
      ---------------------------------------------------------
      INSERT INTO @BrokerKeys (NoBroker, NoSak)
      SELECT DISTINCT b.NoBroker, b.NoSak
      FROM dbo.Broker_d AS b
      WHERE EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputBroker AS map
        WHERE map.NoProduksi=@NoProduksi
          AND map.NoBroker=b.NoBroker
          AND map.NoSak=b.NoSak
      )
      OR EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputBrokerPartial AS mp
        JOIN dbo.BrokerPartial AS bp
          ON bp.NoBrokerPartial = mp.NoBrokerPartial
        WHERE mp.NoProduksi=@NoProduksi
          AND bp.NoBroker=b.NoBroker
          AND bp.NoSak=b.NoSak
      );

      INSERT INTO @BrokerPartialKeys(NoBrokerPartial)
      SELECT DISTINCT mp.NoBrokerPartial
      FROM dbo.GilinganProduksiInputBrokerPartial mp
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.GilinganProduksiInputBrokerPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE bp
      FROM dbo.BrokerPartial bp
      JOIN @BrokerPartialKeys k ON k.NoBrokerPartial = bp.NoBrokerPartial
      WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputBrokerPartial mp2
        WHERE mp2.NoBrokerPartial = bp.NoBrokerPartial
      );

      DELETE FROM dbo.GilinganProduksiInputBroker
      WHERE NoProduksi = @NoProduksi;

      UPDATE b
      SET
        b.DateUsage = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputBroker mb
            WHERE mb.NoBroker=b.NoBroker AND mb.NoSak=b.NoSak
          ) THEN b.DateUsage
          WHEN EXISTS (
            SELECT 1
            FROM dbo.BrokerPartial bp2
            JOIN dbo.GilinganProduksiInputBrokerPartial mp2
              ON mp2.NoBrokerPartial = bp2.NoBrokerPartial
            WHERE bp2.NoBroker=b.NoBroker AND bp2.NoSak=b.NoSak
          ) THEN b.DateUsage
          ELSE NULL
        END,
        b.IsPartial = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.BrokerPartial bp3
            WHERE bp3.NoBroker=b.NoBroker AND bp3.NoSak=b.NoSak
          ) THEN 1 ELSE 0 END
      FROM dbo.Broker_d b
      JOIN @BrokerKeys k ON k.NoBroker=b.NoBroker AND k.NoSak=b.NoSak;

      ---------------------------------------------------------
      -- 3. CRUSHER (FULL ONLY) sebagai input GILINGAN
      ---------------------------------------------------------
      UPDATE c
      SET c.DateUsage = CASE
        WHEN EXISTS (
          SELECT 1
          FROM dbo.GilinganProduksiInputCrusher m2
          WHERE m2.NoCrusher = c.NoCrusher
            AND m2.NoProduksi <> @NoProduksi
        ) THEN c.DateUsage
        ELSE NULL
      END
      FROM dbo.Crusher c
      JOIN dbo.GilinganProduksiInputCrusher map
        ON map.NoCrusher = c.NoCrusher
      WHERE map.NoProduksi = @NoProduksi;

      DELETE FROM dbo.GilinganProduksiInputCrusher
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 4. REJECT (FULL + PARTIAL) sebagai input GILINGAN
      ---------------------------------------------------------
      INSERT INTO @RejectKeys (NoReject)
      SELECT DISTINCT r.NoReject
      FROM dbo.RejectV2 r
      WHERE EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputRejectV2 map
        WHERE map.NoProduksi=@NoProduksi
          AND map.NoReject=r.NoReject
      )
      OR EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputRejectV2Partial mp
        JOIN dbo.RejectV2Partial rp
          ON rp.NoRejectPartial = mp.NoRejectPartial
        WHERE mp.NoProduksi=@NoProduksi
          AND rp.NoReject=r.NoReject
      );

      INSERT INTO @RejectPartialKeys(NoRejectPartial)
      SELECT DISTINCT mp.NoRejectPartial
      FROM dbo.GilinganProduksiInputRejectV2Partial mp
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.GilinganProduksiInputRejectV2Partial
      WHERE NoProduksi = @NoProduksi;

      DELETE rp
      FROM dbo.RejectV2Partial rp
      JOIN @RejectPartialKeys k ON k.NoRejectPartial = rp.NoRejectPartial
      WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputRejectV2Partial mp2
        WHERE mp2.NoRejectPartial = rp.NoRejectPartial
      );

      DELETE FROM dbo.GilinganProduksiInputRejectV2
      WHERE NoProduksi = @NoProduksi;

      UPDATE r
      SET
        r.DateUsage = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputRejectV2 m2
            WHERE m2.NoReject = r.NoReject
              AND m2.NoProduksi <> @NoProduksi
          ) THEN r.DateUsage
          WHEN EXISTS (
            SELECT 1
            FROM dbo.RejectV2Partial rp2
            JOIN dbo.GilinganProduksiInputRejectV2Partial mp2
              ON mp2.NoRejectPartial = rp2.NoRejectPartial
            WHERE rp2.NoReject = r.NoReject
          ) THEN r.DateUsage
          ELSE NULL
        END,
        r.IsPartial = CASE
          WHEN EXISTS (SELECT 1 FROM dbo.RejectV2Partial rp3 WHERE rp3.NoReject = r.NoReject)
          THEN 1 ELSE 0 END
      FROM dbo.RejectV2 r
      JOIN @RejectKeys k ON k.NoReject = r.NoReject;

      ---------------------------------------------------------
      -- 5. TERAKHIR: HAPUS HEADER
      ---------------------------------------------------------
      DELETE FROM dbo.GilinganProduksi_h
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
    SELECT 
      'broker' AS Src,
      ib.NoProduksi,
      ib.NoBroker AS Ref1,
      ib.NoSak    AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      bd.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      bd.IsPartial AS IsPartial,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis,
      CAST(NULL AS datetime) AS DatetimeInput
    FROM dbo.GilinganProduksiInputBroker ib WITH (NOLOCK)
    LEFT JOIN dbo.Broker_d bd        WITH (NOLOCK)
      ON bd.NoBroker = ib.NoBroker AND bd.NoSak = ib.NoSak
    LEFT JOIN dbo.Broker_h bh        WITH (NOLOCK)
      ON bh.NoBroker = ib.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE ib.NoProduksi=@no

    UNION ALL
    SELECT
      'bonggolan' AS Src,
      ibg.NoProduksi,
      ibg.NoBonggolan AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      bg.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      bg.IdBonggolan AS IdJenis,
      mbg.NamaBonggolan AS NamaJenis,
      ibg.DatetimeInput AS DatetimeInput
    FROM dbo.GilinganProduksiInputBonggolan ibg WITH (NOLOCK)
    /* TODO: sesuaikan tabel master/detail bonggolan anda */
    LEFT JOIN dbo.Bonggolan bg     WITH (NOLOCK) ON bg.NoBonggolan = ibg.NoBonggolan
    LEFT JOIN dbo.MstBonggolan mbg WITH (NOLOCK) ON mbg.IdBonggolan = bg.IdBonggolan
    WHERE ibg.NoProduksi=@no

    UNION ALL
    SELECT
      'crusher' AS Src,
      ic.NoProduksi,
      ic.NoCrusher AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      c.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      c.IdCrusher    AS IdJenis,
      mc.NamaCrusher AS NamaJenis,
      ic.DatetimeInput AS DatetimeInput
    FROM dbo.GilinganProduksiInputCrusher ic WITH (NOLOCK)
    LEFT JOIN dbo.Crusher c     WITH (NOLOCK) ON c.NoCrusher = ic.NoCrusher
    LEFT JOIN dbo.MstCrusher mc WITH (NOLOCK) ON mc.IdCrusher = c.IdCrusher
    WHERE ic.NoProduksi=@no

    UNION ALL
    SELECT
      'reject' AS Src,
      ir.NoProduksi,
      ir.NoReject AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      rj.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      rj.IdReject     AS IdJenis,
      mr.NamaReject   AS NamaJenis,
      ir.DatetimeInput AS DatetimeInput
    FROM dbo.GilinganProduksiInputRejectV2 ir WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK) ON rj.NoReject = ir.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK) ON mr.IdReject = rj.IdReject
    WHERE ir.NoProduksi=@no
    ORDER BY Ref1 DESC, Ref2 ASC;

    /* ===================== [2] PARTIALS ===================== */

    /* Broker partial */
    SELECT
      pmap.NoBrokerPartial,
      pdet.NoBroker,
      pdet.NoSak,
      pdet.Berat,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.GilinganProduksiInputBrokerPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BrokerPartial pdet WITH (NOLOCK)
      ON pdet.NoBrokerPartial = pmap.NoBrokerPartial
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = pdet.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE pmap.NoProduksi = @no
    ORDER BY pmap.NoBrokerPartial DESC;

    /* Reject partial */
    SELECT
      rmap.NoRejectPartial,
      rdet.NoReject,
      rdet.Berat,
      rj.IdReject     AS IdJenis,
      mr.NamaReject   AS NamaJenis
    FROM dbo.GilinganProduksiInputRejectV2Partial rmap WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2Partial rdet WITH (NOLOCK)
      ON rdet.NoRejectPartial = rmap.NoRejectPartial
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK)
      ON rj.NoReject = rdet.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK)
      ON mr.IdReject = rj.IdReject
    WHERE rmap.NoProduksi = @no
    ORDER BY rmap.NoRejectPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows   = rs.recordsets?.[0] || [];
  const brkPartial = rs.recordsets?.[1] || [];
  const rejPartial = rs.recordsets?.[2] || [];

  const out = {
    broker: [],
    bonggolan: [],
    crusher: [],
    reject: [],
    summary: { broker: 0, bonggolan: 0, crusher: 0, reject: 0 },
  };

  // MAIN rows
  for (const r of mainRows) {
    const base = {
      berat: r.Berat ?? null,
      beratAct: r.BeratAct ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis: r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
      datetimeInput: r.DatetimeInput ?? null,
    };

    switch (r.Src) {
      case 'broker':
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'bonggolan':
        out.bonggolan.push({ noBonggolan: r.Ref1, ...base });
        break;
      case 'crusher':
        out.crusher.push({ noCrusher: r.Ref1, ...base });
        break;
      case 'reject':
        out.reject.push({ noReject: r.Ref1, ...base });
        break;
    }
  }

  // PARTIAL rows (merge into same bucket)
  for (const p of brkPartial) {
    out.broker.push({
      noBrokerPartial: p.NoBrokerPartial,
      noBroker: p.NoBroker ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  for (const p of rejPartial) {
    out.reject.push({
      noRejectPartial: p.NoRejectPartial,
      noReject: p.NoReject ?? null,
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

/**
 * Payload shape:
 * {
 *   // existing inputs to attach
 *   broker:    [{ noBroker, noSak }],
 *   bonggolan: [{ noBonggolan, datetimeInput? }],
 *   crusher:   [{ noCrusher, datetimeInput? }],
 *   reject:    [{ noReject, datetimeInput? }],
 *
 *   // NEW partials to create + map
 *   brokerPartialNew: [{ noBroker, noSak, berat }],
 *   rejectPartialNew: [{ noReject, berat }]
 * }
 */

async function upsertInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    broker: norm(payload?.broker),
    bonggolan: norm(payload?.bonggolan),
    crusher: norm(payload?.crusher),
    reject: norm(payload?.reject),

    brokerPartialNew: norm(payload?.brokerPartialNew),
    rejectPartialNew: norm(payload?.rejectPartialNew),
  };

  try {
    // ✅ penting: serializable biar konsisten + lock range
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    //    Ini memastikan header ada + jadi acuan tutup transaksi
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'gilinganProduksi', // ✅ pastikan ada di config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // UPSERT = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (UPSERT INPUT/PARTIAL = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx, // WAJIB tx
      action: 'upsert GilinganProduksi inputs/partials',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) Create partials + map them to produksi
    // -------------------------------------------------------
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      brokerPartialNew: body.brokerPartialNew,
      rejectPartialNew: body.rejectPartialNew,
    });

    // -------------------------------------------------------
    // 3) Attach existing inputs (idempotent)
    // -------------------------------------------------------
    const attachments = await _insertInputsWithTx(tx, noProduksi, {
      broker: body.broker,
      bonggolan: body.bonggolan,
      crusher: body.crusher,
      reject: body.reject,
    });

    await tx.commit();

    // ===== response kamu tetap =====
    const totalInserted = Object.values(attachments).reduce((s, x) => s + (x.inserted || 0), 0);
    const totalSkipped  = Object.values(attachments).reduce((s, x) => s + (x.skipped  || 0), 0);
    const totalInvalid  = Object.values(attachments).reduce((s, x) => s + (x.invalid  || 0), 0);
    const totalPartialsCreated = Object.values(partials.summary || {}).reduce((s, x) => s + (x.created || 0), 0);

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = totalInserted === 0 && totalPartialsCreated === 0;

    const response = {
      noProduksi,
      summary: { totalInserted, totalSkipped, totalInvalid, totalPartialsCreated },
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

function _buildInputDetails(attachments, requestBody) {
  const details = [];
  const sections = [
    { key: 'broker', label: 'Broker' },
    { key: 'bonggolan', label: 'Bonggolan' },
    { key: 'crusher', label: 'Crusher' },
    { key: 'reject', label: 'Reject' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (requested === 0) continue;

    const r = attachments[s.key] || { inserted: 0, skipped: 0, invalid: 0 };

    details.push({
      section: s.key,
      label: s.label,
      requested,
      inserted: r.inserted,
      skipped: r.skipped,
      invalid: r.invalid,
      status: r.invalid > 0 ? 'error' : r.skipped > 0 ? 'warning' : 'success',
      message: _buildSectionMessage(s.label, r, requested),
    });
  }
  return details;
}

function _buildPartialDetails(partials, requestBody) {
  const details = [];
  const sections = [
    { key: 'brokerPartialNew', label: 'Broker Partial' },
    { key: 'rejectPartialNew', label: 'Reject Partial' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (requested === 0) continue;

    const created = partials.summary[s.key]?.created || 0;

    details.push({
      section: s.key,
      label: s.label,
      requested,
      created,
      status: created === requested ? 'success' : 'error',
      message: `${created} dari ${requested} ${s.label} berhasil dibuat`,
      codes: partials.createdLists[s.key] || [],
    });
  }
  return details;
}

function _buildSectionMessage(label, result) {
  const parts = [];
  if (result.inserted > 0) parts.push(`${result.inserted} berhasil ditambahkan`);
  if (result.skipped > 0) parts.push(`${result.skipped} sudah ada (dilewati)`);
  if (result.invalid > 0) parts.push(`${result.invalid} tidak valid (tidak ditemukan)`);
  return parts.length ? `${label}: ${parts.join(', ')}` : `Tidak ada ${label} yang diproses`;
}

/* ==========================
   PARTIALS (create + map)
========================== */

async function _insertPartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_PARTIALS = `
  SET NOCOUNT ON;

  -- Get tanggal produksi from header (sesuaikan kalau nama kolom beda)
  DECLARE @tglProduksi datetime;
  SELECT @tglProduksi = Tanggal
  FROM dbo.GilinganProduksi_h WITH (NOLOCK)
  WHERE NoProduksi = @no;

  -- lock global sequence
  DECLARE @lockResult int;
  EXEC @lockResult = sp_getapplock
    @Resource = 'SEQ_PARTIALS',
    @LockMode = 'Exclusive',
    @LockTimeout = 10000,
    @DbPrincipal = 'public';
  IF (@lockResult < 0) RAISERROR('Failed to acquire SEQ_PARTIALS lock', 16, 1);

  DECLARE @broNew TABLE(NoBrokerPartial varchar(50));
  DECLARE @rejNew TABLE(NoRejectPartial varchar(50));

  /* =========================
     BROKER PARTIAL (Q.##########)
     ========================= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.brokerPartialNew'))
  BEGIN
    DECLARE @nextBr int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoBrokerPartial,10) AS int))
      FROM dbo.BrokerPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBrokerPartial LIKE 'Q.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noBroker, noSak, berat,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.brokerPartialNew')
      WITH (
        noBroker varchar(50)   '$.noBroker',
        noSak    int           '$.noSak',
        berat    decimal(18,3) '$.berat'
      )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('Q.', RIGHT(REPLICATE('0',10) + CAST(@nextBr + rn AS varchar(10)), 10)),
        noBroker, noSak, berat
      FROM src
    )
    INSERT INTO dbo.BrokerPartial (NoBrokerPartial, NoBroker, NoSak, Berat)
    OUTPUT INSERTED.NoBrokerPartial INTO @broNew(NoBrokerPartial)
    SELECT NewNo, noBroker, noSak, berat
    FROM numbered;

    -- map ke GILINGAN produksi
    INSERT INTO dbo.GilinganProduksiInputBrokerPartial (NoProduksi, NoBrokerPartial)
    SELECT @no, n.NoBrokerPartial FROM @broNew n;

    -- update broker_d (IsPartial + DateUsage if habis)
    ;WITH existingPartials AS (
      SELECT NoBroker, NoSak, SUM(ISNULL(Berat,0)) AS TotalExisting
      FROM dbo.BrokerPartial WITH (NOLOCK)
      WHERE NoBrokerPartial NOT IN (SELECT NoBrokerPartial FROM @broNew)
      GROUP BY NoBroker, NoSak
    ),
    newPartials AS (
      SELECT noBroker, noSak, SUM(berat) AS TotalNew
      FROM OPENJSON(@jsPartials, '$.brokerPartialNew')
      WITH (
        noBroker varchar(50)   '$.noBroker',
        noSak    int           '$.noSak',
        berat    decimal(18,3) '$.berat'
      )
      GROUP BY noBroker, noSak
    )
    UPDATE d
    SET
      d.IsPartial = 1,
      d.DateUsage = CASE
        WHEN (d.Berat - ISNULL(ep.TotalExisting,0) - ISNULL(np.TotalNew,0)) <= 0.001
        THEN @tglProduksi ELSE d.DateUsage
      END
    FROM dbo.Broker_d d
    LEFT JOIN existingPartials ep ON ep.NoBroker=d.NoBroker AND ep.NoSak=d.NoSak
    INNER JOIN newPartials np ON np.noBroker=d.NoBroker AND np.noSak=d.NoSak;
  END;

  /* =========================
     REJECT PARTIAL (BK.##########)
     ========================= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.rejectPartialNew'))
  BEGIN
    DECLARE @nextRj int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoRejectPartial,10) AS int))
      FROM dbo.RejectV2Partial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoRejectPartial LIKE 'BK.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noReject, berat,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.rejectPartialNew')
      WITH (
        noReject varchar(50)   '$.noReject',
        berat    decimal(18,3) '$.berat'
      )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('BK.', RIGHT(REPLICATE('0',10) + CAST(@nextRj + rn AS varchar(10)), 10)),
        noReject, berat
      FROM src
    )
    INSERT INTO dbo.RejectV2Partial (NoRejectPartial, NoReject, Berat)
    OUTPUT INSERTED.NoRejectPartial INTO @rejNew(NoRejectPartial)
    SELECT NewNo, noReject, berat
    FROM numbered;

    -- map ke GILINGAN produksi
    INSERT INTO dbo.GilinganProduksiInputRejectV2Partial (NoProduksi, NoRejectPartial)
    SELECT @no, n.NoRejectPartial FROM @rejNew n;

    -- update RejectV2 (IsPartial + DateUsage if habis)
    ;WITH existingPartials AS (
      SELECT NoReject, SUM(ISNULL(Berat,0)) AS TotalExisting
      FROM dbo.RejectV2Partial WITH (NOLOCK)
      WHERE NoRejectPartial NOT IN (SELECT NoRejectPartial FROM @rejNew)
      GROUP BY NoReject
    ),
    newPartials AS (
      SELECT noReject, SUM(berat) AS TotalNew
      FROM OPENJSON(@jsPartials, '$.rejectPartialNew')
      WITH (
        noReject varchar(50)   '$.noReject',
        berat    decimal(18,3) '$.berat'
      )
      GROUP BY noReject
    )
    UPDATE r
    SET
      r.IsPartial = 1,
      r.DateUsage = CASE
        WHEN (r.Berat - ISNULL(ep.TotalExisting,0) - ISNULL(np.TotalNew,0)) <= 0.001
        THEN @tglProduksi ELSE r.DateUsage
      END
    FROM dbo.RejectV2 r
    LEFT JOIN existingPartials ep ON ep.NoReject=r.NoReject
    INNER JOIN newPartials np ON np.noReject=r.NoReject;
  END;

  EXEC sp_releaseapplock @Resource='SEQ_PARTIALS', @DbPrincipal='public';

  -- summary
  SELECT 'brokerPartialNew' AS Section, COUNT(*) AS Created FROM @broNew
  UNION ALL
  SELECT 'rejectPartialNew' AS Section, COUNT(*) FROM @rejNew;

  -- return codes recordsets
  SELECT NoBrokerPartial FROM @broNew;  -- recordsets[1]
  SELECT NoRejectPartial FROM @rejNew;  -- recordsets[2]
  `;

  const rs = await req.query(SQL_PARTIALS);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  const createdLists = {
    brokerPartialNew: (rs.recordsets?.[1] || []).map(r => r.NoBrokerPartial),
    rejectPartialNew: (rs.recordsets?.[2] || []).map(r => r.NoRejectPartial),
  };

  return { summary, createdLists };
}

/* ==========================
   INPUTS (attach existing)
========================== */

async function _insertInputsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_ATTACH = `
  SET NOCOUNT ON;

  DECLARE @tglProduksi datetime;
  SELECT @tglProduksi = Tanggal
  FROM dbo.GilinganProduksi_h WITH (NOLOCK)
  WHERE NoProduksi = @no;

  DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);

  /* ========= BROKER (NoBroker + NoSak) ========= */
  DECLARE @brokerInserted int=0, @brokerSkipped int=0, @brokerInvalid int=0;

  ;WITH j AS (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK) WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak)
  )
  INSERT INTO dbo.GilinganProduksiInputBroker (NoProduksi, NoBroker, NoSak)
  SELECT @no, v.noBroker, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.GilinganProduksiInputBroker x
    WHERE x.NoProduksi=@no AND x.NoBroker=v.noBroker AND x.NoSak=v.noSak
  );

  SET @brokerInserted = @@ROWCOUNT;

  IF @brokerInserted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tglProduksi
    FROM dbo.Broker_d b
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.broker')
      WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' ) src
      WHERE b.NoBroker=src.noBroker AND b.NoSak=src.noSak
    );
  END;

  SELECT @brokerSkipped = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.broker')
  WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK) WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak)
    AND EXISTS (SELECT 1 FROM dbo.GilinganProduksiInputBroker x WHERE x.NoProduksi=@no AND x.NoBroker=j.noBroker AND x.NoSak=j.noSak);

  SELECT @brokerInvalid = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.broker')
  WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK) WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak);

  INSERT INTO @out SELECT 'broker', @brokerInserted, @brokerSkipped, @brokerInvalid;

  /* ========= BONGGOLAN (NoBonggolan + DatetimeInput) ========= */
  DECLARE @bongInserted int=0, @bongSkipped int=0, @bongInvalid int=0;

  ;WITH j AS (
    SELECT noBonggolan, datetimeInput
    FROM OPENJSON(@jsInputs, '$.bonggolan')
    WITH (
      noBonggolan varchar(50) '$.noBonggolan',
      datetimeInput datetime  '$.datetimeInput'
    )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan)
  )
  INSERT INTO dbo.GilinganProduksiInputBonggolan (NoProduksi, NoBonggolan, DatetimeInput)
  SELECT @no, v.noBonggolan, ISNULL(v.datetimeInput, GETDATE())
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.GilinganProduksiInputBonggolan x
    WHERE x.NoProduksi=@no AND x.NoBonggolan=v.noBonggolan
  );

  SET @bongInserted = @@ROWCOUNT;

  -- OPTIONAL: kalau Bonggolan punya DateUsage
   IF @bongInserted > 0
   BEGIN
     UPDATE b SET b.DateUsage=@tglProduksi
     FROM dbo.Bonggolan b
     WHERE EXISTS (
       SELECT 1 FROM OPENJSON(@jsInputs,'$.bonggolan')
       WITH (noBonggolan varchar(50) '$.noBonggolan') src
       WHERE b.NoBonggolan=src.noBonggolan
     );
   END;

  SELECT @bongSkipped = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.bonggolan')
  WITH ( noBonggolan varchar(50) '$.noBonggolan' ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan)
    AND EXISTS (SELECT 1 FROM dbo.GilinganProduksiInputBonggolan x WHERE x.NoProduksi=@no AND x.NoBonggolan=j.noBonggolan);

  SELECT @bongInvalid = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.bonggolan')
  WITH ( noBonggolan varchar(50) '$.noBonggolan' ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan);

  INSERT INTO @out SELECT 'bonggolan', @bongInserted, @bongSkipped, @bongInvalid;

  /* ========= CRUSHER (NoCrusher + DatetimeInput) ========= */
  DECLARE @crInserted int=0, @crSkipped int=0, @crInvalid int=0;

  ;WITH j AS (
    SELECT noCrusher, datetimeInput
    FROM OPENJSON(@jsInputs, '$.crusher')
    WITH ( noCrusher varchar(50) '$.noCrusher', datetimeInput datetime '$.datetimeInput' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher)
  )
  INSERT INTO dbo.GilinganProduksiInputCrusher (NoProduksi, NoCrusher, DatetimeInput)
  SELECT @no, v.noCrusher, ISNULL(v.datetimeInput, GETDATE())
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.GilinganProduksiInputCrusher x
    WHERE x.NoProduksi=@no AND x.NoCrusher=v.noCrusher
  );

  SET @crInserted = @@ROWCOUNT;

  -- OPTIONAL: kalau Crusher punya DateUsage
   IF @crInserted > 0
   BEGIN
     UPDATE c SET c.DateUsage=@tglProduksi
     FROM dbo.Crusher c
     WHERE EXISTS (
       SELECT 1 FROM OPENJSON(@jsInputs,'$.crusher')
       WITH (noCrusher varchar(50) '$.noCrusher') src
       WHERE c.NoCrusher=src.noCrusher
     );
   END;

  SELECT @crSkipped = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.crusher')
  WITH ( noCrusher varchar(50) '$.noCrusher' ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher)
    AND EXISTS (SELECT 1 FROM dbo.GilinganProduksiInputCrusher x WHERE x.NoProduksi=@no AND x.NoCrusher=j.noCrusher);

  SELECT @crInvalid = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.crusher')
  WITH ( noCrusher varchar(50) '$.noCrusher' ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher);

  INSERT INTO @out SELECT 'crusher', @crInserted, @crSkipped, @crInvalid;

  /* ========= REJECT (NoReject + DatetimeInput) ========= */
  DECLARE @rjInserted int=0, @rjSkipped int=0, @rjInvalid int=0;

  ;WITH j AS (
    SELECT noReject, datetimeInput
    FROM OPENJSON(@jsInputs, '$.reject')
    WITH ( noReject varchar(50) '$.noReject', datetimeInput datetime '$.datetimeInput' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.RejectV2 r WITH (NOLOCK) WHERE r.NoReject=j.noReject)
  )
  INSERT INTO dbo.GilinganProduksiInputRejectV2 (NoProduksi, NoReject, DatetimeInput)
  SELECT @no, v.noReject, ISNULL(v.datetimeInput, GETDATE())
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.GilinganProduksiInputRejectV2 x
    WHERE x.NoProduksi=@no AND x.NoReject=v.noReject
  );

  SET @rjInserted = @@ROWCOUNT;

  IF @rjInserted > 0
  BEGIN
    UPDATE r
    SET r.DateUsage = @tglProduksi
    FROM dbo.RejectV2 r
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.reject')
      WITH ( noReject varchar(50) '$.noReject' ) src
      WHERE r.NoReject = src.noReject
    );
  END;

  SELECT @rjSkipped = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.reject')
  WITH ( noReject varchar(50) '$.noReject' ) j
  WHERE EXISTS (SELECT 1 FROM dbo.RejectV2 r WITH (NOLOCK) WHERE r.NoReject=j.noReject)
    AND EXISTS (SELECT 1 FROM dbo.GilinganProduksiInputRejectV2 x WHERE x.NoProduksi=@no AND x.NoReject=j.noReject);

  SELECT @rjInvalid = COUNT(*)
  FROM OPENJSON(@jsInputs, '$.reject')
  WITH ( noReject varchar(50) '$.noReject' ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.RejectV2 r WITH (NOLOCK) WHERE r.NoReject=j.noReject);

  INSERT INTO @out SELECT 'reject', @rjInserted, @rjSkipped, @rjInvalid;

  SELECT Section, Inserted, Skipped, Invalid FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_ATTACH);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = { inserted: row.Inserted, skipped: row.Skipped, invalid: row.Invalid };
  }
  return out;
}

async function deleteInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    broker: norm(payload?.broker),
    bonggolan: norm(payload?.bonggolan),
    crusher: norm(payload?.crusher),
    reject: norm(payload?.reject),

    brokerPartial: norm(payload?.brokerPartial),
    rejectPartial: norm(payload?.rejectPartial),
  };

  try {
    // ✅ penting: serializable
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // -------------------------------------------------------
    // 0) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'gilinganProduksi', // ✅ pastikan sesuai config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,               // DELETE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 1) GUARD TUTUP TRANSAKSI (DELETE INPUT/PARTIAL = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete GilinganProduksi inputs/partials',
      useLock: true,
    });

    // -------------------------------------------------------
    // 2) Delete partials mappings (+ optional delete row partial di helper)
    // -------------------------------------------------------
    const partialsResult = await _deletePartialsWithTx(tx, noProduksi, {
      brokerPartial: body.brokerPartial,
      rejectPartial: body.rejectPartial,
    });

    // -------------------------------------------------------
    // 3) Delete inputs mappings
    // -------------------------------------------------------
    const inputsResult = await _deleteInputsWithTx(tx, noProduksi, {
      broker: body.broker,
      bonggolan: body.bonggolan,
      crusher: body.crusher,
      reject: body.reject,
    });

    await tx.commit();

    // ===== response kamu tetap =====
    const totalDeleted = Object.values(inputsResult).reduce((s, x) => s + (x.deleted || 0), 0);
    const totalNotFound = Object.values(inputsResult).reduce((s, x) => s + (x.notFound || 0), 0);

    const totalPartialsDeleted = Object.values(partialsResult.summary || {}).reduce((s, x) => s + (x.deleted || 0), 0);
    const totalPartialsNotFound = Object.values(partialsResult.summary || {}).reduce((s, x) => s + (x.notFound || 0), 0);

    const hasNotFound = totalNotFound > 0 || totalPartialsNotFound > 0;
    const hasNoSuccess = totalDeleted === 0 && totalPartialsDeleted === 0;

    const response = {
      noProduksi,
      summary: { totalDeleted, totalNotFound, totalPartialsDeleted, totalPartialsNotFound },
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

function _buildDeleteInputDetails(results, requestBody) {
  const details = [];
  const sections = [
    { key: 'broker', label: 'Broker' },
    { key: 'bonggolan', label: 'Bonggolan' },
    { key: 'crusher', label: 'Crusher' },
    { key: 'reject', label: 'Reject' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (requested === 0) continue;

    const r = results[s.key] || { deleted: 0, notFound: 0 };

    details.push({
      section: s.key,
      label: s.label,
      requested,
      deleted: r.deleted,
      notFound: r.notFound,
      status: r.notFound > 0 ? 'warning' : 'success',
      message: `${s.label}: ${r.deleted} berhasil dihapus${r.notFound > 0 ? `, ${r.notFound} tidak ditemukan` : ''}`,
    });
  }
  return details;
}


function _buildDeletePartialDetails(partialsResult, requestBody) {
  const details = [];
  const sections = [
    { key: 'brokerPartial', label: 'Broker Partial' },
    { key: 'rejectPartial', label: 'Reject Partial' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (requested === 0) continue;

    const r = partialsResult.summary[s.key] || { deleted: 0, notFound: 0 };

    details.push({
      section: s.key,
      label: s.label,
      requested,
      deleted: r.deleted,
      notFound: r.notFound,
      status: r.notFound > 0 ? 'warning' : 'success',
      message: `${s.label}: ${r.deleted} berhasil dihapus${r.notFound > 0 ? `, ${r.notFound} tidak ditemukan` : ''}`,
    });
  }
  return details;
}

/* =========================
   DELETE PARTIALS
========================= */
async function _deletePartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE_PARTIALS = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  /* ========= BROKER PARTIAL ========= */
  DECLARE @brokerDeleted int=0, @brokerNotFound int=0;

  SELECT @brokerDeleted = COUNT(*)
  FROM dbo.GilinganProduksiInputBrokerPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON map.NoBrokerPartial = j.noBrokerPartial
  WHERE map.NoProduksi=@no;

  DECLARE @deletedBrokerPartials TABLE(NoBroker varchar(50), NoSak int);

  INSERT INTO @deletedBrokerPartials(NoBroker, NoSak)
  SELECT DISTINCT bp.NoBroker, bp.NoSak
  FROM dbo.BrokerPartial bp
  INNER JOIN dbo.GilinganProduksiInputBrokerPartial map
    ON bp.NoBrokerPartial = map.NoBrokerPartial
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON map.NoBrokerPartial = j.noBrokerPartial
  WHERE map.NoProduksi=@no;

  DELETE map
  FROM dbo.GilinganProduksiInputBrokerPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON map.NoBrokerPartial = j.noBrokerPartial
  WHERE map.NoProduksi=@no;

  DELETE bp
  FROM dbo.BrokerPartial bp
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON bp.NoBrokerPartial = j.noBrokerPartial;

  IF @brokerDeleted > 0
  BEGIN
    -- masih ada partial lain? IsPartial tetap 1, DateUsage NULL
    UPDATE d
    SET d.DateUsage=NULL, d.IsPartial=1
    FROM dbo.Broker_d d
    INNER JOIN @deletedBrokerPartials del ON d.NoBroker=del.NoBroker AND d.NoSak=del.NoSak
    WHERE EXISTS (
      SELECT 1 FROM dbo.BrokerPartial bp WHERE bp.NoBroker=d.NoBroker AND bp.NoSak=d.NoSak
    );

    -- tidak ada partial lagi? IsPartial 0, DateUsage NULL
    UPDATE d
    SET d.DateUsage=NULL, d.IsPartial=0
    FROM dbo.Broker_d d
    INNER JOIN @deletedBrokerPartials del ON d.NoBroker=del.NoBroker AND d.NoSak=del.NoSak
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.BrokerPartial bp WHERE bp.NoBroker=d.NoBroker AND bp.NoSak=d.NoSak
    );
  END;

  DECLARE @brokerRequested int;
  SELECT @brokerRequested = COUNT(*) FROM OPENJSON(@jsPartials, '$.brokerPartial');
  SET @brokerNotFound = @brokerRequested - @brokerDeleted;

  INSERT INTO @out SELECT 'brokerPartial', @brokerDeleted, @brokerNotFound;

  /* ========= REJECT PARTIAL ========= */
  DECLARE @rejectDeleted int=0, @rejectNotFound int=0;

  SELECT @rejectDeleted = COUNT(*)
  FROM dbo.GilinganProduksiInputRejectV2Partial map
  INNER JOIN OPENJSON(@jsPartials, '$.rejectPartial')
    WITH (noRejectPartial varchar(50) '$.noRejectPartial') j
    ON map.NoRejectPartial = j.noRejectPartial
  WHERE map.NoProduksi=@no;

  DECLARE @deletedRejectPartials TABLE(NoReject varchar(50));

  INSERT INTO @deletedRejectPartials(NoReject)
  SELECT DISTINCT rp.NoReject
  FROM dbo.RejectV2Partial rp
  INNER JOIN dbo.GilinganProduksiInputRejectV2Partial map
    ON rp.NoRejectPartial = map.NoRejectPartial
  INNER JOIN OPENJSON(@jsPartials, '$.rejectPartial')
    WITH (noRejectPartial varchar(50) '$.noRejectPartial') j
    ON map.NoRejectPartial = j.noRejectPartial
  WHERE map.NoProduksi=@no;

  DELETE map
  FROM dbo.GilinganProduksiInputRejectV2Partial map
  INNER JOIN OPENJSON(@jsPartials, '$.rejectPartial')
    WITH (noRejectPartial varchar(50) '$.noRejectPartial') j
    ON map.NoRejectPartial = j.noRejectPartial
  WHERE map.NoProduksi=@no;

  DELETE rp
  FROM dbo.RejectV2Partial rp
  INNER JOIN OPENJSON(@jsPartials, '$.rejectPartial')
    WITH (noRejectPartial varchar(50) '$.noRejectPartial') j
    ON rp.NoRejectPartial = j.noRejectPartial;

  IF @rejectDeleted > 0
  BEGIN
    UPDATE r
    SET r.DateUsage=NULL, r.IsPartial=1
    FROM dbo.RejectV2 r
    INNER JOIN @deletedRejectPartials del ON r.NoReject=del.NoReject
    WHERE EXISTS (SELECT 1 FROM dbo.RejectV2Partial rp WHERE rp.NoReject=r.NoReject);

    UPDATE r
    SET r.DateUsage=NULL, r.IsPartial=0
    FROM dbo.RejectV2 r
    INNER JOIN @deletedRejectPartials del ON r.NoReject=del.NoReject
    WHERE NOT EXISTS (SELECT 1 FROM dbo.RejectV2Partial rp WHERE rp.NoReject=r.NoReject);
  END;

  DECLARE @rejectRequested int;
  SELECT @rejectRequested = COUNT(*) FROM OPENJSON(@jsPartials, '$.rejectPartial');
  SET @rejectNotFound = @rejectRequested - @rejectDeleted;

  INSERT INTO @out SELECT 'rejectPartial', @rejectDeleted, @rejectNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_DELETE_PARTIALS);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return { summary };
}

/* =========================
   DELETE INPUTS (mappings)
========================= */
async function _deleteInputsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE_INPUTS = `
  SET NOCOUNT ON;
  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  /* ========= BROKER ========= */
  DECLARE @brokerDeleted int=0, @brokerNotFound int=0;

  SELECT @brokerDeleted = COUNT(*)
  FROM dbo.GilinganProduksiInputBroker map
  INNER JOIN OPENJSON(@jsInputs, '$.broker')
    WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
    ON map.NoBroker=j.noBroker AND map.NoSak=j.noSak
  WHERE map.NoProduksi=@no;

  IF @brokerDeleted > 0
  BEGIN
    UPDATE d SET d.DateUsage=NULL
    FROM dbo.Broker_d d
    INNER JOIN dbo.GilinganProduksiInputBroker map ON d.NoBroker=map.NoBroker AND d.NoSak=map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.broker')
      WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
      ON map.NoBroker=j.noBroker AND map.NoSak=j.noSak
    WHERE map.NoProduksi=@no;
  END;

  DELETE map
  FROM dbo.GilinganProduksiInputBroker map
  INNER JOIN OPENJSON(@jsInputs, '$.broker')
    WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
    ON map.NoBroker=j.noBroker AND map.NoSak=j.noSak
  WHERE map.NoProduksi=@no;

  DECLARE @brokerRequested int;
  SELECT @brokerRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.broker');
  SET @brokerNotFound = @brokerRequested - @brokerDeleted;

  INSERT INTO @out SELECT 'broker', @brokerDeleted, @brokerNotFound;

  /* ========= BONGGOLAN ========= */
  DECLARE @bongDeleted int=0, @bongNotFound int=0;

  SELECT @bongDeleted = COUNT(*)
  FROM dbo.GilinganProduksiInputBonggolan map
  INNER JOIN OPENJSON(@jsInputs, '$.bonggolan')
    WITH (noBonggolan varchar(50) '$.noBonggolan') j
    ON map.NoBonggolan=j.noBonggolan
  WHERE map.NoProduksi=@no;

  -- OPTIONAL jika master bonggolan punya DateUsage
  IF @bongDeleted > 0
  BEGIN
     UPDATE b SET b.DateUsage=NULL
     FROM dbo.Bonggolan b
     INNER JOIN OPENJSON(@jsInputs,'$.bonggolan')
       WITH (noBonggolan varchar(50) '$.noBonggolan') j
       ON b.NoBonggolan=j.noBonggolan;
   END;

  DELETE map
  FROM dbo.GilinganProduksiInputBonggolan map
  INNER JOIN OPENJSON(@jsInputs, '$.bonggolan')
    WITH (noBonggolan varchar(50) '$.noBonggolan') j
    ON map.NoBonggolan=j.noBonggolan
  WHERE map.NoProduksi=@no;

  DECLARE @bongRequested int;
  SELECT @bongRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.bonggolan');
  SET @bongNotFound = @bongRequested - @bongDeleted;

  INSERT INTO @out SELECT 'bonggolan', @bongDeleted, @bongNotFound;

  /* ========= CRUSHER ========= */
  DECLARE @crDeleted int=0, @crNotFound int=0;

  SELECT @crDeleted = COUNT(*)
  FROM dbo.GilinganProduksiInputCrusher map
  INNER JOIN OPENJSON(@jsInputs, '$.crusher')
    WITH (noCrusher varchar(50) '$.noCrusher') j
    ON map.NoCrusher=j.noCrusher
  WHERE map.NoProduksi=@no;

  -- OPTIONAL jika master crusher punya DateUsage
   IF @crDeleted > 0
   BEGIN
     UPDATE c SET c.DateUsage=NULL
     FROM dbo.Crusher c
     INNER JOIN OPENJSON(@jsInputs,'$.crusher')
       WITH (noCrusher varchar(50) '$.noCrusher') j
       ON c.NoCrusher=j.noCrusher;
   END;

  DELETE map
  FROM dbo.GilinganProduksiInputCrusher map
  INNER JOIN OPENJSON(@jsInputs, '$.crusher')
    WITH (noCrusher varchar(50) '$.noCrusher') j
    ON map.NoCrusher=j.noCrusher
  WHERE map.NoProduksi=@no;

  DECLARE @crRequested int;
  SELECT @crRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.crusher');
  SET @crNotFound = @crRequested - @crDeleted;

  INSERT INTO @out SELECT 'crusher', @crDeleted, @crNotFound;

  /* ========= REJECT ========= */
  DECLARE @rjDeleted int=0, @rjNotFound int=0;

  SELECT @rjDeleted = COUNT(*)
  FROM dbo.GilinganProduksiInputRejectV2 map
  INNER JOIN OPENJSON(@jsInputs, '$.reject')
    WITH (noReject varchar(50) '$.noReject') j
    ON map.NoReject=j.noReject
  WHERE map.NoProduksi=@no;

  IF @rjDeleted > 0
  BEGIN
    UPDATE r SET r.DateUsage=NULL
    FROM dbo.RejectV2 r
    INNER JOIN OPENJSON(@jsInputs, '$.reject')
      WITH (noReject varchar(50) '$.noReject') j
      ON r.NoReject=j.noReject;
  END;

  DELETE map
  FROM dbo.GilinganProduksiInputRejectV2 map
  INNER JOIN OPENJSON(@jsInputs, '$.reject')
    WITH (noReject varchar(50) '$.noReject') j
    ON map.NoReject=j.noReject
  WHERE map.NoProduksi=@no;

  DECLARE @rjRequested int;
  SELECT @rjRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.reject');
  SET @rjNotFound = @rjRequested - @rjDeleted;

  INSERT INTO @out SELECT 'reject', @rjDeleted, @rjNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_DELETE_INPUTS);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return out;
}



module.exports = { getProduksiByDate, getAllProduksi, createGilinganProduksi, updateGilinganProduksi, deleteGilinganProduksi, fetchInputs, validateLabel, upsertInputsAndPartials, deleteInputsAndPartials  };

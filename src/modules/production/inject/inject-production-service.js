// services/inject-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  assertNotLocked,
  loadDocDateOnlyFromConfig,
} = require('../../../core/shared/tutup-transaksi-guard');

// If you already have these in another shared module, keep using them.
// If not, keep these minimal local helpers.
function padLeft(n, width) {
  const s = String(n);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

// helper normalisasi jam 'HH:mm' -> 'HH:mm:ss' (untuk HourStart/HourEnd yang tipe TIME)
function normalizeTimeStr(t) {
  if (t == null) return null;
  const s = String(t).trim();
  if (!s) return null;
  if (/^\d{1,2}:\d{2}$/.test(s)) return `${s}:00`;
  if (/^\d{1,2}:\d{2}:\d{2}$/.test(s)) return s;
  return s; // biar SQL yang reject kalau format aneh
}

/**
 * Generate next NoProduksi Inject
 * Contoh: S.0000000006  (10 digit angka)
 */
async function generateNextNoProduksiInject(tx, { prefix = 'S.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoProduksi
    FROM dbo.InjectProduksi_h AS h WITH (UPDLOCK, HOLDLOCK)
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

function badReq(message) {
  const e = new Error(message);
  e.statusCode = 400;
  return e;
}

// ============================================================
// ✅ GET ALL (paged + search + lastClosed + isLocked)
// ============================================================
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
    FROM dbo.InjectProduksi_h h WITH (NOLOCK)
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

      h.Jam,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,

      h.IdCetakan,
      h.IdWarna,

      h.EnableOffset,
      h.OffsetCurrent,
      h.OffsetNext,

      h.IdFurnitureMaterial,
      h.HourMeter,
      h.BeratProdukHasilTimbang,

      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd,

      lc.LastClosedDate AS LastClosedDate,

      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.TglProduksi) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.InjectProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

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

// ============================================================
// ✅ GET BY DATE
// ============================================================
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
      h.IdCetakan,
      h.IdWarna,
      h.EnableOffset,
      h.OffsetCurrent,
      h.OffsetNext,
      h.IdFurnitureMaterial,
      h.HourMeter,
      h.BeratProdukHasilTimbang,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd
    FROM dbo.InjectProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin m WITH (NOLOCK) ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @date
    ORDER BY h.Jam ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

// ============================================================
// 🔹 FurnitureWIP kandidat dari header
// ============================================================
async function getFurnitureWipListByNoProduksi(noProduksi) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('noProduksi', sql.VarChar(50), noProduksi);

  const query = `
    SELECT
      h.BeratProdukHasilTimbang,
      d.IdFurnitureWIP,
      cab.Nama AS NamaFurnitureWIP
    FROM dbo.InjectProduksi_h AS h WITH (NOLOCK)
    INNER JOIN dbo.CetakanWarnaToFurnitureWIP_d AS d WITH (NOLOCK)
      ON d.IdCetakan = h.IdCetakan
     AND d.IdWarna   = h.IdWarna
     AND (
          (d.IdFurnitureMaterial IS NULL
              AND (h.IdFurnitureMaterial = 0 OR h.IdFurnitureMaterial IS NULL))
          OR d.IdFurnitureMaterial = h.IdFurnitureMaterial
         )
    INNER JOIN dbo.MstCabinetWIP AS cab WITH (NOLOCK)
      ON cab.IdCabinetWIP = d.IdFurnitureWIP
    WHERE h.NoProduksi = @noProduksi
      AND h.IdCetakan IS NOT NULL
    ORDER BY cab.Nama ASC;
  `;

  const result = await request.query(query);
  return result.recordset;
}

// ============================================================
// 🔹 BarangJadi kandidat dari header
// ============================================================
async function getPackingListByNoProduksi(noProduksi) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('noProduksi', sql.VarChar(50), noProduksi);

  const query = `
    SELECT
      h.BeratProdukHasilTimbang,
      d.IdBarangJadi AS IdBJ,
      mbj.NamaBJ
    FROM dbo.InjectProduksi_h AS h WITH (NOLOCK)
    INNER JOIN dbo.CetakanWarnaToProduk_d AS d WITH (NOLOCK)
      ON d.IdCetakan = h.IdCetakan
     AND d.IdWarna   = h.IdWarna
     AND (
          (d.IdFurnitureMaterial IS NULL AND (h.IdFurnitureMaterial = 0 OR h.IdFurnitureMaterial IS NULL))
          OR d.IdFurnitureMaterial = h.IdFurnitureMaterial
         )
    INNER JOIN dbo.MstBarangJadi AS mbj WITH (NOLOCK)
      ON mbj.IdBJ = d.IdBarangJadi
    WHERE h.NoProduksi = @noProduksi
      AND h.IdCetakan IS NOT NULL
    ORDER BY mbj.NamaBJ ASC;
  `;

  const result = await request.query(query);
  return result.recordset;
}

// ============================================================
// ✅ CREATE header InjectProduksi_h
// ============================================================
async function createInjectProduksi(payload) {
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
    // 0) effective date + guard tutup transaksi
    const effectiveDate = resolveEffectiveDateForCreate(payload.tglProduksi);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create InjectProduksi',
      useLock: true,
    });

    // 1) generate NoProduksi (S.)
    const no1 = await generateNextNoProduksiInject(tx, { prefix: 'S.', width: 10 });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.InjectProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksiInject(tx, { prefix: 'S.', width: 10 })
      : no1;

    // 2) insert header
    const rqIns = new sql.Request(tx);

    rqIns
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .input('TglProduksi', sql.Date, effectiveDate)
      .input('IdMesin', sql.Int, payload.idMesin)
      .input('IdOperator', sql.Int, payload.idOperator)
      .input('Shift', sql.Int, payload.shift)
      // ✅ Jam adalah INT
      .input('Jam', sql.Int, payload.jam ?? null)

      .input('CreateBy', sql.VarChar(100), payload.createBy)
      .input('CheckBy1', sql.VarChar(100), payload.checkBy1 ?? null)
      .input('CheckBy2', sql.VarChar(100), payload.checkBy2 ?? null)
      .input('ApproveBy', sql.VarChar(100), payload.approveBy ?? null)

      .input('JmlhAnggota', sql.Int, payload.jmlhAnggota ?? null)
      .input('Hadir', sql.Int, payload.hadir ?? null)

      .input('IdCetakan', sql.Int, payload.idCetakan ?? null)
      .input('IdWarna', sql.Int, payload.idWarna ?? null)

      .input('EnableOffset', sql.Bit, payload.enableOffset ?? 0)
      .input('OffsetCurrent', sql.Int, payload.offsetCurrent ?? null)
      .input('OffsetNext', sql.Int, payload.offsetNext ?? null)

      .input('IdFurnitureMaterial', sql.Int, payload.idFurnitureMaterial ?? null)

      .input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null)
      .input('BeratProdukHasilTimbang', sql.Decimal(18, 2), payload.beratProdukHasilTimbang ?? null)

      .input('HourStart', sql.VarChar(20), normalizeTimeStr(payload.hourStart))
      .input('HourEnd', sql.VarChar(20), normalizeTimeStr(payload.hourEnd));

    const insertSql = `
      INSERT INTO dbo.InjectProduksi_h (
        NoProduksi, IdOperator, IdMesin, TglProduksi, Jam, Shift,
        CreateBy, CheckBy1, CheckBy2, ApproveBy,
        JmlhAnggota, Hadir,
        IdCetakan, IdWarna,
        EnableOffset, OffsetCurrent, OffsetNext,
        IdFurnitureMaterial,
        HourMeter, BeratProdukHasilTimbang,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi, @IdOperator, @IdMesin, @TglProduksi,
        @Jam,
        @Shift,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy,
        @JmlhAnggota, @Hadir,
        @IdCetakan, @IdWarna,
        @EnableOffset, @OffsetCurrent, @OffsetNext,
        @IdFurnitureMaterial,
        @HourMeter, @BeratProdukHasilTimbang,
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

// ============================================================
// ✅ UPDATE header InjectProduksi_h + sync DateUsage (inputs)
// ============================================================
async function updateInjectProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock + get old date
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'injectProduksi', // samakan dengan config tutup-transaksi
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) handle date change
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq('tglProduksi tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // 2) guard tutup transaksi
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: 'update InjectProduksi (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update InjectProduksi (new date)',
        useLock: true,
      });
    }

    // 3) build dynamic SET
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

    // ✅ Jam is INT (NO CAST)
    if (payload.jam !== undefined) {
      sets.push('Jam = @Jam');
      rqUpd.input('Jam', sql.Int, payload.jam ?? null);
    }

    if (payload.jmlhAnggota !== undefined) {
      sets.push('JmlhAnggota = @JmlhAnggota');
      rqUpd.input('JmlhAnggota', sql.Int, payload.jmlhAnggota ?? null);
    }

    if (payload.hadir !== undefined) {
      sets.push('Hadir = @Hadir');
      rqUpd.input('Hadir', sql.Int, payload.hadir ?? null);
    }

    if (payload.idCetakan !== undefined) {
      sets.push('IdCetakan = @IdCetakan');
      rqUpd.input('IdCetakan', sql.Int, payload.idCetakan ?? null);
    }

    if (payload.idWarna !== undefined) {
      sets.push('IdWarna = @IdWarna');
      rqUpd.input('IdWarna', sql.Int, payload.idWarna ?? null);
    }

    if (payload.enableOffset !== undefined) {
      sets.push('EnableOffset = @EnableOffset');
      rqUpd.input('EnableOffset', sql.Bit, payload.enableOffset ?? null);
    }

    if (payload.offsetCurrent !== undefined) {
      sets.push('OffsetCurrent = @OffsetCurrent');
      rqUpd.input('OffsetCurrent', sql.Int, payload.offsetCurrent ?? null);
    }

    if (payload.offsetNext !== undefined) {
      sets.push('OffsetNext = @OffsetNext');
      rqUpd.input('OffsetNext', sql.Int, payload.offsetNext ?? null);
    }

    if (payload.idFurnitureMaterial !== undefined) {
      sets.push('IdFurnitureMaterial = @IdFurnitureMaterial');
      rqUpd.input('IdFurnitureMaterial', sql.Int, payload.idFurnitureMaterial ?? null);
    }

    if (payload.hourMeter !== undefined) {
      sets.push('HourMeter = @HourMeter');
      rqUpd.input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    if (payload.beratProdukHasilTimbang !== undefined) {
      sets.push('BeratProdukHasilTimbang = @BeratProdukHasilTimbang');
      rqUpd.input('BeratProdukHasilTimbang', sql.Decimal(18, 2), payload.beratProdukHasilTimbang ?? null);
    }

    // TIME
    if (payload.hourStart !== undefined) {
      sets.push(`
        HourStart =
          CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL
               ELSE CAST(@HourStart AS time(7)) END
      `);
      rqUpd.input('HourStart', sql.VarChar(20), normalizeTimeStr(payload.hourStart));
    }

    if (payload.hourEnd !== undefined) {
      sets.push(`
        HourEnd =
          CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL
               ELSE CAST(@HourEnd AS time(7)) END
      `);
      rqUpd.input('HourEnd', sql.VarChar(20), normalizeTimeStr(payload.hourEnd));
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

    if (sets.length === 0) throw badReq('No fields to update');

    rqUpd.input('NoProduksi', sql.VarChar(50), noProduksi);

    const updateSql = `
      UPDATE dbo.InjectProduksi_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.InjectProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // 4) if date changed -> sync DateUsage for all referenced input labels (full+partial)
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.TglProduksi);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('Tanggal', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BROKER (FULL)
        -------------------------------------------------------
        UPDATE br
        SET br.DateUsage = @Tanggal
        FROM dbo.Broker_d AS br
        WHERE br.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.InjectProduksiInputBroker AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoBroker   = br.NoBroker
              AND map.NoSak      = br.NoSak
          );

        -------------------------------------------------------
        -- MIXER (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE m
        SET m.DateUsage = @Tanggal
        FROM dbo.Mixer_d AS m
        WHERE m.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.InjectProduksiInputMixer AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoMixer    = m.NoMixer
                AND map.NoSak      = m.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.InjectProduksiInputMixerPartial AS mp
              JOIN dbo.MixerPartial AS mpd
                ON mpd.NoMixerPartial = mp.NoMixerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND mpd.NoMixer   = m.NoMixer
                AND mpd.NoSak     = m.NoSak
            )
          );

        -------------------------------------------------------
        -- GILINGAN (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE g
        SET g.DateUsage = @Tanggal
        FROM dbo.Gilingan AS g
        WHERE g.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.InjectProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.InjectProduksiInputGilinganPartial AS mp
              JOIN dbo.GilinganPartial AS gp
                ON gp.NoGilinganPartial = mp.NoGilinganPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND gp.NoGilingan = g.NoGilingan
            )
          );

        -------------------------------------------------------
        -- FURNITURE WIP (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.InjectProduksiInputFurnitureWIP AS map
              WHERE map.NoProduksi     = @NoProduksi
                AND map.NoFurnitureWIP = fw.NoFurnitureWIP
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.InjectProduksiInputFurnitureWIPPartial AS mp
              JOIN dbo.FurnitureWIPPartial AS fwp
                ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND fwp.NoFurnitureWIP = fw.NoFurnitureWIP
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

// ============================================================
// ✅ DELETE header InjectProduksi_h + delete inputs + reset DateUsage
// ============================================================
async function deleteInjectProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock + get date
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'injectProduksi',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete InjectProduksi',
      useLock: true,
    });

    // 2) cek output
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT
          SUM(CASE WHEN Src='BONGGOLAN' THEN Cnt ELSE 0 END) AS CntOutputBonggolan,
          SUM(CASE WHEN Src='MIXER'     THEN Cnt ELSE 0 END) AS CntOutputMixer,
          SUM(CASE WHEN Src='REJECT'    THEN Cnt ELSE 0 END) AS CntOutputReject,
          SUM(CASE WHEN Src='FWIP'      THEN Cnt ELSE 0 END) AS CntOutputFWIP,
          SUM(CASE WHEN Src='BJ'        THEN Cnt ELSE 0 END) AS CntOutputBJ
        FROM (
          SELECT 'BONGGOLAN' AS Src, COUNT(1) AS Cnt
          FROM dbo.InjectProduksiOutputBonggolan WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL
          SELECT 'MIXER' AS Src, COUNT(1) AS Cnt
          FROM dbo.InjectProduksiOutputMixer WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL
          SELECT 'REJECT' AS Src, COUNT(1) AS Cnt
          FROM dbo.InjectProduksiOutputRejectV2 WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL
          SELECT 'FWIP' AS Src, COUNT(1) AS Cnt
          FROM dbo.InjectProduksiOutputFurnitureWIP WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL
          SELECT 'BJ' AS Src, COUNT(1) AS Cnt
          FROM dbo.InjectProduksiOutputBarangJadi WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
        ) X;
      `);

    const row = outRes.recordset?.[0] || {};
    const hasOutput =
      (row.CntOutputBonggolan || 0) > 0 ||
      (row.CntOutputMixer || 0) > 0 ||
      (row.CntOutputReject || 0) > 0 ||
      (row.CntOutputFWIP || 0) > 0 ||
      (row.CntOutputBJ || 0) > 0;

    if (hasOutput) {
      throw badReq('Tidak dapat menghapus Nomor Produksi ini karena sudah memiliki data output.');
    }

    // 3) delete inputs + reset dateusage + delete header
    const rq = new sql.Request(tx);
    rq.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- SIMPAN KEY YANG TERDAMPAK (untuk reset DateUsage)
      ---------------------------------------------------------
      DECLARE @BrokerKeys TABLE (
        NoBroker varchar(50) NOT NULL,
        NoSak    int         NOT NULL,
        PRIMARY KEY (NoBroker, NoSak)
      );

      DECLARE @MixerKeys TABLE (
        NoMixer varchar(50) NOT NULL,
        NoSak   int         NOT NULL,
        PRIMARY KEY (NoMixer, NoSak)
      );

      DECLARE @GilinganKeys TABLE (
        NoGilingan varchar(50) PRIMARY KEY
      );

      DECLARE @FWIPKeys TABLE (
        NoFurnitureWIP varchar(50) PRIMARY KEY
      );

      ---------------------------------------------------------
      -- A) KUMPULKAN KEY dari INPUT (FULL)
      ---------------------------------------------------------
      INSERT INTO @BrokerKeys (NoBroker, NoSak)
      SELECT DISTINCT NoBroker, NoSak
      FROM dbo.InjectProduksiInputBroker
      WHERE NoProduksi = @NoProduksi
        AND NoBroker IS NOT NULL
        AND NoSak IS NOT NULL;

      INSERT INTO @MixerKeys (NoMixer, NoSak)
      SELECT DISTINCT NoMixer, NoSak
      FROM dbo.InjectProduksiInputMixer
      WHERE NoProduksi = @NoProduksi
        AND NoMixer IS NOT NULL
        AND NoSak IS NOT NULL;

      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT NoGilingan
      FROM dbo.InjectProduksiInputGilingan
      WHERE NoProduksi = @NoProduksi
        AND NoGilingan IS NOT NULL;

      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT NoFurnitureWIP
      FROM dbo.InjectProduksiInputFurnitureWIP
      WHERE NoProduksi = @NoProduksi
        AND NoFurnitureWIP IS NOT NULL;

      ---------------------------------------------------------
      -- B) KUMPULKAN KEY dari INPUT (PARTIAL -> FULL KEY)
      ---------------------------------------------------------
      INSERT INTO @MixerKeys (NoMixer, NoSak)
      SELECT DISTINCT mpd.NoMixer, mpd.NoSak
      FROM dbo.InjectProduksiInputMixerPartial AS mp
      JOIN dbo.MixerPartial AS mpd
        ON mpd.NoMixerPartial = mp.NoMixerPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND mpd.NoMixer IS NOT NULL
        AND mpd.NoSak IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM @MixerKeys k
          WHERE k.NoMixer = mpd.NoMixer AND k.NoSak = mpd.NoSak
        );

      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT gp.NoGilingan
      FROM dbo.InjectProduksiInputGilinganPartial AS mp
      JOIN dbo.GilinganPartial AS gp
        ON gp.NoGilinganPartial = mp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND gp.NoGilingan IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM @GilinganKeys k WHERE k.NoGilingan = gp.NoGilingan
        );

      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.InjectProduksiInputFurnitureWIPPartial AS mp
      JOIN dbo.FurnitureWIPPartial AS fwp
        ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND fwp.NoFurnitureWIP IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM @FWIPKeys k WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP
        );

      ---------------------------------------------------------
      -- C) HAPUS ROW PARTIAL yang dipakai produksi ini
      ---------------------------------------------------------
      DELETE mpd
      FROM dbo.MixerPartial AS mpd
      JOIN dbo.InjectProduksiInputMixerPartial AS mp
        ON mp.NoMixerPartial = mpd.NoMixerPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE gp
      FROM dbo.GilinganPartial AS gp
      JOIN dbo.InjectProduksiInputGilinganPartial AS mp
        ON mp.NoGilinganPartial = gp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE fwp
      FROM dbo.FurnitureWIPPartial AS fwp
      JOIN dbo.InjectProduksiInputFurnitureWIPPartial AS mp
        ON mp.NoFurnitureWIPPartial = fwp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- D) HAPUS INPUT MAPPING (PARTIAL & FULL)
      ---------------------------------------------------------
      DELETE FROM dbo.InjectProduksiInputMixerPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.InjectProduksiInputGilinganPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.InjectProduksiInputFurnitureWIPPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.InjectProduksiInputBroker
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.InjectProduksiInputMixer
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.InjectProduksiInputGilingan
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.InjectProduksiInputFurnitureWIP
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- E) RESET DATEUSAGE label input (FULL tables)
      ---------------------------------------------------------
      UPDATE br
      SET br.DateUsage = NULL
      FROM dbo.Broker_d AS br
      JOIN @BrokerKeys k
        ON k.NoBroker = br.NoBroker
       AND k.NoSak    = br.NoSak;

      UPDATE m
      SET m.DateUsage = NULL
      FROM dbo.Mixer_d AS m
      JOIN @MixerKeys k
        ON k.NoMixer = m.NoMixer
       AND k.NoSak   = m.NoSak;

      UPDATE g
      SET g.DateUsage = NULL
      FROM dbo.Gilingan AS g
      JOIN @GilinganKeys k
        ON k.NoGilingan = g.NoGilingan;

      UPDATE fw
      SET fw.DateUsage = NULL,
          fw.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.FurnitureWIPPartial p
              WHERE p.NoFurnitureWIP = fw.NoFurnitureWIP
            ) THEN 1 ELSE 0 END
      FROM dbo.FurnitureWIP AS fw
      JOIN @FWIPKeys k
        ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      ---------------------------------------------------------
      -- F) TERAKHIR: HAPUS HEADER
      ---------------------------------------------------------
      DELETE FROM dbo.InjectProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    await rq.query(sqlDelete);

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

    /* ===== FurnitureWIP (FULL) ===== */
    SELECT
      'fwip' AS Src,
      f.NoProduksi,
      f.NoFurnitureWIP          AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      fw.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      fw.IsPartial AS IsPartial,
      fw.IDFurnitureWIP AS IdJenis,
      mcw.Nama          AS NamaJenis,
      CAST(NULL AS varchar(50)) AS NamaUOM,
      fw.Pcs AS Pcs
    FROM dbo.InjectProduksiInputFurnitureWIP f WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
      ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
    WHERE f.NoProduksi = @no

    UNION ALL

    /* ===== Broker (FULL) ===== */
    SELECT
      'broker' AS Src,
      b.NoProduksi,
      b.NoBroker AS Ref1,
      b.NoSak    AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      brd.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      brd.IsPartial AS IsPartial,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis,
      CAST(NULL AS varchar(50)) AS NamaUOM,
      CAST(NULL AS int) AS Pcs
    FROM dbo.InjectProduksiInputBroker b WITH (NOLOCK)
    LEFT JOIN dbo.Broker_d brd WITH (NOLOCK)
      ON brd.NoBroker = b.NoBroker AND brd.NoSak = b.NoSak
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = b.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE b.NoProduksi = @no

    UNION ALL

    /* ===== Mixer (FULL) ===== */
    SELECT
      'mixer' AS Src,
      m.NoProduksi,
      m.NoMixer AS Ref1,
      m.NoSak   AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      md.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      md.IsPartial AS IsPartial,
      mh.IdMixer AS IdJenis,
      mm.Jenis   AS NamaJenis,
      CAST(NULL AS varchar(50)) AS NamaUOM,
      CAST(NULL AS int) AS Pcs
    FROM dbo.InjectProduksiInputMixer m WITH (NOLOCK)
    LEFT JOIN dbo.Mixer_d md WITH (NOLOCK)
      ON md.NoMixer = m.NoMixer AND md.NoSak = m.NoSak
    LEFT JOIN dbo.Mixer_h mh WITH (NOLOCK)
      ON mh.NoMixer = m.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE m.NoProduksi = @no

    UNION ALL

    /* ===== Gilingan (FULL) ===== */
    SELECT
      'gilingan' AS Src,
      g.NoProduksi,
      g.NoGilingan AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      gl.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      gl.IsPartial AS IsPartial,
      gl.IdGilingan    AS IdJenis,
      mg.NamaGilingan  AS NamaJenis,
      CAST(NULL AS varchar(50)) AS NamaUOM,
      CAST(NULL AS int) AS Pcs
    FROM dbo.InjectProduksiInputGilingan g WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan gl WITH (NOLOCK)
      ON gl.NoGilingan = g.NoGilingan
    LEFT JOIN dbo.MstGilingan mg WITH (NOLOCK)
      ON mg.IdGilingan = gl.IdGilingan
    WHERE g.NoProduksi = @no

    UNION ALL

    /* ===== Cabinet Material ===== */
    SELECT
      'material' AS Src,
      cm.NoProduksi,
      CAST(cm.IdCabinetMaterial AS varchar(50)) AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      CAST(NULL AS decimal(18,3)) AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      CAST(NULL AS int) AS IdJenis,
      mm.Nama AS NamaJenis,
      uom.NamaUOM AS NamaUOM,
      CAST(cm.Pcs AS int) AS Pcs
    FROM dbo.InjectProduksiInputCabinetMaterial cm WITH (NOLOCK)
    LEFT JOIN dbo.MstCabinetMaterial mm WITH (NOLOCK)
      ON mm.IdCabinetMaterial = cm.IdCabinetMaterial
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mm.IdUOM
    WHERE cm.NoProduksi = @no

    ORDER BY Ref1 DESC, Ref2 ASC;

    /* ===================== [2] PARTIALS ===================== */

    /* ===== FurnitureWIP Partial ===== */
    SELECT
      'fwip' AS Src,
      pmap.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs,
      fw.Berat,
      fw.IDFurnitureWIP AS IdJenis,
      mcw.Nama          AS NamaJenis
    FROM dbo.InjectProduksiInputFurnitureWIPPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
      ON fwp.NoFurnitureWIPPartial = pmap.NoFurnitureWIPPartial
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
      ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
    WHERE pmap.NoProduksi = @no
    ORDER BY pmap.NoFurnitureWIPPartial DESC;

    /* ===== Broker Partial ===== */
    SELECT
      bmap.NoBrokerPartial,
      bdet.NoBroker,
      bdet.NoSak,
      bdet.Berat,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.InjectProduksiInputBrokerPartial bmap WITH (NOLOCK)
    LEFT JOIN dbo.BrokerPartial bdet WITH (NOLOCK)
      ON bdet.NoBrokerPartial = bmap.NoBrokerPartial
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = bdet.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE bmap.NoProduksi = @no
    ORDER BY bmap.NoBrokerPartial DESC;

    /* ===== Mixer Partial ===== */
    SELECT
      mmap.NoMixerPartial,
      mdet.NoMixer,
      mdet.NoSak,
      mdet.Berat,
      mh.IdMixer AS IdJenis,
      mm.Jenis   AS NamaJenis
    FROM dbo.InjectProduksiInputMixerPartial mmap WITH (NOLOCK)
    LEFT JOIN dbo.MixerPartial mdet WITH (NOLOCK)
      ON mdet.NoMixerPartial = mmap.NoMixerPartial
    LEFT JOIN dbo.Mixer_h mh WITH (NOLOCK)
      ON mh.NoMixer = mdet.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE mmap.NoProduksi = @no
    ORDER BY mmap.NoMixerPartial DESC;

    /* ===== Gilingan Partial ===== */
    SELECT
      gmap.NoGilinganPartial,
      gdet.NoGilingan,
      gdet.Berat,
      gh.IdGilingan   AS IdJenis,
      mg.NamaGilingan AS NamaJenis
    FROM dbo.InjectProduksiInputGilinganPartial gmap WITH (NOLOCK)
    LEFT JOIN dbo.GilinganPartial gdet WITH (NOLOCK)
      ON gdet.NoGilinganPartial = gmap.NoGilinganPartial
    LEFT JOIN dbo.Gilingan gh WITH (NOLOCK)
      ON gh.NoGilingan = gdet.NoGilingan
    LEFT JOIN dbo.MstGilingan mg WITH (NOLOCK)
      ON mg.IdGilingan = gh.IdGilingan
    WHERE gmap.NoProduksi = @no
    ORDER BY gmap.NoGilinganPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows   = rs.recordsets?.[0] || [];
  const fwipPart   = rs.recordsets?.[1] || [];
  const brokerPart = rs.recordsets?.[2] || [];
  const mixerPart  = rs.recordsets?.[3] || [];
  const gilingPart = rs.recordsets?.[4] || [];

  const out = {
    furnitureWip: [],
    broker: [],
    mixer: [],
    gilingan: [],
    cabinetMaterial: [],
    summary: {
      furnitureWip: 0,
      broker: 0,
      mixer: 0,
      gilingan: 0,
      cabinetMaterial: 0,
    },
  };

  // ================= MAIN ROWS =================
  for (const r of mainRows) {
    const base = {
      berat: r.Berat ?? null,
      beratAct: r.BeratAct ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis: r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
    };

    switch (r.Src) {
      case 'fwip':
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          pcs: r.Pcs ?? null,
          ...base,
        });
        break;

      case 'broker':
        out.broker.push({
          noBroker: r.Ref1,
          noSak: r.Ref2,
          ...base,
        });
        break;

      case 'mixer':
        out.mixer.push({
          noMixer: r.Ref1,
          noSak: r.Ref2,
          ...base,
        });
        break;

      case 'gilingan':
        out.gilingan.push({
          noGilingan: r.Ref1,
          ...base,
        });
        break;

      case 'material':
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1,     // kalau mau int: Number(r.Ref1)
          pcs: r.Pcs ?? null,
          namaJenis: r.NamaJenis ?? null,
          namaUom: r.NamaUOM ?? null,
        });
        break;
    }
  }

  // ================= PARTIAL ROWS =================

  // FWIP partial
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

  // Broker partial
  for (const p of brokerPart) {
    out.broker.push({
      noBrokerPartial: p.NoBrokerPartial,
      noBroker: p.NoBroker ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // Mixer partial
  for (const p of mixerPart) {
    out.mixer.push({
      noMixerPartial: p.NoMixerPartial,
      noMixer: p.NoMixer ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // Gilingan partial
  for (const p of gilingPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan: p.NoGilingan ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // ================= SUMMARY =================
  out.summary.furnitureWip = out.furnitureWip.length;
  out.summary.broker = out.broker.length;
  out.summary.mixer = out.mixer.length;
  out.summary.gilingan = out.gilingan.length;
  out.summary.cabinetMaterial = out.cabinetMaterial.length;

  return out;
}

async function validateLabel(labelCode) {
  const pool = await poolPromise;

  // ---------- helpers ----------
  const toCamel = (s) => {
    if (!s) return s;
    let out = s.replace(/[_-]+([a-zA-Z0-9])/g, (_, c) => c.toUpperCase());
    out = out.charAt(0).toLowerCase() + out.slice(1);
    return out;
  };

  const camelize = (val) => {
    if (Array.isArray(val)) return val.map(camelize);
    if (val && typeof val === 'object') {
      const o = {};
      for (const [k, v] of Object.entries(val)) o[toCamel(k)] = camelize(v);
      return o;
    }
    return val;
  };

  // ---------- normalize label ----------
  const raw = String(labelCode || '').trim();
  if (!raw) throw new Error('Label code is required');

  // prefix rule: untuk BF. (3 char), lainnya 2 char (mis: D., H., V., BB.)
  let prefix = '';
  if (raw.substring(0, 3).toUpperCase() === 'BF.') prefix = 'BF.';
  else if (raw.substring(0, 3).toUpperCase() === 'BB.') prefix = 'BB.'; // ✅ FWIP (umum)
  else prefix = raw.substring(0, 2).toUpperCase();

  let query = '';
  let tableName = '';

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
    // =========================================================
    // BB. = FurnitureWIP / FurnitureWIPPartial (Inject pakai ini juga)
    // =========================================================
    case 'BB.': {
      // 1) coba FULL dulu: FurnitureWIP.NoFurnitureWIP (DateUsage IS NULL)
      {
        tableName = 'FurnitureWIP';
        query = `
          SELECT
            fw.NoFurnitureWIP,
            fw.DateCreate,
            fw.Jam,
            fw.Pcs,
            fw.IDFurnitureWIP AS idJenis,
            mcw.Nama          AS namaJenis,
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
          LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
            ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
          WHERE fw.NoFurnitureWIP = @labelCode
            AND fw.DateUsage IS NULL;
        `;
        const full = await run(raw);
        if (full.found) return full;
      }

      // 2) kalau tidak ketemu full, coba PARTIAL: FurnitureWIPPartial.NoFurnitureWIPPartial
      {
        tableName = 'FurnitureWIPPartial';
        query = `
          SELECT
            fwp.NoFurnitureWIPPartial,
            fwp.NoFurnitureWIP,
            fwp.Pcs AS pcsPartial,

            fw.DateCreate,
            fw.Jam,
            fw.Pcs AS pcsHeader,
            fw.IDFurnitureWIP AS idJenis,
            mcw.Nama          AS namaJenis,
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
          LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
            ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
          WHERE fwp.NoFurnitureWIPPartial = @labelCode
            AND fw.DateUsage IS NULL;
        `;
        return await run(raw);
      }
    }

    // =========================================================
    // D. = Broker_d (sisa berat = Berat - SUM(BrokerPartial))
    // =========================================================
    case 'D.': {
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
          d.NoBroker AS noBroker,
          d.NoSak    AS noSak,
          CAST(d.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          d.DateUsage AS dateUsage,
          CASE WHEN ISNULL(ps.BeratPartial, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS isPartial,
          h.IdJenisPlastik AS idJenis,
          jp.Jenis         AS namaJenis
        FROM dbo.Broker_d AS d WITH (NOLOCK)
        LEFT JOIN PartialSum AS ps
          ON ps.NoBroker = d.NoBroker AND ps.NoSak = d.NoSak
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
    }

    // =========================================================
    // H. = Mixer_d (sisa berat = Berat - SUM(MixerPartial))
    // =========================================================
    case 'H.': {
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
          d.NoMixer AS noMixer,
          d.NoSak   AS noSak,
          CAST(d.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          d.DateUsage AS dateUsage,
          CASE WHEN ISNULL(ps.BeratPartial, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS isPartial,
          d.IdLokasi AS idLokasi,
          h.IdMixer  AS idJenis,
          mm.Jenis   AS namaJenis
        FROM dbo.Mixer_d AS d WITH (NOLOCK)
        LEFT JOIN PartialSum AS ps
          ON ps.NoMixer = d.NoMixer AND ps.NoSak = d.NoSak
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
    }

    // =========================================================
    // V. = Gilingan (sisa berat = Berat - SUM(GilinganPartial))
    // =========================================================
    case 'V.': {
      tableName = 'Gilingan';
      query = `
        ;WITH PartialAgg AS (
          SELECT gp.NoGilingan, SUM(ISNULL(gp.Berat, 0)) AS PartialBerat
          FROM dbo.GilinganPartial AS gp WITH (NOLOCK)
          GROUP BY gp.NoGilingan
        )
        SELECT
          g.NoGilingan,
          g.DateCreate,
          g.IdGilingan AS idJenis,
          mg.NamaGilingan AS namaJenis,
          g.DateUsage,
          Berat = CASE
                    WHEN g.Berat - ISNULL(pa.PartialBerat, 0) < 0 THEN 0
                    ELSE g.Berat - ISNULL(pa.PartialBerat, 0)
                  END,
          g.IsPartial AS isPartial
        FROM dbo.Gilingan AS g WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa ON pa.NoGilingan = g.NoGilingan
        LEFT JOIN dbo.MstGilingan AS mg WITH (NOLOCK)
          ON mg.IdGilingan = g.IdGilingan
        WHERE g.NoGilingan = @labelCode
          AND g.DateUsage IS NULL
        ORDER BY g.NoGilingan;
      `;
      return await run(raw);
    }

    default:
      throw new Error(
        `Invalid prefix: ${prefix}. Valid prefixes (Inject): BB., D., H., V.`
      );
  }
}


/**
 * Single entry: create NEW partials + link them, and attach EXISTING inputs.
 * All in one transaction.
 *
 * Payload shape (arrays optional):
 * {
 *   // existing inputs to attach
 *   broker:   [{ noBroker, noSak }],
 *   mixer:    [{ noMixer, noSak }],
 *   gilingan: [{ noGilingan }],
 *
 *   // NEW partials to create + map
 *   brokerPartialNew:   [{ noBroker, noSak, berat }],
 *   mixerPartialNew:    [{ noMixer, noSak, berat }],
 *   gilinganPartialNew: [{ noGilingan, berat }]
 * }
 */
async function upsertInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    // existing inputs (FULL)
    broker: norm(payload?.broker),
    mixer: norm(payload?.mixer),
    gilingan: norm(payload?.gilingan),
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),

    // existing partial labels (attach)
    brokerPartial: norm(payload?.brokerPartial),
    mixerPartial: norm(payload?.mixerPartial),
    gilinganPartial: norm(payload?.gilinganPartial),
    furnitureWipPartial: norm(payload?.furnitureWipPartial),

    // NEW partials (create + map)
    brokerPartialNew: norm(payload?.brokerPartialNew),
    mixerPartialNew: norm(payload?.mixerPartialNew),
    gilinganPartialNew: norm(payload?.gilinganPartialNew),
    furnitureWipPartialNew: norm(payload?.furnitureWipPartialNew),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'injectProduksi',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'upsert InjectProduksi inputs/partials',
      useLock: true,
    });

    // 1) create NEW partials + map
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      brokerPartialNew: body.brokerPartialNew,
      mixerPartialNew: body.mixerPartialNew,
      gilinganPartialNew: body.gilinganPartialNew,
      furnitureWipPartialNew: body.furnitureWipPartialNew,
    });

    // 2) attach existing inputs + attach existing partial labels
    const attachments = await _insertInputsWithTx(tx, noProduksi, {
      broker: body.broker,
      mixer: body.mixer,
      gilingan: body.gilingan,
      furnitureWip: body.furnitureWip,
      cabinetMaterial: body.cabinetMaterial,

      brokerPartial: body.brokerPartial,
      mixerPartial: body.mixerPartial,
      gilinganPartial: body.gilinganPartial,
      furnitureWipPartial: body.furnitureWipPartial,
    });

    await tx.commit();

    // ===== response (same pattern) =====
    const totalInserted = Object.values(attachments).reduce((s, x) => s + (x.inserted || 0), 0);
    const totalUpdated  = Object.values(attachments).reduce((s, x) => s + (x.updated || 0), 0);
    const totalSkipped  = Object.values(attachments).reduce((s, x) => s + (x.skipped || 0), 0);
    const totalInvalid  = Object.values(attachments).reduce((s, x) => s + (x.invalid || 0), 0);

    const totalPartialsCreated = Object.values(partials.summary || {}).reduce((s, x) => s + (x.created || 0), 0);

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = (totalInserted + totalUpdated) === 0 && totalPartialsCreated === 0;

    const response = {
      noProduksi,
      summary: { totalInserted, totalUpdated, totalSkipped, totalInvalid, totalPartialsCreated },
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
    { key: 'broker', label: 'Broker (Full)' },
    { key: 'mixer', label: 'Mixer (Full)' },
    { key: 'gilingan', label: 'Gilingan (Full)' },
    { key: 'furnitureWip', label: 'Furniture WIP (Full)' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },

    { key: 'brokerPartial', label: 'Broker (Partial Existing)' },
    { key: 'mixerPartial', label: 'Mixer (Partial Existing)' },
    { key: 'gilinganPartial', label: 'Gilingan (Partial Existing)' },
    { key: 'furnitureWipPartial', label: 'Furniture WIP (Partial Existing)' },
  ];

  for (const section of sections) {
    const requested = requestBody[section.key]?.length || 0;
    if (!requested) continue;

    const r = attachments[section.key] || { inserted: 0, updated: 0, skipped: 0, invalid: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested,
      inserted: r.inserted,
      updated: r.updated,
      skipped: r.skipped,
      invalid: r.invalid,
      status: r.invalid > 0 ? 'error' : r.skipped > 0 ? 'warning' : 'success',
      message: _buildSectionMessage(section.label, r),
    });
  }
  return details;
}

function _buildPartialDetails(partials, requestBody) {
  const details = [];

  const sections = [
    { key: 'brokerPartialNew', label: 'Broker Partial New' },
    { key: 'mixerPartialNew', label: 'Mixer Partial New' },
    { key: 'gilinganPartialNew', label: 'Gilingan Partial New' },
    { key: 'furnitureWipPartialNew', label: 'Furniture WIP Partial New' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (!requested) continue;

    const created = partials.summary?.[s.key]?.created || 0;
    details.push({
      section: s.key,
      label: s.label,
      requested,
      created,
      status: created === requested ? 'success' : 'error',
      message: `${created} dari ${requested} ${s.label} berhasil dibuat`,
      codes: partials.createdLists?.[s.key] || [],
    });
  }
  return details;
}

function _buildSectionMessage(label, r) {
  const parts = [];
  if (r.inserted > 0) parts.push(`${r.inserted} inserted`);
  if (r.updated > 0) parts.push(`${r.updated} updated`);
  if (r.skipped > 0) parts.push(`${r.skipped} skipped`);
  if (r.invalid > 0) parts.push(`${r.invalid} invalid`);
  return parts.length ? `${label}: ${parts.join(', ')}` : `${label}: no-op`;
}


async function _insertPartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tglProduksi datetime;
  SELECT @tglProduksi = CAST(h.TglProduksi AS datetime)
  FROM dbo.InjectProduksi_h h WITH (NOLOCK)
  WHERE h.NoProduksi = @no;

  IF @tglProduksi IS NULL
  BEGIN
    RAISERROR('Header InjectProduksi_h tidak ditemukan / TglProduksi NULL', 16, 1);
    RETURN;
  END;

  DECLARE @lockResult int;
  EXEC @lockResult = sp_getapplock
    @Resource = 'SEQ_PARTIALS',
    @LockMode = 'Exclusive',
    @LockTimeout = 10000,
    @DbPrincipal = 'public';

  IF (@lockResult < 0)
  BEGIN
    RAISERROR('Failed to acquire SEQ_PARTIALS lock', 16, 1);
    RETURN;
  END;

  DECLARE @broNew TABLE(NoBrokerPartial varchar(50));
  DECLARE @mixNew TABLE(NoMixerPartial varchar(50));
  DECLARE @gilNew TABLE(NoGilinganPartial varchar(50));
  DECLARE @fwNew  TABLE(NoFurnitureWIPPartial varchar(50));

  /* ---------- BROKER PARTIAL NEW (Q.) ---------- */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.brokerPartialNew'))
  BEGIN
    DECLARE @nextBr int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoBrokerPartial,10) AS int))
      FROM dbo.BrokerPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBrokerPartial LIKE 'Q.%'
    ), 0);

    ;WITH src AS (
      SELECT noBroker, noSak, berat,
             ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.brokerPartialNew')
      WITH (
        noBroker varchar(50) '$.noBroker',
        noSak    int         '$.noSak',
        berat    decimal(18,3) '$.berat'
      )
      WHERE NULLIF(noBroker,'') IS NOT NULL AND ISNULL(noSak,0)>0 AND ISNULL(berat,0)>0
        AND EXISTS (SELECT 1 FROM dbo.Broker_d d WITH (NOLOCK)
                    WHERE d.NoBroker=noBroker AND d.NoSak=noSak AND d.DateUsage IS NULL)
    ),
    numbered AS (
      SELECT CONCAT('Q.', RIGHT(REPLICATE('0',10) + CAST(@nextBr + rn AS varchar(10)), 10)) AS NewNo,
             noBroker, noSak, berat
      FROM src
    )
    INSERT INTO dbo.BrokerPartial (NoBrokerPartial, NoBroker, NoSak, Berat)
    OUTPUT INSERTED.NoBrokerPartial INTO @broNew(NoBrokerPartial)
    SELECT NewNo, noBroker, noSak, berat FROM numbered;

    INSERT INTO dbo.InjectProduksiInputBrokerPartial (NoProduksi, NoBrokerPartial)
    SELECT @no, x.NoBrokerPartial FROM @broNew x;

    ;WITH ex AS (
      SELECT NoBroker, NoSak, SUM(ISNULL(Berat,0)) AS TotalExisting
      FROM dbo.BrokerPartial WITH (NOLOCK)
      WHERE NoBrokerPartial NOT IN (SELECT NoBrokerPartial FROM @broNew)
      GROUP BY NoBroker, NoSak
    ),
    nw AS (
      SELECT noBroker, noSak, SUM(berat) AS TotalNew
      FROM OPENJSON(@jsPartials, '$.brokerPartialNew')
      WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak', berat decimal(18,3) '$.berat')
      GROUP BY noBroker, noSak
    )
    UPDATE d
    SET d.IsPartial=1,
        d.DateUsage = CASE WHEN (d.Berat - ISNULL(ex.TotalExisting,0) - ISNULL(nw.TotalNew,0)) <= 0.001
                           THEN @tglProduksi ELSE d.DateUsage END
    FROM dbo.Broker_d d
    LEFT JOIN ex ON ex.NoBroker=d.NoBroker AND ex.NoSak=d.NoSak
    INNER JOIN nw ON nw.noBroker=d.NoBroker AND nw.noSak=d.NoSak;
  END;

  /* ---------- MIXER PARTIAL NEW (T.) ---------- */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.mixerPartialNew'))
  BEGIN
    DECLARE @nextM int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoMixerPartial,10) AS int))
      FROM dbo.MixerPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoMixerPartial LIKE 'T.%'
    ), 0);

    ;WITH src AS (
      SELECT noMixer, noSak, berat,
             ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.mixerPartialNew')
      WITH (
        noMixer varchar(50) '$.noMixer',
        noSak   int         '$.noSak',
        berat   decimal(18,3) '$.berat'
      )
      WHERE NULLIF(noMixer,'') IS NOT NULL AND ISNULL(noSak,0)>0 AND ISNULL(berat,0)>0
        AND EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK)
                    WHERE d.NoMixer=noMixer AND d.NoSak=noSak AND d.DateUsage IS NULL)
    ),
    numbered AS (
      SELECT CONCAT('T.', RIGHT(REPLICATE('0',10) + CAST(@nextM + rn AS varchar(10)), 10)) AS NewNo,
             noMixer, noSak, berat
      FROM src
    )
    INSERT INTO dbo.MixerPartial (NoMixerPartial, NoMixer, NoSak, Berat)
    OUTPUT INSERTED.NoMixerPartial INTO @mixNew(NoMixerPartial)
    SELECT NewNo, noMixer, noSak, berat FROM numbered;

    INSERT INTO dbo.InjectProduksiInputMixerPartial (NoProduksi, NoMixerPartial)
    SELECT @no, x.NoMixerPartial FROM @mixNew x;

    ;WITH ex AS (
      SELECT NoMixer, NoSak, SUM(ISNULL(Berat,0)) AS TotalExisting
      FROM dbo.MixerPartial WITH (NOLOCK)
      WHERE NoMixerPartial NOT IN (SELECT NoMixerPartial FROM @mixNew)
      GROUP BY NoMixer, NoSak
    ),
    nw AS (
      SELECT noMixer, noSak, SUM(berat) AS TotalNew
      FROM OPENJSON(@jsPartials, '$.mixerPartialNew')
      WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak', berat decimal(18,3) '$.berat')
      GROUP BY noMixer, noSak
    )
    UPDATE d
    SET d.IsPartial=1,
        d.DateUsage = CASE WHEN (d.Berat - ISNULL(ex.TotalExisting,0) - ISNULL(nw.TotalNew,0)) <= 0.001
                           THEN @tglProduksi ELSE d.DateUsage END
    FROM dbo.Mixer_d d
    LEFT JOIN ex ON ex.NoMixer=d.NoMixer AND ex.NoSak=d.NoSak
    INNER JOIN nw ON nw.noMixer=d.NoMixer AND nw.noSak=d.NoSak;
  END;

  /* ---------- GILINGAN PARTIAL NEW (Y.) ---------- */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.gilinganPartialNew'))
  BEGIN
    DECLARE @nextG int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoGilinganPartial,10) AS int))
      FROM dbo.GilinganPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoGilinganPartial LIKE 'Y.%'
    ), 0);

    ;WITH src AS (
      SELECT noGilingan, berat,
             ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.gilinganPartialNew')
      WITH (
        noGilingan varchar(50) '$.noGilingan',
        berat      decimal(18,3) '$.berat'
      )
      WHERE NULLIF(noGilingan,'') IS NOT NULL AND ISNULL(berat,0)>0
        AND EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK)
                    WHERE g.NoGilingan=noGilingan AND g.DateUsage IS NULL)
    ),
    numbered AS (
      SELECT CONCAT('Y.', RIGHT(REPLICATE('0',10) + CAST(@nextG + rn AS varchar(10)), 10)) AS NewNo,
             noGilingan, berat
      FROM src
    )
    INSERT INTO dbo.GilinganPartial (NoGilinganPartial, NoGilingan, Berat)
    OUTPUT INSERTED.NoGilinganPartial INTO @gilNew(NoGilinganPartial)
    SELECT NewNo, noGilingan, berat FROM numbered;

    INSERT INTO dbo.InjectProduksiInputGilinganPartial (NoProduksi, NoGilinganPartial)
    SELECT @no, x.NoGilinganPartial FROM @gilNew x;

    ;WITH ex AS (
      SELECT NoGilingan, SUM(ISNULL(Berat,0)) AS TotalExisting
      FROM dbo.GilinganPartial WITH (NOLOCK)
      WHERE NoGilinganPartial NOT IN (SELECT NoGilinganPartial FROM @gilNew)
      GROUP BY NoGilingan
    ),
    nw AS (
      SELECT noGilingan, SUM(berat) AS TotalNew
      FROM OPENJSON(@jsPartials, '$.gilinganPartialNew')
      WITH (noGilingan varchar(50) '$.noGilingan', berat decimal(18,3) '$.berat')
      GROUP BY noGilingan
    )
    UPDATE g
    SET g.IsPartial=1,
        g.DateUsage = CASE WHEN (g.Berat - ISNULL(ex.TotalExisting,0) - ISNULL(nw.TotalNew,0)) <= 0.001
                           THEN @tglProduksi ELSE g.DateUsage END
    FROM dbo.Gilingan g
    LEFT JOIN ex ON ex.NoGilingan=g.NoGilingan
    INNER JOIN nw ON nw.noGilingan=g.NoGilingan;
  END;

  /* ---------- FWIP PARTIAL NEW (BC.) ---------- */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew'))
  BEGIN
    DECLARE @nextFW int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoFurnitureWIPPartial,10) AS int))
      FROM dbo.FurnitureWIPPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoFurnitureWIPPartial LIKE 'BC.%'
    ), 0);

    ;WITH src AS (
      SELECT noFurnitureWip, pcs,
             ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew')
      WITH (
        noFurnitureWip varchar(50) '$.noFurnitureWip',
        pcs int '$.pcs'
      )
      WHERE NULLIF(noFurnitureWip,'') IS NOT NULL AND ISNULL(pcs,0)>0
        AND EXISTS (SELECT 1 FROM dbo.FurnitureWIP fw WITH (NOLOCK)
                    WHERE fw.NoFurnitureWIP=noFurnitureWip AND fw.DateUsage IS NULL)
    ),
    numbered AS (
      SELECT CONCAT('BC.', RIGHT(REPLICATE('0',10) + CAST(@nextFW + rn AS varchar(10)), 10)) AS NewNo,
             noFurnitureWip, pcs
      FROM src
    )
    INSERT INTO dbo.FurnitureWIPPartial (NoFurnitureWIPPartial, NoFurnitureWIP, Pcs)
    OUTPUT INSERTED.NoFurnitureWIPPartial INTO @fwNew(NoFurnitureWIPPartial)
    SELECT NewNo, noFurnitureWip, pcs FROM numbered;

    INSERT INTO dbo.InjectProduksiInputFurnitureWIPPartial (NoProduksi, NoFurnitureWIPPartial)
    SELECT @no, x.NoFurnitureWIPPartial FROM @fwNew x;

    -- update parent: set ispartial + dateusage if pcs fully consumed
    ;WITH ex AS (
      SELECT NoFurnitureWIP, SUM(ISNULL(Pcs,0)) AS TotalExisting
      FROM dbo.FurnitureWIPPartial WITH (NOLOCK)
      WHERE NoFurnitureWIPPartial NOT IN (SELECT NoFurnitureWIPPartial FROM @fwNew)
      GROUP BY NoFurnitureWIP
    ),
    nw AS (
      SELECT noFurnitureWip, SUM(pcs) AS TotalNew
      FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew')
      WITH (noFurnitureWip varchar(50) '$.noFurnitureWip', pcs int '$.pcs')
      GROUP BY noFurnitureWip
    )
    UPDATE fw
    SET fw.IsPartial=1,
        fw.DateUsage = CASE WHEN (fw.Pcs - ISNULL(ex.TotalExisting,0) - ISNULL(nw.TotalNew,0)) <= 0
                            THEN @tglProduksi ELSE fw.DateUsage END
    FROM dbo.FurnitureWIP fw
    LEFT JOIN ex ON ex.NoFurnitureWIP=fw.NoFurnitureWIP
    INNER JOIN nw ON nw.noFurnitureWip=fw.NoFurnitureWIP;
  END;

  EXEC sp_releaseapplock @Resource='SEQ_PARTIALS', @DbPrincipal='public';

  -- summary
  SELECT 'brokerPartialNew' AS Section, COUNT(*) AS Created FROM @broNew
  UNION ALL SELECT 'mixerPartialNew', COUNT(*) FROM @mixNew
  UNION ALL SELECT 'gilinganPartialNew', COUNT(*) FROM @gilNew
  UNION ALL SELECT 'furnitureWipPartialNew', COUNT(*) FROM @fwNew;

  -- codes (recordsets[1..4])
  SELECT NoBrokerPartial FROM @broNew;
  SELECT NoMixerPartial FROM @mixNew;
  SELECT NoGilinganPartial FROM @gilNew;
  SELECT NoFurnitureWIPPartial FROM @fwNew;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) summary[row.Section] = { created: row.Created };

  const createdLists = {
    brokerPartialNew: (rs.recordsets?.[1] || []).map(r => r.NoBrokerPartial),
    mixerPartialNew: (rs.recordsets?.[2] || []).map(r => r.NoMixerPartial),
    gilinganPartialNew: (rs.recordsets?.[3] || []).map(r => r.NoGilinganPartial),
    furnitureWipPartialNew: (rs.recordsets?.[4] || []).map(r => r.NoFurnitureWIPPartial),
  };

  return { summary, createdLists };
}


async function _insertInputsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tglProduksi datetime;
  SELECT @tglProduksi = CAST(h.TglProduksi AS datetime)
  FROM dbo.InjectProduksi_h h WITH (NOLOCK)
  WHERE h.NoProduksi = @no;

  IF @tglProduksi IS NULL
  BEGIN
    RAISERROR('Header InjectProduksi_h tidak ditemukan / TglProduksi NULL', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Inserted int, Updated int, Skipped int, Invalid int);

  /* =========================
     1) FULL: BROKER
     ========================= */
  DECLARE @i int=0,@u int=0,@s int=0,@inv int=0;

  ;WITH j AS (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs,'$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noBroker,'') IS NOT NULL AND ISNULL(noSak,0)>0
      AND EXISTS (SELECT 1 FROM dbo.Broker_d d WITH (NOLOCK)
                  WHERE d.NoBroker=j.noBroker AND d.NoSak=j.noSak AND d.DateUsage IS NULL)
  )
  INSERT INTO dbo.InjectProduksiInputBroker(NoProduksi,NoBroker,NoSak)
  SELECT @no, v.noBroker, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputBroker x
    WHERE x.NoProduksi=@no AND x.NoBroker=v.noBroker AND x.NoSak=v.noSak
  );
  SET @i=@@ROWCOUNT;

  IF @i>0
  BEGIN
    UPDATE d SET d.DateUsage=@tglProduksi
    FROM dbo.Broker_d d
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs,'$.broker')
      WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' ) src
      WHERE d.NoBroker=src.noBroker AND d.NoSak=src.noSak
    );
  END;

  SELECT @s = COUNT(*) FROM (
    SELECT noBroker,noSak FROM OPENJSON(@jsInputs,'$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputBroker x WHERE x.NoProduksi=@no AND x.NoBroker=j.noBroker AND x.NoSak=j.noSak);

  SELECT @inv = COUNT(*) FROM (
    SELECT noBroker,noSak FROM OPENJSON(@jsInputs,'$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ) j
  WHERE NULLIF(j.noBroker,'') IS NOT NULL AND ISNULL(j.noSak,0)>0
    AND NOT EXISTS (SELECT 1 FROM dbo.Broker_d d WITH (NOLOCK) WHERE d.NoBroker=j.noBroker AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'broker', @i, 0, @s, @inv;

  /* =========================
     2) FULL: MIXER
     ========================= */
  SELECT @i=0,@s=0,@inv=0;

  ;WITH j AS (
    SELECT noMixer, noSak
    FROM OPENJSON(@jsInputs,'$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noMixer,'') IS NOT NULL AND ISNULL(noSak,0)>0
      AND EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK)
                  WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak AND d.DateUsage IS NULL)
  )
  INSERT INTO dbo.InjectProduksiInputMixer(NoProduksi,NoMixer,NoSak)
  SELECT @no, v.noMixer, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputMixer x
    WHERE x.NoProduksi=@no AND x.NoMixer=v.noMixer AND x.NoSak=v.noSak
  );
  SET @i=@@ROWCOUNT;

  IF @i>0
  BEGIN
    UPDATE d SET d.DateUsage=@tglProduksi
    FROM dbo.Mixer_d d
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs,'$.mixer')
      WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' ) src
      WHERE d.NoMixer=src.noMixer AND d.NoSak=src.noSak
    );
  END;

  SELECT @s = COUNT(*) FROM (
    SELECT noMixer,noSak FROM OPENJSON(@jsInputs,'$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputMixer x WHERE x.NoProduksi=@no AND x.NoMixer=j.noMixer AND x.NoSak=j.noSak);

  SELECT @inv = COUNT(*) FROM (
    SELECT noMixer,noSak FROM OPENJSON(@jsInputs,'$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ) j
  WHERE NULLIF(j.noMixer,'') IS NOT NULL AND ISNULL(j.noSak,0)>0
    AND NOT EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK) WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'mixer', @i, 0, @s, @inv;

  /* =========================
     3) FULL: GILINGAN
     ========================= */
  SELECT @i=0,@s=0,@inv=0;

  ;WITH j AS (
    SELECT noGilingan
    FROM OPENJSON(@jsInputs,'$.gilingan')
    WITH ( noGilingan varchar(50) '$.noGilingan' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noGilingan,'') IS NOT NULL
      AND EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK)
                  WHERE g.NoGilingan=j.noGilingan AND g.DateUsage IS NULL)
  )
  INSERT INTO dbo.InjectProduksiInputGilingan(NoProduksi,NoGilingan)
  SELECT @no, v.noGilingan
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputGilingan x
    WHERE x.NoProduksi=@no AND x.NoGilingan=v.noGilingan
  );
  SET @i=@@ROWCOUNT;

  IF @i>0
  BEGIN
    UPDATE g SET g.DateUsage=@tglProduksi
    FROM dbo.Gilingan g
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs,'$.gilingan')
      WITH ( noGilingan varchar(50) '$.noGilingan' ) src
      WHERE g.NoGilingan=src.noGilingan
    );
  END;

  SELECT @s = COUNT(*) FROM (
    SELECT noGilingan FROM OPENJSON(@jsInputs,'$.gilingan')
    WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputGilingan x WHERE x.NoProduksi=@no AND x.NoGilingan=j.noGilingan);

  SELECT @inv = COUNT(*) FROM (
    SELECT noGilingan FROM OPENJSON(@jsInputs,'$.gilingan')
    WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE NULLIF(j.noGilingan,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan);

  INSERT INTO @out SELECT 'gilingan', @i, 0, @s, @inv;

  /* =========================
     4) FULL: FURNITURE WIP
     ========================= */
  SELECT @i=0,@s=0,@inv=0;

  ;WITH j AS (
    SELECT noFurnitureWip
    FROM OPENJSON(@jsInputs,'$.furnitureWip')
    WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noFurnitureWip,'') IS NOT NULL
      AND EXISTS (SELECT 1 FROM dbo.FurnitureWIP fw WITH (NOLOCK)
                  WHERE fw.NoFurnitureWIP=j.noFurnitureWip AND fw.DateUsage IS NULL)
  )
  INSERT INTO dbo.InjectProduksiInputFurnitureWIP(NoProduksi,NoFurnitureWIP)
  SELECT @no, v.noFurnitureWip
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputFurnitureWIP x
    WHERE x.NoProduksi=@no AND x.NoFurnitureWIP=v.noFurnitureWip
  );
  SET @i=@@ROWCOUNT;

  IF @i>0
  BEGIN
    UPDATE fw SET fw.DateUsage=@tglProduksi
    FROM dbo.FurnitureWIP fw
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs,'$.furnitureWip')
      WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' ) src
      WHERE fw.NoFurnitureWIP=src.noFurnitureWip
    );
  END;

  SELECT @s = COUNT(*) FROM (
    SELECT noFurnitureWip FROM OPENJSON(@jsInputs,'$.furnitureWip')
    WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputFurnitureWIP x WHERE x.NoProduksi=@no AND x.NoFurnitureWIP=j.noFurnitureWip);

  SELECT @inv = COUNT(*) FROM (
    SELECT noFurnitureWip FROM OPENJSON(@jsInputs,'$.furnitureWip')
    WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  ) j
  WHERE NULLIF(j.noFurnitureWip,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM dbo.FurnitureWIP fw WITH (NOLOCK) WHERE fw.NoFurnitureWIP=j.noFurnitureWip);

  INSERT INTO @out SELECT 'furnitureWip', @i, 0, @s, @inv;

  /* =========================
     5) CABINET MATERIAL (UPSERT)
     ========================= */
  DECLARE @mIns int=0, @mUpd int=0, @mInv int=0;

  DECLARE @MatSrc TABLE(IdCabinetMaterial int, Pcs int);

  INSERT INTO @MatSrc(IdCabinetMaterial, Pcs)
  SELECT IdCabinetMaterial, SUM(ISNULL(Pcs,0)) AS Pcs
  FROM OPENJSON(@jsInputs,'$.cabinetMaterial')
  WITH ( IdCabinetMaterial int '$.idCabinetMaterial', Pcs int '$.pcs' )
  WHERE IdCabinetMaterial IS NOT NULL
  GROUP BY IdCabinetMaterial;

  -- invalid: pcs<=0 OR material not enable
  SELECT @mInv = COUNT(*)
  FROM @MatSrc s
  WHERE s.Pcs <= 0
     OR NOT EXISTS (
       SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
       WHERE m.IdCabinetMaterial=s.IdCabinetMaterial AND m.Enable=1
     );

  UPDATE tgt
  SET tgt.Pcs = src.Pcs
  FROM dbo.InjectProduksiInputCabinetMaterial tgt
  JOIN @MatSrc src ON src.IdCabinetMaterial=tgt.IdCabinetMaterial
  WHERE tgt.NoProduksi=@no
    AND src.Pcs > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    );
  SET @mUpd = @@ROWCOUNT;

  INSERT INTO dbo.InjectProduksiInputCabinetMaterial(NoProduksi,IdCabinetMaterial,Pcs)
  SELECT @no, src.IdCabinetMaterial, src.Pcs
  FROM @MatSrc src
  WHERE src.Pcs > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    )
    AND NOT EXISTS (
      SELECT 1 FROM dbo.InjectProduksiInputCabinetMaterial x WITH (NOLOCK)
      WHERE x.NoProduksi=@no AND x.IdCabinetMaterial=src.IdCabinetMaterial
    );
  SET @mIns = @@ROWCOUNT;

  INSERT INTO @out SELECT 'cabinetMaterial', @mIns, @mUpd, 0, @mInv;

  /* =========================
     6) ATTACH EXISTING PARTIAL LABELS
     ========================= */

  -- brokerPartial
  SELECT @i=0,@s=0,@inv=0;
  ;WITH j AS (
    SELECT noBrokerPartial
    FROM OPENJSON(@jsInputs,'$.brokerPartial')
    WITH ( noBrokerPartial varchar(50) '$.noBrokerPartial' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noBrokerPartial,'') IS NOT NULL
      AND EXISTS (SELECT 1 FROM dbo.BrokerPartial bp WITH (NOLOCK) WHERE bp.NoBrokerPartial=j.noBrokerPartial)
  )
  INSERT INTO dbo.InjectProduksiInputBrokerPartial(NoProduksi,NoBrokerPartial)
  SELECT @no, v.noBrokerPartial
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputBrokerPartial x
    WHERE x.NoProduksi=@no AND x.NoBrokerPartial=v.noBrokerPartial
  );
  SET @i=@@ROWCOUNT;

  SELECT @s=COUNT(*) FROM (
    SELECT noBrokerPartial FROM OPENJSON(@jsInputs,'$.brokerPartial')
    WITH ( noBrokerPartial varchar(50) '$.noBrokerPartial' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputBrokerPartial x WHERE x.NoProduksi=@no AND x.NoBrokerPartial=j.noBrokerPartial);

  SELECT @inv=COUNT(*) FROM (
    SELECT noBrokerPartial FROM OPENJSON(@jsInputs,'$.brokerPartial')
    WITH ( noBrokerPartial varchar(50) '$.noBrokerPartial' )
  ) j
  WHERE NULLIF(j.noBrokerPartial,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM dbo.BrokerPartial bp WITH (NOLOCK) WHERE bp.NoBrokerPartial=j.noBrokerPartial);

  INSERT INTO @out SELECT 'brokerPartial', @i, 0, @s, @inv;

  -- mixerPartial
  SELECT @i=0,@s=0,@inv=0;
  ;WITH j AS (
    SELECT noMixerPartial
    FROM OPENJSON(@jsInputs,'$.mixerPartial')
    WITH ( noMixerPartial varchar(50) '$.noMixerPartial' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noMixerPartial,'') IS NOT NULL
      AND EXISTS (SELECT 1 FROM dbo.MixerPartial mp WITH (NOLOCK) WHERE mp.NoMixerPartial=j.noMixerPartial)
  )
  INSERT INTO dbo.InjectProduksiInputMixerPartial(NoProduksi,NoMixerPartial)
  SELECT @no, v.noMixerPartial
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputMixerPartial x
    WHERE x.NoProduksi=@no AND x.NoMixerPartial=v.noMixerPartial
  );
  SET @i=@@ROWCOUNT;

  SELECT @s=COUNT(*) FROM (
    SELECT noMixerPartial FROM OPENJSON(@jsInputs,'$.mixerPartial')
    WITH ( noMixerPartial varchar(50) '$.noMixerPartial' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputMixerPartial x WHERE x.NoProduksi=@no AND x.NoMixerPartial=j.noMixerPartial);

  SELECT @inv=COUNT(*) FROM (
    SELECT noMixerPartial FROM OPENJSON(@jsInputs,'$.mixerPartial')
    WITH ( noMixerPartial varchar(50) '$.noMixerPartial' )
  ) j
  WHERE NULLIF(j.noMixerPartial,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM dbo.MixerPartial mp WITH (NOLOCK) WHERE mp.NoMixerPartial=j.noMixerPartial);

  INSERT INTO @out SELECT 'mixerPartial', @i, 0, @s, @inv;

  -- gilinganPartial
  SELECT @i=0,@s=0,@inv=0;
  ;WITH j AS (
    SELECT noGilinganPartial
    FROM OPENJSON(@jsInputs,'$.gilinganPartial')
    WITH ( noGilinganPartial varchar(50) '$.noGilinganPartial' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noGilinganPartial,'') IS NOT NULL
      AND EXISTS (SELECT 1 FROM dbo.GilinganPartial gp WITH (NOLOCK) WHERE gp.NoGilinganPartial=j.noGilinganPartial)
  )
  INSERT INTO dbo.InjectProduksiInputGilinganPartial(NoProduksi,NoGilinganPartial)
  SELECT @no, v.noGilinganPartial
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputGilinganPartial x
    WHERE x.NoProduksi=@no AND x.NoGilinganPartial=v.noGilinganPartial
  );
  SET @i=@@ROWCOUNT;

  SELECT @s=COUNT(*) FROM (
    SELECT noGilinganPartial FROM OPENJSON(@jsInputs,'$.gilinganPartial')
    WITH ( noGilinganPartial varchar(50) '$.noGilinganPartial' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputGilinganPartial x WHERE x.NoProduksi=@no AND x.NoGilinganPartial=j.noGilinganPartial);

  SELECT @inv=COUNT(*) FROM (
    SELECT noGilinganPartial FROM OPENJSON(@jsInputs,'$.gilinganPartial')
    WITH ( noGilinganPartial varchar(50) '$.noGilinganPartial' )
  ) j
  WHERE NULLIF(j.noGilinganPartial,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM dbo.GilinganPartial gp WITH (NOLOCK) WHERE gp.NoGilinganPartial=j.noGilinganPartial);

  INSERT INTO @out SELECT 'gilinganPartial', @i, 0, @s, @inv;

  -- furnitureWipPartial
  SELECT @i=0,@s=0,@inv=0;
  ;WITH j AS (
    SELECT noFurnitureWipPartial
    FROM OPENJSON(@jsInputs,'$.furnitureWipPartial')
    WITH ( noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE NULLIF(noFurnitureWipPartial,'') IS NOT NULL
      AND EXISTS (SELECT 1 FROM dbo.FurnitureWIPPartial fp WITH (NOLOCK) WHERE fp.NoFurnitureWIPPartial=j.noFurnitureWipPartial)
  )
  INSERT INTO dbo.InjectProduksiInputFurnitureWIPPartial(NoProduksi,NoFurnitureWIPPartial)
  SELECT @no, v.noFurnitureWipPartial
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.InjectProduksiInputFurnitureWIPPartial x
    WHERE x.NoProduksi=@no AND x.NoFurnitureWIPPartial=v.noFurnitureWipPartial
  );
  SET @i=@@ROWCOUNT;

  SELECT @s=COUNT(*) FROM (
    SELECT noFurnitureWipPartial FROM OPENJSON(@jsInputs,'$.furnitureWipPartial')
    WITH ( noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.InjectProduksiInputFurnitureWIPPartial x WHERE x.NoProduksi=@no AND x.NoFurnitureWIPPartial=j.noFurnitureWipPartial);

  SELECT @inv=COUNT(*) FROM (
    SELECT noFurnitureWipPartial FROM OPENJSON(@jsInputs,'$.furnitureWipPartial')
    WITH ( noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial' )
  ) j
  WHERE NULLIF(j.noFurnitureWipPartial,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM dbo.FurnitureWIPPartial fp WITH (NOLOCK) WHERE fp.NoFurnitureWIPPartial=j.noFurnitureWipPartial);

  INSERT INTO @out SELECT 'furnitureWipPartial', @i, 0, @s, @inv;

  SELECT Section, Inserted, Updated, Skipped, Invalid FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = {
      inserted: row.Inserted || 0,
      updated: row.Updated || 0,
      skipped: row.Skipped || 0,
      invalid: row.Invalid || 0,
    };
  }
  return out;
}


// DELETE INPUT AND PARTIAL SERVICE - INJECT
async function deleteInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    // inputs
    broker: norm(payload.broker),
    mixer: norm(payload.mixer),
    gilingan: norm(payload.gilingan),
    furnitureWip: norm(payload.furnitureWip),
    cabinetMaterial: norm(payload.cabinetMaterial),

    // partials
    brokerPartial: norm(payload.brokerPartial),
    mixerPartial: norm(payload.mixerPartial),
    gilinganPartial: norm(payload.gilinganPartial),
    furnitureWipPartial: norm(payload.furnitureWipPartial),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) docDateOnly (tutup transaksi)
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'injectProduksi',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete InjectProduksi inputs/partials',
      useLock: true,
    });

    // 2) delete partials
    const partialsResult = await _deletePartialsWithTx(tx, noProduksi, {
      brokerPartial: body.brokerPartial,
      mixerPartial: body.mixerPartial,
      gilinganPartial: body.gilinganPartial,
      furnitureWipPartial: body.furnitureWipPartial,
    });

    // 3) delete inputs
    const inputsResult = await _deleteInputsWithTx(tx, noProduksi, {
      broker: body.broker,
      mixer: body.mixer,
      gilingan: body.gilingan,
      furnitureWip: body.furnitureWip,
      cabinetMaterial: body.cabinetMaterial,
    });

    await tx.commit();

    const totalDeleted = Object.values(inputsResult).reduce((s, x) => s + (x.deleted || 0), 0);
    const totalNotFound = Object.values(inputsResult).reduce((s, x) => s + (x.notFound || 0), 0);

    const totalPartialsDeleted = Object.values(partialsResult.summary).reduce((s, x) => s + (x.deleted || 0), 0);
    const totalPartialsNotFound = Object.values(partialsResult.summary).reduce((s, x) => s + (x.notFound || 0), 0);

    const hasNotFound = totalNotFound > 0 || totalPartialsNotFound > 0;
    const hasNoSuccess = totalDeleted === 0 && totalPartialsDeleted === 0;

    return {
      success: !hasNoSuccess,
      hasWarnings: hasNotFound,
      data: {
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
      },
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
    { key: 'mixer', label: 'Mixer' },
    { key: 'gilingan', label: 'Gilingan' },
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (!requested) continue;
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
    { key: 'mixerPartial', label: 'Mixer Partial' },
    { key: 'gilinganPartial', label: 'Gilingan Partial' },
    { key: 'furnitureWipPartial', label: 'Furniture WIP Partial' },
  ];

  for (const s of sections) {
    const requested = requestBody[s.key]?.length || 0;
    if (!requested) continue;

    const r = partialsResult.summary?.[s.key] || { deleted: 0, notFound: 0 };
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


async function _deletePartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  /* =======================
     BROKER PARTIAL
     ======================= */
  DECLARE @brokerDeleted int=0, @brokerNotFound int=0;

  SELECT @brokerDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputBrokerPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON map.NoBrokerPartial = j.noBrokerPartial
  WHERE map.NoProduksi = @no;

  DECLARE @deletedBrokerPartials TABLE (NoBroker varchar(50), NoSak int);

  INSERT INTO @deletedBrokerPartials (NoBroker, NoSak)
  SELECT DISTINCT bp.NoBroker, bp.NoSak
  FROM dbo.BrokerPartial bp
  INNER JOIN dbo.InjectProduksiInputBrokerPartial map ON bp.NoBrokerPartial = map.NoBrokerPartial
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON map.NoBrokerPartial = j.noBrokerPartial
  WHERE map.NoProduksi = @no;

  DELETE map
  FROM dbo.InjectProduksiInputBrokerPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON map.NoBrokerPartial = j.noBrokerPartial
  WHERE map.NoProduksi = @no;

  DELETE bp
  FROM dbo.BrokerPartial bp
  INNER JOIN OPENJSON(@jsPartials, '$.brokerPartial')
    WITH (noBrokerPartial varchar(50) '$.noBrokerPartial') j
    ON bp.NoBrokerPartial = j.noBrokerPartial;

  IF @brokerDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage=NULL,
        d.IsPartial = CASE WHEN EXISTS (
          SELECT 1 FROM dbo.BrokerPartial x WHERE x.NoBroker=d.NoBroker AND x.NoSak=d.NoSak
        ) THEN 1 ELSE 0 END
    FROM dbo.Broker_d d
    INNER JOIN @deletedBrokerPartials del ON d.NoBroker=del.NoBroker AND d.NoSak=del.NoSak;
  END;

  DECLARE @brokerRequested int;
  SELECT @brokerRequested = COUNT(*) FROM OPENJSON(@jsPartials,'$.brokerPartial');
  SET @brokerNotFound = @brokerRequested - @brokerDeleted;

  INSERT INTO @out SELECT 'brokerPartial', @brokerDeleted, @brokerNotFound;

  /* =======================
     MIXER PARTIAL
     ======================= */
  DECLARE @mixerDeleted int=0, @mixerNotFound int=0;

  SELECT @mixerDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputMixerPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.mixerPartial')
    WITH (noMixerPartial varchar(50) '$.noMixerPartial') j
    ON map.NoMixerPartial = j.noMixerPartial
  WHERE map.NoProduksi = @no;

  DECLARE @deletedMixerPartials TABLE (NoMixer varchar(50), NoSak int);

  INSERT INTO @deletedMixerPartials (NoMixer, NoSak)
  SELECT DISTINCT mp.NoMixer, mp.NoSak
  FROM dbo.MixerPartial mp
  INNER JOIN dbo.InjectProduksiInputMixerPartial map ON mp.NoMixerPartial = map.NoMixerPartial
  INNER JOIN OPENJSON(@jsPartials, '$.mixerPartial')
    WITH (noMixerPartial varchar(50) '$.noMixerPartial') j
    ON map.NoMixerPartial = j.noMixerPartial
  WHERE map.NoProduksi = @no;

  DELETE map
  FROM dbo.InjectProduksiInputMixerPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.mixerPartial')
    WITH (noMixerPartial varchar(50) '$.noMixerPartial') j
    ON map.NoMixerPartial = j.noMixerPartial
  WHERE map.NoProduksi = @no;

  DELETE mp
  FROM dbo.MixerPartial mp
  INNER JOIN OPENJSON(@jsPartials, '$.mixerPartial')
    WITH (noMixerPartial varchar(50) '$.noMixerPartial') j
    ON mp.NoMixerPartial = j.noMixerPartial;

  IF @mixerDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage=NULL,
        d.IsPartial = CASE WHEN EXISTS (
          SELECT 1 FROM dbo.MixerPartial x WHERE x.NoMixer=d.NoMixer AND x.NoSak=d.NoSak
        ) THEN 1 ELSE 0 END
    FROM dbo.Mixer_d d
    INNER JOIN @deletedMixerPartials del ON d.NoMixer=del.NoMixer AND d.NoSak=del.NoSak;
  END;

  DECLARE @mixerRequested int;
  SELECT @mixerRequested = COUNT(*) FROM OPENJSON(@jsPartials,'$.mixerPartial');
  SET @mixerNotFound = @mixerRequested - @mixerDeleted;

  INSERT INTO @out SELECT 'mixerPartial', @mixerDeleted, @mixerNotFound;

  /* =======================
     GILINGAN PARTIAL
     ======================= */
  DECLARE @gilinganDeleted int=0, @gilinganNotFound int=0;

  SELECT @gilinganDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputGilinganPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.gilinganPartial')
    WITH (noGilinganPartial varchar(50) '$.noGilinganPartial') j
    ON map.NoGilinganPartial = j.noGilinganPartial
  WHERE map.NoProduksi = @no;

  DECLARE @deletedGilinganPartials TABLE (NoGilingan varchar(50));

  INSERT INTO @deletedGilinganPartials (NoGilingan)
  SELECT DISTINCT gp.NoGilingan
  FROM dbo.GilinganPartial gp
  INNER JOIN dbo.InjectProduksiInputGilinganPartial map ON gp.NoGilinganPartial = map.NoGilinganPartial
  INNER JOIN OPENJSON(@jsPartials, '$.gilinganPartial')
    WITH (noGilinganPartial varchar(50) '$.noGilinganPartial') j
    ON map.NoGilinganPartial = j.noGilinganPartial
  WHERE map.NoProduksi = @no;

  DELETE map
  FROM dbo.InjectProduksiInputGilinganPartial map
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
    SET g.DateUsage=NULL,
        g.IsPartial = CASE WHEN EXISTS (
          SELECT 1 FROM dbo.GilinganPartial x WHERE x.NoGilingan=g.NoGilingan
        ) THEN 1 ELSE 0 END
    FROM dbo.Gilingan g
    INNER JOIN @deletedGilinganPartials del ON g.NoGilingan=del.NoGilingan;
  END;

  DECLARE @gilinganRequested int;
  SELECT @gilinganRequested = COUNT(*) FROM OPENJSON(@jsPartials,'$.gilinganPartial');
  SET @gilinganNotFound = @gilinganRequested - @gilinganDeleted;

  INSERT INTO @out SELECT 'gilinganPartial', @gilinganDeleted, @gilinganNotFound;

  /* =======================
     FURNITURE WIP PARTIAL
     ======================= */
  DECLARE @fwipDeleted int=0, @fwipNotFound int=0;

  SELECT @fwipDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputFurnitureWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;

  DECLARE @deletedFwipParents TABLE (NoFurnitureWIP varchar(50));

  INSERT INTO @deletedFwipParents(NoFurnitureWIP)
  SELECT DISTINCT fp.NoFurnitureWIP
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN dbo.InjectProduksiInputFurnitureWIPPartial map ON fp.NoFurnitureWIPPartial = map.NoFurnitureWIPPartial
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;

  DELETE map
  FROM dbo.InjectProduksiInputFurnitureWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;

  DELETE fp
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON fp.NoFurnitureWIPPartial = j.noFurnitureWipPartial;

  IF @fwipDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage=NULL,
        f.IsPartial = CASE WHEN EXISTS (
          SELECT 1 FROM dbo.FurnitureWIPPartial x WHERE x.NoFurnitureWIP=f.NoFurnitureWIP
        ) THEN 1 ELSE 0 END
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedFwipParents p ON p.NoFurnitureWIP=f.NoFurnitureWIP;
  END;

  DECLARE @fwipRequested int;
  SELECT @fwipRequested = COUNT(*) FROM OPENJSON(@jsPartials,'$.furnitureWipPartial');
  SET @fwipNotFound = @fwipRequested - @fwipDeleted;

  INSERT INTO @out SELECT 'furnitureWipPartial', @fwipDeleted, @fwipNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return { summary };
}


async function _deleteInputsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  /* ============ BROKER ============ */
  DECLARE @brokerDeleted int=0, @brokerNotFound int=0;

  SELECT @brokerDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputBroker map
  INNER JOIN OPENJSON(@jsInputs, '$.broker')
    WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
    ON map.NoBroker=j.noBroker AND map.NoSak=j.noSak
  WHERE map.NoProduksi=@no;

  IF @brokerDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage=NULL
    FROM dbo.Broker_d d
    INNER JOIN dbo.InjectProduksiInputBroker map ON d.NoBroker=map.NoBroker AND d.NoSak=map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.broker')
      WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
      ON map.NoBroker=j.noBroker AND map.NoSak=j.noSak
    WHERE map.NoProduksi=@no;
  END;

  DELETE map
  FROM dbo.InjectProduksiInputBroker map
  INNER JOIN OPENJSON(@jsInputs, '$.broker')
    WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
    ON map.NoBroker=j.noBroker AND map.NoSak=j.noSak
  WHERE map.NoProduksi=@no;

  DECLARE @brokerRequested int;
  SELECT @brokerRequested = COUNT(*) FROM OPENJSON(@jsInputs,'$.broker');
  SET @brokerNotFound = @brokerRequested - @brokerDeleted;
  INSERT INTO @out SELECT 'broker', @brokerDeleted, @brokerNotFound;

  /* ============ MIXER ============ */
  DECLARE @mixerDeleted int=0, @mixerNotFound int=0;

  SELECT @mixerDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputMixer map
  INNER JOIN OPENJSON(@jsInputs, '$.mixer')
    WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak') j
    ON map.NoMixer=j.noMixer AND map.NoSak=j.noSak
  WHERE map.NoProduksi=@no;

  IF @mixerDeleted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage=NULL
    FROM dbo.Mixer_d d
    INNER JOIN dbo.InjectProduksiInputMixer map ON d.NoMixer=map.NoMixer AND d.NoSak=map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.mixer')
      WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak') j
      ON map.NoMixer=j.noMixer AND map.NoSak=j.noSak
    WHERE map.NoProduksi=@no;
  END;

  DELETE map
  FROM dbo.InjectProduksiInputMixer map
  INNER JOIN OPENJSON(@jsInputs, '$.mixer')
    WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak') j
    ON map.NoMixer=j.noMixer AND map.NoSak=j.noSak
  WHERE map.NoProduksi=@no;

  DECLARE @mixerRequested int;
  SELECT @mixerRequested = COUNT(*) FROM OPENJSON(@jsInputs,'$.mixer');
  SET @mixerNotFound = @mixerRequested - @mixerDeleted;
  INSERT INTO @out SELECT 'mixer', @mixerDeleted, @mixerNotFound;

  /* ============ GILINGAN ============ */
  DECLARE @gilinganDeleted int=0, @gilinganNotFound int=0;

  SELECT @gilinganDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputGilingan map
  INNER JOIN OPENJSON(@jsInputs, '$.gilingan')
    WITH (noGilingan varchar(50) '$.noGilingan') j
    ON map.NoGilingan=j.noGilingan
  WHERE map.NoProduksi=@no;

  IF @gilinganDeleted > 0
  BEGIN
    UPDATE g
    SET g.DateUsage=NULL
    FROM dbo.Gilingan g
    INNER JOIN dbo.InjectProduksiInputGilingan map ON g.NoGilingan=map.NoGilingan
    INNER JOIN OPENJSON(@jsInputs, '$.gilingan')
      WITH (noGilingan varchar(50) '$.noGilingan') j
      ON map.NoGilingan=j.noGilingan
    WHERE map.NoProduksi=@no;
  END;

  DELETE map
  FROM dbo.InjectProduksiInputGilingan map
  INNER JOIN OPENJSON(@jsInputs, '$.gilingan')
    WITH (noGilingan varchar(50) '$.noGilingan') j
    ON map.NoGilingan=j.noGilingan
  WHERE map.NoProduksi=@no;

  DECLARE @gilinganRequested int;
  SELECT @gilinganRequested = COUNT(*) FROM OPENJSON(@jsInputs,'$.gilingan');
  SET @gilinganNotFound = @gilinganRequested - @gilinganDeleted;
  INSERT INTO @out SELECT 'gilingan', @gilinganDeleted, @gilinganNotFound;

  /* ============ FURNITURE WIP ============ */
  DECLARE @fwipDeleted int=0, @fwipNotFound int=0;

  SELECT @fwipDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP=j.noFurnitureWip
  WHERE map.NoProduksi=@no;

  IF @fwipDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage=NULL
    FROM dbo.FurnitureWIP f
    INNER JOIN dbo.InjectProduksiInputFurnitureWIP map ON f.NoFurnitureWIP=map.NoFurnitureWIP
    INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
      WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
      ON map.NoFurnitureWIP=j.noFurnitureWip
    WHERE map.NoProduksi=@no;
  END;

  DELETE map
  FROM dbo.InjectProduksiInputFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP=j.noFurnitureWip
  WHERE map.NoProduksi=@no;

  DECLARE @fwipRequested int;
  SELECT @fwipRequested = COUNT(*) FROM OPENJSON(@jsInputs,'$.furnitureWip');
  SET @fwipNotFound = @fwipRequested - @fwipDeleted;
  INSERT INTO @out SELECT 'furnitureWip', @fwipDeleted, @fwipNotFound;

  /* ============ CABINET MATERIAL ============ */
  DECLARE @cmDeleted int=0, @cmNotFound int=0;

  SELECT @cmDeleted = COUNT(*)
  FROM dbo.InjectProduksiInputCabinetMaterial map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetMaterial=j.idCabinetMaterial
  WHERE map.NoProduksi=@no;

  DELETE map
  FROM dbo.InjectProduksiInputCabinetMaterial map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetMaterial=j.idCabinetMaterial
  WHERE map.NoProduksi=@no;

  DECLARE @cmRequested int;
  SELECT @cmRequested = COUNT(*) FROM OPENJSON(@jsInputs,'$.cabinetMaterial');
  SET @cmNotFound = @cmRequested - @cmDeleted;
  INSERT INTO @out SELECT 'cabinetMaterial', @cmDeleted, @cmNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return out;
}


module.exports = {
  getAllProduksi,
  getProduksiByDate,
  getFurnitureWipListByNoProduksi,
  getPackingListByNoProduksi,
  createInjectProduksi,
  updateInjectProduksi,
  deleteInjectProduksi,
  fetchInputs,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials
};

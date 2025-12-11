// services/gilingan-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

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
  const offset = (page - 1) * pageSize;
  const searchTerm = (search || '').trim();

  // WHERE clause yang dipakai untuk count & data
  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
  `;

  // 1) Count (tanpa join, lebih ringan)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.GilinganProduksi_h h WITH (NOLOCK)
    ${whereClause};
  `;
  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);
  const countRes = await countReq.query(countQry);

  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) return { data: [], total };

  // 2) Page data + joins
  const dataQry = `
    SELECT
      h.NoProduksi,
      h.Tanggal      AS TglProduksi,   -- kalau mau samakan nama di FE, bisa di-alias
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
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd
    FROM dbo.GilinganProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator
    ${whereClause}
    ORDER BY h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, pageSize);

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
  // ❌ jam tidak wajib dan tidak dipakai
  if (payload?.shift == null) must.push('shift');
  // aku anggap HourStart dan HourEnd wajib, karena kamu mau pakai itu
  if (!payload?.hourStart) must.push('hourStart');
  if (!payload?.hourEnd) must.push('hourEnd');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    const no1 = await generateNextNoProduksi(tx, { prefix: 'W.', width: 10 });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar, no1)
      .query(`
        SELECT 1
        FROM dbo.GilinganProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksi(tx, { prefix: 'W.', width: 10 })
      : no1;

    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),   noProduksi)
      .input('Tanggal',     sql.Date,          payload.tglProduksi) // kolom di DB
      .input('IdMesin',     sql.Int,           payload.idMesin)
      .input('IdOperator',  sql.Int,           payload.idOperator)
      // ❌ Jam dihilangkan
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
      INSERT INTO dbo.GilinganProduksi_h (
        NoProduksi, Tanggal, IdMesin, IdOperator, Shift,
        CreateBy, CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi, @Tanggal, @IdMesin, @IdOperator, @Shift,
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
    // 1. Lock row + cek ada / tidak
    const rqGet = new sql.Request(tx);
    const current = await rqGet
      .input('NoProduksi', sql.VarChar, noProduksi)
      .query(`
        SELECT *
        FROM dbo.GilinganProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (current.recordset.length === 0) {
      throw notFound('GilinganProduksi_h tidak ditemukan');
    }

    // 2. Build SET dinamis (partial update)
    const sets = [];
    const rqUpd = new sql.Request(tx);

    // Tanggal (di DB namanya Tanggal)
    if (payload.tglProduksi !== undefined) {
      sets.push('Tanggal = @Tanggal');
      rqUpd.input('Tanggal', sql.Date, payload.tglProduksi);
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

    // ❌ Tidak ada kolom Jam di-update (sudah tidak dipakai)

    // HourStart / HourEnd (cast ke time)
    if (payload.hourStart !== undefined) {
      sets.push('HourStart = CAST(@HourStart AS time(7))');
      rqUpd.input('HourStart', sql.VarChar(20), payload.hourStart);
    }

    if (payload.hourEnd !== undefined) {
      sets.push('HourEnd = CAST(@HourEnd AS time(7))');
      rqUpd.input('HourEnd', sql.VarChar(20), payload.hourEnd);
    }

    if (sets.length === 0) {
      await tx.rollback();
      throw badReq('No fields to update');
    }

    rqUpd.input('NoProduksi', sql.VarChar, noProduksi);

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

    // 3. Kalau tanggal berubah -> sync DateUsage input (Broker, Crusher, Reject)
    if (payload.tglProduksi !== undefined && updatedHeader) {
      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar, noProduksi)
        .input('Tanggal', sql.Date, updatedHeader.Tanggal);

      const sqlUpdateUsage = `
        -------------------------------------------------------
        -- BROKER (FULL + PARTIAL) sebagai input GILINGAN
        -------------------------------------------------------
        UPDATE br
        SET br.DateUsage = @Tanggal
        FROM dbo.Broker_d AS br
        WHERE br.DateUsage IS NOT NULL
          AND (
            -- FULL
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputBroker AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoBroker   = br.NoBroker
                AND map.NoSak      = br.NoSak
            )
            OR
            -- PARTIAL
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
            -- FULL
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputRejectV2 AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoReject   = r.NoReject
            )
            OR
            -- PARTIAL
            EXISTS (
              SELECT 1
              FROM dbo.GilinganProduksiInputRejectV2Partial AS mp
              JOIN dbo.RejectV2Partial AS rp
                ON rp.NoRejectPartial = mp.NoRejectPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND rp.NoReject   = r.NoReject
            )
          );

        -------------------------------------------------------
        -- TODO: INPUT "BONGGOLAN"
        -- Kamu sudah punya mapping: GilinganProduksiInputBonggolan (NoProduksi, NoBonggolan)
        -- Tapi belum kelihatan tabel datenya (mungkin Bonggolan / Bonggolan_d).
        -- Begitu tabel date-usage Bonggolan jelas, bisa ditambah block UPDATE di sini,
        -- mirip pola di atas (JOIN via NoBonggolan).
        -------------------------------------------------------
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
    // -1. Cek header ada atau tidak + lock row
    // -------------------------------------------------------
    const rqCheckHead = new sql.Request(tx);
    const head = await rqCheckHead
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT 1
        FROM dbo.GilinganProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (head.recordset.length === 0) {
      await tx.rollback();
      throw notFound('GilinganProduksi_h tidak ditemukan');
    }

    // -------------------------------------------------------
    // 0. (TODO) CEK OUTPUT GILINGAN JIKA NANTI ADA TABELNYA
    //    Misal: GilinganProduksiOutput, dst.
    // -------------------------------------------------------
    // Untuk sekarang belum ada info tabel output, jadi kita skip.

    // -------------------------------------------------------
    // 1. DELETE INPUT + PARTIAL + RESET DATEUSAGE
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
    ---------------------------------------------------------
    -- TABLE VARIABLE UNTUK MENYIMPAN KEY YANG TERDAMPAK
    ---------------------------------------------------------
    DECLARE @BrokerKeys TABLE (
      NoBroker varchar(50),
      NoSak    varchar(50)
    );

    DECLARE @RejectKeys TABLE (
      NoReject varchar(50)
    );

    ---------------------------------------------------------
    -- 1. BONGGOLAN (GilinganProduksiInputBonggolan)
    --    (belum ada info tabel master + DateUsage Bonggolan,
    --     jadi di sini kita hanya hapus mapping input-nya saja)
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
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoBroker   = b.NoBroker
              AND map.NoSak      = b.NoSak
          )
       OR EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputBrokerPartial AS mp
            JOIN dbo.BrokerPartial AS bp
              ON bp.NoBrokerPartial = mp.NoBrokerPartial
            WHERE mp.NoProduksi = @NoProduksi
              AND bp.NoBroker   = b.NoBroker
              AND bp.NoSak      = b.NoSak
          );

    -- Hapus detail partial broker
    DELETE bp
    FROM dbo.BrokerPartial AS bp
    JOIN dbo.GilinganProduksiInputBrokerPartial AS mp
      ON mp.NoBrokerPartial = bp.NoBrokerPartial
    WHERE mp.NoProduksi = @NoProduksi;

    -- Hapus mapping partial broker
    DELETE FROM dbo.GilinganProduksiInputBrokerPartial
    WHERE NoProduksi = @NoProduksi;

    -- Hapus mapping full broker
    DELETE FROM dbo.GilinganProduksiInputBroker
    WHERE NoProduksi = @NoProduksi;

    -- Reset DateUsage & IsPartial di Broker_d
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
    -- 3. CRUSHER (FULL ONLY) sebagai input GILINGAN
    ---------------------------------------------------------
    UPDATE c
    SET c.DateUsage = NULL
    FROM dbo.Crusher AS c
    JOIN dbo.GilinganProduksiInputCrusher AS map
      ON map.NoCrusher = c.NoCrusher
    WHERE map.NoProduksi = @NoProduksi;

    DELETE FROM dbo.GilinganProduksiInputCrusher
    WHERE NoProduksi = @NoProduksi;

    ---------------------------------------------------------
    -- 4. REJECT (FULL + PARTIAL) sebagai input GILINGAN
    ---------------------------------------------------------
    INSERT INTO @RejectKeys (NoReject)
    SELECT DISTINCT r.NoReject
    FROM dbo.RejectV2 AS r
    WHERE EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputRejectV2 AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoReject   = r.NoReject
          )
       OR EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputRejectV2Partial AS mp
            JOIN dbo.RejectV2Partial AS rp
              ON rp.NoRejectPartial = mp.NoRejectPartial
            WHERE mp.NoProduksi = @NoProduksi
              AND rp.NoReject   = r.NoReject
          );

    -- Hapus detail partial reject
    DELETE rp
    FROM dbo.RejectV2Partial AS rp
    JOIN dbo.GilinganProduksiInputRejectV2Partial AS mp
      ON mp.NoRejectPartial = rp.NoRejectPartial
    WHERE mp.NoProduksi = @NoProduksi;

    -- Hapus mapping partial reject
    DELETE FROM dbo.GilinganProduksiInputRejectV2Partial
    WHERE NoProduksi = @NoProduksi;

    -- Hapus mapping full reject
    DELETE FROM dbo.GilinganProduksiInputRejectV2
    WHERE NoProduksi = @NoProduksi;

    -- Reset DateUsage & IsPartial di RejectV2
    UPDATE r
    SET r.DateUsage = NULL,
        r.IsPartial = CASE 
          WHEN EXISTS (
            SELECT 1 FROM dbo.RejectV2Partial AS rp
            WHERE rp.NoReject = r.NoReject
          ) THEN 1 ELSE 0 END
    FROM dbo.RejectV2 AS r
    JOIN @RejectKeys AS k
      ON k.NoReject = r.NoReject;

    ---------------------------------------------------------
    -- 5. TERAKHIR: HAPUS HEADER GILINGANPRODUKSI_H
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

module.exports = { getProduksiByDate, getAllProduksi, createGilinganProduksi, updateGilinganProduksi, deleteGilinganProduksi };

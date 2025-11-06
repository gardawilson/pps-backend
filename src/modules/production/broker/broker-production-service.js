// services/broker-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');


/**
 * Paginated fetch for dbo.BrokerProduksi_h
 * Columns available:
 *  NoProduksi, TglProduksi, IdMesin, IdOperator, Jam, Shift, CreateBy,
 *  CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter
 *
 * We LEFT JOIN to masters and ALIAS Jam -> JamKerja for UI compatibility.
 */
async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;
  const offset = (page - 1) * pageSize;
  const searchTerm = (search || '').trim();

  // We'll reuse the same WHERE clause for count and data queries
  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
  `;

  // 1) Count (lightweight, no joins)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.BrokerProduksi_h h WITH (NOLOCK)
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
      h.TglProduksi,
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
      h.HourStart,
      h.HourEnd
    FROM dbo.BrokerProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator
    ${whereClause}
    ORDER BY h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit',  sql.Int, pageSize);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}



// fetchInputs(): main items + partial items (with full keys) in SAME list
async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noProduksi);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */
    SELECT 
      'broker'  AS Src, ib.NoProduksi, ib.NoBroker AS Ref1, ib.NoSak AS Ref2, CAST(NULL AS varchar(50)) AS Ref3,
      br.Berat AS Berat, CAST(NULL AS decimal(18,3)) AS BeratAct, br.IsPartial AS IsPartial
    FROM dbo.BrokerProduksiInputBroker ib WITH (NOLOCK)
    LEFT JOIN dbo.Broker_d br WITH (NOLOCK)
      ON br.NoBroker = ib.NoBroker AND br.NoSak = ib.NoSak
    WHERE ib.NoProduksi=@no

    UNION ALL
    SELECT
      'bb' AS Src, ibb.NoProduksi, ibb.NoBahanBaku AS Ref1, ibb.NoPallet AS Ref2, ibb.NoSak AS Ref3,
      bb.Berat AS Berat, bb.BeratAct AS BeratAct, bb.IsPartial AS IsPartial
    FROM dbo.BrokerProduksiInputBB ibb WITH (NOLOCK)
    LEFT JOIN dbo.BahanBaku_d bb WITH (NOLOCK)
      ON bb.NoBahanBaku = ibb.NoBahanBaku AND bb.NoPallet = ibb.NoPallet AND bb.NoSak = ibb.NoSak
    WHERE ibb.NoProduksi=@no

    UNION ALL
    SELECT
      'washing' AS Src, iw.NoProduksi, iw.NoWashing AS Ref1, iw.NoSak AS Ref2, CAST(NULL AS varchar(50)) AS Ref3,
      wd.Berat AS Berat, CAST(NULL AS decimal(18,3)) AS BeratAct, CAST(NULL AS bit) AS IsPartial
    FROM dbo.BrokerProduksiInputWashing iw WITH (NOLOCK)
    LEFT JOIN dbo.Washing_d wd WITH (NOLOCK)
      ON wd.NoWashing = iw.NoWashing AND wd.NoSak = iw.NoSak
    WHERE iw.NoProduksi=@no

    UNION ALL
    SELECT
      'crusher' AS Src, ic.NoProduksi, ic.NoCrusher AS Ref1, CAST(NULL AS varchar(50)) AS Ref2, CAST(NULL AS varchar(50)) AS Ref3,
      c.Berat AS Berat, CAST(NULL AS decimal(18,3)) AS BeratAct, CAST(NULL AS bit) AS IsPartial
    FROM dbo.BrokerProduksiInputCrusher ic WITH (NOLOCK)
    LEFT JOIN dbo.Crusher c WITH (NOLOCK)
      ON c.NoCrusher = ic.NoCrusher
    WHERE ic.NoProduksi=@no

    UNION ALL
    SELECT
      'gilingan' AS Src, ig.NoProduksi, ig.NoGilingan AS Ref1, CAST(NULL AS varchar(50)) AS Ref2, CAST(NULL AS varchar(50)) AS Ref3,
      g.Berat AS Berat, CAST(NULL AS decimal(18,3)) AS BeratAct, g.IsPartial AS IsPartial
    FROM dbo.BrokerProduksiInputGilingan ig WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan g WITH (NOLOCK)
      ON g.NoGilingan = ig.NoGilingan
    WHERE ig.NoProduksi=@no

    UNION ALL
    SELECT
      'mixer' AS Src, im.NoProduksi, im.NoMixer AS Ref1, im.NoSak AS Ref2, CAST(NULL AS varchar(50)) AS Ref3,
      md.Berat AS Berat, CAST(NULL AS decimal(18,3)) AS BeratAct, md.IsPartial AS IsPartial
    FROM dbo.BrokerProduksiInputMixer im WITH (NOLOCK)
    LEFT JOIN dbo.Mixer_d md WITH (NOLOCK)
      ON md.NoMixer = im.NoMixer AND md.NoSak = im.NoSak
    WHERE im.NoProduksi=@no

    UNION ALL
    SELECT
      'reject' AS Src, ir.NoProduksi, ir.NoReject AS Ref1, CAST(NULL AS varchar(50)) AS Ref2, CAST(NULL AS varchar(50)) AS Ref3,
      rj.Berat AS Berat, CAST(NULL AS decimal(18,3)) AS BeratAct, CAST(NULL AS bit) AS IsPartial
    FROM dbo.BrokerProduksiInputReject ir WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj WITH (NOLOCK)
      ON rj.NoReject = ir.NoReject
    WHERE ir.NoProduksi=@no;

    /* =========== [2] PARTIAL VIA MAPPING (NoProduksi → PartialNo → Detail) =========== */

    /* bb partial → include NoBahanBaku, NoPallet, NoSak, Berat */
    SELECT pmap.NoBBPartial, pdet.NoBahanBaku, pdet.NoPallet, pdet.NoSak, pdet.Berat
    FROM dbo.BrokerProduksiInputBBPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BahanBakuPartial pdet WITH (NOLOCK)
      ON pdet.NoBBPartial = pmap.NoBBPartial
    WHERE pmap.NoProduksi = @no;

    /* gilingan partial → include NoGilingan, Berat */
    SELECT gmap.NoGilinganPartial, gdet.NoGilingan, gdet.Berat
    FROM dbo.BrokerProduksiInputGilinganPartial gmap WITH (NOLOCK)
    LEFT JOIN dbo.GilinganPartial gdet WITH (NOLOCK)
      ON gdet.NoGilinganPartial = gmap.NoGilinganPartial
    WHERE gmap.NoProduksi = @no;

    /* mixer partial → include NoMixer, NoSak, Berat */
    SELECT mmap.NoMixerPartial, mdet.NoMixer, mdet.NoSak, mdet.Berat
    FROM dbo.BrokerProduksiInputMixerPartial mmap WITH (NOLOCK)
    LEFT JOIN dbo.MixerPartial mdet WITH (NOLOCK)
      ON mdet.NoMixerPartial = mmap.NoMixerPartial
    WHERE mmap.NoProduksi = @no;

    /* reject partial → include NoReject, Berat */
    SELECT rmap.NoRejectPartial, rdet.NoReject, rdet.Berat
    FROM dbo.BrokerProduksiInputRejectPartial rmap WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2Partial rdet WITH (NOLOCK)
      ON rdet.NoRejectPartial = rmap.NoRejectPartial
    WHERE rmap.NoProduksi = @no;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bbPart   = rs.recordsets?.[1] || [];
  const gilPart  = rs.recordsets?.[2] || [];
  const mixPart  = rs.recordsets?.[3] || [];
  const rejPart  = rs.recordsets?.[4] || [];

  const out = {
    broker: [], bb: [], washing: [], crusher: [], gilingan: [], mixer: [], reject: [],
    summary: { broker:0, bb:0, washing:0, crusher:0, gilingan:0, mixer:0, reject:0 },
  };

  // MAIN rows (tetap)
  for (const r of mainRows) {
    const base = { berat: r.Berat ?? null, beratAct: r.BeratAct ?? null, isPartial: r.IsPartial ?? null };
    switch (r.Src) {
      case 'broker':
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'bb':
        out.bb.push({ noBahanBaku: r.Ref1, noPallet: r.Ref2, noSak: r.Ref3, ...base });
        break;
      case 'washing':
        out.washing.push({ noWashing: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'crusher':
        out.crusher.push({ noCrusher: r.Ref1, ...base });
        break;
      case 'gilingan':
        out.gilingan.push({ noGilingan: r.Ref1, ...base });
        break;
      case 'mixer':
        out.mixer.push({ noMixer: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'reject':
        out.reject.push({ noReject: r.Ref1, ...base });
        break;
    }
  }

  // PARTIAL rows (dengan full keys)
  // bb partial → { noBBPartial, noBahanBaku, noPallet, noSak, berat }
  for (const p of bbPart) {
    out.bb.push({
      noBBPartial: p.NoBBPartial,
      noBahanBaku: p.NoBahanBaku ?? null,
      noPallet:    p.NoPallet ?? null,
      noSak:       p.NoSak ?? null,
      berat:       p.Berat ?? null
    });
  }

  // gilingan partial → { noGilinganPartial, noGilingan, berat }
  for (const p of gilPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan:        p.NoGilingan ?? null,
      berat:             p.Berat ?? null
    });
  }

  // mixer partial → { noMixerPartial, noMixer, noSak, berat }
  for (const p of mixPart) {
    out.mixer.push({
      noMixerPartial: p.NoMixerPartial,
      noMixer:        p.NoMixer ?? null,
      noSak:          p.NoSak ?? null,
      berat:          p.Berat ?? null
    });
  }

  // reject partial → { noRejectPartial, noReject, berat }
  for (const p of rejPart) {
    out.reject.push({
      noRejectPartial: p.NoRejectPartial,
      noReject:        p.NoReject ?? null,
      berat:           p.Berat ?? null
    });
  }

  // Summary (main + partial)
  for (const k of Object.keys(out.summary)) out.summary[k] = out[k].length;

  return out;
}




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
    FROM dbo.BrokerProduksi_h h
    LEFT JOIN dbo.MstMesin m ON h.IdMesin = m.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @date
    ORDER BY h.Jam ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}



function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

async function generateNextNoProduksi(tx, { prefix = 'E.', width = 10 } = {}) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoProduksi
    FROM dbo.BrokerProduksi_h AS h WITH (UPDLOCK, HOLDLOCK)
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

function badReq(msg) {
  const e = new Error(msg);
  e.statusCode = 400;
  return e;
}

async function createBrokerProduksi (payload) {
  const must = [];
  if (!payload?.tglProduksi) must.push('tglProduksi');
  if (payload?.idMesin == null) must.push('idMesin');
  if (payload?.idOperator == null) must.push('idOperator');
  if (payload?.jam == null) must.push('jam');          // tetep, buat durasi
  if (payload?.shift == null) must.push('shift');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    const no1 = await generateNextNoProduksi(tx, { prefix: 'E.', width: 10 });
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar, no1)
      .query(`
        SELECT 1
        FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);
    const noProduksi = exist.recordset.length
      ? await generateNextNoProduksi(tx, { prefix: 'E.', width: 10 })
      : no1;

    const jamInt = parseJamToInt(payload.jam);

    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi',  sql.VarChar(50),   noProduksi)
      .input('TglProduksi', sql.Date,          payload.tglProduksi)
      .input('IdMesin',     sql.Int,           payload.idMesin)
      .input('IdOperator',  sql.Int,           payload.idOperator)
      .input('Jam',         sql.Int,           jamInt)
      .input('Shift',       sql.Int,           payload.shift)
      .input('CreateBy',    sql.VarChar(100),  payload.createBy)
      .input('CheckBy1',    sql.VarChar(100),  payload.checkBy1 ?? null)
      .input('CheckBy2',    sql.VarChar(100),  payload.checkBy2 ?? null)
      .input('ApproveBy',   sql.VarChar(100),  payload.approveBy ?? null)
      .input('JmlhAnggota', sql.Int,           payload.jmlhAnggota ?? null)
      .input('Hadir',       sql.Int,           payload.hadir ?? null)
      .input('HourMeter',   sql.Decimal(18, 2), payload.hourMeter ?? null)
      // ⬇️ tambahin ini kalau kolomnya ada
      .input('HourStart', sql.VarChar(20), payload.hourStart ?? null)
      .input('HourEnd',   sql.VarChar(20), payload.hourEnd ?? null);

    const insertSql = `
      INSERT INTO dbo.BrokerProduksi_h (
        NoProduksi, TglProduksi, IdMesin, IdOperator, Jam, Shift,
        CreateBy, CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi, @TglProduksi, @IdMesin, @IdOperator, @Jam, @Shift,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @JmlhAnggota, @Hadir, @HourMeter,
        CAST(@HourStart AS time(7)),  -- ⬅️ ini penting
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


async function updateBrokerProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 1. cek dulu ada gak
    const rqGet = new sql.Request(tx);
    const current = await rqGet
      .input('NoProduksi', sql.VarChar, noProduksi)
      .query(`
        SELECT *
        FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (current.recordset.length === 0) {
      throw badReq('Data not found');
    }

    // 2. build SET dinamis
    const sets = [];
    const rqUpd = new sql.Request(tx);

    // tglProduksi
    if (payload.tglProduksi !== undefined) {
      sets.push('TglProduksi = @TglProduksi');
      rqUpd.input('TglProduksi', sql.Date, payload.tglProduksi);
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

    // jam: kita parse ke int lagi
    if (payload.jam !== undefined) {
      const jamInt = payload.jam === null ? null : parseJamToInt(payload.jam);
      sets.push('Jam = @Jam');
      rqUpd.input('Jam', sql.Int, jamInt);
    }

    // hourStart / hourEnd
    if (payload.hourStart !== undefined) {
      sets.push('HourStart = CAST(@HourStart AS time(7))');
      rqUpd.input('HourStart', sql.VarChar(20), payload.hourStart);
    }
    if (payload.hourEnd !== undefined) {
      sets.push('HourEnd = CAST(@HourEnd AS time(7))');
      rqUpd.input('HourEnd', sql.VarChar(20), payload.hourEnd);
    }

    if (sets.length === 0) {
      // ga ada yang mau diupdate
      await tx.rollback();
      throw badReq('No fields to update');
    }

    rqUpd.input('NoProduksi', sql.VarChar, noProduksi);

    const updateSql = `
      UPDATE dbo.BrokerProduksi_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.BrokerProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);

    await tx.commit();

    return { header: updRes.recordset?.[0] || null };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}

async function deleteBrokerProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 1. cek dulu datanya ada gak
    const rqCheck = new sql.Request(tx);
    const ex = await rqCheck
      .input('NoProduksi', sql.VarChar, noProduksi)
      .query(`
        SELECT 1
        FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (ex.recordset.length === 0) {
      throw badReq('Data not found');
    }

    // 2. hapus
    const rqDel = new sql.Request(tx);
    await rqDel
      .input('NoProduksi', sql.VarChar, noProduksi)
      .query(`
        DELETE FROM dbo.BrokerProduksi_h
        WHERE NoProduksi = @NoProduksi;
      `);

    await tx.commit();
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
}



module.exports = { getAllProduksi, getProduksiByDate, fetchInputs, createBrokerProduksi, updateBrokerProduksi, deleteBrokerProduksi };

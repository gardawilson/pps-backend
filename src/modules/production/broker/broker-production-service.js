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
      'broker'  AS Src,
      ib.NoProduksi,
      ib.NoBroker AS Ref1,
      ib.NoSak    AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      br.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      br.IsPartial AS IsPartial,
      -- ✔ jenis PLASTIK → tampilkan sebagai IdJenis/NamaJenis (seragam)
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.BrokerProduksiInputBroker ib WITH (NOLOCK)
    LEFT JOIN dbo.Broker_d br        WITH (NOLOCK) ON br.NoBroker = ib.NoBroker AND br.NoSak = ib.NoSak
    LEFT JOIN dbo.Broker_h bh        WITH (NOLOCK) ON bh.NoBroker = ib.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK) ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE ib.NoProduksi=@no

    UNION ALL
    SELECT
      'bb' AS Src,
      ibb.NoProduksi,
      ibb.NoBahanBaku AS Ref1,
      ibb.NoPallet    AS Ref2,
      ibb.NoSak       AS Ref3,
      bb.Berat AS Berat,
      bb.BeratAct AS BeratAct,
      bb.IsPartial AS IsPartial,
      -- ✔ jenis PLASTIK (dari header pallet)
      bbh.IdJenisPlastik AS IdJenis,
      jpb.Jenis          AS NamaJenis
    FROM dbo.BrokerProduksiInputBB ibb WITH (NOLOCK)
    LEFT JOIN dbo.BahanBaku_d bb            WITH (NOLOCK)
      ON bb.NoBahanBaku = ibb.NoBahanBaku AND bb.NoPallet = ibb.NoPallet AND bb.NoSak = ibb.NoSak
    LEFT JOIN dbo.BahanBakuPallet_h bbh     WITH (NOLOCK)
      ON bbh.NoBahanBaku = ibb.NoBahanBaku AND bbh.NoPallet = ibb.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpb       WITH (NOLOCK)
      ON jpb.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE ibb.NoProduksi=@no

    UNION ALL
    SELECT
      'washing' AS Src,
      iw.NoProduksi,
      iw.NoWashing AS Ref1,
      iw.NoSak     AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      wd.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      -- ✔ jenis PLASTIK (dari Washing_h)
      wh.IdJenisPlastik AS IdJenis,
      jpw.Jenis          AS NamaJenis
    FROM dbo.BrokerProduksiInputWashing iw WITH (NOLOCK)
    LEFT JOIN dbo.Washing_d wd         WITH (NOLOCK) ON wd.NoWashing = iw.NoWashing AND wd.NoSak = iw.NoSak
    LEFT JOIN dbo.Washing_h wh         WITH (NOLOCK) ON wh.NoWashing = iw.NoWashing
    LEFT JOIN dbo.MstJenisPlastik jpw  WITH (NOLOCK) ON jpw.IdJenisPlastik = wh.IdJenisPlastik
    WHERE iw.NoProduksi=@no

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
      -- ✔ jenis CRUSHER → map ke IdJenis/NamaJenis
      c.IdCrusher    AS IdJenis,
      mc.NamaCrusher AS NamaJenis
    FROM dbo.BrokerProduksiInputCrusher ic WITH (NOLOCK)
    LEFT JOIN dbo.Crusher c     WITH (NOLOCK) ON c.NoCrusher = ic.NoCrusher
    LEFT JOIN dbo.MstCrusher mc WITH (NOLOCK) ON mc.IdCrusher = c.IdCrusher
    WHERE ic.NoProduksi=@no

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
      -- ✔ jenis GILINGAN → map ke IdJenis/NamaJenis
      g.IdGilingan    AS IdJenis,
      mg.NamaGilingan AS NamaJenis
    FROM dbo.BrokerProduksiInputGilingan ig WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan g        WITH (NOLOCK) ON g.NoGilingan = ig.NoGilingan
    LEFT JOIN dbo.MstGilingan mg    WITH (NOLOCK) ON mg.IdGilingan = g.IdGilingan
    WHERE ig.NoProduksi=@no

    UNION ALL
    SELECT
      'mixer' AS Src,
      im.NoProduksi,
      im.NoMixer AS Ref1,
      im.NoSak   AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      md.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      md.IsPartial AS IsPartial,
      -- ✔ jenis MIXER → map ke IdJenis/NamaJenis
      mh.IdMixer  AS IdJenis,
      mm.Jenis    AS NamaJenis
    FROM dbo.BrokerProduksiInputMixer im WITH (NOLOCK)
    LEFT JOIN dbo.Mixer_d md  WITH (NOLOCK)
      ON md.NoMixer = im.NoMixer AND md.NoSak = im.NoSak
    LEFT JOIN dbo.Mixer_h mh  WITH (NOLOCK)
      ON mh.NoMixer = im.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE im.NoProduksi=@no

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
      -- ✔ jenis REJECT → map ke IdJenis/NamaJenis
      rj.IdReject     AS IdJenis,
      mr.NamaReject   AS NamaJenis
    FROM dbo.BrokerProduksiInputReject ir WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK) ON rj.NoReject = ir.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK) ON mr.IdReject = rj.IdReject
    WHERE ir.NoProduksi=@no;

    /* =========== [2] PARTIALS (pakai IdJenis/NamaJenis juga) =========== */

    /* BB partial → jenis plastik dari header pallet */
    SELECT
      pmap.NoBBPartial,
      pdet.NoBahanBaku,
      pdet.NoPallet,
      pdet.NoSak,
      pdet.Berat,
      bbh.IdJenisPlastik AS IdJenis,
      jpp.Jenis          AS NamaJenis
    FROM dbo.BrokerProduksiInputBBPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BahanBakuPartial pdet WITH (NOLOCK)
      ON pdet.NoBBPartial = pmap.NoBBPartial
    LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
      ON bbh.NoBahanBaku = pdet.NoBahanBaku AND bbh.NoPallet = pdet.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpp WITH (NOLOCK)
      ON jpp.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE pmap.NoProduksi = @no;

    /* Gilingan partial → jenis gilingan */
    SELECT
      gmap.NoGilinganPartial,
      gdet.NoGilingan,
      gdet.Berat,
      gh.IdGilingan    AS IdJenis,
      mg.NamaGilingan  AS NamaJenis
    FROM dbo.BrokerProduksiInputGilinganPartial gmap WITH (NOLOCK)
    LEFT JOIN dbo.GilinganPartial gdet WITH (NOLOCK)
      ON gdet.NoGilinganPartial = gmap.NoGilinganPartial
    LEFT JOIN dbo.Gilingan gh      WITH (NOLOCK) ON gh.NoGilingan = gdet.NoGilingan
    LEFT JOIN dbo.MstGilingan mg   WITH (NOLOCK) ON mg.IdGilingan = gh.IdGilingan
    WHERE gmap.NoProduksi = @no;

    /* Mixer partial → jenis mixer */
    SELECT
      mmap.NoMixerPartial,
      mdet.NoMixer,
      mdet.NoSak,
      mdet.Berat,
      mh.IdMixer  AS IdJenis,
      mm.Jenis    AS NamaJenis
    FROM dbo.BrokerProduksiInputMixerPartial mmap WITH (NOLOCK)
    LEFT JOIN dbo.MixerPartial mdet WITH (NOLOCK)
      ON mdet.NoMixerPartial = mmap.NoMixerPartial
    LEFT JOIN dbo.Mixer_h mh  WITH (NOLOCK)
      ON mh.NoMixer = mdet.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE mmap.NoProduksi = @no;

    /* Reject partial → jenis reject */
    SELECT
      rmap.NoRejectPartial,
      rdet.NoReject,
      rdet.Berat,
      rj.IdReject     AS IdJenis,
      mr.NamaReject   AS NamaJenis
    FROM dbo.BrokerProduksiInputRejectPartial rmap WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2Partial rdet WITH (NOLOCK)
      ON rdet.NoRejectPartial = rmap.NoRejectPartial
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK)
      ON rj.NoReject = rdet.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK)
      ON mr.IdReject = rj.IdReject
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

  // MAIN rows (pakai idJenis/namaJenis yang seragam)
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

  // PARTIAL rows (semua pakai idJenis/namaJenis)
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

  for (const p of gilPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan:        p.NoGilingan ?? null,
      berat:             p.Berat ?? null,
      idJenis:           p.IdJenis ?? null,
      namaJenis:         p.NamaJenis ?? null,
    });
  }

  for (const p of mixPart) {
    out.mixer.push({
      noMixerPartial: p.NoMixerPartial,
      noMixer:        p.NoMixer ?? null,
      noSak:          p.NoSak ?? null,
      berat:          p.Berat ?? null,
      idJenis:        p.IdJenis ?? null,
      namaJenis:      p.NamaJenis ?? null,
    });
  }

  for (const p of rejPart) {
    out.reject.push({
      noRejectPartial: p.NoRejectPartial,
      noReject:        p.NoReject ?? null,
      berat:           p.Berat ?? null,
      idJenis:         p.IdJenis ?? null,
      namaJenis:       p.NamaJenis ?? null,
    });
  }

  // Summary
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
          d.Berat,
          d.BeratAct,
          d.DateUsage,
          d.IsPartial,
          ph.IdJenisPlastik      AS idJenis,
          jp.Jenis               AS namaJenis,
          ISNULL(pa.PartialBerat, 0) AS PartialBerat,
          SisaBerat = CASE
                        WHEN ISNULL(NULLIF(d.BeratAct, 0), d.Berat) - ISNULL(pa.PartialBerat, 0) < 0
                          THEN 0
                        ELSE ISNULL(NULLIF(d.BeratAct, 0), d.Berat) - ISNULL(pa.PartialBerat, 0)
                      END
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
        SELECT
          d.NoBroker, d.NoSak,
          berat     = CASE WHEN ISNULL(d.Berat,0) - ISNULL(pa.TotalPartial,0) < 0 THEN 0
                              ELSE ISNULL(d.Berat,0) - ISNULL(pa.TotalPartial,0) END,
          d.DateUsage, d.IsPartial,
          h.IdJenisPlastik AS idJenis, jp.Jenis AS namaJenis
        FROM dbo.Broker_d AS d WITH (NOLOCK)
        OUTER APPLY (
          SELECT SUM(ISNULL(bp.Berat,0)) AS TotalPartial
          FROM dbo.BrokerPartial AS bp WITH (NOLOCK)
          WHERE bp.NoBroker = d.NoBroker AND bp.NoSak = d.NoSak
            AND bp.NoBroker = @labelCode
        ) pa
        LEFT JOIN dbo.Broker_h AS h WITH (NOLOCK)  ON h.NoBroker = d.NoBroker
        LEFT JOIN dbo.MstJenisPlastik AS jp WITH (NOLOCK) ON jp.IdJenisPlastik = h.IdJenisPlastik
        WHERE d.NoBroker = @labelCode
          AND d.DateUsage IS NULL
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
          g.Berat,
          g.IsPartial,
          g.IdWarehouse,
          g.IdStatus,
          g.Blok,
          g.IdLokasi,
          BeratBase       = g.Berat,
          PartialTerpakai = ISNULL(pa.PartialBerat, 0),
          SisaBerat       = CASE
                              WHEN g.Berat - ISNULL(pa.PartialBerat, 0) < 0 THEN 0
                              ELSE g.Berat - ISNULL(pa.PartialBerat, 0)
                            END
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
        SELECT
          d.NoMixer,
          d.NoSak,
          d.Berat,
          d.DateUsage,
          d.IsPartial,
          d.IdLokasi,
          h.IdMixer     AS idJenis,
          mm.Jenis      AS namaJenis
        FROM dbo.Mixer_d AS d WITH (NOLOCK)
        LEFT JOIN dbo.Mixer_h AS h WITH (NOLOCK)
          ON h.NoMixer = d.NoMixer
        LEFT JOIN dbo.MstMixer AS mm WITH (NOLOCK)
          ON mm.IdMixer = h.IdMixer
        WHERE d.NoMixer = @labelCode
          AND d.DateUsage IS NULL
        ORDER BY d.NoMixer, d.NoSak;
      `;
      return await run(raw);

    // =========================
    // BF. RejectV2
    // =========================
    case 'BF.':
      tableName = 'RejectV2';
      query = `
        SELECT
          r.NoReject,
          r.IdReject     AS idJenis,
          mr.NamaReject  AS namaJenis,
          r.DateCreate,
          r.DateUsage,
          r.IdWarehouse,
          r.Berat,
          r.Jam,
          r.CreateBy,
          r.DateTimeCreate,
          r.Blok,
          r.IdLokasi
        FROM dbo.RejectV2 AS r WITH (NOLOCK)
        LEFT JOIN dbo.MstReject AS mr WITH (NOLOCK)
          ON mr.IdReject = r.IdReject
        WHERE r.NoReject = @labelCode
          AND r.DateUsage IS NULL
        ORDER BY r.NoReject;
      `;
      return await run(raw);

    default:
      throw new Error(`Invalid prefix: ${prefix}. Valid prefixes: A., B., D., M., F., V., H., BF.`);
  }
}


/**
 * Single entry: create NEW partials + link them, and attach EXISTING inputs.
 * All in one transaction.
 *
 * Payload shape (arrays optional):
 * {
 *   // existing inputs to attach
 *   broker:  [{ noBroker, noSak }],
 *   bb:      [{ noBahanBaku, noPallet, noSak }],
 *   washing: [{ noWashing, noSak }],
 *   crusher: [{ noCrusher }],
 *   gilingan:[{ noGilingan }],
 *   mixer:   [{ noMixer, noSak }],
 *   reject:  [{ noReject }],
 *
 *   // NEW partials to create + map
 *   bbPartialNew:       [{ noBahanBaku, noPallet, noSak, berat }],
 *   gilinganPartialNew: [{ noGilingan, berat }],
 *   mixerPartialNew:    [{ noMixer, noSak, berat }],
 *   rejectPartialNew:   [{ noReject, berat }]
 * }
 */
async function upsertInputsAndPartials(noProduksi, payload) {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => Array.isArray(a) ? a : [];

  const body = {
    broker:  norm(payload.broker),
    bb:      norm(payload.bb),
    washing: norm(payload.washing),
    crusher: norm(payload.crusher),
    gilingan:norm(payload.gilingan),
    mixer:   norm(payload.mixer),
    reject:  norm(payload.reject),

    bbPartialNew:       norm(payload.bbPartialNew),
    gilinganPartialNew: norm(payload.gilinganPartialNew),
    mixerPartialNew:    norm(payload.mixerPartialNew),
    rejectPartialNew:   norm(payload.rejectPartialNew),
  };

  try {
    await tx.begin();

    // 1) Create partials + map them to produksi
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      bbPartialNew:       body.bbPartialNew,
      gilinganPartialNew: body.gilinganPartialNew,
      mixerPartialNew:    body.mixerPartialNew,
      rejectPartialNew:   body.rejectPartialNew,
    });

    // 2) Attach existing inputs (idempotent)
    const attachments = await _insertInputsWithTx(tx, noProduksi, {
      broker:  body.broker,
      bb:      body.bb,
      washing: body.washing,
      crusher: body.crusher,
      gilingan:body.gilingan,
      mixer:   body.mixer,
      reject:  body.reject,
    });

    await tx.commit();

    return {
      noProduksi,
      createdPartials: partials.createdLists,  // lists of generated codes
      summary: {
        partials: partials.summary,            // created counts
        attachments: attachments,              // inserted/skipped/invalid per section
      }
    };
  } catch (err) {
    try { await tx.rollback(); } catch {}
    throw err;
  }
}


/**
 * Single entry: create NEW partials + link them, and attach EXISTING inputs.
 * All in one transaction.
 *
 * Payload shape (arrays optional):
 * {
 *   // existing inputs to attach
 *   broker:  [{ noBroker, noSak }],
 *   bb:      [{ noBahanBaku, noPallet, noSak }],
 *   washing: [{ noWashing, noSak }],
 *   crusher: [{ noCrusher }],
 *   gilingan:[{ noGilingan }],
 *   mixer:   [{ noMixer, noSak }],
 *   reject:  [{ noReject }],
 *
 *   // NEW partials to create + map
 *   bbPartialNew:       [{ noBahanBaku, noPallet, noSak, berat }],
 *   gilinganPartialNew: [{ noGilingan, berat }],
 *   mixerPartialNew:    [{ noMixer, noSak, berat }],
 *   rejectPartialNew:   [{ noReject, berat }]
 * }
 */
async function upsertInputsAndPartials(noProduksi, payload) {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    broker: norm(payload.broker),
    bb: norm(payload.bb),
    washing: norm(payload.washing),
    crusher: norm(payload.crusher),
    gilingan: norm(payload.gilingan),
    mixer: norm(payload.mixer),
    reject: norm(payload.reject),

    bbPartialNew: norm(payload.bbPartialNew),
    gilinganPartialNew: norm(payload.gilinganPartialNew),
    mixerPartialNew: norm(payload.mixerPartialNew),
    rejectPartialNew: norm(payload.rejectPartialNew),
  };

  try {
    await tx.begin();

    // 1) Create partials + map them to produksi
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      bbPartialNew: body.bbPartialNew,
      gilinganPartialNew: body.gilinganPartialNew,
      mixerPartialNew: body.mixerPartialNew,
      rejectPartialNew: body.rejectPartialNew,
    });

    // 2) Attach existing inputs (idempotent)
    const attachments = await _insertInputsWithTx(tx, noProduksi, {
      broker: body.broker,
      bb: body.bb,
      washing: body.washing,
      crusher: body.crusher,
      gilingan: body.gilingan,
      mixer: body.mixer,
      reject: body.reject,
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
        totalPartialsCreated,
        hasErrors: hasInvalid,
        hasWarnings: totalSkipped > 0,
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
    { key: 'broker', label: 'Broker' },
    { key: 'bb', label: 'Bahan Baku' },
    { key: 'washing', label: 'Washing' },
    { key: 'crusher', label: 'Crusher' },
    { key: 'gilingan', label: 'Gilingan' },
    { key: 'mixer', label: 'Mixer' },
    { key: 'reject', label: 'Reject' },
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
    { key: 'mixerPartialNew', label: 'Mixer Partial' },
    { key: 'rejectPartialNew', label: 'Reject Partial' },
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
  FROM dbo.BrokerProduksi_h WITH (NOLOCK)
  WHERE NoProduksi = @no;

  -- Global lock for sequence generation (10s timeout)
  DECLARE @lockResult int;
  EXEC @lockResult = sp_getapplock
    @Resource = 'SEQ_PARTIALS',
    @LockMode = 'Exclusive',
    @LockTimeout = 10000,
    @DbPrincipal = 'public';

  IF (@lockResult < 0)
  BEGIN
    RAISERROR('Failed to acquire SEQ_PARTIALS lock', 16, 1);
  END;

  -- Capture generated codes for response
  DECLARE @bbNew TABLE(NoBBPartial varchar(50));
  DECLARE @gilNew TABLE(NoGilinganPartial varchar(50));
  DECLARE @mixNew TABLE(NoMixerPartial varchar(50));
  DECLARE @rejNew TABLE(NoRejectPartial varchar(50));

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

    -- Map to produksi
    INSERT INTO dbo.BrokerProduksiInputBBPartial (NoProduksi, NoBBPartial)
    SELECT @no, n.NoBBPartial
    FROM @bbNew n;

    -- Update BahanBaku_d: Set IsPartial=1, reduce BeratAct, set DateUsage if remaining=0
    UPDATE bb
    SET 
      bb.IsPartial = 1,
      bb.DateUsage = CASE 
        WHEN (bb.BeratAct - src.berat) <= 0 THEN @tglProduksi 
        ELSE bb.DateUsage 
      END
    FROM dbo.BahanBaku_d bb
    INNER JOIN (
      SELECT noBahanBaku, noPallet, noSak, berat
      FROM OPENJSON(@jsPartials, '$.bbPartialNew')
      WITH (
        noBahanBaku varchar(50) '$.noBahanBaku',
        noPallet    int         '$.noPallet',
        noSak       int         '$.noSak',
        berat       decimal(18,3) '$.berat'
      )
    ) src ON bb.NoBahanBaku = src.noBahanBaku 
         AND bb.NoPallet = src.noPallet 
         AND bb.NoSak = src.noSak;
  END;

  /* ==============================
     GILINGAN PARTIAL (Y.##########)
     ============================== */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.gilinganPartialNew'))
  BEGIN
    DECLARE @nextG int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoGilinganPartial,10) AS int))
      FROM dbo.GilinganPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoGilinganPartial LIKE 'Y.%'
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
        NewNo = CONCAT('Y.', RIGHT(REPLICATE('0',10) + CAST(@nextG + rn AS varchar(10)), 10)),
        noGilingan, berat
      FROM src
    )
    INSERT INTO dbo.GilinganPartial (NoGilinganPartial, NoGilingan, Berat)
    OUTPUT INSERTED.NoGilinganPartial INTO @gilNew(NoGilinganPartial)
    SELECT NewNo, noGilingan, berat
    FROM numbered;

    INSERT INTO dbo.BrokerProduksiInputGilinganPartial (NoProduksi, NoGilinganPartial)
    SELECT @no, n.NoGilinganPartial
    FROM @gilNew n;

    -- Update Gilingan: Set IsPartial=1, reduce Berat, set DateUsage if remaining=0
    UPDATE g
    SET 
      g.IsPartial = 1,
      g.DateUsage = CASE 
        WHEN (g.Berat - src.berat) <= 0 THEN @tglProduksi 
        ELSE g.DateUsage 
      END
    FROM dbo.Gilingan g
    INNER JOIN (
      SELECT noGilingan, berat
      FROM OPENJSON(@jsPartials, '$.gilinganPartialNew')
      WITH (
        noGilingan varchar(50) '$.noGilingan',
        berat      decimal(18,3) '$.berat'
      )
    ) src ON g.NoGilingan = src.noGilingan;
  END;

  /* ==========================
     MIXER PARTIAL (R.##########)
     ========================== */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.mixerPartialNew'))
  BEGIN
    DECLARE @nextM int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoMixerPartial,10) AS int))
      FROM dbo.MixerPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoMixerPartial LIKE 'R.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noMixer,
        noSak,
        berat,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.mixerPartialNew')
      WITH (
        noMixer varchar(50) '$.noMixer',
        noSak   int         '$.noSak',
        berat   decimal(18,3) '$.berat'
      )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('R.', RIGHT(REPLICATE('0',10) + CAST(@nextM + rn AS varchar(10)), 10)),
        noMixer, noSak, berat
      FROM src
    )
    INSERT INTO dbo.MixerPartial (NoMixerPartial, NoMixer, NoSak, Berat)
    OUTPUT INSERTED.NoMixerPartial INTO @mixNew(NoMixerPartial)
    SELECT NewNo, noMixer, noSak, berat
    FROM numbered;

    -- Per spec: link produksi goes to BrokerProduksiInputMixer (not a *_Partial table)
    INSERT INTO dbo.BrokerProduksiInputMixer (NoProduksi, NoMixer, NoSak)
    SELECT @no, s.noMixer, s.noSak
    FROM OPENJSON(@jsPartials, '$.mixerPartialNew')
    WITH (
      noMixer varchar(50) '$.noMixer',
      noSak   int         '$.noSak',
      berat   decimal(18,3) '$.berat'
    ) s
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.BrokerProduksiInputMixer x
      WHERE x.NoProduksi=@no AND x.NoMixer=s.noMixer AND x.NoSak=s.noSak
    );

    -- Update Mixer_d: Set IsPartial=1, reduce Berat, set DateUsage if remaining=0
    UPDATE m
    SET 
      m.IsPartial = 1,
      m.DateUsage = CASE 
        WHEN (m.Berat - src.berat) <= 0 THEN @tglProduksi 
        ELSE m.DateUsage 
      END
    FROM dbo.Mixer_d m
    INNER JOIN (
      SELECT noMixer, noSak, berat
      FROM OPENJSON(@jsPartials, '$.mixerPartialNew')
      WITH (
        noMixer varchar(50) '$.noMixer',
        noSak   int         '$.noSak',
        berat   decimal(18,3) '$.berat'
      )
    ) src ON m.NoMixer = src.noMixer AND m.NoSak = src.noSak;
  END;

  /* ===============================
     REJECT PARTIAL (BK.##########)
     =============================== */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.rejectPartialNew'))
  BEGIN
    DECLARE @nextRj int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoRejectPartial,10) AS int))
      FROM dbo.RejectV2Partial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoRejectPartial LIKE 'BK.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noReject,
        berat,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.rejectPartialNew')
      WITH (
        noReject varchar(50) '$.noReject',
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

    INSERT INTO dbo.BrokerProduksiInputRejectPartial (NoProduksi, NoRejectPartial)
    SELECT @no, n.NoRejectPartial
    FROM @rejNew n;

    -- Update RejectV2: reduce Berat, set DateUsage if remaining=0
    -- Note: RejectV2 doesn't have IsPartial column based on the schema you provided
    UPDATE r
    SET 
      r.DateUsage = CASE 
        WHEN (r.Berat - src.berat) <= 0 THEN @tglProduksi 
        ELSE r.DateUsage 
      END
    FROM dbo.RejectV2 r
    INNER JOIN (
      SELECT noReject, berat
      FROM OPENJSON(@jsPartials, '$.rejectPartialNew')
      WITH (
        noReject varchar(50) '$.noReject',
        berat    decimal(18,3) '$.berat'
      )
    ) src ON r.NoReject = src.noReject;
  END;

  -- Release the applock
  EXEC sp_releaseapplock @Resource = 'SEQ_PARTIALS', @DbPrincipal = 'public';

  -- Summaries
  SELECT 'bbPartialNew'       AS Section, COUNT(*) AS Created FROM @bbNew
  UNION ALL
  SELECT 'gilinganPartialNew' AS Section, COUNT(*) FROM @gilNew
  UNION ALL
  SELECT 'mixerPartialNew'    AS Section, COUNT(*) FROM @mixNew
  UNION ALL
  SELECT 'rejectPartialNew'   AS Section, COUNT(*) FROM @rejNew;

  -- Return generated codes as separate recordsets (for UI)
  SELECT NoBBPartial        FROM @bbNew;
  SELECT NoGilinganPartial  FROM @gilNew;
  SELECT NoMixerPartial     FROM @mixNew;
  SELECT NoRejectPartial    FROM @rejNew;
  `;

  const rs = await req.query(SQL_PARTIALS);

  // Recordset[0]: summary rows
  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  // Recordsets[1..4]: codes
  const createdLists = {
    bbPartialNew: (rs.recordsets?.[1] || []).map((r) => r.NoBBPartial),
    gilinganPartialNew: (rs.recordsets?.[2] || []).map((r) => r.NoGilinganPartial),
    mixerPartialNew: (rs.recordsets?.[3] || []).map((r) => r.NoMixerPartial),
    rejectPartialNew: (rs.recordsets?.[4] || []).map((r) => r.NoRejectPartial),
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
  FROM dbo.BrokerProduksi_h WITH (NOLOCK)
  WHERE NoProduksi = @no;

  DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);

  -- BROKER
  DECLARE @brokerInserted int = 0;
  DECLARE @brokerSkipped int = 0;
  DECLARE @brokerInvalid int = 0;

  ;WITH j AS (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK) WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak)
  )
  INSERT INTO dbo.BrokerProduksiInputBroker (NoProduksi, NoBroker, NoSak)
  SELECT @no, v.noBroker, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputBroker x 
    WHERE x.NoProduksi=@no AND x.NoBroker=v.noBroker AND x.NoSak=v.noSak
  );

  SET @brokerInserted = @@ROWCOUNT;

  -- Update DateUsage for Broker_d
  IF @brokerInserted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tglProduksi
    FROM dbo.Broker_d b
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.broker')
      WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' ) src
      WHERE b.NoBroker = src.noBroker AND b.NoSak = src.noSak
    );
  END;

  SELECT @brokerSkipped = COUNT(*) FROM (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK) WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak)
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputBroker x WHERE x.NoProduksi=@no AND x.NoBroker=j.noBroker AND x.NoSak=j.noSak);

  SELECT @brokerInvalid = COUNT(*) FROM (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK) WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak);

  INSERT INTO @out SELECT 'broker', @brokerInserted, @brokerSkipped, @brokerInvalid;

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
    WHERE EXISTS (SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK) WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak)
  )
  INSERT INTO dbo.BrokerProduksiInputBB (NoProduksi, NoBahanBaku, NoPallet, NoSak)
  SELECT @no, v.noBahanBaku, v.noPallet, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputBB x 
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
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputBB x WHERE x.NoProduksi=@no AND x.NoBahanBaku=j.noBahanBaku AND x.NoPallet=j.noPallet AND x.NoSak=j.noSak);

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
  INSERT INTO dbo.BrokerProduksiInputWashing (NoProduksi, NoWashing, NoSak)
  SELECT @no, v.noWashing, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputWashing x 
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
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputWashing x WHERE x.NoProduksi=@no AND x.NoWashing=j.noWashing AND x.NoSak=j.noSak);

  SELECT @washingInvalid = COUNT(*) FROM (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK) WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'washing', @washingInserted, @washingSkipped, @washingInvalid;

  -- CRUSHER
  DECLARE @crusherInserted int = 0;
  DECLARE @crusherSkipped int = 0;
  DECLARE @crusherInvalid int = 0;

  ;WITH j AS (
    SELECT noCrusher
    FROM OPENJSON(@jsInputs, '$.crusher') WITH ( noCrusher varchar(50) '$.noCrusher' )
  ),
  v AS (
    SELECT j.* FROM j WHERE EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher)
  )
  INSERT INTO dbo.BrokerProduksiInputCrusher (NoProduksi, NoCrusher)
  SELECT @no, v.noCrusher
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputCrusher x WHERE x.NoProduksi=@no AND x.NoCrusher=v.noCrusher
  );

  SET @crusherInserted = @@ROWCOUNT;

  -- Update DateUsage for Crusher
  IF @crusherInserted > 0
  BEGIN
    UPDATE c
    SET c.DateUsage = @tglProduksi
    FROM dbo.Crusher c
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.crusher')
      WITH ( noCrusher varchar(50) '$.noCrusher' ) src
      WHERE c.NoCrusher = src.noCrusher
    );
  END;

  SELECT @crusherSkipped = COUNT(*) FROM (
    SELECT noCrusher
    FROM OPENJSON(@jsInputs, '$.crusher') WITH ( noCrusher varchar(50) '$.noCrusher' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher)
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputCrusher x WHERE x.NoProduksi=@no AND x.NoCrusher=j.noCrusher);

  SELECT @crusherInvalid = COUNT(*) FROM (
    SELECT noCrusher
    FROM OPENJSON(@jsInputs, '$.crusher') WITH ( noCrusher varchar(50) '$.noCrusher' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher);

  INSERT INTO @out SELECT 'crusher', @crusherInserted, @crusherSkipped, @crusherInvalid;

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
  INSERT INTO dbo.BrokerProduksiInputGilingan (NoProduksi, NoGilingan)
  SELECT @no, v.noGilingan
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputGilingan x WHERE x.NoProduksi=@no AND x.NoGilingan=v.noGilingan
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
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputGilingan x WHERE x.NoProduksi=@no AND x.NoGilingan=j.noGilingan);

  SELECT @gilinganInvalid = COUNT(*) FROM (
    SELECT noGilingan
    FROM OPENJSON(@jsInputs, '$.gilingan') WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan);

  INSERT INTO @out SELECT 'gilingan', @gilinganInserted, @gilinganSkipped, @gilinganInvalid;

  -- MIXER
  DECLARE @mixerInserted int = 0;
  DECLARE @mixerSkipped int = 0;
  DECLARE @mixerInvalid int = 0;

  ;WITH j AS (
    SELECT noMixer, noSak
    FROM OPENJSON(@jsInputs, '$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK) WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak)
  )
  INSERT INTO dbo.BrokerProduksiInputMixer (NoProduksi, NoMixer, NoSak)
  SELECT @no, v.noMixer, v.noSak
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputMixer x 
    WHERE x.NoProduksi=@no AND x.NoMixer=v.noMixer AND x.NoSak=v.noSak
  );

  SET @mixerInserted = @@ROWCOUNT;

  -- Update DateUsage for Mixer_d
  IF @mixerInserted > 0
  BEGIN
    UPDATE m
    SET m.DateUsage = @tglProduksi
    FROM dbo.Mixer_d m
    WHERE EXISTS (
      SELECT 1 FROM OPENJSON(@jsInputs, '$.mixer')
      WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' ) src
      WHERE m.NoMixer = src.noMixer AND m.NoSak = src.noSak
    );
  END;

  SELECT @mixerSkipped = COUNT(*) FROM (
    SELECT noMixer, noSak
    FROM OPENJSON(@jsInputs, '$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK) WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak)
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputMixer x WHERE x.NoProduksi=@no AND x.NoMixer=j.noMixer AND x.NoSak=j.noSak);

  SELECT @mixerInvalid = COUNT(*) FROM (
    SELECT noMixer, noSak
    FROM OPENJSON(@jsInputs, '$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK) WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak);

  INSERT INTO @out SELECT 'mixer', @mixerInserted, @mixerSkipped, @mixerInvalid;

  -- REJECT
  DECLARE @rejectInserted int = 0;
  DECLARE @rejectSkipped int = 0;
  DECLARE @rejectInvalid int = 0;

  ;WITH j AS (
    SELECT noReject
    FROM OPENJSON(@jsInputs, '$.reject') WITH ( noReject varchar(50) '$.noReject' )
  ),
  v AS (
    SELECT j.* FROM j WHERE EXISTS (SELECT 1 FROM dbo.RejectV2 r WITH (NOLOCK) WHERE r.NoReject=j.noReject)
  )
  INSERT INTO dbo.BrokerProduksiInputReject (NoProduksi, NoReject)
  SELECT @no, v.noReject
  FROM v WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BrokerProduksiInputReject x WHERE x.NoProduksi=@no AND x.NoReject=v.noReject
  );

  SET @rejectInserted = @@ROWCOUNT;

  -- Update DateUsage for RejectV2
  IF @rejectInserted > 0
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

  SELECT @rejectSkipped = COUNT(*) FROM (
    SELECT noReject
    FROM OPENJSON(@jsInputs, '$.reject') WITH ( noReject varchar(50) '$.noReject' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.RejectV2 r WITH (NOLOCK) WHERE r.NoReject=j.noReject)
    AND EXISTS (SELECT 1 FROM dbo.BrokerProduksiInputReject x WHERE x.NoProduksi=@no AND x.NoReject=j.noReject);

  SELECT @rejectInvalid = COUNT(*) FROM (
    SELECT noReject
    FROM OPENJSON(@jsInputs, '$.reject') WITH ( noReject varchar(50) '$.noReject' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.RejectV2 r WITH (NOLOCK) WHERE r.NoReject=j.noReject);

  INSERT INTO @out SELECT 'reject', @rejectInserted, @rejectSkipped, @rejectInvalid;

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

module.exports = { getAllProduksi, getProduksiByDate, fetchInputs, createBrokerProduksi, updateBrokerProduksi, deleteBrokerProduksi, validateLabel, upsertInputsAndPartials };

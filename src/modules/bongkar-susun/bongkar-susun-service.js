// services/bongkar-susun-service.js
const { sql, poolPromise } = require('../../core/config/db');

async function getByDate(date /* 'YYYY-MM-DD' */) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      NoBongkarSusun,
      Tanggal,
      IdUsername,
      Note
    FROM BongkarSusun_h
    WHERE CONVERT(date, Tanggal) = @date
    ORDER BY Tanggal DESC;
  `;

  request.input('date', sql.Date, date);

  const result = await request.query(query);
  return result.recordset;
}

/**
 * Paginated fetch for dbo.BongkarSusun_h
 * Columns:
 *  NoBongkarSusun, Tanggal, IdUsername, Note + Username (from MstUsername)
 */
async function getAllBongkarSusun(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;
  const offset = (page - 1) * pageSize;
  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoBongkarSusun LIKE '%' + @search + '%')
  `;

  // 1) Hitung total
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.BongkarSusun_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);
  const countRes = await countReq.query(countQry);

  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) {
    return { data: [], total };
  }

  // 2) Ambil page data (JOIN ke MstUsername)
  const dataQry = `
    SELECT
      h.NoBongkarSusun,
      h.Tanggal,
      h.IdUsername,
      u.Username,
      h.Note
    FROM dbo.BongkarSusun_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstUsername u WITH (NOLOCK)
      ON u.IdUsername = h.IdUsername
    ${whereClause}
    ORDER BY h.NoBongkarSusun DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, pageSize);

  const dataRes = await dataReq.query(dataQry);

  return {
    data: dataRes.recordset || [],
    total,
  };
}

function badReq(msg) {
  const e = new Error(msg);
  e.statusCode = 400;
  return e;
}

function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

async function generateNextNoBongkarSusun(
  tx,
  { prefix = 'BG.', width = 10 } = {}
) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoBongkarSusun
    FROM dbo.BongkarSusun_h AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE h.NoBongkarSusun LIKE @prefix + '%'
    ORDER BY
      TRY_CONVERT(BIGINT, SUBSTRING(h.NoBongkarSusun, LEN(@prefix) + 1, 50)) DESC,
      h.NoBongkarSusun DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoBongkarSusun;
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

// ===========================
//  CREATE BongkarSusun_h
//  idUsername diambil via Username (dari JWT)
// ===========================
async function createBongkarSusun(payload) {
  const must = [];
  if (!payload?.tanggal) must.push('tanggal');
  if (!payload?.username) must.push('username');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 1) Resolve Username -> IdUsername
    const rqUser = new sql.Request(tx);
    const userRes = await rqUser
      .input('Username', sql.VarChar(100), payload.username)
      .query(`
        SELECT TOP 1 IdUsername
        FROM dbo.MstUsername WITH (NOLOCK)
        WHERE Username = @Username;
      `);

    if (userRes.recordset.length === 0) {
      throw badReq(`Username "${payload.username}" tidak ditemukan di MstUsername`);
    }

    const idUsername = userRes.recordset[0].IdUsername;

    // 2) Generate nomor baru BG.XXXXXXXXXX
    const no1 = await generateNextNoBongkarSusun(tx, {
      prefix: 'BG.',
      width: 10,
    });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBongkarSusun', sql.VarChar, no1)
      .query(`
        SELECT 1
        FROM dbo.BongkarSusun_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBongkarSusun = @NoBongkarSusun
      `);

    const noBongkarSusun = exist.recordset.length
      ? await generateNextNoBongkarSusun(tx, { prefix: 'BG.', width: 10 })
      : no1;

    // 3) Insert header
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoBongkarSusun', sql.VarChar(50), noBongkarSusun)
      .input('Tanggal', sql.Date, payload.tanggal) // 'YYYY-MM-DD'
      .input('IdUsername', sql.Int, idUsername)
      .input('Note', sql.VarChar(255), payload.note ?? null);

    const insertSql = `
      INSERT INTO dbo.BongkarSusun_h (
        NoBongkarSusun, Tanggal, IdUsername, Note
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoBongkarSusun, @Tanggal, @IdUsername, @Note
      );
    `;

    const insRes = await rqIns.query(insertSql);
    await tx.commit();

    return { header: insRes.recordset?.[0] || null };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

// ===========================
//  UPDATE BongkarSusun_h
// ===========================
async function updateBongkarSusun(noBongkarSusun, payload) {
  if (!noBongkarSusun) {
    throw badReq('noBongkarSusun wajib diisi');
  }

  // Kumpulkan field yang akan di-update
  const setClauses = [];
  const params = [];

  if (Object.prototype.hasOwnProperty.call(payload, 'tanggal')) {
    setClauses.push('Tanggal = @Tanggal');
    params.push({ name: 'Tanggal', type: sql.Date, value: payload.tanggal });
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'idUsername')) {
    setClauses.push('IdUsername = @IdUsername');
    params.push({
      name: 'IdUsername',
      type: sql.Int,
      value: payload.idUsername,
    });
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'note')) {
    setClauses.push('Note = @Note');
    params.push({
      name: 'Note',
      type: sql.VarChar(255),
      value: payload.note ?? null,
    });
  }

  if (setClauses.length === 0) {
    throw badReq('Tidak ada field yang diupdate');
  }

  const pool = await poolPromise;
  const request = pool.request();

  params.forEach((p) => {
    request.input(p.name, p.type, p.value);
  });

  request.input('NoBongkarSusun', sql.VarChar(50), noBongkarSusun);

  const sqlUpdate = `
    UPDATE dbo.BongkarSusun_h
    SET ${setClauses.join(', ')}
    OUTPUT INSERTED.*
    WHERE NoBongkarSusun = @NoBongkarSusun;
  `;

  const result = await request.query(sqlUpdate);

  if (result.recordset.length === 0) {
    throw badReq('BongkarSusun tidak ditemukan');
  }

  return { header: result.recordset[0] };
}

// ===========================
//  DELETE BongkarSusun_h
// ===========================
async function deleteBongkarSusun(noBongkarSusun) {
  if (!noBongkarSusun) {
    throw badReq('noBongkarSusun wajib diisi');
  }

  const pool = await poolPromise;
  const request = pool.request();
  request.input('NoBongkarSusun', sql.VarChar(50), noBongkarSusun);

  const sqlDelete = `
    DELETE FROM dbo.BongkarSusun_h
    WHERE NoBongkarSusun = @NoBongkarSusun;
  `;

  const result = await request.query(sqlDelete);

  if (result.rowsAffected[0] === 0) {
    throw badReq('BongkarSusun tidak ditemukan atau sudah dihapus');
  }

  return true;
}


async function fetchInputs(noBongkarSusun) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noBongkarSusun);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */
    SELECT
      Src,
      NoBongkarSusun,
      Ref1,
      Ref2,
      Ref3,
      Pcs,
      Berat,
      BeratAct,
      IsPartial,
      IdJenis,
      NamaJenis
    FROM (
      /* ===================== BB ===================== */
      SELECT
        'bb' AS Src,
        ib.NoBongkarSusun,
        ib.NoBahanBaku AS Ref1,
        ib.NoPallet    AS Ref2,
        ib.NoSak       AS Ref3,
        CAST(NULL AS int) AS Pcs,
        bb.Berat       AS Berat,
        bb.BeratAct    AS BeratAct,
        bb.IsPartial   AS IsPartial,
        bbh.IdJenisPlastik AS IdJenis,
        jpb.Jenis          AS NamaJenis
      FROM dbo.BongkarSusunInputBahanBaku ib WITH (NOLOCK)
      LEFT JOIN dbo.BahanBaku_d bb WITH (NOLOCK)
        ON bb.NoBahanBaku = ib.NoBahanBaku AND bb.NoPallet = ib.NoPallet AND bb.NoSak = ib.NoSak
      LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
        ON bbh.NoBahanBaku = ib.NoBahanBaku AND bbh.NoPallet = ib.NoPallet
      LEFT JOIN dbo.MstJenisPlastik jpb WITH (NOLOCK)
        ON jpb.IdJenisPlastik = bbh.IdJenisPlastik
      WHERE ib.NoBongkarSusun = @no

      UNION ALL

      /* ===================== WASHING ===================== */
      SELECT
        'washing' AS Src,
        iw.NoBongkarSusun,
        iw.NoWashing AS Ref1,
        iw.NoSak     AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        CAST(NULL AS int) AS Pcs,
        wd.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        CAST(NULL AS bit) AS IsPartial,
        wh.IdJenisPlastik AS IdJenis,
        jpw.Jenis          AS NamaJenis
      FROM dbo.BongkarSusunInputWashing iw WITH (NOLOCK)
      LEFT JOIN dbo.Washing_d wd WITH (NOLOCK)
        ON wd.NoWashing = iw.NoWashing AND wd.NoSak = iw.NoSak
      LEFT JOIN dbo.Washing_h wh WITH (NOLOCK)
        ON wh.NoWashing = iw.NoWashing
      LEFT JOIN dbo.MstJenisPlastik jpw WITH (NOLOCK)
        ON jpw.IdJenisPlastik = wh.IdJenisPlastik
      WHERE iw.NoBongkarSusun = @no

      UNION ALL

      /* ===================== BROKER ===================== */
      SELECT
        'broker' AS Src,
        ibk.NoBongkarSusun,
        ibk.NoBroker AS Ref1,
        ibk.NoSak    AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        CAST(NULL AS int) AS Pcs,
        br.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        br.IsPartial AS IsPartial,
        bh.IdJenisPlastik AS IdJenis,
        jp.Jenis          AS NamaJenis
      FROM dbo.BongkarSusunInputBroker ibk WITH (NOLOCK)
      LEFT JOIN dbo.Broker_d br WITH (NOLOCK)
        ON br.NoBroker = ibk.NoBroker AND br.NoSak = ibk.NoSak
      LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
        ON bh.NoBroker = ibk.NoBroker
      LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
        ON jp.IdJenisPlastik = bh.IdJenisPlastik
      WHERE ibk.NoBongkarSusun = @no

      UNION ALL

      /* ===================== CRUSHER ===================== */
      SELECT
        'crusher' AS Src,
        ic.NoBongkarSusun,
        ic.NoCrusher AS Ref1,
        CAST(NULL AS varchar(50)) AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        CAST(NULL AS int) AS Pcs,
        c.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        CAST(NULL AS bit) AS IsPartial,
        c.IdCrusher    AS IdJenis,
        mc.NamaCrusher AS NamaJenis
      FROM dbo.BongkarSusunInputCrusher ic WITH (NOLOCK)
      LEFT JOIN dbo.Crusher c WITH (NOLOCK)
        ON c.NoCrusher = ic.NoCrusher
      LEFT JOIN dbo.MstCrusher mc WITH (NOLOCK)
        ON mc.IdCrusher = c.IdCrusher
      WHERE ic.NoBongkarSusun = @no

      UNION ALL

      /* ===================== BONGGOLAN ===================== */
      SELECT
        'bonggolan' AS Src,
        ibg.NoBongkarSusun,
        ibg.NoBonggolan AS Ref1,
        CAST(NULL AS varchar(50)) AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        CAST(NULL AS int) AS Pcs,
        bg.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        CAST(NULL AS bit) AS IsPartial,
        bg.IdBonggolan AS IdJenis,
        mb.NamaBonggolan AS NamaJenis
      FROM dbo.BongkarSusunInputBonggolan ibg WITH (NOLOCK)
      LEFT JOIN dbo.Bonggolan bg WITH (NOLOCK)
        ON bg.NoBonggolan = ibg.NoBonggolan
      LEFT JOIN dbo.MstBonggolan mb WITH (NOLOCK)
        ON mb.IdBonggolan = bg.IdBonggolan
      WHERE ibg.NoBongkarSusun = @no

      UNION ALL

      /* ===================== GILINGAN ===================== */
      SELECT
        'gilingan' AS Src,
        ig.NoBongkarSusun,
        ig.NoGilingan AS Ref1,
        CAST(NULL AS varchar(50)) AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        CAST(NULL AS int) AS Pcs,
        g.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        g.IsPartial AS IsPartial,
        g.IdGilingan    AS IdJenis,
        mg.NamaGilingan AS NamaJenis
      FROM dbo.BongkarSusunInputGilingan ig WITH (NOLOCK)
      LEFT JOIN dbo.Gilingan g WITH (NOLOCK)
        ON g.NoGilingan = ig.NoGilingan
      LEFT JOIN dbo.MstGilingan mg WITH (NOLOCK)
        ON mg.IdGilingan = g.IdGilingan
      WHERE ig.NoBongkarSusun = @no

      UNION ALL

      /* ===================== MIXER ===================== */
      SELECT
        'mixer' AS Src,
        im.NoBongkarSusun,
        im.NoMixer AS Ref1,
        im.NoSak   AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        CAST(NULL AS int) AS Pcs,
        md.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        md.IsPartial AS IsPartial,
        mh.IdMixer AS IdJenis,
        mm.Jenis   AS NamaJenis
      FROM dbo.BongkarSusunInputMixer im WITH (NOLOCK)
      LEFT JOIN dbo.Mixer_d md WITH (NOLOCK)
        ON md.NoMixer = im.NoMixer AND md.NoSak = im.NoSak
      LEFT JOIN dbo.Mixer_h mh WITH (NOLOCK)
        ON mh.NoMixer = im.NoMixer
      LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
        ON mm.IdMixer = mh.IdMixer
      WHERE im.NoBongkarSusun = @no

      UNION ALL

      /* ===================== FURNITURE WIP (MAIN) ===================== */
      SELECT
        'furniture_wip' AS Src,
        ifw.NoBongkarSusun,
        ifw.NoFurnitureWIP AS Ref1,
        CAST(NULL AS varchar(50)) AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        fw.Pcs AS Pcs,
        fw.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        fw.IsPartial AS IsPartial,
        fw.IDFurnitureWIP AS IdJenis,
        CAST(NULL AS varchar(100)) AS NamaJenis
      FROM dbo.BongkarSusunInputFurnitureWIP ifw WITH (NOLOCK)
      LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
        ON fw.NoFurnitureWIP = ifw.NoFurnitureWIP
      WHERE ifw.NoBongkarSusun = @no

      UNION ALL

      /* ===================== BARANG JADI (MAIN) ===================== */
      SELECT
        'barang_jadi' AS Src,
        ibj.NoBongkarSusun,
        ibj.NoBJ AS Ref1,
        CAST(NULL AS varchar(50)) AS Ref2,
        CAST(NULL AS varchar(50)) AS Ref3,
        bj.Pcs AS Pcs,
        bj.Berat AS Berat,
        CAST(NULL AS decimal(18,3)) AS BeratAct,
        bj.IsPartial AS IsPartial,
        bj.IdBJ AS IdJenis,
        mbj.NamaBJ AS NamaJenis
      FROM dbo.BongkarSusunInputBarangJadi ibj WITH (NOLOCK)
      LEFT JOIN dbo.BarangJadi bj WITH (NOLOCK)
        ON bj.NoBJ = ibj.NoBJ
      LEFT JOIN dbo.MstBarangJadi mbj WITH (NOLOCK)
        ON mbj.IdBJ = bj.IdBJ
      WHERE ibj.NoBongkarSusun = @no
    ) X
    ORDER BY X.Src, X.Ref1 DESC, X.Ref2 ASC, X.Ref3 ASC;

    /* ===================== [2] BB PARTIAL ===================== */
    SELECT
      p.NoBBPartial,
      p.NoBahanBaku,
      p.NoPallet,
      p.NoSak,
      p.Berat,
      bbh.IdJenisPlastik AS IdJenis,
      jpp.Jenis          AS NamaJenis
    FROM dbo.BahanBakuPartial p WITH (NOLOCK)
    LEFT JOIN dbo.BahanBakuPallet_h bbh WITH (NOLOCK)
      ON bbh.NoBahanBaku = p.NoBahanBaku AND bbh.NoPallet = p.NoPallet
    LEFT JOIN dbo.MstJenisPlastik jpp WITH (NOLOCK)
      ON jpp.IdJenisPlastik = bbh.IdJenisPlastik
    WHERE EXISTS (
      SELECT 1
      FROM dbo.BongkarSusunInputBahanBaku ib WITH (NOLOCK)
      WHERE ib.NoBongkarSusun = @no
        AND ib.NoBahanBaku = p.NoBahanBaku
        AND ib.NoPallet    = p.NoPallet
        AND ib.NoSak       = p.NoSak
    )
    ORDER BY p.NoBBPartial DESC;

    /* ===================== [3] GILINGAN PARTIAL ===================== */
    SELECT
      gp.NoGilinganPartial,
      gp.NoGilingan,
      gp.Berat,
      g.IdGilingan   AS IdJenis,
      mg.NamaGilingan AS NamaJenis
    FROM dbo.GilinganPartial gp WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan g WITH (NOLOCK)
      ON g.NoGilingan = gp.NoGilingan
    LEFT JOIN dbo.MstGilingan mg WITH (NOLOCK)
      ON mg.IdGilingan = g.IdGilingan
    WHERE EXISTS (
      SELECT 1
      FROM dbo.BongkarSusunInputGilingan ig WITH (NOLOCK)
      WHERE ig.NoBongkarSusun = @no
        AND ig.NoGilingan = gp.NoGilingan
    )
    ORDER BY gp.NoGilinganPartial DESC;

    /* ===================== [4] MIXER PARTIAL ===================== */
    SELECT
      mp.NoMixerPartial,
      mp.NoMixer,
      mp.NoSak,
      mp.Berat,
      mh.IdMixer AS IdJenis,
      mm.Jenis   AS NamaJenis
    FROM dbo.MixerPartial mp WITH (NOLOCK)
    LEFT JOIN dbo.Mixer_h mh WITH (NOLOCK)
      ON mh.NoMixer = mp.NoMixer
    LEFT JOIN dbo.MstMixer mm WITH (NOLOCK)
      ON mm.IdMixer = mh.IdMixer
    WHERE EXISTS (
      SELECT 1
      FROM dbo.BongkarSusunInputMixer im WITH (NOLOCK)
      WHERE im.NoBongkarSusun = @no
        AND im.NoMixer = mp.NoMixer
        AND im.NoSak   = mp.NoSak
    )
    ORDER BY mp.NoMixerPartial DESC;

    /* ===================== [5] BROKER PARTIAL ===================== */
    SELECT
      bp.NoBrokerPartial,
      bp.NoBroker,
      bp.NoSak,
      bp.Berat,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.BrokerPartial bp WITH (NOLOCK)
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = bp.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE EXISTS (
      SELECT 1
      FROM dbo.BongkarSusunInputBroker ibk WITH (NOLOCK)
      WHERE ibk.NoBongkarSusun = @no
        AND ibk.NoBroker = bp.NoBroker
        AND ibk.NoSak    = bp.NoSak
    )
    ORDER BY bp.NoBrokerPartial DESC;

    /* ===================== [6] BARANG JADI PARTIAL ===================== */
    SELECT
      p.NoBJPartial,
      p.NoBJ,
      p.Pcs,
      bj.IdBJ AS IdJenis,
      mbj.NamaBJ AS NamaJenis
    FROM dbo.BarangJadiPartial p WITH (NOLOCK)
    LEFT JOIN dbo.BarangJadi bj WITH (NOLOCK)
      ON bj.NoBJ = p.NoBJ
    LEFT JOIN dbo.MstBarangJadi mbj WITH (NOLOCK)
      ON mbj.IdBJ = bj.IdBJ
    WHERE EXISTS (
      SELECT 1
      FROM dbo.BongkarSusunInputBarangJadi ibj WITH (NOLOCK)
      WHERE ibj.NoBongkarSusun = @no
        AND ibj.NoBJ = p.NoBJ
    )
    ORDER BY p.NoBJPartial DESC;

    /* ===================== [7] FURNITURE WIP PARTIAL ===================== */
    SELECT
      p.NoFurnitureWIPPartial,
      p.NoFurnitureWIP,
      p.Pcs,
      fw.IDFurnitureWIP AS IdJenis
    FROM dbo.FurnitureWIPPartial p WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = p.NoFurnitureWIP
    WHERE EXISTS (
      SELECT 1
      FROM dbo.BongkarSusunInputFurnitureWIP ifw WITH (NOLOCK)
      WHERE ifw.NoBongkarSusun = @no
        AND ifw.NoFurnitureWIP = p.NoFurnitureWIP
    )
    ORDER BY p.NoFurnitureWIPPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bbPart   = rs.recordsets?.[1] || [];
  const gilPart  = rs.recordsets?.[2] || [];
  const mixPart  = rs.recordsets?.[3] || [];
  const brkPart  = rs.recordsets?.[4] || [];
  const bjPart   = rs.recordsets?.[5] || [];
  const fwPart   = rs.recordsets?.[6] || [];

  const out = {
    bb: [],
    washing: [],
    broker: [],
    crusher: [],
    bonggolan: [],
    gilingan: [],
    mixer: [],
    furnitureWip: [],
    barangJadi: [],
    summary: {
      bb: 0,
      washing: 0,
      broker: 0,
      crusher: 0,
      bonggolan: 0,
      gilingan: 0,
      mixer: 0,
      furnitureWip: 0,
      barangJadi: 0,
    },
  };

  // MAIN rows
  for (const r of mainRows) {
    const base = {
      pcs: r.Pcs ?? null,
      berat: r.Berat ?? null,
      beratAct: r.BeratAct ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis: r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
    };

    switch (r.Src) {
      case 'bb':
        out.bb.push({ noBahanBaku: r.Ref1, noPallet: r.Ref2, noSak: r.Ref3, ...base });
        break;
      case 'washing':
        out.washing.push({ noWashing: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'broker':
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'crusher':
        out.crusher.push({ noCrusher: r.Ref1, ...base });
        break;
      case 'bonggolan':
        out.bonggolan.push({ noBonggolan: r.Ref1, ...base });
        break;
      case 'gilingan':
        out.gilingan.push({ noGilingan: r.Ref1, ...base });
        break;
      case 'mixer':
        out.mixer.push({ noMixer: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case 'furniture_wip':
        out.furnitureWip.push({ noFurnitureWIP: r.Ref1, ...base });
        break;
      case 'barang_jadi':
        out.barangJadi.push({ noBJ: r.Ref1, ...base });
        break;
    }
  }

  // PARTIAL: BB
  for (const p of bbPart) {
    out.bb.push({
      noBBPartial: p.NoBBPartial,
      noBahanBaku: p.NoBahanBaku ?? null,
      noPallet:    p.NoPallet ?? null,
      noSak:       p.NoSak ?? null,
      pcs:         null,
      berat:       p.Berat ?? null,
      beratAct:    null,
      isPartial:   true,
      idJenis:     p.IdJenis ?? null,
      namaJenis:   p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Gilingan
  for (const p of gilPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan:        p.NoGilingan ?? null,
      pcs:               null,
      berat:             p.Berat ?? null,
      beratAct:          null,
      isPartial:         true,
      idJenis:           p.IdJenis ?? null,
      namaJenis:         p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Mixer
  for (const p of mixPart) {
    out.mixer.push({
      noMixerPartial: p.NoMixerPartial,
      noMixer:        p.NoMixer ?? null,
      noSak:          p.NoSak ?? null,
      pcs:            null,
      berat:          p.Berat ?? null,
      beratAct:       null,
      isPartial:      true,
      idJenis:        p.IdJenis ?? null,
      namaJenis:      p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Broker
  for (const p of brkPart) {
    out.broker.push({
      noBrokerPartial: p.NoBrokerPartial,
      noBroker:        p.NoBroker ?? null,
      noSak:           p.NoSak ?? null,
      pcs:             null,
      berat:           p.Berat ?? null,
      beratAct:        null,
      isPartial:       true,
      idJenis:         p.IdJenis ?? null,
      namaJenis:       p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Barang Jadi
  for (const p of bjPart) {
    out.barangJadi.push({
      noBJPartial: p.NoBJPartial,
      noBJ:        p.NoBJ ?? null,
      pcs:         p.Pcs ?? null,
      berat:       null,
      beratAct:    null,
      isPartial:   true,
      idJenis:     p.IdJenis ?? null,
      namaJenis:   p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Furniture WIP
  for (const p of fwPart) {
    out.furnitureWip.push({
      noFurnitureWIPPartial: p.NoFurnitureWIPPartial,
      noFurnitureWIP:        p.NoFurnitureWIP ?? null,
      pcs:                   p.Pcs ?? null,
      berat:                 null,
      beratAct:              null,
      isPartial:             true,
      idJenis:               p.IdJenis ?? null,
      namaJenis:             null, // kalau ada master furniture wip nanti kita isi
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
  const p3 = raw.substring(0, 3).toUpperCase(); // BF. / BB. / BA.
  if (p3 === 'BF.' || p3 === 'BB.' || p3 === 'BA.') {
    prefix = p3;
  } else {
    prefix = raw.substring(0, 2).toUpperCase(); // A. B. D. M. F. V. H. C. dll
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
// BB. FurnitureWIP (BB.0000036786)
// =========================
case 'BB.':
  tableName = 'FurnitureWIP';
  query = `
    ;WITH PartialAgg AS (
      SELECT
        p.NoFurnitureWIP,
        SUM(ISNULL(p.Pcs, 0)) AS PcsPartial
      FROM dbo.FurnitureWIPPartial AS p WITH (NOLOCK)
      GROUP BY p.NoFurnitureWIP
    )
    SELECT
      f.NoFurnitureWIP                 AS noFurnitureWip,
      f.DateCreate                     AS dateCreate,
      f.Jam                            AS jam,
      Pcs = CASE
              WHEN ISNULL(f.Pcs, 0) - ISNULL(pa.PcsPartial, 0) < 0 THEN 0
              ELSE ISNULL(f.Pcs, 0) - ISNULL(pa.PcsPartial, 0)
            END,
      f.IDFurnitureWIP                 AS idJenis,
      mc.Nama                          AS namaJenis,
      f.Berat                          AS berat,
      f.DateUsage                      AS dateUsage,
      f.IdWarehouse                    AS idWarehouse,
      f.IdWarna                        AS idWarna,
      f.CreateBy                       AS createBy,
      f.DateTimeCreate                 AS dateTimeCreate,
      f.Blok                           AS blok,
      f.IdLokasi                       AS idLokasi,
      CASE
        WHEN ISNULL(pa.PcsPartial, 0) > 0 THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END                              AS isPartial
    FROM dbo.FurnitureWIP AS f WITH (NOLOCK)
    LEFT JOIN PartialAgg AS pa
      ON pa.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP AS mc WITH (NOLOCK)
      ON mc.IdCabinetWIP = f.IDFurnitureWIP
    WHERE f.NoFurnitureWIP = @labelCode
      AND f.DateUsage IS NULL
      AND (ISNULL(f.Pcs, 0) - ISNULL(pa.PcsPartial, 0)) > 0
    ORDER BY f.NoFurnitureWIP;
  `;
  return await run(raw);


  // =========================
// BA. / C. BarangJadi (prefix bisa BA.000... atau BA.000... tergantung sistemmu)
// =========================
case 'BA.':
    tableName = 'BarangJadi';
    query = `
      ;WITH PartialAgg AS (
        SELECT
          p.NoBJ,
          SUM(ISNULL(p.Pcs, 0)) AS PcsPartial
        FROM dbo.BarangJadiPartial AS p WITH (NOLOCK)
        GROUP BY p.NoBJ
      )
      SELECT
        b.NoBJ                           AS noBj,
        b.IdBJ                           AS idJenis,
        mb.NamaBJ                        AS namaJenis,
        b.DateCreate                     AS dateCreate,
        b.DateUsage                      AS dateUsage,
        b.Jam                            AS jam,
        Pcs = CASE
                WHEN ISNULL(b.Pcs, 0) - ISNULL(pa.PcsPartial, 0) < 0 THEN 0
                ELSE ISNULL(b.Pcs, 0) - ISNULL(pa.PcsPartial, 0)
              END,
        b.Berat                          AS berat,
        b.IdWarehouse                    AS idWarehouse,
        b.CreateBy                       AS createBy,
        b.DateTimeCreate                 AS dateTimeCreate,
        b.Blok                           AS blok,
        b.IdLokasi                       AS idLokasi,
        CASE
          WHEN ISNULL(pa.PcsPartial, 0) > 0 THEN CAST(1 AS bit)
          ELSE CAST(0 AS bit)
        END                              AS isPartial
      FROM dbo.BarangJadi AS b WITH (NOLOCK)
      LEFT JOIN PartialAgg AS pa
        ON pa.NoBJ = b.NoBJ
      LEFT JOIN dbo.MstBarangJadi AS mb WITH (NOLOCK)
        ON mb.IdBJ = b.IdBJ
      WHERE b.NoBJ = @labelCode
        AND b.DateUsage IS NULL
        AND (ISNULL(b.Pcs, 0) - ISNULL(pa.PcsPartial, 0)) > 0
      ORDER BY b.NoBJ;
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
      throw new Error(`Invalid prefix: ${prefix}. Valid prefixes: A., B., C., D., M., F., V., H., BB., BA., BF.`);
    }
}

/**
 * Payload shape (arrays optional):
 * {
 *   broker:      [{ noBroker, noSak }],
 *   bb:          [{ noBahanBaku, noPallet, noSak }],
 *   washing:     [{ noWashing, noSak }],
 *   crusher:     [{ noCrusher }],
 *   gilingan:    [{ noGilingan }],
 *   mixer:       [{ noMixer, noSak }],
 *   reject:      [{ noReject }],          // kalau memang dipakai di bongkar susun kamu
 *   bonggolan:   [{ noBonggolan }],
 *   furnitureWip:[{ noFurnitureWip }],
 *   barangJadi:  [{ noBj }]
 * }
 */

async function upsertInputs(noBongkarSusun, payload) {
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

    bonggolan: norm(payload.bonggolan),
    furnitureWip: norm(payload.furnitureWip),
    barangJadi: norm(payload.barangJadi),
  };

  try {
    await tx.begin();

    const attachments = await _insertInputsWithTx(tx, noBongkarSusun, body);

    await tx.commit();

    const totalInserted = Object.values(attachments).reduce((s, x) => s + (x.inserted || 0), 0);
    const totalSkipped  = Object.values(attachments).reduce((s, x) => s + (x.skipped  || 0), 0);
    const totalInvalid  = Object.values(attachments).reduce((s, x) => s + (x.invalid  || 0), 0);

    const response = {
      noBongkarSusun,
      summary: { totalInserted, totalSkipped, totalInvalid },
      details: attachments
    };

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = totalInserted === 0;

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


async function _insertInputsWithTx(tx, noBongkarSusun, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBongkarSusun);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_ATTACH = `
  SET NOCOUNT ON;

  -----------------------------------------------------------------------
  -- 1) Ambil tanggal dari header bongkar susun
  -----------------------------------------------------------------------
  DECLARE @tgl datetime;

  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BongkarSusun_h h WITH (NOLOCK)
  WHERE h.NoBongkarSusun = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BongkarSusun_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Inserted int, Skipped int, Invalid int);

  /* =====================================================================
     BROKER -> BongkarSusunInputBroker + update Broker_d.DateUsage
     ===================================================================== */
  DECLARE @brokerInserted int = 0, @brokerSkipped int = 0, @brokerInvalid int = 0;
  DECLARE @insBroker TABLE(NoBroker varchar(50), NoSak int);

  ;WITH j AS (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK)
      WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak
        AND b.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputBroker (NoBongkarSusun, NoBroker, NoSak)
  OUTPUT INSERTED.NoBroker, INSERTED.NoSak INTO @insBroker(NoBroker, NoSak)
  SELECT @no, v.noBroker, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputBroker x
    WHERE x.NoBongkarSusun=@no AND x.NoBroker=v.noBroker AND x.NoSak=v.noSak
  );

  SET @brokerInserted = @@ROWCOUNT;

  IF @brokerInserted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tgl
    FROM dbo.Broker_d b
    INNER JOIN @insBroker i ON i.NoBroker=b.NoBroker AND i.NoSak=b.NoSak;
  END;

  SELECT @brokerSkipped = COUNT(*) FROM (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (
    SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK)
    WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak AND b.DateUsage IS NULL
  )
  AND EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputBroker x
    WHERE x.NoBongkarSusun=@no AND x.NoBroker=j.noBroker AND x.NoSak=j.noSak
  );

  SELECT @brokerInvalid = COUNT(*) FROM (
    SELECT noBroker, noSak
    FROM OPENJSON(@jsInputs, '$.broker')
    WITH ( noBroker varchar(50) '$.noBroker', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.Broker_d b WITH (NOLOCK)
    WHERE b.NoBroker=j.noBroker AND b.NoSak=j.noSak AND b.DateUsage IS NULL
  );

  INSERT INTO @out SELECT 'broker', @brokerInserted, @brokerSkipped, @brokerInvalid;


  /* =====================================================================
     BB -> BongkarSusunInputBahanBaku + update BahanBaku_d.DateUsage
     ===================================================================== */
  DECLARE @bbInserted int = 0, @bbSkipped int = 0, @bbInvalid int = 0;
  DECLARE @insBB TABLE(NoBahanBaku varchar(50), NoPallet int, NoSak int);

  ;WITH j AS (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH (
      noBahanBaku varchar(50) '$.noBahanBaku',
      noPallet    int         '$.noPallet',
      noSak       int         '$.noSak'
    )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK)
      WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak
        AND d.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputBahanBaku (NoBongkarSusun, NoBahanBaku, NoPallet, NoSak)
  OUTPUT INSERTED.NoBahanBaku, INSERTED.NoPallet, INSERTED.NoSak INTO @insBB(NoBahanBaku, NoPallet, NoSak)
  SELECT @no, v.noBahanBaku, v.noPallet, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputBahanBaku x
    WHERE x.NoBongkarSusun=@no
      AND x.NoBahanBaku=v.noBahanBaku AND x.NoPallet=v.noPallet AND x.NoSak=v.noSak
  );

  SET @bbInserted = @@ROWCOUNT;

  IF @bbInserted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage = @tgl
    FROM dbo.BahanBaku_d d
    INNER JOIN @insBB i
      ON i.NoBahanBaku=d.NoBahanBaku AND i.NoPallet=d.NoPallet AND i.NoSak=d.NoSak;
  END;

  SELECT @bbSkipped = COUNT(*) FROM (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (
    SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK)
    WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak AND d.DateUsage IS NULL
  )
  AND EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputBahanBaku x
    WHERE x.NoBongkarSusun=@no AND x.NoBahanBaku=j.noBahanBaku AND x.NoPallet=j.noPallet AND x.NoSak=j.noSak
  );

  SELECT @bbInvalid = COUNT(*) FROM (
    SELECT noBahanBaku, noPallet, noSak
    FROM OPENJSON(@jsInputs, '$.bb')
    WITH ( noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BahanBaku_d d WITH (NOLOCK)
    WHERE d.NoBahanBaku=j.noBahanBaku AND d.NoPallet=j.noPallet AND d.NoSak=j.noSak AND d.DateUsage IS NULL
  );

  INSERT INTO @out SELECT 'bb', @bbInserted, @bbSkipped, @bbInvalid;


  /* =====================================================================
     WASHING -> BongkarSusunInputWashing + update Washing_d.DateUsage
     ===================================================================== */
  DECLARE @washingInserted int = 0, @washingSkipped int = 0, @washingInvalid int = 0;
  DECLARE @insWashing TABLE(NoWashing varchar(50), NoSak int);

  ;WITH j AS (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK)
      WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak AND d.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputWashing (NoBongkarSusun, NoWashing, NoSak)
  OUTPUT INSERTED.NoWashing, INSERTED.NoSak INTO @insWashing(NoWashing, NoSak)
  SELECT @no, v.noWashing, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputWashing x
    WHERE x.NoBongkarSusun=@no AND x.NoWashing=v.noWashing AND x.NoSak=v.noSak
  );

  SET @washingInserted = @@ROWCOUNT;

  IF @washingInserted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage = @tgl
    FROM dbo.Washing_d d
    INNER JOIN @insWashing i ON i.NoWashing=d.NoWashing AND i.NoSak=d.NoSak;
  END;

  SELECT @washingSkipped = COUNT(*) FROM (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (
    SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK)
    WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak AND d.DateUsage IS NULL
  )
  AND EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputWashing x
    WHERE x.NoBongkarSusun=@no AND x.NoWashing=j.noWashing AND x.NoSak=j.noSak
  );

  SELECT @washingInvalid = COUNT(*) FROM (
    SELECT noWashing, noSak
    FROM OPENJSON(@jsInputs, '$.washing')
    WITH ( noWashing varchar(50) '$.noWashing', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.Washing_d d WITH (NOLOCK)
    WHERE d.NoWashing=j.noWashing AND d.NoSak=j.noSak AND d.DateUsage IS NULL
  );

  INSERT INTO @out SELECT 'washing', @washingInserted, @washingSkipped, @washingInvalid;


  /* =====================================================================
     CRUSHER -> BongkarSusunInputCrusher + update Crusher.DateUsage
     ===================================================================== */
  DECLARE @crusherInserted int = 0, @crusherSkipped int = 0, @crusherInvalid int = 0;
  DECLARE @insCrusher TABLE(NoCrusher varchar(50));

  ;WITH j AS (
    SELECT noCrusher
    FROM OPENJSON(@jsInputs, '$.crusher')
    WITH ( noCrusher varchar(50) '$.noCrusher' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.Crusher c WITH (NOLOCK)
      WHERE c.NoCrusher=j.noCrusher AND c.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputCrusher (NoBongkarSusun, NoCrusher)
  OUTPUT INSERTED.NoCrusher INTO @insCrusher(NoCrusher)
  SELECT @no, v.noCrusher
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputCrusher x
    WHERE x.NoBongkarSusun=@no AND x.NoCrusher=v.noCrusher
  );

  SET @crusherInserted = @@ROWCOUNT;

  IF @crusherInserted > 0
  BEGIN
    UPDATE c
    SET c.DateUsage = @tgl
    FROM dbo.Crusher c
    INNER JOIN @insCrusher i ON i.NoCrusher=c.NoCrusher;
  END;

  SELECT @crusherSkipped = COUNT(*) FROM (
    SELECT noCrusher FROM OPENJSON(@jsInputs, '$.crusher')
    WITH ( noCrusher varchar(50) '$.noCrusher' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher AND c.DateUsage IS NULL)
    AND EXISTS (SELECT 1 FROM dbo.BongkarSusunInputCrusher x WHERE x.NoBongkarSusun=@no AND x.NoCrusher=j.noCrusher);

  SELECT @crusherInvalid = COUNT(*) FROM (
    SELECT noCrusher FROM OPENJSON(@jsInputs, '$.crusher')
    WITH ( noCrusher varchar(50) '$.noCrusher' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Crusher c WITH (NOLOCK) WHERE c.NoCrusher=j.noCrusher AND c.DateUsage IS NULL);

  INSERT INTO @out SELECT 'crusher', @crusherInserted, @crusherSkipped, @crusherInvalid;


  /* =====================================================================
     GILINGAN -> BongkarSusunInputGilingan + update Gilingan.DateUsage
     ===================================================================== */
  DECLARE @gilinganInserted int = 0, @gilinganSkipped int = 0, @gilinganInvalid int = 0;
  DECLARE @insGilingan TABLE(NoGilingan varchar(50));

  ;WITH j AS (
    SELECT noGilingan
    FROM OPENJSON(@jsInputs, '$.gilingan')
    WITH ( noGilingan varchar(50) '$.noGilingan' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK)
      WHERE g.NoGilingan=j.noGilingan AND g.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputGilingan (NoBongkarSusun, NoGilingan)
  OUTPUT INSERTED.NoGilingan INTO @insGilingan(NoGilingan)
  SELECT @no, v.noGilingan
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputGilingan x
    WHERE x.NoBongkarSusun=@no AND x.NoGilingan=v.noGilingan
  );

  SET @gilinganInserted = @@ROWCOUNT;

  IF @gilinganInserted > 0
  BEGIN
    UPDATE g
    SET g.DateUsage = @tgl
    FROM dbo.Gilingan g
    INNER JOIN @insGilingan i ON i.NoGilingan=g.NoGilingan;
  END;

  SELECT @gilinganSkipped = COUNT(*) FROM (
    SELECT noGilingan FROM OPENJSON(@jsInputs, '$.gilingan')
    WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan AND g.DateUsage IS NULL)
    AND EXISTS (SELECT 1 FROM dbo.BongkarSusunInputGilingan x WHERE x.NoBongkarSusun=@no AND x.NoGilingan=j.noGilingan);

  SELECT @gilinganInvalid = COUNT(*) FROM (
    SELECT noGilingan FROM OPENJSON(@jsInputs, '$.gilingan')
    WITH ( noGilingan varchar(50) '$.noGilingan' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Gilingan g WITH (NOLOCK) WHERE g.NoGilingan=j.noGilingan AND g.DateUsage IS NULL);

  INSERT INTO @out SELECT 'gilingan', @gilinganInserted, @gilinganSkipped, @gilinganInvalid;


  /* =====================================================================
     MIXER -> BongkarSusunInputMixer + update Mixer_d.DateUsage
     ===================================================================== */
  DECLARE @mixerInserted int = 0, @mixerSkipped int = 0, @mixerInvalid int = 0;
  DECLARE @insMixer TABLE(NoMixer varchar(50), NoSak int);

  ;WITH j AS (
    SELECT noMixer, noSak
    FROM OPENJSON(@jsInputs, '$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK)
      WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak AND d.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputMixer (NoBongkarSusun, NoMixer, NoSak)
  OUTPUT INSERTED.NoMixer, INSERTED.NoSak INTO @insMixer(NoMixer, NoSak)
  SELECT @no, v.noMixer, v.noSak
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputMixer x
    WHERE x.NoBongkarSusun=@no AND x.NoMixer=v.noMixer AND x.NoSak=v.noSak
  );

  SET @mixerInserted = @@ROWCOUNT;

  IF @mixerInserted > 0
  BEGIN
    UPDATE d
    SET d.DateUsage = @tgl
    FROM dbo.Mixer_d d
    INNER JOIN @insMixer i ON i.NoMixer=d.NoMixer AND i.NoSak=d.NoSak;
  END;

  SELECT @mixerSkipped = COUNT(*) FROM (
    SELECT noMixer, noSak FROM OPENJSON(@jsInputs, '$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK) WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak AND d.DateUsage IS NULL)
    AND EXISTS (SELECT 1 FROM dbo.BongkarSusunInputMixer x WHERE x.NoBongkarSusun=@no AND x.NoMixer=j.noMixer AND x.NoSak=j.noSak);

  SELECT @mixerInvalid = COUNT(*) FROM (
    SELECT noMixer, noSak FROM OPENJSON(@jsInputs, '$.mixer')
    WITH ( noMixer varchar(50) '$.noMixer', noSak int '$.noSak' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Mixer_d d WITH (NOLOCK) WHERE d.NoMixer=j.noMixer AND d.NoSak=j.noSak AND d.DateUsage IS NULL);

  INSERT INTO @out SELECT 'mixer', @mixerInserted, @mixerSkipped, @mixerInvalid;


  /* =====================================================================
     BONGGOLAN -> BongkarSusunInputBonggolan + update Bonggolan.DateUsage
     ===================================================================== */
  DECLARE @bonggolanInserted int = 0, @bonggolanSkipped int = 0, @bonggolanInvalid int = 0;
  DECLARE @insBonggolan TABLE(NoBonggolan varchar(50));

  ;WITH j AS (
    SELECT noBonggolan
    FROM OPENJSON(@jsInputs, '$.bonggolan')
    WITH ( noBonggolan varchar(50) '$.noBonggolan' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK)
      WHERE b.NoBonggolan=j.noBonggolan AND b.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputBonggolan (NoBongkarSusun, NoBonggolan)
  OUTPUT INSERTED.NoBonggolan INTO @insBonggolan(NoBonggolan)
  SELECT @no, v.noBonggolan
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputBonggolan x
    WHERE x.NoBongkarSusun=@no AND x.NoBonggolan=v.noBonggolan
  );

  SET @bonggolanInserted = @@ROWCOUNT;

  IF @bonggolanInserted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tgl
    FROM dbo.Bonggolan b
    INNER JOIN @insBonggolan i ON i.NoBonggolan=b.NoBonggolan;
  END;

  SELECT @bonggolanSkipped = COUNT(*) FROM (
    SELECT noBonggolan FROM OPENJSON(@jsInputs, '$.bonggolan')
    WITH ( noBonggolan varchar(50) '$.noBonggolan' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan AND b.DateUsage IS NULL)
    AND EXISTS (SELECT 1 FROM dbo.BongkarSusunInputBonggolan x WHERE x.NoBongkarSusun=@no AND x.NoBonggolan=j.noBonggolan);

  SELECT @bonggolanInvalid = COUNT(*) FROM (
    SELECT noBonggolan FROM OPENJSON(@jsInputs, '$.bonggolan')
    WITH ( noBonggolan varchar(50) '$.noBonggolan' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.Bonggolan b WITH (NOLOCK) WHERE b.NoBonggolan=j.noBonggolan AND b.DateUsage IS NULL);

  INSERT INTO @out SELECT 'bonggolan', @bonggolanInserted, @bonggolanSkipped, @bonggolanInvalid;


  /* =====================================================================
     FURNITURE WIP -> BongkarSusunInputFurnitureWIP + update FurnitureWIP.DateUsage
     ===================================================================== */
  DECLARE @fwInserted int = 0, @fwSkipped int = 0, @fwInvalid int = 0;
  DECLARE @insFW TABLE(NoFurnitureWIP varchar(50));

  ;WITH j AS (
    SELECT noFurnitureWip
    FROM OPENJSON(@jsInputs, '$.furnitureWip')
    WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK)
      WHERE f.NoFurnitureWIP=j.noFurnitureWip AND f.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputFurnitureWIP (NoBongkarSusun, NoFurnitureWIP)
  OUTPUT INSERTED.NoFurnitureWIP INTO @insFW(NoFurnitureWIP)
  SELECT @no, v.noFurnitureWip
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputFurnitureWIP x
    WHERE x.NoBongkarSusun=@no AND x.NoFurnitureWIP=v.noFurnitureWip
  );

  SET @fwInserted = @@ROWCOUNT;

  IF @fwInserted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = @tgl
    FROM dbo.FurnitureWIP f
    INNER JOIN @insFW i ON i.NoFurnitureWIP=f.NoFurnitureWIP;
  END;

  SELECT @fwSkipped = COUNT(*) FROM (
    SELECT noFurnitureWip FROM OPENJSON(@jsInputs, '$.furnitureWip')
    WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK) WHERE f.NoFurnitureWIP=j.noFurnitureWip AND f.DateUsage IS NULL)
    AND EXISTS (SELECT 1 FROM dbo.BongkarSusunInputFurnitureWIP x WHERE x.NoBongkarSusun=@no AND x.NoFurnitureWIP=j.noFurnitureWip);

  SELECT @fwInvalid = COUNT(*) FROM (
    SELECT noFurnitureWip FROM OPENJSON(@jsInputs, '$.furnitureWip')
    WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK) WHERE f.NoFurnitureWIP=j.noFurnitureWip AND f.DateUsage IS NULL);

  INSERT INTO @out SELECT 'furnitureWip', @fwInserted, @fwSkipped, @fwInvalid;


  /* =====================================================================
     BARANG JADI -> BongkarSusunInputBarangJadi + update BarangJadi.DateUsage
     ===================================================================== */
  DECLARE @bjInserted int = 0, @bjSkipped int = 0, @bjInvalid int = 0;
  DECLARE @insBJ TABLE(NoBJ varchar(50));

  ;WITH j AS (
    SELECT noBj
    FROM OPENJSON(@jsInputs, '$.barangJadi')
    WITH ( noBj varchar(50) '$.noBj' )
  ),
  v AS (
    SELECT j.* FROM j
    WHERE EXISTS (
      SELECT 1 FROM dbo.BarangJadi b WITH (NOLOCK)
      WHERE b.NoBJ=j.noBj AND b.DateUsage IS NULL
    )
  )
  INSERT INTO dbo.BongkarSusunInputBarangJadi (NoBongkarSusun, NoBJ)
  OUTPUT INSERTED.NoBJ INTO @insBJ(NoBJ)
  SELECT @no, v.noBj
  FROM v
  WHERE NOT EXISTS (
    SELECT 1 FROM dbo.BongkarSusunInputBarangJadi x
    WHERE x.NoBongkarSusun=@no AND x.NoBJ=v.noBj
  );

  SET @bjInserted = @@ROWCOUNT;

  IF @bjInserted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tgl
    FROM dbo.BarangJadi b
    INNER JOIN @insBJ i ON i.NoBJ=b.NoBJ;
  END;

  SELECT @bjSkipped = COUNT(*) FROM (
    SELECT noBj FROM OPENJSON(@jsInputs, '$.barangJadi')
    WITH ( noBj varchar(50) '$.noBj' )
  ) j
  WHERE EXISTS (SELECT 1 FROM dbo.BarangJadi b WITH (NOLOCK) WHERE b.NoBJ=j.noBj AND b.DateUsage IS NULL)
    AND EXISTS (SELECT 1 FROM dbo.BongkarSusunInputBarangJadi x WHERE x.NoBongkarSusun=@no AND x.NoBJ=j.noBj);

  SELECT @bjInvalid = COUNT(*) FROM (
    SELECT noBj FROM OPENJSON(@jsInputs, '$.barangJadi')
    WITH ( noBj varchar(50) '$.noBj' )
  ) j
  WHERE NOT EXISTS (SELECT 1 FROM dbo.BarangJadi b WITH (NOLOCK) WHERE b.NoBJ=j.noBj AND b.DateUsage IS NULL);

  INSERT INTO @out SELECT 'barangJadi', @bjInserted, @bjSkipped, @bjInvalid;


  -----------------------------------------------------------------------
  -- OUTPUT ringkasan
  -----------------------------------------------------------------------
  SELECT Section, Inserted, Skipped, Invalid FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_ATTACH);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = { inserted: row.Inserted, skipped: row.Skipped, invalid: row.Invalid };
  }
  return out;
}



async function deleteInputs(noBongkarSusun, payload) {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    broker: norm(payload.broker),             // [{ noBroker, noSak }]
    bb: norm(payload.bb),                     // [{ noBahanBaku, noPallet, noSak }]
    washing: norm(payload.washing),           // [{ noWashing, noSak }]
    crusher: norm(payload.crusher),           // [{ noCrusher }]
    gilingan: norm(payload.gilingan),         // [{ noGilingan }]
    mixer: norm(payload.mixer),               // [{ noMixer, noSak }]
    bonggolan: norm(payload.bonggolan),       // [{ noBonggolan }]
    furnitureWip: norm(payload.furnitureWip), // [{ noFurnitureWip }]
    barangJadi: norm(payload.barangJadi),     // [{ noBj }]
  };

  try {
    await tx.begin();

    const inputsResult = await _deleteBongkarSusunInputsWithTx(tx, noBongkarSusun, body);

    await tx.commit();

    const totalDeleted = Object.values(inputsResult).reduce((s, x) => s + (x.deleted || 0), 0);
    const totalNotFound = Object.values(inputsResult).reduce((s, x) => s + (x.notFound || 0), 0);

    return {
      success: totalDeleted > 0,
      hasWarnings: totalNotFound > 0,
      data: {
        noBongkarSusun,
        summary: { totalDeleted, totalNotFound },
        details: inputsResult,
      },
    };
  } catch (err) {
    try { await tx.rollback(); } catch {}
    throw err;
  }
}


async function _deleteBongkarSusunInputsWithTx(tx, noBongkarSusun, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBongkarSusun);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL_DELETE = `
  SET NOCOUNT ON;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  /* =========================
     BROKER
     ========================= */
  DECLARE @brokerDeleted int = 0, @brokerNotFound int = 0;

  SELECT @brokerDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputBroker map
  INNER JOIN OPENJSON(@jsInputs, '$.broker')
  WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
    ON map.NoBroker = j.noBroker AND map.NoSak = j.noSak
  WHERE map.NoBongkarSusun = @no;

  IF @brokerDeleted > 0
  BEGIN
    UPDATE d SET d.DateUsage = NULL
    FROM dbo.Broker_d d
    INNER JOIN dbo.BongkarSusunInputBroker map
      ON d.NoBroker = map.NoBroker AND d.NoSak = map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.broker')
    WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
      ON map.NoBroker = j.noBroker AND map.NoSak = j.noSak
    WHERE map.NoBongkarSusun = @no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputBroker map
  INNER JOIN OPENJSON(@jsInputs, '$.broker')
  WITH (noBroker varchar(50) '$.noBroker', noSak int '$.noSak') j
    ON map.NoBroker = j.noBroker AND map.NoSak = j.noSak
  WHERE map.NoBongkarSusun = @no;

  DECLARE @brokerRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.broker'));
  SET @brokerNotFound = @brokerRequested - @brokerDeleted;

  INSERT INTO @out SELECT 'broker', @brokerDeleted, @brokerNotFound;


  /* =========================
     BB (Bahan Baku)
     ========================= */
  DECLARE @bbDeleted int = 0, @bbNotFound int = 0;

  SELECT @bbDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputBahanBaku map
  INNER JOIN OPENJSON(@jsInputs, '$.bb')
  WITH (
    noBahanBaku varchar(50) '$.noBahanBaku',
    noPallet int '$.noPallet',
    noSak int '$.noSak'
  ) j
    ON map.NoBahanBaku=j.noBahanBaku AND map.NoPallet=j.noPallet AND map.NoSak=j.noSak
  WHERE map.NoBongkarSusun = @no;

  IF @bbDeleted > 0
  BEGIN
    UPDATE d SET d.DateUsage = NULL
    FROM dbo.BahanBaku_d d
    INNER JOIN dbo.BongkarSusunInputBahanBaku map
      ON d.NoBahanBaku=map.NoBahanBaku AND d.NoPallet=map.NoPallet AND d.NoSak=map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.bb')
    WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
      ON map.NoBahanBaku=j.noBahanBaku AND map.NoPallet=j.noPallet AND map.NoSak=j.noSak
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputBahanBaku map
  INNER JOIN OPENJSON(@jsInputs, '$.bb')
  WITH (noBahanBaku varchar(50) '$.noBahanBaku', noPallet int '$.noPallet', noSak int '$.noSak') j
    ON map.NoBahanBaku=j.noBahanBaku AND map.NoPallet=j.noPallet AND map.NoSak=j.noSak
  WHERE map.NoBongkarSusun=@no;

  DECLARE @bbRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.bb'));
  SET @bbNotFound = @bbRequested - @bbDeleted;
  INSERT INTO @out SELECT 'bb', @bbDeleted, @bbNotFound;


  /* =========================
     WASHING
     ========================= */
  DECLARE @washingDeleted int = 0, @washingNotFound int = 0;

  SELECT @washingDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputWashing map
  INNER JOIN OPENJSON(@jsInputs, '$.washing')
  WITH (noWashing varchar(50) '$.noWashing', noSak int '$.noSak') j
    ON map.NoWashing=j.noWashing AND map.NoSak=j.noSak
  WHERE map.NoBongkarSusun=@no;

  IF @washingDeleted > 0
  BEGIN
    UPDATE d SET d.DateUsage = NULL
    FROM dbo.Washing_d d
    INNER JOIN dbo.BongkarSusunInputWashing map
      ON d.NoWashing=map.NoWashing AND d.NoSak=map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.washing')
    WITH (noWashing varchar(50) '$.noWashing', noSak int '$.noSak') j
      ON map.NoWashing=j.noWashing AND map.NoSak=j.noSak
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputWashing map
  INNER JOIN OPENJSON(@jsInputs, '$.washing')
  WITH (noWashing varchar(50) '$.noWashing', noSak int '$.noSak') j
    ON map.NoWashing=j.noWashing AND map.NoSak=j.noSak
  WHERE map.NoBongkarSusun=@no;

  DECLARE @washingRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.washing'));
  SET @washingNotFound = @washingRequested - @washingDeleted;
  INSERT INTO @out SELECT 'washing', @washingDeleted, @washingNotFound;


  /* =========================
     CRUSHER
     ========================= */
  DECLARE @crusherDeleted int = 0, @crusherNotFound int = 0;

  SELECT @crusherDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputCrusher map
  INNER JOIN OPENJSON(@jsInputs, '$.crusher')
  WITH (noCrusher varchar(50) '$.noCrusher') j
    ON map.NoCrusher=j.noCrusher
  WHERE map.NoBongkarSusun=@no;

  IF @crusherDeleted > 0
  BEGIN
    UPDATE c SET c.DateUsage = NULL
    FROM dbo.Crusher c
    INNER JOIN dbo.BongkarSusunInputCrusher map ON c.NoCrusher = map.NoCrusher
    INNER JOIN OPENJSON(@jsInputs, '$.crusher')
    WITH (noCrusher varchar(50) '$.noCrusher') j
      ON map.NoCrusher=j.noCrusher
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputCrusher map
  INNER JOIN OPENJSON(@jsInputs, '$.crusher')
  WITH (noCrusher varchar(50) '$.noCrusher') j
    ON map.NoCrusher=j.noCrusher
  WHERE map.NoBongkarSusun=@no;

  DECLARE @crusherRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.crusher'));
  SET @crusherNotFound = @crusherRequested - @crusherDeleted;
  INSERT INTO @out SELECT 'crusher', @crusherDeleted, @crusherNotFound;


  /* =========================
     GILINGAN
     ========================= */
  DECLARE @gilinganDeleted int = 0, @gilinganNotFound int = 0;

  SELECT @gilinganDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputGilingan map
  INNER JOIN OPENJSON(@jsInputs, '$.gilingan')
  WITH (noGilingan varchar(50) '$.noGilingan') j
    ON map.NoGilingan=j.noGilingan
  WHERE map.NoBongkarSusun=@no;

  IF @gilinganDeleted > 0
  BEGIN
    UPDATE g SET g.DateUsage = NULL
    FROM dbo.Gilingan g
    INNER JOIN dbo.BongkarSusunInputGilingan map ON g.NoGilingan=map.NoGilingan
    INNER JOIN OPENJSON(@jsInputs, '$.gilingan')
    WITH (noGilingan varchar(50) '$.noGilingan') j
      ON map.NoGilingan=j.noGilingan
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputGilingan map
  INNER JOIN OPENJSON(@jsInputs, '$.gilingan')
  WITH (noGilingan varchar(50) '$.noGilingan') j
    ON map.NoGilingan=j.noGilingan
  WHERE map.NoBongkarSusun=@no;

  DECLARE @gilinganRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.gilingan'));
  SET @gilinganNotFound = @gilinganRequested - @gilinganDeleted;
  INSERT INTO @out SELECT 'gilingan', @gilinganDeleted, @gilinganNotFound;


  /* =========================
     MIXER
     ========================= */
  DECLARE @mixerDeleted int = 0, @mixerNotFound int = 0;

  SELECT @mixerDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputMixer map
  INNER JOIN OPENJSON(@jsInputs, '$.mixer')
  WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak') j
    ON map.NoMixer=j.noMixer AND map.NoSak=j.noSak
  WHERE map.NoBongkarSusun=@no;

  IF @mixerDeleted > 0
  BEGIN
    UPDATE d SET d.DateUsage = NULL
    FROM dbo.Mixer_d d
    INNER JOIN dbo.BongkarSusunInputMixer map ON d.NoMixer=map.NoMixer AND d.NoSak=map.NoSak
    INNER JOIN OPENJSON(@jsInputs, '$.mixer')
    WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak') j
      ON map.NoMixer=j.noMixer AND map.NoSak=j.noSak
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputMixer map
  INNER JOIN OPENJSON(@jsInputs, '$.mixer')
  WITH (noMixer varchar(50) '$.noMixer', noSak int '$.noSak') j
    ON map.NoMixer=j.noMixer AND map.NoSak=j.noSak
  WHERE map.NoBongkarSusun=@no;

  DECLARE @mixerRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.mixer'));
  SET @mixerNotFound = @mixerRequested - @mixerDeleted;
  INSERT INTO @out SELECT 'mixer', @mixerDeleted, @mixerNotFound;


  /* =========================
     BONGGOLAN
     ========================= */
  DECLARE @bonggolanDeleted int = 0, @bonggolanNotFound int = 0;

  SELECT @bonggolanDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputBonggolan map
  INNER JOIN OPENJSON(@jsInputs, '$.bonggolan')
  WITH (noBonggolan varchar(50) '$.noBonggolan') j
    ON map.NoBonggolan=j.noBonggolan
  WHERE map.NoBongkarSusun=@no;

  IF @bonggolanDeleted > 0
  BEGIN
    UPDATE b SET b.DateUsage = NULL
    FROM dbo.Bonggolan b
    INNER JOIN dbo.BongkarSusunInputBonggolan map ON b.NoBonggolan=map.NoBonggolan
    INNER JOIN OPENJSON(@jsInputs, '$.bonggolan')
    WITH (noBonggolan varchar(50) '$.noBonggolan') j
      ON map.NoBonggolan=j.noBonggolan
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputBonggolan map
  INNER JOIN OPENJSON(@jsInputs, '$.bonggolan')
  WITH (noBonggolan varchar(50) '$.noBonggolan') j
    ON map.NoBonggolan=j.noBonggolan
  WHERE map.NoBongkarSusun=@no;

  DECLARE @bonggolanRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.bonggolan'));
  SET @bonggolanNotFound = @bonggolanRequested - @bonggolanDeleted;
  INSERT INTO @out SELECT 'bonggolan', @bonggolanDeleted, @bonggolanNotFound;


  /* =========================
     FURNITURE WIP
     ========================= */
  DECLARE @fwDeleted int = 0, @fwNotFound int = 0;

  SELECT @fwDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
  WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP=j.noFurnitureWip
  WHERE map.NoBongkarSusun=@no;

  IF @fwDeleted > 0
  BEGIN
    UPDATE f SET f.DateUsage = NULL
    FROM dbo.FurnitureWIP f
    INNER JOIN dbo.BongkarSusunInputFurnitureWIP map ON f.NoFurnitureWIP=map.NoFurnitureWIP
    INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
      ON map.NoFurnitureWIP=j.noFurnitureWip
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
  WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP=j.noFurnitureWip
  WHERE map.NoBongkarSusun=@no;

  DECLARE @fwRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.furnitureWip'));
  SET @fwNotFound = @fwRequested - @fwDeleted;
  INSERT INTO @out SELECT 'furnitureWip', @fwDeleted, @fwNotFound;


  /* =========================
     BARANG JADI
     ========================= */
  DECLARE @bjDeleted int = 0, @bjNotFound int = 0;

  SELECT @bjDeleted = COUNT(*)
  FROM dbo.BongkarSusunInputBarangJadi map
  INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
  WITH (noBj varchar(50) '$.noBj') j
    ON map.NoBJ=j.noBj
  WHERE map.NoBongkarSusun=@no;

  IF @bjDeleted > 0
  BEGIN
    UPDATE b SET b.DateUsage = NULL
    FROM dbo.BarangJadi b
    INNER JOIN dbo.BongkarSusunInputBarangJadi map ON b.NoBJ=map.NoBJ
    INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
    WITH (noBj varchar(50) '$.noBj') j
      ON map.NoBJ=j.noBj
    WHERE map.NoBongkarSusun=@no;
  END

  DELETE map
  FROM dbo.BongkarSusunInputBarangJadi map
  INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
  WITH (noBj varchar(50) '$.noBj') j
    ON map.NoBJ=j.noBj
  WHERE map.NoBongkarSusun=@no;

  DECLARE @bjRequested int = (SELECT COUNT(*) FROM OPENJSON(@jsInputs, '$.barangJadi'));
  SET @bjNotFound = @bjRequested - @bjDeleted;
  INSERT INTO @out SELECT 'barangJadi', @bjDeleted, @bjNotFound;


  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL_DELETE);

  const out = {};
  for (const row of rs.recordset || []) {
    out[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return out;
}






module.exports = {
  getByDate,
  getAllBongkarSusun,
  createBongkarSusun,
  updateBongkarSusun,
  deleteBongkarSusun,
  fetchInputs,
  validateLabel,
  upsertInputs,
  deleteInputs,
};

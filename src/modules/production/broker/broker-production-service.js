// services/broker-production-service.js
const { sql, poolPromise } = require("../../../core/config/db");

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,
  formatYMD,
  loadDocDateOnlyFromConfig,
} = require("../../../core/shared/tutup-transaksi-guard");

const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require("../../../core/utils/jam-kerja-helper");

const sharedInputService = require("../../../core/shared/produksi-input.service");

const {
  badReq,
  conflict,
  notFound,
} = require("../../../core/utils/http-error");

const { applyAuditContext } = require("../../../core/utils/db-audit-context");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");

/**
 * Paginated fetch for dbo.BrokerProduksi_h
 * Columns available:
 *  NoProduksi, TglProduksi, IdMesin, IdOperator, Jam, Shift, CreateBy,
 *  CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter
 *
 * We LEFT JOIN to masters and ALIAS Jam -> JamKerja for UI compatibility.
 */
async function getAllProduksi(
  page = 1,
  pageSize = 20,
  search = "",
  idMesin = null,
  tanggal = null,
  shift = null,
) {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || "").trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
      AND (@idMesin IS NULL OR h.IdMesin = @idMesin)
      AND (@tanggal IS NULL OR CONVERT(date, h.TglProduksi) = @tanggal)
      AND (@shift IS NULL OR h.Shift = @shift)
  `;

  // 1) Count (lightweight)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.BrokerProduksi_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input("search", sql.VarChar(100), searchTerm);
  countReq.input("idMesin", sql.Int, idMesin);
  countReq.input("tanggal", sql.Date, tanggal);
  countReq.input("shift", sql.Int, shift);

  const countRes = await countReq.query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;

  if (total === 0) return { data: [], total: 0 };

  // 2) Data + Flag Tutup Transaksi (ambil lastClosed sekali)
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
      h.OutputJenisId,
      mb.Nama        AS OutputJenisNama,
      h.Jam         AS JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.JmlhAnggota,
      h.Hadir,
      h.HourMeter,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd, 108) AS HourEnd,
      h.IdRegu,
      rg.NamaRegu,

      -- (opsional utk frontend)
      lc.LastClosedDate AS LastClosedDate,

      -- flag tutup transaksi
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.TglProduksi) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.BrokerProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin     = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator  = h.IdOperator
    LEFT JOIN dbo.MstRegu     rg WITH (NOLOCK) ON rg.IdRegu      = h.IdRegu
    LEFT JOIN PPS.dbo.MstBroker mb WITH (NOLOCK) ON mb.IdBroker  = h.OutputJenisId

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    -- rekomendasi: urutkan konsisten berdasarkan tanggal + jam + nomor
    ORDER BY h.TglProduksi DESC, h.Jam ASC, h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input("search", sql.VarChar(100), searchTerm);
  dataReq.input("idMesin", sql.Int, idMesin);
  dataReq.input("tanggal", sql.Date, tanggal);
  dataReq.input("shift", sql.Int, shift);
  dataReq.input("offset", sql.Int, offset);
  dataReq.input("limit", sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

// fetchInputs(): main items + partial items (with full keys) in SAME list
async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

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
      rj.IdReject     AS IdJenis,
      mr.NamaReject   AS NamaJenis
    FROM dbo.BrokerProduksiInputReject ir WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK) ON rj.NoReject = ir.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK) ON mr.IdReject = rj.IdReject
    WHERE ir.NoProduksi=@no
    ORDER BY Ref1 DESC, Ref2 ASC;


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
    WHERE pmap.NoProduksi = @no
    ORDER BY pmap.NoBBPartial DESC;

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
    WHERE gmap.NoProduksi = @no
    ORDER BY gmap.NoGilinganPartial DESC;

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
    WHERE mmap.NoProduksi = @no
    ORDER BY NoMixerPartial DESC;

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
    WHERE rmap.NoProduksi = @no
    ORDER BY NoRejectPartial DESC;

    /* Broker partial → jenis plastik dari header broker */
    SELECT
      bmap.NoBrokerPartial,
      bdet.NoBroker,
      bdet.NoSak,
      bdet.Berat,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.BrokerProduksiInputBrokerPartial bmap WITH (NOLOCK)
    LEFT JOIN dbo.BrokerPartial bdet WITH (NOLOCK)
      ON bdet.NoBrokerPartial = bmap.NoBrokerPartial
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = bdet.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE bmap.NoProduksi = @no
    ORDER BY NoBrokerPartial DESC; 
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bbPart = rs.recordsets?.[1] || [];
  const gilPart = rs.recordsets?.[2] || [];
  const mixPart = rs.recordsets?.[3] || [];
  const rejPart = rs.recordsets?.[4] || [];
  const brkPart = rs.recordsets?.[5] || []; // ⬅️ TAMBAHKAN

  const out = {
    broker: [],
    bb: [],
    washing: [],
    crusher: [],
    gilingan: [],
    mixer: [],
    reject: [],
    summary: {
      broker: 0,
      bb: 0,
      washing: 0,
      crusher: 0,
      gilingan: 0,
      mixer: 0,
      reject: 0,
    },
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
      case "broker":
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case "bb":
        out.bb.push({
          noBahanBaku: r.Ref1,
          noPallet: r.Ref2,
          noSak: r.Ref3,
          ...base,
        });
        break;
      case "washing":
        out.washing.push({ noWashing: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case "crusher":
        out.crusher.push({ noCrusher: r.Ref1, ...base });
        break;
      case "gilingan":
        out.gilingan.push({ noGilingan: r.Ref1, ...base });
        break;
      case "mixer":
        out.mixer.push({ noMixer: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case "reject":
        out.reject.push({ noReject: r.Ref1, ...base });
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

  for (const p of rejPart) {
    out.reject.push({
      noRejectPartial: p.NoRejectPartial,
      noReject: p.NoReject ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // ⬇️ TAMBAHKAN - Broker Partial
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

async function fetchOutputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT
      o.NoProduksi,
      o.NoBroker,
      h.IdJenisPlastik AS IdJenis,
      COALESCE(mb.Nama, jp.Jenis) AS NamaJenis,
      ISNULL(h.HasBeenPrinted, 0) AS HasBeenPrinted,
      o.NoSak,
      d.Berat,
      d.DateUsage,
      d.IsPartial,
      d.IdLokasi
    FROM dbo.BrokerProduksiOutput o WITH (NOLOCK)
    LEFT JOIN dbo.Broker_h h WITH (NOLOCK)
      ON h.NoBroker = o.NoBroker
    LEFT JOIN dbo.MstBroker mb WITH (NOLOCK)
      ON mb.IdBroker = h.IdJenisPlastik
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = h.IdJenisPlastik
    LEFT JOIN dbo.Broker_d d WITH (NOLOCK)
      ON d.NoBroker = o.NoBroker
     AND d.NoSak = o.NoSak
    WHERE o.NoProduksi = @no
    ORDER BY o.NoBroker DESC, o.NoSak;
  `;

  const rs = await req.query(q);
  const rows = rs.recordset || [];
  const byBroker = new Map();

  for (const row of rows) {
    if (!byBroker.has(row.NoBroker)) {
      byBroker.set(row.NoBroker, {
        NoProduksi: row.NoProduksi,
        NoBroker: row.NoBroker,
        IdJenis: row.IdJenis ?? null,
        NamaJenis: row.NamaJenis ?? null,
        HasBeenPrinted: row.HasBeenPrinted,
        DetailSak: [],
      });
    }

    byBroker.get(row.NoBroker).DetailSak.push({
      NoSak: row.NoSak,
      Berat: row.Berat ?? null,
      DateUsage: row.DateUsage ?? null,
      IsPartial: row.IsPartial ?? null,
      IdLokasi: row.IdLokasi ?? null,
    });
  }

  return Array.from(byBroker.values());
}

async function fetchOutputsBonggolan(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT DISTINCT
      o.NoProduksi,
      o.NoBonggolan,
      b.IdBonggolan,
      mb.NamaBonggolan,
      b.Berat,
      ISNULL(b.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.BrokerProduksiOutputBonggolan o WITH (NOLOCK)
    LEFT JOIN dbo.Bonggolan b WITH (NOLOCK)
      ON b.NoBonggolan = o.NoBonggolan
    LEFT JOIN dbo.MstBonggolan mb WITH (NOLOCK)
      ON mb.IdBonggolan = b.IdBonggolan
    WHERE o.NoProduksi = @no
    ORDER BY o.NoBonggolan DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
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

  request.input("date", sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

async function getNoProduksiByTanggalShift(tanggal, shift) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input("tanggal", sql.Date, tanggal);
  request.input("shift", sql.Int, shift);

  const query = `
    SELECT
      h.NoProduksi,
      h.TglProduksi,
      h.Shift,
      h.IdMesin,
      m.NamaMesin
    FROM dbo.BrokerProduksi_h h
    LEFT JOIN dbo.MstMesin m ON m.IdMesin = h.IdMesin
    WHERE CONVERT(date, h.TglProduksi) = @tanggal
      AND h.Shift = @shift
    ORDER BY h.NoProduksi DESC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

async function createBrokerProduksi(payload, ctx) {
  // ===============================
  // 0) Validasi payload basic (business)
  // ===============================
  const body = payload && typeof payload === "object" ? payload : {};

  const must = [];
  if (!body?.tglProduksi) must.push("tglProduksi");
  if (body?.idMesin == null) must.push("idMesin");
  if (body?.idOperator == null) must.push("idOperator");
  if (body?.outputJenisId == null) must.push("outputJenisId");
  if (!body?.hourStart) must.push("hourStart");
  if (!body?.hourEnd) must.push("hourEnd");
  if (body?.shift == null) must.push("shift");
  if (must.length) throw badReq(`Field wajib: ${must.join(", ")}`);

  // jam bisa dari body.jam atau dihitung dari hourStart-hourEnd
  let jamKerja = body?.jam ?? null;
  if (jamKerja == null) {
    const calc = calcJamKerjaFromStartEnd(body?.hourStart, body?.hourEnd);
    if (calc != null) jamKerja = calc;
  }
  if (jamKerja == null) throw badReq("Field wajib: jam");

  const jamInt = parseJamToInt(jamKerja);
  const docDateOnly = toDateOnly(body.tglProduksi);

  // ===============================
  // 1) Validasi + normalisasi ctx (audit) seperti upsert
  // ===============================
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
  }

  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim(); // boleh kosong, applyAuditContext akan fallback

  const auditCtx = {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  };

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 2) Set SESSION_CONTEXT untuk trigger audit (1x di awal tx)
    // =====================================================
    const auditReq = new sql.Request(tx);
    const audit = await applyAuditContext(auditReq, auditCtx);

    // =====================================================
    // 3) Guard tutup transaksi (CREATE = WRITE)
    // =====================================================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "create BrokerProduksi",
      useLock: true,
    });

    // =====================================================
    // 4) Guard operator unik per tanggal + shift
    // =====================================================
    const dupOperatorRes = await new sql.Request(tx)
      .input("TglProduksi", sql.Date, docDateOnly)
      .input("Shift", sql.Int, body.shift)
      .input("IdOperator", sql.Int, body.idOperator).query(`
        SELECT TOP 1
          h.NoProduksi,
          h.IdMesin,
          m.NamaMesin
        FROM dbo.BrokerProduksi_h h WITH (UPDLOCK, HOLDLOCK)
        LEFT JOIN dbo.MstMesin m WITH (NOLOCK) ON m.IdMesin = h.IdMesin
        WHERE CONVERT(date, TglProduksi) = @TglProduksi
          AND h.Shift = @Shift
          AND h.IdOperator = @IdOperator
        ORDER BY h.NoProduksi DESC
      `);
    if (dupOperatorRes.recordset.length > 0) {
      const existing = dupOperatorRes.recordset[0];
      const mesinLabel = existing.NamaMesin || "mesin lain";
      throw conflict(
        `Operator saat ini sedang bekerja di mesin ${mesinLabel} pada shift ${body.shift}. Harap pilih operator lain.`,
      );
    }

    // =====================================================
    // 5) Generate NoProduksi
    // =====================================================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.BrokerProduksi_h",
        columnName: "NoProduksi",
        prefix: "E.",
        width: 10,
      });

    let noProduksi = await gen();

    // optional double-check (lebih bagus kalau kolom ada UNIQUE)
    const exist = await new sql.Request(tx).input(
      "NoProduksi",
      sql.VarChar(50),
      noProduksi,
    ).query(`
        SELECT 1
        FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (exist.recordset.length > 0) {
      const retry = await gen();
      const exist2 = await new sql.Request(tx).input(
        "NoProduksi",
        sql.VarChar(50),
        retry,
      ).query(`
          SELECT 1
          FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoProduksi = @NoProduksi
        `);

      if (exist2.recordset.length > 0) {
        throw conflict("Gagal generate NoProduksi unik, coba lagi.");
      }
      noProduksi = retry;
    }

    // =====================================================
    // 6) Insert header (FIX: OUTPUT ... INTO @out)
    // =====================================================
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("TglProduksi", sql.Date, docDateOnly)
      .input("IdMesin", sql.Int, body.idMesin)
      .input("IdOperator", sql.Int, body.idOperator)
      .input("OutputJenisId", sql.Int, body.outputJenisId ?? null)
      .input("Jam", sql.Int, jamInt)
      .input("Shift", sql.Int, body.shift)
      .input("CreateBy", sql.VarChar(100), body.createBy) // controller overwrite
      .input("CheckBy1", sql.VarChar(100), body.checkBy1 ?? null)
      .input("CheckBy2", sql.VarChar(100), body.checkBy2 ?? null)
      .input("ApproveBy", sql.VarChar(100), body.approveBy ?? null)
      .input("JmlhAnggota", sql.Int, body.jmlhAnggota ?? null)
      .input("Hadir", sql.Int, body.hadir ?? null)
      .input("HourMeter", sql.Decimal(18, 2), body.hourMeter ?? null)
      // kirim string, biar SQL yang CAST ke time(7)
      .input("HourStart", sql.VarChar(20), body.hourStart ?? null)
      .input("HourEnd", sql.VarChar(20), body.hourEnd ?? null)
      .input("IdRegu", sql.Int, body.idRegu ?? null);

    const insertSql = `
      DECLARE @out TABLE (
        NoProduksi   varchar(50),
        TglProduksi  date,
        IdMesin      int,
        IdOperator   int,
        OutputJenisId int,
        Jam          int,
        Shift        int,
        CreateBy     varchar(100),
        CheckBy1     varchar(100),
        CheckBy2     varchar(100),
        ApproveBy    varchar(100),
        JmlhAnggota  int,
        Hadir        int,
        HourMeter    decimal(18,2),
        HourStart    time(7),
        HourEnd      time(7),
        IdRegu       int
      );

      INSERT INTO dbo.BrokerProduksi_h (
        NoProduksi,
        TglProduksi,
        IdMesin,
        IdOperator,
        OutputJenisId,
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
        HourEnd,
        IdRegu
      )
      OUTPUT
        INSERTED.NoProduksi,
        INSERTED.TglProduksi,
        INSERTED.IdMesin,
        INSERTED.IdOperator,
        INSERTED.OutputJenisId,
        INSERTED.Jam,
        INSERTED.Shift,
        INSERTED.CreateBy,
        INSERTED.CheckBy1,
        INSERTED.CheckBy2,
        INSERTED.ApproveBy,
        INSERTED.JmlhAnggota,
        INSERTED.Hadir,
        INSERTED.HourMeter,
        INSERTED.HourStart,
        INSERTED.HourEnd,
        INSERTED.IdRegu
      INTO @out
      VALUES (
        @NoProduksi,
        @TglProduksi,
        @IdMesin,
        @IdOperator,
        @OutputJenisId,
        @Jam,
        @Shift,
        @CreateBy,
        @CheckBy1,
        @CheckBy2,
        @ApproveBy,
        @JmlhAnggota,
        @Hadir,
        @HourMeter,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd   IS NULL OR LTRIM(RTRIM(@HourEnd))   = '' THEN NULL ELSE CAST(@HourEnd   AS time(7)) END,
        @IdRegu
      );

      SELECT * FROM @out;
    `;

    const insRes = await rqIns.query(insertSql);

    await tx.commit();

    return {
      header: insRes.recordset?.[0] || null,
      audit, // optional debug / tracing
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

async function updateBrokerProduksi(noProduksi, payload, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) Set SESSION_CONTEXT untuk trigger audit (1x di awal tx)
    // =====================================================
    const actorIdNum = Number(ctx?.actorId);
    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
    }
    const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
    const requestId = String(ctx?.requestId || "").trim();

    const auditReq = new sql.Request(tx);
    await applyAuditContext(auditReq, {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    // -------------------------------------------------------
    // 1) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "brokerProduksi",
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 2) Jika user mengubah tanggal, hitung tanggal barunya (date-only)
    // -------------------------------------------------------
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq("tglProduksi tidak boleh kosong");
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // -------------------------------------------------------
    // 3) GUARD TUTUP TRANSAKSI
    // -------------------------------------------------------
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: "update BrokerProduksi (current date)",
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update BrokerProduksi (new date)",
        useLock: true,
      });
    }

    // -------------------------------------------------------
    // 4) BUILD SET DINAMIS
    // -------------------------------------------------------
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push("TglProduksi = @TglProduksi");
      rqUpd.input("TglProduksi", sql.Date, newDocDateOnly);
    }

    if (payload.idMesin !== undefined) {
      sets.push("IdMesin = @IdMesin");
      rqUpd.input("IdMesin", sql.Int, payload.idMesin);
    }

    if (payload.idOperator !== undefined) {
      sets.push("IdOperator = @IdOperator");
      rqUpd.input("IdOperator", sql.Int, payload.idOperator);
    }

    if (payload.shift !== undefined) {
      sets.push("Shift = @Shift");
      rqUpd.input("Shift", sql.Int, payload.shift);
    }

    if (payload.outputJenisId !== undefined) {
      sets.push("OutputJenisId = @OutputJenisId");
      rqUpd.input("OutputJenisId", sql.Int, payload.outputJenisId ?? null);
    }

    if (payload.checkBy1 !== undefined) {
      sets.push("CheckBy1 = @CheckBy1");
      rqUpd.input("CheckBy1", sql.VarChar(100), payload.checkBy1 ?? null);
    }

    if (payload.checkBy2 !== undefined) {
      sets.push("CheckBy2 = @CheckBy2");
      rqUpd.input("CheckBy2", sql.VarChar(100), payload.checkBy2 ?? null);
    }

    if (payload.approveBy !== undefined) {
      sets.push("ApproveBy = @ApproveBy");
      rqUpd.input("ApproveBy", sql.VarChar(100), payload.approveBy ?? null);
    }

    if (payload.jmlhAnggota !== undefined) {
      sets.push("JmlhAnggota = @JmlhAnggota");
      rqUpd.input("JmlhAnggota", sql.Int, payload.jmlhAnggota ?? null);
    }

    if (payload.hadir !== undefined) {
      sets.push("Hadir = @Hadir");
      rqUpd.input("Hadir", sql.Int, payload.hadir ?? null);
    }

    if (payload.hourMeter !== undefined) {
      sets.push("HourMeter = @HourMeter");
      rqUpd.input("HourMeter", sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    if (payload.jam !== undefined) {
      const jamInt = payload.jam === null ? null : parseJamToInt(payload.jam);
      sets.push("Jam = @Jam");
      rqUpd.input("Jam", sql.Int, jamInt);
    }

    // hourStart / hourEnd (lebih aman kalau null / kosong)
    if (payload.hourStart !== undefined) {
      sets.push(`
        HourStart =
          CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL
               ELSE CAST(@HourStart AS time(7)) END
      `);
      rqUpd.input("HourStart", sql.VarChar(20), payload.hourStart ?? null);
    }

    if (payload.hourEnd !== undefined) {
      sets.push(`
        HourEnd =
          CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL
               ELSE CAST(@HourEnd AS time(7)) END
      `);
      rqUpd.input("HourEnd", sql.VarChar(20), payload.hourEnd ?? null);
    }

    if (payload.idRegu !== undefined) {
      sets.push("IdRegu = @IdRegu");
      rqUpd.input("IdRegu", sql.Int, payload.idRegu ?? null);
    }

    if (sets.length === 0) throw badReq("No fields to update");

    rqUpd.input("NoProduksi", sql.VarChar(50), noProduksi);

    // -------------------------------------------------------
    // 5) UPDATE + RETURN row (FIX: OUTPUT ... INTO @out)
    // -------------------------------------------------------
    const updateSql = `
      DECLARE @out TABLE (
        NoProduksi   varchar(50),
        TglProduksi  date,
        IdMesin      int,
        IdOperator   int,
        OutputJenisId int,
        Jam          int,
        Shift        int,
        CreateBy     varchar(100),
        CheckBy1     varchar(100),
        CheckBy2     varchar(100),
        ApproveBy    varchar(100),
        JmlhAnggota  int,
        Hadir        int,
        HourMeter    decimal(18,2),
        HourStart    time(7),
        HourEnd      time(7),
        IdRegu       int
      );

      UPDATE dbo.BrokerProduksi_h
      SET ${sets.join(", ")}
      OUTPUT
        INSERTED.NoProduksi,
        INSERTED.TglProduksi,
        INSERTED.IdMesin,
        INSERTED.IdOperator,
        INSERTED.OutputJenisId,
        INSERTED.Jam,
        INSERTED.Shift,
        INSERTED.CreateBy,
        INSERTED.CheckBy1,
        INSERTED.CheckBy2,
        INSERTED.ApproveBy,
        INSERTED.JmlhAnggota,
        INSERTED.Hadir,
        INSERTED.HourMeter,
        INSERTED.HourStart,
        INSERTED.HourEnd,
        INSERTED.IdRegu
      INTO @out
      WHERE NoProduksi = @NoProduksi;

      SELECT * FROM @out;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    if (!updatedHeader)
      throw notFound(`NoProduksi tidak ditemukan: ${noProduksi}`);

    // -------------------------------------------------------
    // 6) Jika TglProduksi berubah → sinkron DateUsage full + partial
    //    (pakai tanggal hasil DB agar konsisten)
    // -------------------------------------------------------
    if (isChangingDate) {
      const usageDate = resolveEffectiveDateForCreate(
        updatedHeader.TglProduksi,
      );

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input("NoProduksi", sql.VarChar(50), noProduksi)
        .input("TglProduksi", sql.Date, usageDate);

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
              FROM dbo.BrokerProduksiInputBB AS map
              WHERE map.NoProduksi   = @NoProduksi
                AND map.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak        = bb.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoProduksi   = @NoProduksi
                AND bp.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak        = bb.NoSak
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
              FROM dbo.BrokerProduksiInputBroker AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoBroker   = br.NoBroker
                AND map.NoSak      = br.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputBrokerPartial AS mp
              JOIN dbo.BrokerPartial AS bp
                ON bp.NoBrokerPartial = mp.NoBrokerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND bp.NoBroker   = br.NoBroker
                AND bp.NoSak      = br.NoSak
            )
          );

        -------------------------------------------------------
        -- WASHING (FULL ONLY)
        -------------------------------------------------------
        UPDATE w
        SET w.DateUsage = @TglProduksi
        FROM dbo.Washing_d AS w
        WHERE w.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.BrokerProduksiInputWashing AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoWashing  = w.NoWashing
              AND map.NoSak      = w.NoSak
          );

        -------------------------------------------------------
        -- CRUSHER (FULL ONLY)
        -------------------------------------------------------
        UPDATE c
        SET c.DateUsage = @TglProduksi
        FROM dbo.Crusher AS c
        WHERE c.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.BrokerProduksiInputCrusher AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoCrusher  = c.NoCrusher
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
              FROM dbo.BrokerProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoGilingan = g.NoGilingan
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputGilinganPartial AS mp
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
              FROM dbo.BrokerProduksiInputMixer AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoMixer    = m.NoMixer
                AND map.NoSak      = m.NoSak
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputMixerPartial AS mp
              JOIN dbo.MixerPartial AS mpd
                ON mpd.NoMixerPartial = mp.NoMixerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND mpd.NoMixer   = m.NoMixer
                AND mpd.NoSak     = m.NoSak
            )
          );

        -------------------------------------------------------
        -- REJECT (FULL + PARTIAL)
        -------------------------------------------------------
        UPDATE r
        SET r.DateUsage = @TglProduksi
        FROM dbo.RejectV2 AS r
        WHERE r.DateUsage IS NOT NULL
          AND (
            EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputReject AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoReject   = r.NoReject
            )
            OR
            EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputRejectPartial AS mp
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
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

async function deleteBrokerProduksi(noProduksi, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) Set SESSION_CONTEXT untuk trigger audit (1x di awal tx)
    // =====================================================
    const actorIdNum = Number(ctx?.actorId);
    if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
      throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
    }
    const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
    const requestId = String(ctx?.requestId || "").trim();

    const auditReq = new sql.Request(tx);
    await applyAuditContext(auditReq, {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    // -------------------------------------------------------
    // 1) AMBIL docDateOnly DARI CONFIG (LOCK HEADER ROW)
    // -------------------------------------------------------
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "brokerProduksi",
      codeValue: noProduksi,
      runner: tx,
      useLock: true, // DELETE = write action
      throwIfNotFound: true,
    });

    // -------------------------------------------------------
    // 2) GUARD TUTUP TRANSAKSI (DELETE = WRITE)
    // -------------------------------------------------------
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "delete BrokerProduksi",
      useLock: true,
    });

    // -------------------------------------------------------
    // 3) CEK DULU: SUDAH PUNYA OUTPUT / BONGGOLAN ATAU BELUM
    // -------------------------------------------------------
    const rqCheck = new sql.Request(tx);
    const outCheck = await rqCheck.input(
      "NoProduksi",
      sql.VarChar(50),
      noProduksi,
    ).query(`
        SELECT
          SUM(CASE WHEN Src = 'OUT'  THEN Cnt ELSE 0 END) AS CntOutput,
          SUM(CASE WHEN Src = 'BONG' THEN Cnt ELSE 0 END) AS CntOutputBong
        FROM (
          SELECT 'OUT' AS Src, COUNT(*) AS Cnt
          FROM dbo.BrokerProduksiOutput
          WHERE NoProduksi = @NoProduksi

          UNION ALL

          SELECT 'BONG' AS Src, COUNT(*) AS Cnt
          FROM dbo.BrokerProduksiOutputBonggolan
          WHERE NoProduksi = @NoProduksi
        ) AS X;
      `);

    const row = outCheck.recordset?.[0] || { CntOutput: 0, CntOutputBong: 0 };
    const hasOutput = (row.CntOutput || 0) > 0;
    const hasOutputBong = (row.CntOutputBong || 0) > 0;

    if (hasOutput || hasOutputBong) {
      throw badReq(
        "Tidak dapat menghapus Nomor Produksi ini karena memiliki data output.",
      );
    }

    // -------------------------------------------------------
    // 4) DELETE INPUT + PARTIAL + RESET DATEUSAGE + DELETE HEADER (OUTPUT INTO)
    // -------------------------------------------------------
    const req = new sql.Request(tx);
    req.input("NoProduksi", sql.VarChar(50), noProduksi);

    const sqlDelete = `
      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK MENYIMPAN KEY YANG TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BBKeys TABLE (
        NoBahanBaku varchar(50),
        NoPallet    varchar(50),
        NoSak       varchar(50)
      );

      DECLARE @BrokerKeys TABLE (
        NoBroker varchar(50),
        NoSak    varchar(50)
      );

      DECLARE @GilinganKeys TABLE ( NoGilingan varchar(50) );

      DECLARE @MixerKeys TABLE (
        NoMixer varchar(50),
        NoSak   varchar(50)
      );

      DECLARE @RejectKeys TABLE ( NoReject varchar(50) );

      ---------------------------------------------------------
      -- (OPSIONAL) OUT TABLE: RETURN HEADER YANG TERHAPUS
      -- Sesuaikan kolom jika tabel header kamu bertambah.
      ---------------------------------------------------------
      DECLARE @outHeader TABLE (
        NoProduksi   varchar(50),
        TglProduksi  date,
        IdMesin      int,
        IdOperator   int,
        Jam          int,
        Shift        int,
        CreateBy     varchar(100),
        CheckBy1     varchar(100),
        CheckBy2     varchar(100),
        ApproveBy    varchar(100),
        JmlhAnggota  int,
        Hadir        int,
        HourMeter    decimal(18,2),
        HourStart    time(7),
        HourEnd      time(7)
      );

      ---------------------------------------------------------
      -- 0. PASTIKAN HEADER ADA (kalau tidak, stop cepat)
      ---------------------------------------------------------
      IF NOT EXISTS (SELECT 1 FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK) WHERE NoProduksi = @NoProduksi)
      BEGIN
        -- hasil kosong, biar service lempar notFound
        SELECT * FROM @outHeader;
        RETURN;
      END

      ---------------------------------------------------------
      -- 1. BAHAN BAKU (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BBKeys (NoBahanBaku, NoPallet, NoSak)
      SELECT DISTINCT bb.NoBahanBaku, bb.NoPallet, bb.NoSak
      FROM dbo.BahanBaku_d AS bb
      WHERE EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputBB AS map
              WHERE map.NoProduksi   = @NoProduksi
                AND map.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(map.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND map.NoSak        = bb.NoSak
            )
         OR EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputBBPartial AS mp
              JOIN dbo.BahanBakuPartial AS bp
                ON bp.NoBBPartial = mp.NoBBPartial
              WHERE mp.NoProduksi   = @NoProduksi
                AND bp.NoBahanBaku  = bb.NoBahanBaku
                AND ISNULL(bp.NoPallet,'') = ISNULL(bb.NoPallet,'')
                AND bp.NoSak        = bb.NoSak
            );

      DELETE bp
      FROM dbo.BahanBakuPartial AS bp
      JOIN dbo.BrokerProduksiInputBBPartial AS mp
        ON mp.NoBBPartial = bp.NoBBPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputBBPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputBB
      WHERE NoProduksi = @NoProduksi;

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
      -- 2. BROKER (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BrokerKeys (NoBroker, NoSak)
      SELECT DISTINCT b.NoBroker, b.NoSak
      FROM dbo.Broker_d AS b
      WHERE EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputBroker AS map
              WHERE map.NoProduksi = @NoProduksi
                AND map.NoBroker   = b.NoBroker
                AND map.NoSak      = b.NoSak
            )
         OR EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputBrokerPartial AS mp
              JOIN dbo.BrokerPartial AS bp
                ON bp.NoBrokerPartial = mp.NoBrokerPartial
              WHERE mp.NoProduksi = @NoProduksi
                AND bp.NoBroker   = b.NoBroker
                AND bp.NoSak      = b.NoSak
            );

      DELETE bp
      FROM dbo.BrokerPartial AS bp
      JOIN dbo.BrokerProduksiInputBrokerPartial AS mp
        ON mp.NoBrokerPartial = bp.NoBrokerPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputBrokerPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputBroker
      WHERE NoProduksi = @NoProduksi;

      UPDATE b
      SET b.DateUsage = NULL,
          b.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1 FROM dbo.BrokerPartial AS bp
              WHERE bp.NoBroker = b.NoBroker AND bp.NoSak = b.NoSak
            ) THEN 1 ELSE 0 END
      FROM dbo.Broker_d AS b
      JOIN @BrokerKeys AS k
        ON k.NoBroker = b.NoBroker AND k.NoSak = b.NoSak;

      ---------------------------------------------------------
      -- 3. WASHING (FULL ONLY)
      ---------------------------------------------------------
      UPDATE w
      SET w.DateUsage = NULL
      FROM dbo.Washing_d AS w
      JOIN dbo.BrokerProduksiInputWashing AS map
        ON map.NoWashing = w.NoWashing AND map.NoSak = w.NoSak
      WHERE map.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputWashing
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 4. CRUSHER (FULL ONLY)
      ---------------------------------------------------------
      UPDATE c
      SET c.DateUsage = NULL
      FROM dbo.Crusher AS c
      JOIN dbo.BrokerProduksiInputCrusher AS map
        ON map.NoCrusher = c.NoCrusher
      WHERE map.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputCrusher
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 5. GILINGAN (ADA PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT g.NoGilingan
      FROM dbo.Gilingan AS g
      WHERE EXISTS (
              SELECT 1 FROM dbo.BrokerProduksiInputGilingan AS map
              WHERE map.NoProduksi = @NoProduksi AND map.NoGilingan = g.NoGilingan
            )
         OR EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputGilinganPartial AS mp
              JOIN dbo.GilinganPartial AS gp
                ON gp.NoGilinganPartial = mp.NoGilinganPartial
              WHERE mp.NoProduksi = @NoProduksi AND gp.NoGilingan = g.NoGilingan
            );

      DELETE gp
      FROM dbo.GilinganPartial AS gp
      JOIN dbo.BrokerProduksiInputGilinganPartial AS mp
        ON mp.NoGilinganPartial = gp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputGilinganPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputGilingan
      WHERE NoProduksi = @NoProduksi;

      UPDATE g
      SET g.DateUsage = NULL,
          g.IsPartial = CASE
            WHEN EXISTS (SELECT 1 FROM dbo.GilinganPartial gp WHERE gp.NoGilingan = g.NoGilingan)
            THEN 1 ELSE 0 END
      FROM dbo.Gilingan AS g
      JOIN @GilinganKeys AS k ON k.NoGilingan = g.NoGilingan;

      ---------------------------------------------------------
      -- 6. MIXER (ADA PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @MixerKeys (NoMixer, NoSak)
      SELECT DISTINCT m.NoMixer, m.NoSak
      FROM dbo.Mixer_d AS m
      WHERE EXISTS (
              SELECT 1 FROM dbo.BrokerProduksiInputMixer AS map
              WHERE map.NoProduksi = @NoProduksi AND map.NoMixer = m.NoMixer AND map.NoSak = m.NoSak
            )
         OR EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputMixerPartial AS mp
              JOIN dbo.MixerPartial AS mpd
                ON mpd.NoMixerPartial = mp.NoMixerPartial
              WHERE mp.NoProduksi = @NoProduksi AND mpd.NoMixer = m.NoMixer AND mpd.NoSak = m.NoSak
            );

      DELETE mpd
      FROM dbo.MixerPartial AS mpd
      JOIN dbo.BrokerProduksiInputMixerPartial AS mp
        ON mp.NoMixerPartial = mpd.NoMixerPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputMixerPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputMixer
      WHERE NoProduksi = @NoProduksi;

      UPDATE m
      SET m.DateUsage = NULL,
          m.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1 FROM dbo.MixerPartial mpd
              WHERE mpd.NoMixer = m.NoMixer AND mpd.NoSak = m.NoSak
            ) THEN 1 ELSE 0 END
      FROM dbo.Mixer_d AS m
      JOIN @MixerKeys AS k ON k.NoMixer = m.NoMixer AND k.NoSak = m.NoSak;

      ---------------------------------------------------------
      -- 7. REJECT (ADA PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @RejectKeys (NoReject)
      SELECT DISTINCT r.NoReject
      FROM dbo.RejectV2 AS r
      WHERE EXISTS (
              SELECT 1 FROM dbo.BrokerProduksiInputReject AS map
              WHERE map.NoProduksi = @NoProduksi AND map.NoReject = r.NoReject
            )
         OR EXISTS (
              SELECT 1
              FROM dbo.BrokerProduksiInputRejectPartial AS mp
              JOIN dbo.RejectV2Partial AS rp
                ON rp.NoRejectPartial = mp.NoRejectPartial
              WHERE mp.NoProduksi = @NoProduksi AND rp.NoReject = r.NoReject
            );

      DELETE rp
      FROM dbo.RejectV2Partial AS rp
      JOIN dbo.BrokerProduksiInputRejectPartial AS mp
        ON mp.NoRejectPartial = rp.NoRejectPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputRejectPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.BrokerProduksiInputReject
      WHERE NoProduksi = @NoProduksi;

      UPDATE r
      SET r.DateUsage = NULL,
          r.IsPartial = CASE
            WHEN EXISTS (SELECT 1 FROM dbo.RejectV2Partial rp WHERE rp.NoReject = r.NoReject)
            THEN 1 ELSE 0 END
      FROM dbo.RejectV2 AS r
      JOIN @RejectKeys AS k ON k.NoReject = r.NoReject;

      ---------------------------------------------------------
      -- 8. TERAKHIR: HAPUS HEADER (OUTPUT INTO) + RETURN
      ---------------------------------------------------------
      DELETE FROM dbo.BrokerProduksi_h
      OUTPUT
        DELETED.NoProduksi,
        DELETED.TglProduksi,
        DELETED.IdMesin,
        DELETED.IdOperator,
        DELETED.Jam,
        DELETED.Shift,
        DELETED.CreateBy,
        DELETED.CheckBy1,
        DELETED.CheckBy2,
        DELETED.ApproveBy,
        DELETED.JmlhAnggota,
        DELETED.Hadir,
        DELETED.HourMeter,
        DELETED.HourStart,
        DELETED.HourEnd
      INTO @outHeader
      WHERE NoProduksi = @NoProduksi;

      SELECT * FROM @outHeader;
    `;

    const delRes = await req.query(sqlDelete);
    const deletedHeader = delRes.recordset?.[0] || null;

    if (!deletedHeader)
      throw notFound(`NoProduksi tidak ditemukan: ${noProduksi}`);

    await tx.commit();
    return { success: true, header: deletedHeader };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
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
    if (val && typeof val === "object") {
      const o = {};
      for (const [k, v] of Object.entries(val)) {
        o[toCamel(k)] = camelize(v);
      }
      return o;
    }
    return val;
  };

  // ---------- normalize label ----------
  const raw = String(labelCode || "").trim();
  if (!raw) throw new Error("Label code is required");

  let prefix = "";
  if (raw.substring(0, 3).toUpperCase() === "BF.") {
    prefix = "BF.";
  } else {
    prefix = raw.substring(0, 2).toUpperCase();
  }

  let query = "";
  let tableName = "";

  // Helper eksekusi single-query (untuk semua prefix selain A. yang butuh dua input)
  async function run(label) {
    const req = pool.request();
    req.input("labelCode", sql.VarChar(50), label);
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
    case "A.": {
      tableName = "BahanBaku_d";
      // Format: A.0000000001-1
      const parts = raw.split("-");
      if (parts.length !== 2) {
        throw new Error(
          "Invalid format for A. prefix. Expected: A.0000000001-1",
        );
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
      reqA.input("noBahanBaku", sql.VarChar(50), noBahanBaku);
      reqA.input("noPallet", sql.Int, noPallet);
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
    case "B.":
      tableName = "Washing_d";
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
    case "D.":
      tableName = "Broker_d";
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
    case "M.":
      tableName = "Bonggolan";
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
    case "F.":
      tableName = "Crusher";
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
    case "V.":
      tableName = "Gilingan";
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
    case "H.":
      tableName = "Mixer_d";
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
    case "BF.":
      tableName = "RejectV2";
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
      throw new Error(
        `Invalid prefix: ${prefix}. Valid prefixes: A., B., D., M., F., V., H., BF.`,
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
async function upsertInputsAndPartials(noProduksi, payload, ctx) {
  const no = String(noProduksi || "").trim();
  if (!no) throw badReq("noProduksi wajib diisi");

  const body = payload && typeof payload === "object" ? payload : {};

  // ✅ ctx wajib (audit)
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
  }

  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";

  // requestId wajib string (kalau kosong, nanti di applyAuditContext dibuat fallback juga)
  const requestId = String(ctx?.requestId || "").trim();

  // ✅ forward ctx yang sudah dinormalisasi
  return sharedInputService.upsertInputsAndPartials(
    "brokerProduksi",
    no,
    body,
    {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    },
  );
}

/**
 * ✅ Delete inputs & partials dengan audit context
 */
async function deleteInputsAndPartials(noProduksi, payload, ctx) {
  const no = String(noProduksi || "").trim();
  if (!no) throw badReq("noProduksi wajib diisi");

  const body = payload && typeof payload === "object" ? payload : {};

  // ✅ Validate audit context
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
  }

  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim();

  // ✅ Forward to shared service
  return sharedInputService.deleteInputsAndPartials(
    "brokerProduksi",
    no,
    body,
    {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    },
  );
}

/**
 * Pindahkan output broker (rows di BrokerProduksiOutput) dari satu NoProduksi ke NoProduksi lain.
 *
 * @param {string} fromNoProduksi  - NoProduksi asal (dari URL param)
 * @param {string} targetNoProduksi - NoProduksi tujuan (dari body)
 * @param {Array<{noBroker:string, noSak:number|string}>} items - daftar output yang dipindahkan
 * @param {object} ctx - { actorId, actorUsername, requestId }
 */
async function moveOutputs(fromNoProduksi, targetNoProduksi, items, ctx) {
  if (!fromNoProduksi) throw badReq("fromNoProduksi wajib");
  if (!targetNoProduksi) throw badReq("targetNoProduksi wajib");
  if (fromNoProduksi === targetNoProduksi)
    throw badReq("targetNoProduksi harus berbeda dengan NoProduksi asal");
  if (!Array.isArray(items) || items.length === 0)
    throw badReq("items wajib berupa array dan tidak boleh kosong");

  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0)
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");

  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim();

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    const auditReq = new sql.Request(tx);
    await applyAuditContext(auditReq, {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    // Ambil tanggal dari kedua header (sekaligus lock row)
    const { docDateOnly: fromDate } = await loadDocDateOnlyFromConfig({
      entityKey: "brokerProduksi",
      codeValue: fromNoProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    const { docDateOnly: targetDate } = await loadDocDateOnlyFromConfig({
      entityKey: "brokerProduksi",
      codeValue: targetNoProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    await assertNotLocked({
      date: fromDate,
      runner: tx,
      action: "move output BrokerProduksi (asal)",
      useLock: true,
    });

    await assertNotLocked({
      date: targetDate,
      runner: tx,
      action: "move output BrokerProduksi (tujuan)",
      useLock: true,
    });

    // Bangun kondisi WHERE untuk setiap pasangan (NoBroker, NoSak)
    const rqMove = new sql.Request(tx);
    rqMove.input("FromNoProduksi", sql.VarChar(50), fromNoProduksi);
    rqMove.input("TargetNoProduksi", sql.VarChar(50), targetNoProduksi);

    const pairConditions = items.map((item, i) => {
      rqMove.input(`nb${i}`, sql.VarChar(50), String(item.noBroker || ""));
      rqMove.input(`ns${i}`, sql.Int, Number(item.noSak));
      return `(NoBroker = @nb${i} AND NoSak = @ns${i})`;
    });

    const moveSql = `
      UPDATE dbo.BrokerProduksiOutput
      SET NoProduksi = @TargetNoProduksi
      WHERE NoProduksi = @FromNoProduksi
        AND (${pairConditions.join(" OR ")});

      SELECT @@ROWCOUNT AS MovedCount;
    `;

    const moveRes = await rqMove.query(moveSql);
    const movedCount = moveRes.recordset?.[0]?.MovedCount ?? 0;

    await tx.commit();
    return { movedCount, fromNoProduksi, targetNoProduksi };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

/**
 * Pindahkan output bonggolan (rows di BrokerProduksiOutputBonggolan) dari satu NoProduksi ke NoProduksi lain.
 *
 * @param {string} fromNoProduksi
 * @param {string} targetNoProduksi
 * @param {string[]} noBonggolanList - array NoBonggolan yang dipindahkan
 * @param {object} ctx - { actorId, actorUsername, requestId }
 */
async function moveOutputsBonggolan(
  fromNoProduksi,
  targetNoProduksi,
  noBonggolanList,
  ctx,
) {
  if (!fromNoProduksi) throw badReq("fromNoProduksi wajib");
  if (!targetNoProduksi) throw badReq("targetNoProduksi wajib");
  if (fromNoProduksi === targetNoProduksi)
    throw badReq("targetNoProduksi harus berbeda dengan NoProduksi asal");
  if (!Array.isArray(noBonggolanList) || noBonggolanList.length === 0)
    throw badReq("noBonggolanList wajib berupa array dan tidak boleh kosong");

  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0)
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");

  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim();

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    const auditReq = new sql.Request(tx);
    await applyAuditContext(auditReq, {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    const { docDateOnly: fromDate } = await loadDocDateOnlyFromConfig({
      entityKey: "brokerProduksi",
      codeValue: fromNoProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    const { docDateOnly: targetDate } = await loadDocDateOnlyFromConfig({
      entityKey: "brokerProduksi",
      codeValue: targetNoProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    await assertNotLocked({
      date: fromDate,
      runner: tx,
      action: "move output bonggolan BrokerProduksi (asal)",
      useLock: true,
    });

    await assertNotLocked({
      date: targetDate,
      runner: tx,
      action: "move output bonggolan BrokerProduksi (tujuan)",
      useLock: true,
    });

    const rqMove = new sql.Request(tx);
    rqMove.input("FromNoProduksi", sql.VarChar(50), fromNoProduksi);
    rqMove.input("TargetNoProduksi", sql.VarChar(50), targetNoProduksi);

    const placeholders = noBonggolanList.map((nb, i) => {
      rqMove.input(`nb${i}`, sql.VarChar(50), String(nb));
      return `@nb${i}`;
    });

    const moveSql = `
      UPDATE dbo.BrokerProduksiOutputBonggolan
      SET NoProduksi = @TargetNoProduksi
      WHERE NoProduksi = @FromNoProduksi
        AND NoBonggolan IN (${placeholders.join(", ")});

      SELECT @@ROWCOUNT AS MovedCount;
    `;

    const moveRes = await rqMove.query(moveSql);
    const movedCount = moveRes.recordset?.[0]?.MovedCount ?? 0;

    await tx.commit();
    return { movedCount, fromNoProduksi, targetNoProduksi };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

async function splitProduksiTime(selector, payload, ctx) {
  const idMesin = Number(selector?.idMesin);
  const tanggal = String(selector?.tanggal || "").trim();
  if (!Number.isInteger(idMesin) || idMesin <= 0) {
    throw badReq("idMesin harus integer positif");
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(tanggal)) {
    throw badReq("tanggal harus format YYYY-MM-DD");
  }

  const hourStart = String(payload?.hourStart || "").trim();
  const outputJenisId = Number(payload?.outputJenisId);
  if (!hourStart) throw badReq("hourStart wajib diisi");
  if (!Number.isInteger(outputJenisId) || outputJenisId <= 0) {
    throw badReq("outputJenisId wajib integer positif");
  }
  const toSeconds = (hhmmss) => {
    const m = /^(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(
      String(hhmmss || "").trim(),
    );
    if (!m) return null;
    const hh = Number(m[1]);
    const mm = Number(m[2]);
    const ss = Number(m[3] || "0");
    if (hh > 23 || mm > 59 || ss > 59) return null;
    return hh * 3600 + mm * 60 + ss;
  };
  const normalizeTimeValue = (v) => {
    if (v == null) return null;
    if (v instanceof Date) {
      // SQL time biasanya dipetakan ke Date UTC. Pakai UTC getter agar tidak kena offset timezone server.
      const hh = String(v.getUTCHours()).padStart(2, "0");
      const mm = String(v.getUTCMinutes()).padStart(2, "0");
      const ss = String(v.getUTCSeconds()).padStart(2, "0");
      return `${hh}:${mm}:${ss}`;
    }
    const s = String(v).trim();
    const m = /(\d{2}):(\d{2}):(\d{2})/.exec(s);
    return m ? `${m[1]}:${m[2]}:${m[3]}` : null;
  };
  const reqStartSec = toSeconds(hourStart);
  if (reqStartSec == null) {
    throw badReq("Format hourStart harus HH:mm atau HH:mm:ss");
  }
  const normalizeIntoShiftWindow = (sec, shiftStartSec, shiftEndSec) => {
    const isOvernight = shiftStartSec > shiftEndSec;
    if (!isOvernight) return sec;
    return sec < shiftStartSec ? sec + 86400 : sec;
  };

  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
  }
  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim();

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    await applyAuditContext(new sql.Request(tx), {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    });

    const srcRes = await new sql.Request(tx)
      .input("IdMesin", sql.Int, idMesin)
      .input("Tanggal", sql.Date, tanggal).query(`
        SELECT TOP 1 *
        FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE IdMesin = @IdMesin
          AND CONVERT(date, TglProduksi) = @Tanggal
        ORDER BY HourStart DESC, NoProduksi DESC
      `);

    const src = srcRes.recordset?.[0];
    if (!src) {
      throw notFound(
        `Produksi broker tidak ditemukan untuk idMesin ${idMesin} dan tanggal ${tanggal}`,
      );
    }
    const sourceNo = String(src.NoProduksi || "").trim();
    if (!sourceNo) throw conflict("Data produksi terakhir tidak valid");
    const srcShift = Number(src.Shift);
    if (!Number.isInteger(srcShift) || srcShift <= 0) {
      throw conflict(
        `Data shift produksi sumber tidak valid pada ${sourceNo}.`,
      );
    }

    const shiftRefRes = await new sql.Request(tx)
      .input("Tanggal", sql.Date, tanggal)
      .input("NoShift", sql.Int, srcShift).query(`
        ;WITH LatestShiftSet AS (
          SELECT TOP 1
            h.IdShiftHourSet,
            h.ValidFrmDate
          FROM dbo.MstShiftHourSet h WITH (NOLOCK)
          WHERE CONVERT(date, h.ValidFrmDate) <= @Tanggal
          ORDER BY CONVERT(date, h.ValidFrmDate) DESC, h.IdShiftHourSet DESC
        )
        SELECT TOP 1
          ls.IdShiftHourSet,
          ls.ValidFrmDate,
          d.NoShift,
          CONVERT(varchar(8), d.HourStart, 108) AS HourStart,
          CONVERT(varchar(8), d.HourEnd, 108) AS HourEnd
        FROM LatestShiftSet ls
        INNER JOIN dbo.MstShiftHourSet_d d WITH (NOLOCK)
          ON d.IdShiftHourSet = ls.IdShiftHourSet
        WHERE d.NoShift = @NoShift;
      `);

    const shiftRef = shiftRefRes.recordset?.[0];
    if (!shiftRef) {
      throw notFound(
        `Master shift tidak ditemukan untuk tanggal ${tanggal} dan shift ${srcShift}.`,
      );
    }

    const shiftStartSec = toSeconds(shiftRef.HourStart);
    const hourEnd = String(shiftRef.HourEnd || "").trim();
    const shiftEndSec = toSeconds(hourEnd);
    if (shiftStartSec == null || shiftEndSec == null) {
      throw conflict("Master shift memiliki HourStart/HourEnd tidak valid.");
    }

    const reqStartInWindow = normalizeIntoShiftWindow(
      reqStartSec,
      shiftStartSec,
      shiftEndSec,
    );
    const reqEndInWindow = normalizeIntoShiftWindow(
      shiftEndSec,
      shiftStartSec,
      shiftEndSec,
    );
    const shiftEndBound =
      shiftStartSec > shiftEndSec ? shiftEndSec + 86400 : shiftEndSec;

    if (
      reqStartInWindow < shiftStartSec ||
      reqStartInWindow > shiftEndBound ||
      reqEndInWindow < shiftStartSec ||
      reqEndInWindow > shiftEndBound
    ) {
      throw badReq(
        `Range jam harus berada dalam batas shift ${srcShift} (${shiftRef.HourStart}-${shiftRef.HourEnd}) untuk tanggal ${tanggal}.`,
      );
    }
    if (reqEndInWindow <= reqStartInWindow) {
      throw badReq(
        "hourEnd harus lebih besar dari hourStart dalam rentang shift yang sama",
      );
    }
    const srcHourStartStr = normalizeTimeValue(src.HourStart);
    const srcHourEndStr = normalizeTimeValue(src.HourEnd);
    const srcStartSec = toSeconds(srcHourStartStr);
    const srcEndSec = toSeconds(srcHourEndStr);
    if (srcStartSec == null || srcEndSec == null) {
      throw conflict(
        `Data jam produksi sumber tidak valid pada ${sourceNo} (HourStart/HourEnd).`,
      );
    }
    const reqStartInSource = normalizeIntoShiftWindow(
      reqStartSec,
      srcStartSec,
      srcEndSec,
    );
    if (reqStartInSource <= srcStartSec) {
      throw badReq(`Jam Mulai harus lebih besar dari ${srcHourStartStr}.`);
    }
    // Jika split di tengah rentang lama, hourEnd baru tidak boleh melewati HourEnd lama
    // Jika hourStart tepat di HourEnd lama, ini mode "lanjutan", hourEnd boleh lebih besar.
    // if (reqStartSec < srcEndSec && reqEndSec > srcEndSec) {
    //   throw badReq(
    //     `hourEnd tidak boleh melebihi HourEnd produksi terakhir (${srcHourEndStr}).`,
    //   );
    // }

    // Idempotency guard:
    // cegah request split berulang dengan rentang waktu yang sama pada mesin + tanggal yang sama
    const duplicateRes = await new sql.Request(tx)
      .input("IdMesin", sql.Int, idMesin)
      .input("Tanggal", sql.Date, tanggal)
      .input("HourStart", sql.VarChar(20), hourStart)
      .input("HourEnd", sql.VarChar(20), hourEnd).query(`
        SELECT TOP 1 NoProduksi
        FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE IdMesin = @IdMesin
          AND CONVERT(date, TglProduksi) = @Tanggal
          AND HourStart = CAST(@HourStart AS time(7))
          AND HourEnd = CAST(@HourEnd AS time(7))
        ORDER BY NoProduksi DESC
      `);
    if (duplicateRes.recordset?.length) {
      const existingNo = duplicateRes.recordset[0].NoProduksi;
      throw conflict(
        `Rentang waktu ${hourStart}-${hourEnd} sudah ada pada produksi ${existingNo}.`,
      );
    }

    const docDateOnly = toDateOnly(src.TglProduksi);
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: `split time BrokerProduksi ${sourceNo}`,
      useLock: true,
    });

    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.BrokerProduksi_h",
        columnName: "NoProduksi",
        prefix: "E.",
        width: 10,
      });

    let newNoProduksi = await gen();
    const exists = await new sql.Request(tx).input(
      "NoProduksi",
      sql.VarChar(50),
      newNoProduksi,
    ).query(`
        SELECT 1 FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);
    if (exists.recordset.length > 0) {
      const retry = await gen();
      const exists2 = await new sql.Request(tx).input(
        "NoProduksi",
        sql.VarChar(50),
        retry,
      ).query(`
          SELECT 1 FROM dbo.BrokerProduksi_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoProduksi = @NoProduksi
        `);
      if (exists2.recordset.length > 0) {
        throw conflict("Gagal generate NoProduksi unik, coba lagi.");
      }
      newNoProduksi = retry;
    }

    const insReq = new sql.Request(tx);
    insReq
      .input("NewNoProduksi", sql.VarChar(50), newNoProduksi)
      .input("SourceNoProduksi", sql.VarChar(50), sourceNo)
      .input("NewHourStart", sql.VarChar(20), hourStart)
      .input("NewHourEnd", sql.VarChar(20), hourEnd)
      .input("OutputJenisId", sql.Int, outputJenisId);

    const insertRes = await insReq.query(`
      DECLARE @out TABLE (
        NoProduksi varchar(50),
        TglProduksi date,
        IdMesin int,
        IdOperator int,
        OutputJenisId int,
        Jam int,
        Shift int,
        CreateBy varchar(100),
        CheckBy1 varchar(100),
        CheckBy2 varchar(100),
        ApproveBy varchar(100),
        JmlhAnggota int,
        Hadir int,
        HourMeter decimal(18,2),
        HourStart time(7),
        HourEnd time(7),
        IdRegu int
      );

      INSERT INTO dbo.BrokerProduksi_h (
        NoProduksi, TglProduksi, IdMesin, IdOperator, OutputJenisId, Jam, Shift, CreateBy,
        CheckBy1, CheckBy2, ApproveBy, JmlhAnggota, Hadir, HourMeter,
        HourStart, HourEnd, IdRegu
      )
      OUTPUT
        INSERTED.NoProduksi, INSERTED.TglProduksi, INSERTED.IdMesin, INSERTED.IdOperator,
        INSERTED.OutputJenisId,
        INSERTED.Jam, INSERTED.Shift, INSERTED.CreateBy, INSERTED.CheckBy1, INSERTED.CheckBy2,
        INSERTED.ApproveBy, INSERTED.JmlhAnggota, INSERTED.Hadir, INSERTED.HourMeter,
        INSERTED.HourStart, INSERTED.HourEnd, INSERTED.IdRegu
      INTO @out
      SELECT
        @NewNoProduksi,
        h.TglProduksi,
        h.IdMesin,
        h.IdOperator,
        @OutputJenisId,
        h.Jam,
        h.Shift,
        h.CreateBy,
        h.CheckBy1,
        h.CheckBy2,
        h.ApproveBy,
        h.JmlhAnggota,
        h.Hadir,
        h.HourMeter,
        CAST(@NewHourStart AS time(7)),
        CAST(@NewHourEnd AS time(7)),
        h.IdRegu
      FROM dbo.BrokerProduksi_h h WITH (UPDLOCK, HOLDLOCK)
      WHERE h.NoProduksi = @SourceNoProduksi;

      SELECT
        o.*,
        mb.Nama AS OutputJenisNama
      FROM @out o
      LEFT JOIN PPS.dbo.MstBroker mb WITH (NOLOCK)
        ON mb.IdBroker = o.OutputJenisId;
    `);

    await new sql.Request(tx)
      .input("SourceNoProduksi", sql.VarChar(50), sourceNo)
      .input("NewHourStart", sql.VarChar(20), hourStart).query(`
        UPDATE dbo.BrokerProduksi_h
        SET HourEnd = CAST(@NewHourStart AS time(7))
        WHERE NoProduksi = @SourceNoProduksi
      `);

    await tx.commit();
    return {
      idMesin,
      tanggal,
      sourceNoProduksi: sourceNo,
      newNoProduksi,
      sourceHourEndUpdatedTo: hourStart,
      newHourStart: hourStart,
      newHourEnd: hourEnd,
      header: insertRes.recordset?.[0] || null,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

module.exports = {
  getAllProduksi,
  getProduksiByDate,
  getNoProduksiByTanggalShift,
  fetchInputs,
  fetchOutputs,
  fetchOutputsBonggolan,
  createBrokerProduksi,
  updateBrokerProduksi,
  deleteBrokerProduksi,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
  moveOutputs,
  moveOutputsBonggolan,
  splitProduksiTime,
};

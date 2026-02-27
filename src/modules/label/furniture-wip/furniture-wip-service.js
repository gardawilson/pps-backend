// services/labels/furniture-wip-service.js
const { sql, poolPromise } = require("../../../core/config/db");
const {
  getBlokLokasiFromKodeProduksi,
} = require("../../../core/shared/mesin-location-helper");

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,
  formatYMD,
} = require("../../../core/shared/tutup-transaksi-guard");

const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");

const hasOwn = (obj, key) =>
  Object.prototype.hasOwnProperty.call(obj || {}, key);

exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama AS NamaFurnitureWIP,

      -- 🔹 Pcs sudah dikurangi partial (jika IsPartial = 1)
      CASE 
        WHEN f.IsPartial = 1 THEN
          CASE
            WHEN ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0) < 0 
              THEN 0
            ELSE ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0)
          END
        ELSE ISNULL(f.Pcs, 0)
      END AS Pcs,

      ISNULL(f.Berat, 0) AS Berat,

      f.IsPartial,
      MAX(ISNULL(CAST(f.HasBeenPrinted AS int), 0)) AS HasBeenPrinted,
      f.IdWarna,
      f.Blok,
      f.IdLokasi,

      -- 🔗 TIPE SUMBER (HOTSTAMPING / PASANG_KUNCI / BONGKAR_SUSUN / RETUR / SPANNER / INJECT)
      CASE
        WHEN MAX(hsmap.NoProduksi)     IS NOT NULL THEN 'HOTSTAMPING'
        WHEN MAX(pkmap.NoProduksi)     IS NOT NULL THEN 'PASANG_KUNCI'
        WHEN MAX(bsmap.NoBongkarSusun) IS NOT NULL THEN 'BONGKAR_SUSUN'
        WHEN MAX(retmap.NoRetur)       IS NOT NULL THEN 'RETUR'
        WHEN MAX(spmap.NoProduksi)     IS NOT NULL THEN 'SPANNER'
        WHEN MAX(injmap.NoProduksi)    IS NOT NULL THEN 'INJECT'
        ELSE NULL
      END AS OutputType,

      -- 🔗 KODE SUMBER (BH./BI./BG./L./BJ./S.)
      MAX(
        COALESCE(
          hsmap.NoProduksi,
          pkmap.NoProduksi,
          spmap.NoProduksi,
          injmap.NoProduksi,
          bsmap.NoBongkarSusun,
          retmap.NoRetur
        )
      ) AS OutputCode,

      -- 🔗 NAMA MESIN / NAMA PEMBELI / 'Bongkar Susun'
      MAX(
        COALESCE(
          mHs.NamaMesin,
          mPk.NamaMesin,
          mSp.NamaMesin,
          mInj.NamaMesin,
          CASE WHEN bsmap.NoBongkarSusun IS NOT NULL THEN 'Bongkar Susun' END,
          pemb.NamaPembeli
        )
      ) AS OutputNamaMesin

    FROM [dbo].[FurnitureWIP] f

    -- 🔹 Aggregate partial per NoFurnitureWIP
    LEFT JOIN (
      SELECT
        NoFurnitureWIP,
        SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
      FROM [dbo].[FurnitureWIPPartial]
      GROUP BY NoFurnitureWIP
    ) fp
      ON fp.NoFurnitureWIP = f.NoFurnitureWIP

    -- 🔗 Master nama furniture WIP
    LEFT JOIN [dbo].[MstCabinetWIP] cw
      ON cw.IdCabinetWIP = f.IdFurnitureWIP

    ----------------------------------------------------------------------
    -- 🔗 MAPPING HOT STAMPING (BH.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[HotStampingOutputLabelFWIP] hsmap
           ON hsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[HotStamping_h] hsh
           ON hsh.NoProduksi = hsmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHs
           ON mHs.IdMesin = hsh.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING PASANG KUNCI (BI.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[PasangKunciOutputLabelFWIP] pkmap
           ON pkmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[PasangKunci_h] pkh
           ON pkh.NoProduksi = pkmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mPk
           ON mPk.IdMesin = pkh.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING BONGKAR SUSUN (BG.)
    --     Tidak ada mesin → NamaMesin = 'Bongkar Susun'
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[BongkarSusunOutputFurnitureWIP] bsmap
           ON bsmap.NoFurnitureWIP = f.NoFurnitureWIP

    ----------------------------------------------------------------------
    -- 🔗 MAPPING RETUR (L.) → pakai NamaPembeli
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[BJReturFurnitureWIP_d] retmap
           ON retmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJRetur_h] bjh
           ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb
           ON pemb.IdPembeli = bjh.IdPembeli

    ----------------------------------------------------------------------
    -- 🔗 MAPPING SPANNER (BJ.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[SpannerOutputLabelFWIP] spmap
           ON spmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[Spanner_h] sph
           ON sph.NoProduksi = spmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSp
           ON mSp.IdMesin = sph.IdMesin

    ----------------------------------------------------------------------
    -- 🔗 MAPPING INJECT PRODUKSI (S.)
    ----------------------------------------------------------------------
    LEFT JOIN [dbo].[InjectProduksiOutputFurnitureWIP] injmap
           ON injmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[InjectProduksi_h] injh
           ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj
           ON mInj.IdMesin = injh.IdMesin

    WHERE 1=1
      AND f.DateUsage IS NULL
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search

               -- cari berdasarkan kode sumber
               OR ISNULL(hsmap.NoProduksi,'')     LIKE @search
               OR ISNULL(pkmap.NoProduksi,'')     LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'') LIKE @search
               OR ISNULL(retmap.NoRetur,'')       LIKE @search
               OR ISNULL(spmap.NoProduksi,'')     LIKE @search
               OR ISNULL(injmap.NoProduksi,'')    LIKE @search

               -- cari berdasarkan nama mesin / pembeli
               OR ISNULL(mHs.NamaMesin,'')        LIKE @search
               OR ISNULL(mPk.NamaMesin,'')        LIKE @search
               OR ISNULL(mSp.NamaMesin,'')        LIKE @search
               OR ISNULL(mInj.NamaMesin,'')       LIKE @search
               OR ISNULL(pemb.NamaPembeli,'')     LIKE @search
             )`
          : ""
      }
    GROUP BY
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama,
      f.Pcs,
      f.Berat,
      f.IsPartial,
      f.IdWarna,
      f.Blok,
      f.IdLokasi
    ORDER BY f.NoFurnitureWIP DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT f.NoFurnitureWIP) AS total
    FROM [dbo].[FurnitureWIP] f
    LEFT JOIN [dbo].[MstCabinetWIP] cw
      ON cw.IdCabinetWIP = f.IdFurnitureWIP

    LEFT JOIN [dbo].[HotStampingOutputLabelFWIP] hsmap
           ON hsmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[HotStamping_h] hsh
           ON hsh.NoProduksi = hsmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHs
           ON mHs.IdMesin = hsh.IdMesin

    LEFT JOIN [dbo].[PasangKunciOutputLabelFWIP] pkmap
           ON pkmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[PasangKunci_h] pkh
           ON pkh.NoProduksi = pkmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mPk
           ON mPk.IdMesin = pkh.IdMesin

    LEFT JOIN [dbo].[BongkarSusunOutputFurnitureWIP] bsmap
           ON bsmap.NoFurnitureWIP = f.NoFurnitureWIP

    LEFT JOIN [dbo].[BJReturFurnitureWIP_d] retmap
           ON retmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[BJRetur_h] bjh
           ON bjh.NoRetur = retmap.NoRetur
    LEFT JOIN [dbo].[MstPembeli] pemb
           ON pemb.IdPembeli = bjh.IdPembeli

    LEFT JOIN [dbo].[SpannerOutputLabelFWIP] spmap
           ON spmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[Spanner_h] sph
           ON sph.NoProduksi = spmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSp
           ON mSp.IdMesin = sph.IdMesin

    LEFT JOIN [dbo].[InjectProduksiOutputFurnitureWIP] injmap
           ON injmap.NoFurnitureWIP = f.NoFurnitureWIP
    LEFT JOIN [dbo].[InjectProduksi_h] injh
           ON injh.NoProduksi = injmap.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInj
           ON mInj.IdMesin = injh.IdMesin

    WHERE 1=1
      AND f.DateUsage IS NULL
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search
               OR ISNULL(hsmap.NoProduksi,'')     LIKE @search
               OR ISNULL(pkmap.NoProduksi,'')     LIKE @search
               OR ISNULL(bsmap.NoBongkarSusun,'') LIKE @search
               OR ISNULL(retmap.NoRetur,'')       LIKE @search
               OR ISNULL(spmap.NoProduksi,'')     LIKE @search
               OR ISNULL(injmap.NoProduksi,'')    LIKE @search
               OR ISNULL(mHs.NamaMesin,'')        LIKE @search
               OR ISNULL(mPk.NamaMesin,'')        LIKE @search
               OR ISNULL(mSp.NamaMesin,'')        LIKE @search
               OR ISNULL(mInj.NamaMesin,'')       LIKE @search
               OR ISNULL(pemb.NamaPembeli,'')     LIKE @search
             )`
          : ""
      }
  `;

  request.input("offset", sql.Int, offset);
  request.input("limit", sql.Int, limit);
  if (search) {
    request.input("search", sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};

/**
 * insert 1 row FurnitureWIP + mapping output
 */
async function insertSingleFurnitureWip({
  tx,
  header,
  idFurnitureWip,
  outputCode,
  outputType,
  mappingTable,
  effectiveDateCreate,
  nowDateTime,
}) {
  // ===============================
  // 1) Generate NoFurnitureWIP (pakai generateNextCode)
  // ===============================
  const gen = async () =>
    generateNextCode(tx, {
      tableName: "dbo.FurnitureWIP",
      columnName: "NoFurnitureWIP",
      prefix: "BB.",
      width: 10,
    });

  const generatedNo = await gen();

  // double-check belum dipakai (lock)
  const exist = await new sql.Request(tx).input(
    "NoFurnitureWIP",
    sql.VarChar(50),
    generatedNo,
  ).query(`
      SELECT 1
      FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
      WHERE NoFurnitureWIP = @NoFurnitureWIP
    `);

  let noFurnitureWip = generatedNo;

  if (exist.recordset.length > 0) {
    const retryNo = await gen();
    const exist2 = await new sql.Request(tx).input(
      "NoFurnitureWIP",
      sql.VarChar(50),
      retryNo,
    ).query(`
        SELECT 1
        FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
        WHERE NoFurnitureWIP = @NoFurnitureWIP
      `);

    if (exist2.recordset.length > 0) {
      throw conflict("Gagal generate NoFurnitureWIP unik, coba lagi.");
    }
    noFurnitureWip = retryNo;
  }

  // ===============================
  // 2) Insert header (DateTimeCreate dari app, bukan GETDATE())
  // ===============================
  const insertHeaderSql = `
    INSERT INTO dbo.FurnitureWIP (
      NoFurnitureWIP,
      DateCreate,
      Jam,
      Pcs,
      IDFurnitureWIP,
      Berat,
      IsPartial,
      DateUsage,
      IdWarehouse,
      IdWarna,
      CreateBy,
      DateTimeCreate,
      Blok,
      IdLokasi
    )
    VALUES (
      @NoFurnitureWIP,
      @DateCreate,
      @Jam,
      @Pcs,
      @IDFurnitureWIP,
      @Berat,
      @IsPartial,
      NULL,
      @IdWarehouse,
      @IdWarna,
      @CreateBy,
      @DateTimeCreate,
      @Blok,
      @IdLokasi
    );
  `;

  await new sql.Request(tx)
    .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip)
    .input("DateCreate", sql.Date, effectiveDateCreate)
    .input("Jam", sql.VarChar(20), header.Jam ?? null) // kalau tipe TIME, ganti ke sql.Time
    .input("Pcs", sql.Decimal(18, 3), header.Pcs ?? null)
    .input("IDFurnitureWIP", sql.Int, idFurnitureWip)
    .input("Berat", sql.Decimal(18, 3), header.Berat ?? null)
    .input("IsPartial", sql.Bit, header.IsPartial ?? 0)
    .input("IdWarehouse", sql.Int, header.IdWarehouse) // wajib
    .input("IdWarna", sql.Int, header.IdWarna ?? null)
    .input("CreateBy", sql.VarChar(50), header.CreateBy) // controller overwrite dari token
    .input("DateTimeCreate", sql.DateTime, nowDateTime)
    .input("Blok", sql.VarChar(50), header.Blok ?? null)
    .input("IdLokasi", sql.Int, header.IdLokasi ?? null)
    .query(insertHeaderSql);

  // ===============================
  // 3) Insert mapping berdasarkan mappingTable
  // ===============================
  const rqMap = new sql.Request(tx)
    .input("OutputCode", sql.VarChar(50), outputCode)
    .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip);

  if (mappingTable === "HotStampingOutputLabelFWIP") {
    await rqMap.query(`
      INSERT INTO dbo.HotStampingOutputLabelFWIP (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `);
  } else if (mappingTable === "PasangKunciOutputLabelFWIP") {
    await rqMap.query(`
      INSERT INTO dbo.PasangKunciOutputLabelFWIP (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `);
  } else if (mappingTable === "SpannerOutputLabelFWIP") {
    await rqMap.query(`
      INSERT INTO dbo.SpannerOutputLabelFWIP (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `);
  } else if (mappingTable === "BongkarSusunOutputFurnitureWIP") {
    await rqMap.query(`
      INSERT INTO dbo.BongkarSusunOutputFurnitureWIP (NoBongkarSusun, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `);
  } else if (mappingTable === "BJReturFurnitureWIP_d") {
    await rqMap.query(`
      INSERT INTO dbo.BJReturFurnitureWIP_d (NoRetur, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `);
  } else if (mappingTable === "InjectProduksiOutputFurnitureWIP") {
    await rqMap.query(`
      INSERT INTO dbo.InjectProduksiOutputFurnitureWIP (NoProduksi, NoFurnitureWIP)
      VALUES (@OutputCode, @NoFurnitureWIP);
    `);
  }

  return {
    NoFurnitureWIP: noFurnitureWip,
    DateCreate: formatYMD(effectiveDateCreate),
    Jam: header.Jam ?? null,
    Pcs: header.Pcs ?? null,
    IDFurnitureWIP: idFurnitureWip,
    Berat: header.Berat ?? null,
    IsPartial: header.IsPartial ?? 0,
    DateUsage: null,
    IdWarehouse: header.IdWarehouse,
    IdWarna: header.IdWarna ?? null,
    CreateBy: header.CreateBy,
    DateTimeCreate: nowDateTime,
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
    OutputCode: outputCode,
    OutputType: outputType,
  };
}

/**
 * INJECT multi-create:
 * - ambil InjectProduksi_h
 * - mapping CetakanWarnaToFurnitureWIP_d
 * - loop insertSingleFurnitureWip
 */
async function createFromInjectMapping({
  tx,
  header,
  outputCode,
  mappingTable,
  outputType,
  effectiveDateCreate,
  nowDateTime,
}) {
  const injRes = await new sql.Request(tx).input(
    "NoProduksi",
    sql.VarChar(50),
    outputCode,
  ).query(`
      SELECT TOP 1 IdCetakan, IdWarna, IdFurnitureMaterial
      FROM dbo.InjectProduksi_h WITH (UPDLOCK, HOLDLOCK)
      WHERE NoProduksi = @NoProduksi
        AND IdCetakan IS NOT NULL;
    `);

  if (!injRes.recordset.length) {
    throw badReq(
      `InjectProduksi_h ${outputCode} tidak ditemukan atau IdCetakan NULL`,
    );
  }

  const inj = injRes.recordset[0];

  const mapRes = await new sql.Request(tx)
    .input("IdCetakan", sql.Int, inj.IdCetakan)
    .input("IdWarna", sql.Int, inj.IdWarna)
    .input("IdFurnitureMaterial", sql.Int, inj.IdFurnitureMaterial ?? 0).query(`
      SELECT IdFurnitureWIP
      FROM dbo.CetakanWarnaToFurnitureWIP_d
      WHERE IdCetakan = @IdCetakan
        AND IdWarna = @IdWarna
        AND (
          (IdFurnitureMaterial IS NULL AND @IdFurnitureMaterial = 0)
          OR IdFurnitureMaterial = @IdFurnitureMaterial
        );
    `);

  if (!mapRes.recordset.length) {
    throw badReq(
      `Mapping FurnitureWIP tidak ditemukan untuk Inject ${outputCode} (IdCetakan=${inj.IdCetakan}, IdWarna=${inj.IdWarna})`,
    );
  }

  const created = [];
  for (const row of mapRes.recordset) {
    created.push(
      await insertSingleFurnitureWip({
        tx,
        header,
        idFurnitureWip: row.IdFurnitureWIP,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate,
        nowDateTime,
      }),
    );
  }

  return created;
}

exports.createFurnitureWip = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = String(payload?.outputCode || "").trim();

  // =========================
  // validation dasar
  // =========================
  if (!outputCode)
    throw badReq("outputCode wajib diisi (BH., BI., BG., L., BJ., S.)");
  if (!header.CreateBy)
    throw badReq(
      "CreateBy wajib diisi (controller harus overwrite dari token)",
    );

  // =========================
  // [AUDIT] actorId + requestId
  // =========================
  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  // mapping prefix -> outputType/mappingTable
  let outputType = null;
  let mappingTable = null;

  if (outputCode.startsWith("BH.")) {
    outputType = "HOTSTAMPING";
    mappingTable = "HotStampingOutputLabelFWIP";
  } else if (outputCode.startsWith("BI.")) {
    outputType = "PASANG_KUNCI";
    mappingTable = "PasangKunciOutputLabelFWIP";
  } else if (outputCode.startsWith("BG.")) {
    outputType = "BONGKAR_SUSUN";
    mappingTable = "BongkarSusunOutputFurnitureWIP";
  } else if (outputCode.startsWith("L.")) {
    outputType = "RETUR";
    mappingTable = "BJReturFurnitureWIP_d";
  } else if (outputCode.startsWith("BJ.")) {
    outputType = "SPANNER";
    mappingTable = "SpannerOutputLabelFWIP";
  } else if (outputCode.startsWith("S.")) {
    outputType = "INJECT";
    mappingTable = "InjectProduksiOutputFurnitureWIP";
  } else
    throw badReq(
      "outputCode prefix tidak dikenali (BH., BI., BG., L., BJ., S.)",
    );

  const isInject = outputType === "INJECT";

  // untuk non inject, IdFurnitureWIP wajib ada (dua kemungkinan nama field)
  const idFwipSingle = header.IdFurnitureWIP ?? header.IDFurnitureWIP ?? null;
  if (!isInject && !idFwipSingle)
    throw badReq("IdFurnitureWIP wajib diisi untuk mode non-INJECT");

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =========================
    // [AUDIT CTX] set session context for triggers
    // =========================
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE)
    // ===============================
    const effectiveDateCreate = resolveEffectiveDateForCreate(
      header.DateCreate,
    );
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: "create furniture wip",
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari kode (jika belum ada)
    // ===============================
    const needBlok = header.Blok == null || String(header.Blok).trim() === "";
    const needLokasi = header.IdLokasi == null;

    if (needBlok || needLokasi) {
      const lokasi = await getBlokLokasiFromKodeProduksi({
        kode: outputCode,
        runner: tx,
      });
      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    const nowDateTime = new Date();

    let headers = [];

    if (isInject && !idFwipSingle) {
      headers = await createFromInjectMapping({
        tx,
        header,
        outputCode,
        mappingTable,
        outputType,
        effectiveDateCreate,
        nowDateTime,
      });
    } else {
      const created = await insertSingleFurnitureWip({
        tx,
        header,
        idFurnitureWip: idFwipSingle,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate,
        nowDateTime,
      });
      headers = [created];
    }

    await tx.commit();

    return {
      headers,
      output: {
        code: outputCode,
        type: outputType,
        mappingTable,
        isMulti: headers.length > 1,
        count: headers.length,
      },
      audit: { actorId, requestId }, // ✅ sama seperti crusher
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

/**
 * Hapus semua mapping FurnitureWIP ke proses manapun
 */
async function deleteAllMappings(tx, noFurnitureWip) {
  await new sql.Request(tx).input("NoFurnitureWIP", sql.VarChar, noFurnitureWip)
    .query(`
        DELETE FROM [dbo].[HotStampingOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[PasangKunciOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[BongkarSusunOutputFurnitureWIP]       WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[BJReturFurnitureWIP_d]                WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[SpannerOutputLabelFWIP]               WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[InjectProduksiOutputFurnitureWIP]     WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);
}

exports.updateFurnitureWip = async (noFurnitureWip, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const hasOutputCodeField = hasOwn(payload, "outputCode");
  const outputCode = String(payload?.outputCode || "").trim();

  // =========================
  // [AUDIT] actorId + requestId (WAJIB seperti create)
  // =========================
  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =========================
    // [AUDIT CTX] set session context for triggers
    // =========================
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 1) Ambil data existing + lock
    // ===============================
    const existingRes = await new sql.Request(tx).input(
      "NoFurnitureWIP",
      sql.VarChar(50),
      noFurnitureWip,
    ).query(`
        SELECT TOP 1
          NoFurnitureWIP,
          CONVERT(date, DateCreate) AS DateCreate,
          Jam,
          Pcs,
          IDFurnitureWIP,
          Berat,
          IsPartial,
          DateUsage,
          IdWarehouse,
          IdWarna,
          CreateBy,
          DateTimeCreate,
          Blok,
          IdLokasi
        FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    if (existingRes.recordset.length === 0) {
      throw notFound("Furniture WIP not found");
    }

    const current = existingRes.recordset[0];

    // ===============================
    // 1b) TUTUP TRANSAKSI CHECK (UPDATE) - cek tanggal existing
    // ===============================
    const existingDateCreate = current.DateCreate
      ? toDateOnly(current.DateCreate)
      : null;

    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: "update furniture wip",
      useLock: true,
    });

    // ===============================
    // 2) Merge field (partial update)
    // ===============================
    const merged = {
      // identity / required
      IDFurnitureWIP:
        header.IdFurnitureWIP ??
        header.IDFurnitureWIP ??
        current.IDFurnitureWIP,

      // optional fields
      Jam: hasOwn(header, "Jam") ? header.Jam : current.Jam,
      Pcs: hasOwn(header, "Pcs") ? header.Pcs : current.Pcs,
      Berat: hasOwn(header, "Berat") ? header.Berat : current.Berat,
      IsPartial: hasOwn(header, "IsPartial")
        ? header.IsPartial
        : current.IsPartial,
      IdWarehouse: hasOwn(header, "IdWarehouse")
        ? header.IdWarehouse
        : current.IdWarehouse,
      IdWarna: hasOwn(header, "IdWarna") ? header.IdWarna : current.IdWarna,
      Blok: hasOwn(header, "Blok") ? header.Blok : current.Blok,
      IdLokasi: hasOwn(header, "IdLokasi") ? header.IdLokasi : current.IdLokasi,

      // DateCreate bisa diupdate
      DateCreate: hasOwn(header, "DateCreate")
        ? header.DateCreate
        : current.DateCreate,

      // CreateBy (biasanya overwrite dari token)
      CreateBy: hasOwn(header, "CreateBy") ? header.CreateBy : current.CreateBy,
    };

    if (!merged.IDFurnitureWIP) throw badReq("IdFurnitureWIP cannot be empty");

    // ===============================
    // 2b) Jika DateCreate dikirim user, cek tutup transaksi untuk tanggal baru
    // ===============================
    let dateCreateParam = null;

    if (hasOwn(header, "DateCreate")) {
      if (header.DateCreate === null || header.DateCreate === "") {
        dateCreateParam = toDateOnly(new Date());
      } else {
        dateCreateParam = toDateOnly(header.DateCreate);
        if (!dateCreateParam) {
          throw badReq("Invalid DateCreate");
        }
      }

      await assertNotLocked({
        date: dateCreateParam,
        runner: tx,
        action: "update furniture wip (DateCreate)",
        useLock: true,
      });
    }

    // ===============================
    // 3) UPDATE header
    // ===============================
    const rqUpdate = new sql.Request(tx)
      .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip)
      .input("IDFurnitureWIP", sql.Int, merged.IDFurnitureWIP)
      .input("Jam", sql.VarChar(20), merged.Jam ?? null) // jika kolom time => sql.Time
      .input("Pcs", sql.Decimal(18, 3), merged.Pcs ?? null)
      .input("Berat", sql.Decimal(18, 3), merged.Berat ?? null)
      .input("IsPartial", sql.Bit, merged.IsPartial ?? 0)
      .input("IdWarehouse", sql.Int, merged.IdWarehouse)
      .input("IdWarna", sql.Int, merged.IdWarna ?? null)
      .input("Blok", sql.VarChar(50), merged.Blok ?? null)
      .input("IdLokasi", sql.Int, merged.IdLokasi ?? null) // ✅ int
      .input("CreateBy", sql.VarChar(50), merged.CreateBy ?? null);

    if (hasOwn(header, "DateCreate")) {
      rqUpdate.input("DateCreate", sql.Date, dateCreateParam);
    }

    const updateSql = `
      UPDATE dbo.FurnitureWIP
      SET
        IDFurnitureWIP = @IDFurnitureWIP,
        Jam = @Jam,
        Pcs = @Pcs,
        Berat = @Berat,
        IsPartial = @IsPartial,
        IdWarehouse = @IdWarehouse,
        IdWarna = @IdWarna,
        Blok = @Blok,
        IdLokasi = @IdLokasi,
        CreateBy = @CreateBy
        ${hasOwn(header, "DateCreate") ? ", DateCreate = @DateCreate" : ""}
      WHERE NoFurnitureWIP = @NoFurnitureWIP;
    `;
    await rqUpdate.query(updateSql);

    // ===============================
    // 4) Mapping update (optional)
    // ===============================
    let outputType = null;
    let mappingTable = null;

    if (hasOutputCodeField) {
      if (!outputCode) {
        await deleteAllMappings(tx, noFurnitureWip);
      } else {
        if (outputCode.startsWith("BH.")) {
          outputType = "HOTSTAMPING";
          mappingTable = "HotStampingOutputLabelFWIP";
        } else if (outputCode.startsWith("BI.")) {
          outputType = "PASANG_KUNCI";
          mappingTable = "PasangKunciOutputLabelFWIP";
        } else if (outputCode.startsWith("BG.")) {
          outputType = "BONGKAR_SUSUN";
          mappingTable = "BongkarSusunOutputFurnitureWIP";
        } else if (outputCode.startsWith("L.")) {
          outputType = "RETUR";
          mappingTable = "BJReturFurnitureWIP_d";
        } else if (outputCode.startsWith("BJ.")) {
          outputType = "SPANNER";
          mappingTable = "SpannerOutputLabelFWIP";
        } else if (outputCode.startsWith("S.")) {
          outputType = "INJECT";
          mappingTable = "InjectProduksiOutputFurnitureWIP";
        } else
          throw badReq(
            "outputCode prefix not recognized (supported: BH., BI., BG., L., BJ., S.)",
          );

        await deleteAllMappings(tx, noFurnitureWip);

        const rqMap = new sql.Request(tx)
          .input("OutputCode", sql.VarChar(50), outputCode)
          .input("NoFurnitureWIP", sql.VarChar(50), noFurnitureWip);

        if (mappingTable === "HotStampingOutputLabelFWIP") {
          await rqMap.query(`
            INSERT INTO dbo.HotStampingOutputLabelFWIP (NoProduksi, NoFurnitureWIP)
            VALUES (@OutputCode, @NoFurnitureWIP);
          `);
        } else if (mappingTable === "PasangKunciOutputLabelFWIP") {
          await rqMap.query(`
            INSERT INTO dbo.PasangKunciOutputLabelFWIP (NoProduksi, NoFurnitureWIP)
            VALUES (@OutputCode, @NoFurnitureWIP);
          `);
        } else if (mappingTable === "BongkarSusunOutputFurnitureWIP") {
          await rqMap.query(`
            INSERT INTO dbo.BongkarSusunOutputFurnitureWIP (NoBongkarSusun, NoFurnitureWIP)
            VALUES (@OutputCode, @NoFurnitureWIP);
          `);
        } else if (mappingTable === "BJReturFurnitureWIP_d") {
          await rqMap.query(`
            INSERT INTO dbo.BJReturFurnitureWIP_d (NoRetur, NoFurnitureWIP)
            VALUES (@OutputCode, @NoFurnitureWIP);
          `);
        } else if (mappingTable === "SpannerOutputLabelFWIP") {
          await rqMap.query(`
            INSERT INTO dbo.SpannerOutputLabelFWIP (NoProduksi, NoFurnitureWIP)
            VALUES (@OutputCode, @NoFurnitureWIP);
          `);
        } else if (mappingTable === "InjectProduksiOutputFurnitureWIP") {
          await rqMap.query(`
            INSERT INTO dbo.InjectProduksiOutputFurnitureWIP (NoProduksi, NoFurnitureWIP)
            VALUES (@OutputCode, @NoFurnitureWIP);
          `);
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoFurnitureWIP: noFurnitureWip,
        DateCreate: hasOwn(header, "DateCreate")
          ? dateCreateParam
            ? formatYMD(dateCreateParam)
            : null
          : formatYMD(current.DateCreate),
        Jam: merged.Jam ?? null,
        Pcs: merged.Pcs ?? null,
        IDFurnitureWIP: merged.IDFurnitureWIP,
        Berat: merged.Berat ?? null,
        IsPartial: merged.IsPartial ?? 0,
        IdWarehouse: merged.IdWarehouse,
        IdWarna: merged.IdWarna ?? null,
        CreateBy: merged.CreateBy ?? null,
        Blok: merged.Blok ?? null,
        IdLokasi: merged.IdLokasi ?? null,
      },
      output: hasOutputCodeField
        ? { code: outputCode || null, type: outputType, mappingTable }
        : undefined,
      audit: { actorId, requestId },
    };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw err;
  }
};

exports.deleteFurnitureWip = async (noFurnitureWip, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoFurnitureWIP = String(noFurnitureWip || "").trim();

  if (!NoFurnitureWIP) throw badReq("NoFurnitureWIP is required");

  // =========================
  // [AUDIT] actorId + requestId
  // =========================
  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =========================
    // [AUDIT CTX] set session context for triggers
    // =========================
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 1) Lock header + ambil DateCreate + DateUsage (untuk guard)
    // ===============================
    const headRes = await new sql.Request(tx).input(
      "NoFurnitureWIP",
      sql.VarChar(50),
      NoFurnitureWIP,
    ).query(`
        SELECT TOP 1
          NoFurnitureWIP,
          CONVERT(date, DateCreate) AS DateCreate,
          DateUsage
        FROM dbo.FurnitureWIP WITH (UPDLOCK, HOLDLOCK)
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    if (!headRes.recordset.length) throw notFound("Furniture WIP not found");

    const head = headRes.recordset[0];

    // ===============================
    // 1b) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    const trxDate = head.DateCreate ? toDateOnly(head.DateCreate) : null;

    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: "delete furniture wip",
      useLock: true,
    });

    // ===============================
    // 1c) Guard: jika sudah dipakai, jangan boleh delete
    // ===============================
    if (head.DateUsage) {
      const err = conflict(
        "Cannot delete: FurnitureWIP already used (DateUsage IS NOT NULL).",
      );
      err.code = "FWIP_ALREADY_USED";
      throw err;
    }

    // ===============================
    // 2) Hapus semua mapping (BH/BI/BG/L/BJ/S)
    // ===============================
    await deleteAllMappings(tx, NoFurnitureWIP);

    // ===============================
    // 3) Hapus partial (kalau ada)
    // ===============================
    await new sql.Request(tx).input(
      "NoFurnitureWIP",
      sql.VarChar(50),
      NoFurnitureWIP,
    ).query(`
        DELETE FROM dbo.FurnitureWIPPartial
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    // ===============================
    // 4) Hapus header
    // ===============================
    const delHead = await new sql.Request(tx).input(
      "NoFurnitureWIP",
      sql.VarChar(50),
      NoFurnitureWIP,
    ).query(`
        DELETE FROM dbo.FurnitureWIP
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);

    if ((delHead.rowsAffected?.[0] ?? 0) === 0) {
      throw notFound("Furniture WIP not found");
    }

    await tx.commit();

    return {
      noFurnitureWip: NoFurnitureWIP,
      deleted: true,
      audit: { actorId, requestId },
    };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}

    // FK constraint
    if (err?.number === 547) {
      const e = conflict(
        err.message || "Delete failed due to foreign key constraint.",
      );
      e.original = err;
      throw e;
    }

    throw err;
  }
};

/**
 * Ambil info partial FurnitureWIP per NoFurnitureWIP.
 *
 * Tabel yang dipakai:
 * - dbo.FurnitureWIPPartial                      (Base partial, Pcs)
 * - dbo.InjectProduksiInputFurnitureWIPPartial   (konsumsi partial -> NoProduksi)
 * - dbo.InjectProduksi_h                         (header produksi inject)
 * - dbo.MstMesin                                 (nama mesin)
 */
exports.getPartialInfoByFurnitureWip = async (noFurnitureWip) => {
  const pool = await poolPromise;

  const req = pool
    .request()
    .input("NoFurnitureWIP", sql.VarChar, noFurnitureWip);

  const query = `
      ;WITH BasePartial AS (
        SELECT
          fwp.NoFurnitureWIPPartial,
          fwp.NoFurnitureWIP,
          fwp.Pcs
        FROM dbo.FurnitureWIPPartial fwp
        WHERE fwp.NoFurnitureWIP = @NoFurnitureWIP
      ),
      Consumed AS (
        SELECT
          ip.NoFurnitureWIPPartial,
          'INJECT' AS SourceType,
          ip.NoProduksi
        FROM dbo.InjectProduksiInputFurnitureWIPPartial ip
      )
      SELECT
        bp.NoFurnitureWIPPartial,
        bp.NoFurnitureWIP,
        bp.Pcs,                  -- partial pcs
  
        c.SourceType,            -- 'INJECT' / NULL
        c.NoProduksi,
  
        iph.TglProduksi,
        iph.IdMesin,
        iph.IdOperator,
        iph.Jam,
        iph.Shift,
  
        mm.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoFurnitureWIPPartial = bp.NoFurnitureWIPPartial
  
      LEFT JOIN dbo.InjectProduksi_h iph
        ON iph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MstMesin mm
        ON mm.IdMesin = iph.IdMesin
  
      ORDER BY
        bp.NoFurnitureWIPPartial ASC,
        c.NoProduksi ASC;
    `;

  const result = await req.query(query);

  // total partial pcs (unique per NoFurnitureWIPPartial)
  const seen = new Set();
  let totalPartialPcs = 0;

  for (const row of result.recordset) {
    const key = row.NoFurnitureWIPPartial;
    if (!seen.has(key)) {
      seen.add(key);
      const pcs = typeof row.Pcs === "number" ? row.Pcs : Number(row.Pcs) || 0;
      totalPartialPcs += pcs;
    }
  }

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? "0" + n : "" + n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = result.recordset.map((r) => ({
    NoFurnitureWIPPartial: r.NoFurnitureWIPPartial,
    NoFurnitureWIP: r.NoFurnitureWIP,
    Pcs: r.Pcs,

    SourceType: r.SourceType || null, // 'INJECT' | null
    NoProduksi: r.NoProduksi || null,

    TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
    IdMesin: r.IdMesin || null,
    NamaMesin: r.NamaMesin || null,
    IdOperator: r.IdOperator || null,
    Jam: r.Jam || null,
    Shift: r.Shift || null,
  }));

  return { totalPartialPcs, rows };
};

exports.incrementHasBeenPrinted = async (payload) => {
  const NoFurnitureWIP = String(payload?.NoFurnitureWIP || "").trim();
  if (!NoFurnitureWIP) throw badReq("NoFurnitureWIP wajib diisi");

  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  if (!actorId) {
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );
  }

  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const rs = await new sql.Request(tx).input(
      "NoFurnitureWIP",
      sql.VarChar(50),
      NoFurnitureWIP,
    ).query(`
        DECLARE @out TABLE (
          NoFurnitureWIP varchar(50),
          HasBeenPrinted int
        );

        UPDATE dbo.FurnitureWIP
        SET HasBeenPrinted = ISNULL(HasBeenPrinted, 0) + 1
        OUTPUT
          INSERTED.NoFurnitureWIP,
          INSERTED.HasBeenPrinted
        INTO @out
        WHERE NoFurnitureWIP = @NoFurnitureWIP;

        SELECT NoFurnitureWIP, HasBeenPrinted
        FROM @out;
      `);

    const row = rs.recordset?.[0] || null;
    if (!row) {
      const e = new Error(`NoFurnitureWIP ${NoFurnitureWIP} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoFurnitureWIP: row.NoFurnitureWIP,
      HasBeenPrinted: row.HasBeenPrinted,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

//   // === hanya untuk unit test ===
// exports._test = {
//     padLeft,
//     generateNextNoFurnitureWip,
//   };

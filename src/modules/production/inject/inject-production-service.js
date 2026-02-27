// services/inject-production-service.js
const { sql, poolPromise } = require("../../../core/config/db");

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,
  formatYMD,
  loadDocDateOnlyFromConfig,
} = require("../../../core/shared/tutup-transaksi-guard");
const sharedInputService = require("../../../core/shared/produksi-input.service");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { applyAuditContext } = require("../../../core/utils/db-audit-context");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");

// ============================================================
// ✅ GET ALL (paged + search + lastClosed + isLocked)
// ============================================================
async function getAllProduksi(page = 1, pageSize = 20, search = "") {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || "").trim();

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
  countReq.input("search", sql.VarChar(100), searchTerm);

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
  dataReq.input("search", sql.VarChar(100), searchTerm);
  dataReq.input("offset", sql.Int, offset);
  dataReq.input("limit", sql.Int, ps);

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

  request.input("date", sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

// ============================================================
// 🔹 FurnitureWIP kandidat dari header
// ============================================================
async function getFurnitureWipListByNoProduksi(noProduksi) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input("noProduksi", sql.VarChar(50), noProduksi);

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

  request.input("noProduksi", sql.VarChar(50), noProduksi);

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

async function createInjectProduksi(payload, ctx) {
  const body = payload && typeof payload === "object" ? payload : {};

  // ===============================
  // Validasi wajib
  // ===============================
  const must = [];
  if (!body?.tglProduksi) must.push("tglProduksi");
  if (body?.idMesin == null) must.push("idMesin");
  if (body?.idOperator == null) must.push("idOperator");
  if (body?.shift == null) must.push("shift");
  if (!body?.hourStart) must.push("hourStart");
  if (!body?.hourEnd) must.push("hourEnd");
  if (must.length) throw badReq(`Field wajib: ${must.join(", ")}`);

  const docDateOnly = toDateOnly(body.tglProduksi);

  // ===============================
  // Validasi ctx / audit
  // ===============================
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
  }
  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim();
  const auditCtx = {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  };

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // ===============================
    // Set audit context
    // ===============================
    const auditReq = new sql.Request(tx);
    const audit = await applyAuditContext(auditReq, auditCtx);

    // ===============================
    // Lock tanggal produksi
    // ===============================
    const effectiveDate = resolveEffectiveDateForCreate(body.tglProduksi);
    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: "create InjectProduksi",
      useLock: true,
    });

    // ===============================
    // Generate NoProduksi unik
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.InjectProduksi_h",
        columnName: "NoProduksi",
        prefix: "S.",
        width: 10,
      });
    let noProduksi = await gen();

    // double check anti-race
    const exist = await new sql.Request(tx)
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .query(
        `SELECT 1 FROM dbo.InjectProduksi_h WITH (UPDLOCK, HOLDLOCK) WHERE NoProduksi=@NoProduksi`,
      );
    if (exist.recordset.length > 0) {
      noProduksi = await gen();
    }

    // ===============================
    // Insert header dengan OUTPUT INTO table variable
    // ===============================
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("TglProduksi", sql.Date, effectiveDate)
      .input("IdMesin", sql.Int, body.idMesin)
      .input("IdOperator", sql.Int, body.idOperator)
      .input("Shift", sql.Int, body.shift)
      .input("Jam", sql.Int, body.jam ?? null)
      .input("CreateBy", sql.VarChar(100), body.createBy)
      .input("CheckBy1", sql.VarChar(100), body.checkBy1 ?? null)
      .input("CheckBy2", sql.VarChar(100), body.checkBy2 ?? null)
      .input("ApproveBy", sql.VarChar(100), body.approveBy ?? null)
      .input("JmlhAnggota", sql.Int, body.jmlhAnggota ?? null)
      .input("Hadir", sql.Int, body.hadir ?? null)
      .input("IdCetakan", sql.Int, body.idCetakan ?? null)
      .input("IdWarna", sql.Int, body.idWarna ?? null)
      .input("EnableOffset", sql.Bit, body.enableOffset ?? 0)
      .input("OffsetCurrent", sql.Int, body.offsetCurrent ?? null)
      .input("OffsetNext", sql.Int, body.offsetNext ?? null)
      .input("IdFurnitureMaterial", sql.Int, body.idFurnitureMaterial ?? null)
      .input("HourMeter", sql.Decimal(18, 2), body.hourMeter ?? null)
      .input(
        "BeratProdukHasilTimbang",
        sql.Decimal(18, 2),
        body.beratProdukHasilTimbang ?? null,
      )
      .input("HourStart", sql.VarChar(20), body.hourStart)
      .input("HourEnd", sql.VarChar(20), body.hourEnd);

    const insertSql = `
      DECLARE @tmp TABLE (
        NoProduksi varchar(50), IdOperator int, IdMesin int, TglProduksi date,
        Jam int, Shift int, CreateBy varchar(100), CheckBy1 varchar(100),
        CheckBy2 varchar(100), ApproveBy varchar(100), JmlhAnggota int,
        Hadir int, IdCetakan int, IdWarna int, EnableOffset bit,
        OffsetCurrent int, OffsetNext int, IdFurnitureMaterial int,
        HourMeter decimal(18,2), BeratProdukHasilTimbang decimal(18,2),
        HourStart time(7), HourEnd time(7)
      );

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
      OUTPUT INSERTED.* INTO @tmp
      VALUES (
        @NoProduksi, @IdOperator, @IdMesin, @TglProduksi,
        @Jam, @Shift,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy,
        @JmlhAnggota, @Hadir,
        @IdCetakan, @IdWarna,
        @EnableOffset, @OffsetCurrent, @OffsetNext,
        @IdFurnitureMaterial,
        @HourMeter, @BeratProdukHasilTimbang,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd   IS NULL OR LTRIM(RTRIM(@HourEnd))   = '' THEN NULL ELSE CAST(@HourEnd   AS time(7)) END
      );

      SELECT * FROM @tmp;
    `;

    const insRes = await rqIns.query(insertSql);

    await tx.commit();

    return { header: insRes.recordset?.[0] || null, audit };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

// ============================================================
// ✅ UPDATE header InjectProduksi_h + sync DateUsage (inputs)
// ============================================================
async function updateInjectProduksi(noProduksi, payload, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq("ctx.actorId wajib. Controller harus inject dari token.");
  }

  const actorUsername = String(ctx?.actorUsername || "").trim() || "system";
  const requestId = String(ctx?.requestId || "").trim();

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
    // 0) load old doc date + lock
    // =====================================================
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "injectProduksi",
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // =====================================================
    // 1) handle date change
    // =====================================================
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;
    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq("tglProduksi tidak boleh kosong");
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // =====================================================
    // 2) guard tutup transaksi
    // =====================================================
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: "update InjectProduksi (current date)",
      useLock: true,
    });
    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update InjectProduksi (new date)",
        useLock: true,
      });
    }

    // =====================================================
    // 3) SET fields dynamically
    // =====================================================
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

    if (payload.jam !== undefined) {
      sets.push("Jam = @Jam");
      rqUpd.input("Jam", sql.Int, payload.jam ?? null);
    }

    if (payload.jmlhAnggota !== undefined) {
      sets.push("JmlhAnggota = @JmlhAnggota");
      rqUpd.input("JmlhAnggota", sql.Int, payload.jmlhAnggota ?? null);
    }

    if (payload.hadir !== undefined) {
      sets.push("Hadir = @Hadir");
      rqUpd.input("Hadir", sql.Int, payload.hadir ?? null);
    }

    if (payload.idCetakan !== undefined) {
      sets.push("IdCetakan = @IdCetakan");
      rqUpd.input("IdCetakan", sql.Int, payload.idCetakan ?? null);
    }

    if (payload.idWarna !== undefined) {
      sets.push("IdWarna = @IdWarna");
      rqUpd.input("IdWarna", sql.Int, payload.idWarna ?? null);
    }

    if (payload.enableOffset !== undefined) {
      sets.push("EnableOffset = @EnableOffset");
      rqUpd.input("EnableOffset", sql.Bit, payload.enableOffset ?? null);
    }

    if (payload.offsetCurrent !== undefined) {
      sets.push("OffsetCurrent = @OffsetCurrent");
      rqUpd.input("OffsetCurrent", sql.Int, payload.offsetCurrent ?? null);
    }

    if (payload.offsetNext !== undefined) {
      sets.push("OffsetNext = @OffsetNext");
      rqUpd.input("OffsetNext", sql.Int, payload.offsetNext ?? null);
    }

    if (payload.idFurnitureMaterial !== undefined) {
      sets.push("IdFurnitureMaterial = @IdFurnitureMaterial");
      rqUpd.input(
        "IdFurnitureMaterial",
        sql.Int,
        payload.idFurnitureMaterial ?? null,
      );
    }

    if (payload.hourMeter !== undefined) {
      sets.push("HourMeter = @HourMeter");
      rqUpd.input("HourMeter", sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    if (payload.beratProdukHasilTimbang !== undefined) {
      sets.push("BeratProdukHasilTimbang = @BeratProdukHasilTimbang");
      rqUpd.input(
        "BeratProdukHasilTimbang",
        sql.Decimal(18, 2),
        payload.beratProdukHasilTimbang ?? null,
      );
    }

    if (payload.hourStart !== undefined) {
      sets.push(
        `HourStart = CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END`,
      );
      rqUpd.input("HourStart", sql.VarChar(20), payload.hourStart);
    }

    if (payload.hourEnd !== undefined) {
      sets.push(
        `HourEnd = CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL ELSE CAST(@HourEnd AS time(7)) END`,
      );
      rqUpd.input("HourEnd", sql.VarChar(20), payload.hourEnd);
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

    if (sets.length === 0) throw badReq("No fields to update");

    rqUpd.input("NoProduksi", sql.VarChar(50), noProduksi);

    const updateSql = `
      UPDATE dbo.InjectProduksi_h
      SET ${sets.join(", ")}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.InjectProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    // =====================================================
    // 4) apply audit context
    // =====================================================
    await applyAuditContext(rqUpd, auditCtx);

    // =====================================================
    // 5) execute update
    // =====================================================
    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // =====================================================
    // 6) jika tanggal berubah -> sync DateUsage
    // =====================================================
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(
        updatedHeader.TglProduksi,
      );
      const rqUsage = new sql.Request(tx);
      rqUsage
        .input("NoProduksi", sql.VarChar(50), noProduksi)
        .input("Tanggal", sql.Date, usageDate);

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
    return { header: updatedHeader, audit: auditCtx };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

// ============================================================
// ✅ DELETE header InjectProduksi_h + delete inputs + reset DateUsage
// ============================================================
async function deleteInjectProduksi(noProduksi, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) SET SESSION_CONTEXT (untuk trigger audit)
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

    // =====================================================
    // 1) Ambil docDateOnly dari config (LOCK HEADER)
    // =====================================================
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "injectProduksi",
      codeValue: noProduksi,
      runner: tx,
      useLock: true, // DELETE = write lock
      throwIfNotFound: true,
    });

    // =====================================================
    // 2) Guard tutup transaksi
    // =====================================================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "delete InjectProduksi",
      useLock: true,
    });

    // =====================================================
    // 3) Cek output
    // =====================================================
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut.input("NoProduksi", sql.VarChar(50), noProduksi)
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
          SELECT 'MIXER', COUNT(1)
          FROM dbo.InjectProduksiOutputMixer WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
          UNION ALL
          SELECT 'REJECT', COUNT(1)
          FROM dbo.InjectProduksiOutputRejectV2 WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
          UNION ALL
          SELECT 'FWIP', COUNT(1)
          FROM dbo.InjectProduksiOutputFurnitureWIP WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
          UNION ALL
          SELECT 'BJ', COUNT(1)
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
      throw badReq(
        "Tidak dapat menghapus Nomor Produksi ini karena sudah memiliki data output.",
      );
    }

    // =====================================================
    // 4) DELETE INPUT (FULL + PARTIAL) + RESET DATEUSAGE
    // =====================================================
    const rq = new sql.Request(tx);
    rq.input("NoProduksi", sql.VarChar(50), noProduksi);

    const sqlDelete = `
      SET NOCOUNT ON;

      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK KEY TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BrokerKeys TABLE (NoBroker varchar(50), NoSak int);
      DECLARE @MixerKeys TABLE (NoMixer varchar(50), NoSak int);
      DECLARE @GilinganKeys TABLE (NoGilingan varchar(50));
      DECLARE @FWIPKeys TABLE (NoFurnitureWIP varchar(50));

      ---------------------------------------------------------
      -- INPUT FULL
      ---------------------------------------------------------
      INSERT INTO @BrokerKeys (NoBroker, NoSak)
      SELECT DISTINCT NoBroker, NoSak
      FROM dbo.InjectProduksiInputBroker
      WHERE NoProduksi = @NoProduksi;

      INSERT INTO @MixerKeys (NoMixer, NoSak)
      SELECT DISTINCT NoMixer, NoSak
      FROM dbo.InjectProduksiInputMixer
      WHERE NoProduksi = @NoProduksi;

      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT NoGilingan
      FROM dbo.InjectProduksiInputGilingan
      WHERE NoProduksi = @NoProduksi;

      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT NoFurnitureWIP
      FROM dbo.InjectProduksiInputFurnitureWIP
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- INPUT PARTIAL
      ---------------------------------------------------------
      INSERT INTO @MixerKeys (NoMixer, NoSak)
      SELECT DISTINCT mpd.NoMixer, mpd.NoSak
      FROM dbo.InjectProduksiInputMixerPartial mp
      JOIN dbo.MixerPartial mpd
        ON mp.NoMixerPartial = mpd.NoMixerPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND NOT EXISTS (
          SELECT 1 FROM @MixerKeys k
          WHERE k.NoMixer = mpd.NoMixer AND k.NoSak = mpd.NoSak
        );

      INSERT INTO @GilinganKeys (NoGilingan)
      SELECT DISTINCT gp.NoGilingan
      FROM dbo.InjectProduksiInputGilinganPartial mp
      JOIN dbo.GilinganPartial gp
        ON gp.NoGilinganPartial = mp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND NOT EXISTS (
          SELECT 1 FROM @GilinganKeys k
          WHERE k.NoGilingan = gp.NoGilingan
        );

      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.InjectProduksiInputFurnitureWIPPartial mp
      JOIN dbo.FurnitureWIPPartial fwp
        ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND NOT EXISTS (
          SELECT 1 FROM @FWIPKeys k
          WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP
        );

      ---------------------------------------------------------
      -- HAPUS PARTIAL
      ---------------------------------------------------------
      DELETE mpd
      FROM dbo.MixerPartial mpd
      JOIN dbo.InjectProduksiInputMixerPartial mp
        ON mp.NoMixerPartial = mpd.NoMixerPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE gp
      FROM dbo.GilinganPartial gp
      JOIN dbo.InjectProduksiInputGilinganPartial mp
        ON mp.NoGilinganPartial = gp.NoGilinganPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE fwp
      FROM dbo.FurnitureWIPPartial fwp
      JOIN dbo.InjectProduksiInputFurnitureWIPPartial mp
        ON mp.NoFurnitureWIPPartial = fwp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- HAPUS MAPPING INPUT FULL & PARTIAL
      ---------------------------------------------------------
      DELETE FROM dbo.InjectProduksiInputMixerPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.InjectProduksiInputGilinganPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.InjectProduksiInputFurnitureWIPPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.InjectProduksiInputBroker WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.InjectProduksiInputMixer WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.InjectProduksiInputGilingan WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.InjectProduksiInputFurnitureWIP WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- RESET DATEUSAGE
      ---------------------------------------------------------
      UPDATE b
      SET b.DateUsage = NULL
      FROM dbo.Broker_d b
      JOIN @BrokerKeys k ON k.NoBroker = b.NoBroker AND k.NoSak = b.NoSak;

      UPDATE m
      SET m.DateUsage = NULL
      FROM dbo.Mixer_d m
      JOIN @MixerKeys k ON k.NoMixer = m.NoMixer AND k.NoSak = m.NoSak;

      UPDATE g
      SET g.DateUsage = NULL
      FROM dbo.Gilingan g
      JOIN @GilinganKeys k ON k.NoGilingan = g.NoGilingan;

      UPDATE fw
      SET fw.DateUsage = NULL,
          fw.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.FurnitureWIPPartial p
              WHERE p.NoFurnitureWIP = fw.NoFurnitureWIP
            ) THEN 1 ELSE 0 END
      FROM dbo.FurnitureWIP fw
      JOIN @FWIPKeys k ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      ---------------------------------------------------------
      -- DELETE HEADER
      ---------------------------------------------------------
      DELETE FROM dbo.InjectProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const res = await rq.query(sqlDelete);

    // throw notFound jika header tidak ada
    if (res.rowsAffected?.[res.rowsAffected.length - 1] === 0) {
      throw notFound(`NoProduksi tidak ditemukan: ${noProduksi}`);
    }

    await tx.commit();
    return { success: true, noProduksi };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

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

  const mainRows = rs.recordsets?.[0] || [];
  const fwipPart = rs.recordsets?.[1] || [];
  const brokerPart = rs.recordsets?.[2] || [];
  const mixerPart = rs.recordsets?.[3] || [];
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
      case "fwip":
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          pcs: r.Pcs ?? null,
          ...base,
        });
        break;

      case "broker":
        out.broker.push({
          noBroker: r.Ref1,
          noSak: r.Ref2,
          ...base,
        });
        break;

      case "mixer":
        out.mixer.push({
          noMixer: r.Ref1,
          noSak: r.Ref2,
          ...base,
        });
        break;

      case "gilingan":
        out.gilingan.push({
          noGilingan: r.Ref1,
          ...base,
        });
        break;

      case "material":
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1, // kalau mau int: Number(r.Ref1)
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

async function fetchOutputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT DISTINCT
      o.NoProduksi,
      o.NoMixer
    FROM dbo.InjectProduksiOutputMixer o WITH (NOLOCK)
    WHERE o.NoProduksi = @no
    ORDER BY o.NoMixer DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
}

async function fetchOutputsBonggolan(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT DISTINCT
      o.NoProduksi,
      o.NoBonggolan,
      ISNULL(b.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.InjectProduksiOutputBonggolan o WITH (NOLOCK)
    LEFT JOIN dbo.Bonggolan b WITH (NOLOCK)
      ON b.NoBonggolan = o.NoBonggolan
    WHERE o.NoProduksi = @no
    ORDER BY o.NoBonggolan DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
}

async function fetchOutputsFurnitureWip(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT DISTINCT
      o.NoProduksi,
      o.NoFurnitureWIP,
      ISNULL(fw.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.InjectProduksiOutputFurnitureWIP o WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = o.NoFurnitureWIP
    WHERE o.NoProduksi = @no
    ORDER BY o.NoFurnitureWIP DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
}

async function fetchOutputsPacking(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT DISTINCT
      o.NoProduksi,
      o.NoBJ,
      ISNULL(bj.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.InjectProduksiOutputBarangJadi o WITH (NOLOCK)
    LEFT JOIN dbo.BarangJadi bj WITH (NOLOCK)
      ON bj.NoBJ = o.NoBJ
    WHERE o.NoProduksi = @no
    ORDER BY o.NoBJ DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
}

async function fetchOutputsReject(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    SELECT DISTINCT
      o.NoProduksi,
      o.NoReject,
      ISNULL(rj.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.InjectProduksiOutputRejectV2 o WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj WITH (NOLOCK)
      ON rj.NoReject = o.NoReject
    WHERE o.NoProduksi = @no
    ORDER BY o.NoReject DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
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
    if (val && typeof val === "object") {
      const o = {};
      for (const [k, v] of Object.entries(val)) o[toCamel(k)] = camelize(v);
      return o;
    }
    return val;
  };

  // ---------- normalize label ----------
  const raw = String(labelCode || "").trim();
  if (!raw) throw new Error("Label code is required");

  // prefix rule: untuk BF. (3 char), lainnya 2 char (mis: D., H., V., BB.)
  let prefix = "";
  if (raw.substring(0, 3).toUpperCase() === "BF.") prefix = "BF.";
  else if (raw.substring(0, 3).toUpperCase() === "BB.")
    prefix = "BB."; // ✅ FWIP (umum)
  else prefix = raw.substring(0, 2).toUpperCase();

  let query = "";
  let tableName = "";

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
    // =========================================================
    // BB. = FurnitureWIP / FurnitureWIPPartial (Inject pakai ini juga)
    // =========================================================
    case "BB.": {
      // 1) coba FULL dulu: FurnitureWIP.NoFurnitureWIP (DateUsage IS NULL)
      {
        tableName = "FurnitureWIP";
        query = `
        ;WITH PartialSum AS (
          SELECT
            fwp.NoFurnitureWIP,
            SUM(ISNULL(fwp.Pcs, 0)) AS PcsPartial
          FROM dbo.FurnitureWIPPartial AS fwp WITH (NOLOCK)
          GROUP BY fwp.NoFurnitureWIP
        )
        SELECT
          fw.NoFurnitureWIP,
          fw.DateCreate,
          fw.Jam,
          CAST(fw.Pcs - ISNULL(ps.PcsPartial, 0) AS int) AS Pcs,          -- ✅ sisa pcs
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
          fw.IdLokasi,
          ISNULL(ps.PcsPartial, 0) AS PcsPartial                          -- (opsional) buat debug
        FROM dbo.FurnitureWIP fw WITH (NOLOCK)
        LEFT JOIN PartialSum ps
          ON ps.NoFurnitureWIP = fw.NoFurnitureWIP
        LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
          ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
        WHERE fw.NoFurnitureWIP = @labelCode
          AND fw.DateUsage IS NULL
          AND (fw.Pcs - ISNULL(ps.PcsPartial, 0)) > 0                     -- ✅ masih ada sisa
        ;
        `;
        const full = await run(raw);
        if (full.found) return full;
      }

      // 2) kalau tidak ketemu full, coba PARTIAL: FurnitureWIPPartial.NoFurnitureWIPPartial
      {
        tableName = "FurnitureWIPPartial";
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
    case "D.": {
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
    case "H.": {
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
    case "V.": {
      tableName = "Gilingan";
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
        `Invalid prefix: ${prefix}. Valid prefixes (Inject): BB., D., H., V.`,
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
/**
 * ✅ Upsert inputs & partials untuk Inject Production dengan audit context
 * Support: broker, mixer, gilingan, furnitureWip, cabinetMaterial (UPSERT)
 * Support: partials (existing + new)
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

  // ✅ forward ctx yang sudah dinormalisasi ke shared service
  return sharedInputService.upsertInputsAndPartials(
    "injectProduksi",
    no,
    body,
    {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    },
  );
}

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
    "injectProduksi",
    no,
    body,
    {
      actorId: Math.trunc(actorIdNum),
      actorUsername,
      requestId,
    },
  );
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
  fetchOutputs,
  fetchOutputsBonggolan,
  fetchOutputsFurnitureWip,
  fetchOutputsPacking,
  fetchOutputsReject,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

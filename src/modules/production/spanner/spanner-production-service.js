// services/spanner-production-service.js
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
const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require("../../../core/utils/jam-kerja-helper");

// =====================================================
// GET ALL (paged + search)
// =====================================================
async function getAllProduksi(page = 1, pageSize = 20, search = "") {
  const pool = await poolPromise;

  const offset = (Math.max(page, 1) - 1) * Math.max(pageSize, 1);
  const s = String(search || "").trim();

  const rqCount = pool.request();
  const rqData = pool.request();

  rqCount.input("search", sql.VarChar(50), s);

  rqData.input("search", sql.VarChar(50), s);
  rqData.input("offset", sql.Int, offset);
  rqData.input("pageSize", sql.Int, pageSize);

  const qCount = `
    SELECT COUNT(1) AS Total
    FROM dbo.Spanner_h h WITH (NOLOCK)
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%');
  `;

  const qData = `
    SELECT
      h.NoProduksi,
      h.Tanggal,
      h.IdMesin,
      m.NamaMesin,
      h.IdOperator,
      o.NamaOperator,
      h.Shift,
      h.JamKerja,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter,
      h.HourStart,
      h.HourEnd
    FROM dbo.Spanner_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin m WITH (NOLOCK)
      ON h.IdMesin = m.IdMesin
    LEFT JOIN dbo.MstOperator o WITH (NOLOCK)
      ON h.IdOperator = o.IdOperator
    WHERE (@search = '' OR h.NoProduksi LIKE '%' + @search + '%')
    ORDER BY h.Tanggal DESC, h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY;
  `;

  const countRes = await rqCount.query(qCount);
  const total = countRes.recordset?.[0]?.Total ?? 0;

  const dataRes = await rqData.query(qData);
  const data = dataRes.recordset || [];

  return { data, total };
}

// =====================================================
// GET BY DATE
// =====================================================
async function getProductionByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      h.NoProduksi,
      h.Tanggal,
      h.IdMesin,
      m.NamaMesin,
      h.IdOperator,
      o.NamaOperator,
      h.Shift,
      h.JamKerja,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter,
      h.HourStart,
      h.HourEnd
    FROM dbo.Spanner_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin m WITH (NOLOCK)
      ON h.IdMesin = m.IdMesin
    LEFT JOIN dbo.MstOperator o WITH (NOLOCK)
      ON h.IdOperator = o.IdOperator
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.JamKerja ASC;
  `;

  request.input("date", sql.Date, date);
  const result = await request.query(query);
  return result.recordset || [];
}

// =====================================================
// CREATE Spanner_h
// =====================================================
async function createSpannerProduksi(payload, ctx) {
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
    // Set audit context (SESSION_CONTEXT)
    // ===============================
    const auditReq = new sql.Request(tx);
    const audit = await applyAuditContext(auditReq, auditCtx);

    // ===============================
    // Normalize date + guard tutup transaksi
    // ===============================
    const effectiveDate = resolveEffectiveDateForCreate(body.tglProduksi);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: "create Spanner",
      useLock: true,
    });

    // ===============================
    // Generate NoProduksi
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.Spanner_h",
        columnName: "NoProduksi",
        prefix: "BJ.",
        width: 10,
      });

    let noProduksi = await gen();

    // anti-race double check
    const exist = await new sql.Request(tx).input(
      "NoProduksi",
      sql.VarChar(50),
      noProduksi,
    ).query(`
        SELECT 1
        FROM dbo.Spanner_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    if (exist.recordset.length > 0) {
      noProduksi = await gen();
    }

    // ===============================
    // JamKerja
    // ===============================
    let jamKerjaInt;
    if (
      body.jamKerja !== null &&
      body.jamKerja !== undefined &&
      body.jamKerja !== ""
    ) {
      jamKerjaInt = parseJamToInt(body.jamKerja);
    } else {
      jamKerjaInt = calcJamKerjaFromStartEnd(body.hourStart, body.hourEnd);
    }

    // ===============================
    // INSERT header (TANPA OUTPUT)
    // ===============================
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("Tanggal", sql.Date, effectiveDate)
      .input("IdMesin", sql.Int, body.idMesin)
      .input("IdOperator", sql.Int, body.idOperator)
      .input("Shift", sql.Int, body.shift)
      .input("JamKerja", sql.Int, jamKerjaInt)
      .input("CreateBy", sql.VarChar(100), body.createBy ?? null)
      .input("CheckBy1", sql.VarChar(100), body.checkBy1 ?? null)
      .input("CheckBy2", sql.VarChar(100), body.checkBy2 ?? null)
      .input("ApproveBy", sql.VarChar(100), body.approveBy ?? null)
      .input("HourMeter", sql.Decimal(18, 2), body.hourMeter ?? null)
      .input("HourStart", sql.VarChar(20), body.hourStart)
      .input("HourEnd", sql.VarChar(20), body.hourEnd);

    await rqIns.query(`
      INSERT INTO dbo.Spanner_h (
        NoProduksi, Tanggal, IdMesin, IdOperator, Shift, JamKerja,
        CreateBy, CheckBy1, CheckBy2, ApproveBy,
        HourMeter, HourStart, HourEnd
      )
      VALUES (
        @NoProduksi, @Tanggal, @IdMesin, @IdOperator, @Shift, @JamKerja,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @HourMeter,
        CASE 
          WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' 
          THEN NULL ELSE CAST(@HourStart AS time(7)) 
        END,
        CASE 
          WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' 
          THEN NULL ELSE CAST(@HourEnd AS time(7)) 
        END
      );
    `);

    // ===============================
    // SELECT ulang header (aman dgn trigger)
    // ===============================
    const headerRes = await new sql.Request(tx).input(
      "NoProduksi",
      sql.VarChar(50),
      noProduksi,
    ).query(`
        SELECT *
        FROM dbo.Spanner_h
        WHERE NoProduksi = @NoProduksi
      `);

    await tx.commit();

    return {
      header: headerRes.recordset?.[0] ?? null,
      audit,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

// =====================================================
// UPDATE Spanner_h (dynamic) + sync DateUsage jika Tanggal berubah
// =====================================================
async function updateSpannerProduksi(noProduksi, payload, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  // ===============================
  // Audit context
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
    // =====================================================
    // 0) Load old doc date + lock
    // =====================================================
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "spanner",
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // =====================================================
    // 1) Handle date change
    // =====================================================
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq("tglProduksi tidak boleh kosong");
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // =====================================================
    // 2) Guard tutup transaksi
    // =====================================================
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: "update Spanner (current date)",
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update Spanner (new date)",
        useLock: true,
      });
    }

    // =====================================================
    // 3) SET fields dynamically
    // =====================================================
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push("Tanggal = @Tanggal");
      rqUpd.input("Tanggal", sql.Date, newDocDateOnly);
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

    if (payload.jamKerja !== undefined) {
      const jamInt =
        payload.jamKerja === null ? null : parseJamToInt(payload.jamKerja);
      sets.push("JamKerja = @JamKerja");
      rqUpd.input("JamKerja", sql.Int, jamInt);
    }

    if (payload.hourMeter !== undefined) {
      sets.push("HourMeter = @HourMeter");
      rqUpd.input("HourMeter", sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    if (payload.hourStart !== undefined) {
      sets.push(
        `HourStart =
          CASE
            WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = ''
              THEN NULL
            ELSE CAST(@HourStart AS time(7))
          END`,
      );
      rqUpd.input("HourStart", sql.VarChar(20), payload.hourStart ?? null);
    }

    if (payload.hourEnd !== undefined) {
      sets.push(
        `HourEnd =
          CASE
            WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = ''
              THEN NULL
            ELSE CAST(@HourEnd AS time(7))
          END`,
      );
      rqUpd.input("HourEnd", sql.VarChar(20), payload.hourEnd ?? null);
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

    // =====================================================
    // 4) Apply audit context
    // =====================================================
    await applyAuditContext(rqUpd, auditCtx);

    // =====================================================
    // 5) Execute update
    // =====================================================
    const updateSql = `
      UPDATE dbo.Spanner_h
      SET ${sets.join(", ")}
      WHERE NoProduksi = @NoProduksi;

      SELECT * FROM dbo.Spanner_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // =====================================================
    // 6) Sync DateUsage jika tanggal berubah
    // =====================================================
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input("NoProduksi", sql.VarChar(50), noProduksi)
        .input("Tanggal", sql.Date, usageDate);

      const sqlUpdateUsage = `
        -- FULL
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.SpannerInputLabelFWIP AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoFurnitureWIP = fw.NoFurnitureWIP
          );

        -- PARTIAL
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.SpannerInputLabelFWIPPartial AS mp
            JOIN dbo.FurnitureWIPPartial AS fwp
              ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
            WHERE mp.NoProduksi = @NoProduksi
              AND fwp.NoFurnitureWIP = fw.NoFurnitureWIP
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
    // attach auditCtx agar controller tetap bisa kirim audit info walau error
    throw Object.assign(e, auditCtx);
  }
}

// =====================================================
// DELETE Spanner (cek output dulu) + reset DateUsage
// =====================================================
async function deleteSpannerProduksi(noProduksi, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  // ===============================
  // Audit context
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
    // =====================================================
    // 0) Load docDateOnly + lock header
    // =====================================================
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "spanner",
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // =====================================================
    // 1) Guard tutup transaksi
    // =====================================================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "delete Spanner",
      useLock: true,
    });

    // =====================================================
    // 2) Cek output (FWIP / Reject)
    // =====================================================
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut.input("NoProduksi", sql.VarChar(50), noProduksi)
      .query(`
        SELECT
          SUM(CASE WHEN Src = 'FWIP'   THEN Cnt ELSE 0 END) AS CntOutputFWIP,
          SUM(CASE WHEN Src = 'REJECT' THEN Cnt ELSE 0 END) AS CntOutputReject
        FROM (
          SELECT 'FWIP' AS Src, COUNT(1) AS Cnt
          FROM dbo.SpannerOutputLabelFWIP WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL

          SELECT 'REJECT' AS Src, COUNT(1) AS Cnt
          FROM dbo.SpannerOutputRejectV2 WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
        ) X;
      `);

    const row = outRes.recordset?.[0] || {
      CntOutputFWIP: 0,
      CntOutputReject: 0,
    };

    if ((row.CntOutputFWIP || 0) > 0 || (row.CntOutputReject || 0) > 0) {
      throw badReq(
        "Tidak dapat menghapus Nomor Produksi ini karena sudah memiliki data output.",
      );
    }

    // =====================================================
    // 3) Delete data (dengan audit context)
    // =====================================================
    const rqDel = new sql.Request(tx);
    rqDel.input("NoProduksi", sql.VarChar(50), noProduksi);

    // 🔐 inject audit ke session (trigger)
    await applyAuditContext(rqDel, auditCtx);

    const sqlDelete = `
      DECLARE @FWIPKeys TABLE (NoFurnitureWIP varchar(50) PRIMARY KEY);

      -- A) FULL mapping
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT map.NoFurnitureWIP
      FROM dbo.SpannerInputLabelFWIP AS map
      WHERE map.NoProduksi = @NoProduksi
        AND map.NoFurnitureWIP IS NOT NULL;

      -- B) PARTIAL mapping
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.SpannerInputLabelFWIPPartial AS mp
      JOIN dbo.FurnitureWIPPartial AS fwp
        ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND fwp.NoFurnitureWIP IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM @FWIPKeys k WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP
        );

      -- C) delete partial rows
      DELETE fwp
      FROM dbo.FurnitureWIPPartial AS fwp
      JOIN dbo.SpannerInputLabelFWIPPartial AS mp
        ON mp.NoFurnitureWIPPartial = fwp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi;

      -- D) delete mappings
      DELETE FROM dbo.SpannerInputLabelFWIPPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.SpannerInputLabelFWIP
      WHERE NoProduksi = @NoProduksi;

      -- E) reset DateUsage + IsPartial
      UPDATE fw
      SET fw.DateUsage = NULL,
          fw.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1 FROM dbo.FurnitureWIPPartial p
              WHERE p.NoFurnitureWIP = fw.NoFurnitureWIP
            ) THEN 1 ELSE 0 END
      FROM dbo.FurnitureWIP AS fw
      JOIN @FWIPKeys AS k
        ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      -- F) delete header last
      DELETE FROM dbo.Spanner_h
      WHERE NoProduksi = @NoProduksi;
    `;

    await rqDel.query(sqlDelete);

    await tx.commit();
    return { success: true, audit: auditCtx };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    // ⬅️ tempel audit ke error
    throw Object.assign(e, auditCtx);
  }
}

async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */

    -- FurnitureWIP FULL (BB...)
    SELECT
      'fwip' AS Src,
      map.NoProduksi,
      map.NoFurnitureWIP AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      fw.Berat,
      fw.Pcs,
      fw.IsPartial,
      fw.IDFurnitureWIP AS IdJenis,
      mw.Nama           AS NamaJenis,
      uom.NamaUOM       AS NamaUOM,
      CAST(NULL AS datetime) AS DatetimeInput
    FROM dbo.SpannerInputLabelFWIP map WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = map.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = fw.IDFurnitureWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE map.NoProduksi = @no

    UNION ALL

    -- Cabinet Material (Spanner)
    SELECT
      'material' AS Src,
      im.NoProduksi,
      CAST(im.IdCabinetMaterial AS varchar(50)) AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      CAST(NULL AS decimal(18,3)) AS Berat,
      CAST(im.Jumlah AS int)      AS Pcs,
      CAST(NULL AS bit)           AS IsPartial,
      CAST(NULL AS int)           AS IdJenis,
      mm.Nama                     AS NamaJenis,
      uom.NamaUOM                 AS NamaUOM,
      CAST(NULL AS datetime)      AS DatetimeInput
    FROM dbo.SpannerInputMaterial im WITH (NOLOCK)
    LEFT JOIN dbo.MstCabinetMaterial mm WITH (NOLOCK)
      ON mm.IdCabinetMaterial = im.IdCabinetMaterial
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mm.IdUOM
    WHERE im.NoProduksi = @no

    ORDER BY Ref1 DESC, Ref2 ASC;

    /* ===================== [2] PARTIALS ===================== */

    -- FurnitureWIP Partial (BC...)
    SELECT
      mp.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs                AS PcsPartial,
      fw.Pcs                 AS PcsHeader,
      fw.Berat,
      fw.IDFurnitureWIP      AS IdJenis,
      mw.Nama                AS NamaJenis,
      uom.NamaUOM            AS NamaUOM
    FROM dbo.SpannerInputLabelFWIPPartial mp WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
      ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = fw.IDFurnitureWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE mp.NoProduksi = @no
    ORDER BY mp.NoFurnitureWIPPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const fwipPartial = rs.recordsets?.[1] || [];

  // ✅ Shape tetap sama dengan keyfitting supaya Flutter gampang reuse
  const out = {
    furnitureWip: [],
    cabinetMaterial: [],
    summary: { furnitureWip: 0, cabinetMaterial: 0 },
  };

  // MAIN rows
  for (const r of mainRows) {
    const base = {
      berat: r.Berat ?? null,
      pcs: r.Pcs ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis: r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
      namaUom: r.NamaUOM ?? null,
      datetimeInput: r.DatetimeInput ?? null,
    };

    switch (r.Src) {
      case "fwip":
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          ...base,
        });
        break;

      case "material":
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1, // string cast (konsisten dgn keyfitting)
          jumlah: r.Pcs ?? null, // jumlah disimpan ke jumlah
          ...base,
        });
        break;
    }
  }

  // PARTIALS (merge ke bucket furnitureWip)
  for (const p of fwipPartial) {
    out.furnitureWip.push({
      noFurnitureWipPartial: p.NoFurnitureWIPPartial, // wajib
      noFurnitureWip: p.NoFurnitureWIP ?? null, // header
      pcs: p.PcsPartial ?? null, // pcs partial
      pcsHeader: p.PcsHeader ?? null, // optional
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
      namaUom: p.NamaUOM ?? null,
      isPartial: true,
      isPartialRow: true,
    });
  }

  // Summary
  out.summary.furnitureWip = out.furnitureWip.length;
  out.summary.cabinetMaterial = out.cabinetMaterial.length;

  return out;
}

/**
 * Payload shape (arrays optional):
 * {
 *   furnitureWip:           [{ noFurnitureWip }],
 *   cabinetMaterial:        [{ idCabinetMaterial, jumlah }],
 *   furnitureWipPartialNew: [{ noFurnitureWip, pcs }]
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

  // ✅ forward ctx yang sudah dinormalisasi ke shared service
  return sharedInputService.upsertInputsAndPartials("spanner", no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
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
  return sharedInputService.deleteInputsAndPartials("spanner", no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

module.exports = {
  getAllProduksi,
  getProductionByDate,
  createSpannerProduksi,
  updateSpannerProduksi,
  deleteSpannerProduksi,
  fetchInputs,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

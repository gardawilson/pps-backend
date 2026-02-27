// services/hotstamping-production-service.js
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
      o.NamaOperator,       -- sesuaikan kalau nama kolom beda
      h.Shift,
      h.JamKerja,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter
    FROM [dbo].[HotStamping_h] h
    LEFT JOIN [dbo].[MstMesin] m
      ON h.IdMesin = m.IdMesin
    LEFT JOIN [dbo].[MstOperator] o
      ON h.IdOperator = o.IdOperator
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.JamKerja ASC;
  `;

  request.input("date", sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

// ✅ GET ALL (paged + search + lastClosed + isLocked)
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
    FROM dbo.HotStamping_h h WITH (NOLOCK)
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
      h.Tanggal      AS TglProduksi,
      h.IdMesin,
      ms.NamaMesin,
      h.IdOperator,
      op.NamaOperator,
      h.JamKerja,
      h.Shift,
      h.CreateBy,
      h.CheckBy1,
      h.CheckBy2,
      h.ApproveBy,
      h.HourMeter,
      CONVERT(VARCHAR(8), h.HourStart, 108) AS HourStart,
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd,

      lc.LastClosedDate AS LastClosedDate,

      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.HotStamping_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    ORDER BY h.Tanggal DESC, h.JamKerja ASC, h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input("search", sql.VarChar(100), searchTerm);
  dataReq.input("offset", sql.Int, offset);
  dataReq.input("limit", sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

/**
 * CREATE header HotStamping_h
 * payload:
 *  tglProduksi, idMesin, idOperator, shift,
 *  jamKerja?, hourStart, hourEnd, hourMeter, createBy, checkBy*, approveBy*
 */
async function createHotStampingProduksi(payload, ctx) {
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
      action: "create HotStamping",
      useLock: true,
    });

    // ===============================
    // Generate NoProduksi unik
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.HotStamping_h",
        columnName: "NoProduksi",
        prefix: "BH.",
        width: 10,
      });
    let noProduksi = await gen();

    // double check anti-race
    const exist = await new sql.Request(tx)
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .query(
        `SELECT 1 FROM dbo.HotStamping_h WITH (UPDLOCK, HOLDLOCK) WHERE NoProduksi=@NoProduksi`,
      );
    if (exist.recordset.length > 0) {
      noProduksi = await gen();
    }

    // ===============================
    // JamKerja
    // ===============================
    let jamKerjaInt = null;
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
    // Insert header dengan OUTPUT
    // ===============================
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("Tanggal", sql.Date, effectiveDate)
      .input("IdMesin", sql.Int, body.idMesin)
      .input("IdOperator", sql.Int, body.idOperator)
      .input("Shift", sql.Int, body.shift)
      .input("JamKerja", sql.Int, jamKerjaInt)
      .input("CreateBy", sql.VarChar(100), body.createBy)
      .input("CheckBy1", sql.VarChar(100), body.checkBy1 ?? null)
      .input("CheckBy2", sql.VarChar(100), body.checkBy2 ?? null)
      .input("ApproveBy", sql.VarChar(100), body.approveBy ?? null)
      .input("HourMeter", sql.Decimal(18, 2), body.hourMeter ?? null)
      .input("HourStart", sql.VarChar(20), body.hourStart)
      .input("HourEnd", sql.VarChar(20), body.hourEnd);

    const insertSql = `
      DECLARE @tmp TABLE (
        NoProduksi varchar(50), Tanggal date, IdMesin int, IdOperator int,
        Shift int, JamKerja int, CreateBy varchar(100),
        CheckBy1 varchar(100), CheckBy2 varchar(100), ApproveBy varchar(100),
        HourMeter decimal(18,2), HourStart time(7), HourEnd time(7)
      );

      INSERT INTO dbo.HotStamping_h (
        NoProduksi, Tanggal, IdMesin, IdOperator, Shift, JamKerja,
        CreateBy, CheckBy1, CheckBy2, ApproveBy,
        HourMeter, HourStart, HourEnd
      )
      OUTPUT INSERTED.* INTO @tmp
      VALUES (
        @NoProduksi, @Tanggal, @IdMesin, @IdOperator, @Shift, @JamKerja,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @HourMeter,
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

/**
 * UPDATE header HotStamping_h
 * - Wajib ada noProduksi
 * - Field update bersifat dinamis (yang dikirim saja)
 * - Jika Tanggal berubah -> sync DateUsage furniture wip input
 */
async function updateHotStampingProduksi(noProduksi, payload, ctx) {
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
      entityKey: "hotStamping",
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
      action: "update HotStamping (current date)",
      useLock: true,
    });
    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update HotStamping (new date)",
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
        `HourStart = CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END`,
      );
      rqUpd.input("HourStart", sql.VarChar(20), payload.hourStart ?? null);
    }

    if (payload.hourEnd !== undefined) {
      sets.push(
        `HourEnd = CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL ELSE CAST(@HourEnd AS time(7)) END`,
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
      UPDATE dbo.HotStamping_h
      SET ${sets.join(", ")}
      WHERE NoProduksi = @NoProduksi;

      SELECT * FROM dbo.HotStamping_h WHERE NoProduksi = @NoProduksi;
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
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.HotStampingInputLabelFWIP AS map
            WHERE map.NoProduksi = @NoProduksi
              AND map.NoFurnitureWIP = fw.NoFurnitureWIP
          );

        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.HotStampingInputLabelFWIPPartial AS mp
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
    // attach auditCtx agar controller tetap bisa mengirim audit info walau error
    throw Object.assign(e, auditCtx);
  }
}

async function deleteHotStampingProduksi(noProduksi, ctx) {
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
    // ===============================
    // 0) LOCK HEADER + AMBIL docDateOnly
    // ===============================
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "hotStamping",
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // ===============================
    // 1) GUARD TUTUP TRANSAKSI
    // ===============================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "delete HotStamping",
      useLock: true,
    });

    // ===============================
    // 2) CEK OUTPUT FWIP / REJECT
    // ===============================
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut.input("NoProduksi", sql.VarChar(50), noProduksi)
      .query(`
        SELECT
          SUM(CASE WHEN Src = 'FWIP' THEN Cnt ELSE 0 END) AS CntOutputFWIP,
          SUM(CASE WHEN Src = 'REJECT' THEN Cnt ELSE 0 END) AS CntOutputReject
        FROM (
          SELECT 'FWIP' AS Src, COUNT(1) AS Cnt
          FROM dbo.HotStampingOutputLabelFWIP WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
          UNION ALL
          SELECT 'REJECT' AS Src, COUNT(1) AS Cnt
          FROM dbo.HotStampingOutputRejectV2 WITH (NOLOCK)
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

    // ===============================
    // 3) DELETE INPUT + RESET DATEUSAGE + DELETE HEADER
    // ===============================
    const rqDel = new sql.Request(tx);
    rqDel.input("NoProduksi", sql.VarChar(50), noProduksi);

    // apply audit context sebelum eksekusi
    await applyAuditContext(rqDel, auditCtx);

    const sqlDelete = `
      -- SIMPAN KEY FURNITURE WIP
      DECLARE @FWIPKeys TABLE (NoFurnitureWIP varchar(50) PRIMARY KEY);

      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT map.NoFurnitureWIP
      FROM dbo.HotStampingInputLabelFWIP AS map
      WHERE map.NoProduksi = @NoProduksi AND map.NoFurnitureWIP IS NOT NULL;

      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.HotStampingInputLabelFWIPPartial AS mp
      JOIN dbo.FurnitureWIPPartial AS fwp
        ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND fwp.NoFurnitureWIP IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM @FWIPKeys k WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP);

      DELETE FROM dbo.HotStampingInputMaterial WHERE NoProduksi = @NoProduksi;

      DELETE fwp
      FROM dbo.FurnitureWIPPartial AS fwp
      JOIN dbo.HotStampingInputLabelFWIPPartial AS mp
        ON mp.NoFurnitureWIPPartial = fwp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.HotStampingInputLabelFWIPPartial WHERE NoProduksi = @NoProduksi;
      DELETE FROM dbo.HotStampingInputLabelFWIP WHERE NoProduksi = @NoProduksi;

      UPDATE fw
      SET fw.DateUsage = NULL,
          fw.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1 FROM dbo.FurnitureWIPPartial p
              WHERE p.NoFurnitureWIP = fw.NoFurnitureWIP
            )
            THEN 1 ELSE 0 END
      FROM dbo.FurnitureWIP AS fw
      JOIN @FWIPKeys AS k
        ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      DELETE FROM dbo.HotStamping_h WHERE NoProduksi = @NoProduksi;
    `;

    await rqDel.query(sqlDelete);
    await tx.commit();

    return { success: true, audit: auditCtx };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    // attach audit context agar controller bisa tetap mengembalikan meta.audit walau error
    throw Object.assign(e, auditCtx);
  }
}

async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noProduksi);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */
    
-- Full FurnitureWIP
SELECT
  'fwip' AS Src,
  map.NoProduksi,
  map.NoFurnitureWIP        AS Ref1,
  CAST(NULL AS varchar(50)) AS Ref2,

  fw.Berat,
  fw.Pcs,
  fw.IsPartial,
  fw.IDFurnitureWIP         AS IdJenis,

  mw.Nama                   AS NamaJenis,
  uom.NamaUOM               AS NamaUOM
FROM dbo.HotStampingInputLabelFWIP map WITH (NOLOCK)
LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
  ON fw.NoFurnitureWIP = map.NoFurnitureWIP

LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
  ON mw.IdCabinetWIP = fw.IDFurnitureWIP

LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
  ON uom.IdUOM = mw.IdUOM

WHERE map.NoProduksi = @no


    UNION ALL

    -- Material
    SELECT
      'material' AS Src,
      im.NoProduksi,
      CAST(im.IdCabinetMaterial AS varchar(50)) AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      
      CAST(NULL AS decimal(18,3)) AS Berat,
      CAST(im.Jumlah AS int) AS Pcs,
      CAST(NULL AS bit) AS IsPartial,
      CAST(NULL AS int) AS IdJenis,
      mm.Nama AS NamaJenis,
      uom.NamaUOM
    FROM dbo.HotStampingInputMaterial im WITH (NOLOCK)
    LEFT JOIN dbo.MstCabinetMaterial mm WITH (NOLOCK)
      ON mm.IdCabinetMaterial = im.IdCabinetMaterial
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mm.IdUOM
    WHERE im.NoProduksi = @no

    ORDER BY Ref1 DESC, Ref2 ASC;


    /* ===================== [2] PARTIALS ===================== */
    
-- FurnitureWIP Partial
SELECT
  mp.NoFurnitureWIPPartial,
  fwp.NoFurnitureWIP,
  fwp.Pcs,
  fw.Berat,
  fw.IDFurnitureWIP AS IdJenis,

  mw.Nama           AS NamaJenis,
  uom.NamaUOM       AS NamaUOM
FROM dbo.HotStampingInputLabelFWIPPartial mp WITH (NOLOCK)
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
  const fwipPart = rs.recordsets?.[1] || [];

  const out = {
    furnitureWip: [],
    cabinetMaterial: [],
    summary: {
      furnitureWip: 0,
      cabinetMaterial: 0,
    },
  };

  // ========== MAIN ROWS ==========
  for (const r of mainRows) {
    switch (r.Src) {
      case "fwip":
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          berat: r.Berat ?? null,
          pcs: r.Pcs ?? null,
          isPartial: r.IsPartial ?? null,
          idJenis: r.IdJenis ?? null,
          namaJenis: r.NamaJenis ?? null,
        });
        break;

      case "material":
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1,
          pcs: r.Pcs ?? null,
          namaJenis: r.NamaJenis ?? null, // nama material
          namaUom: r.NamaUOM ?? null, // nama UOM untuk display
        });
        break;
    }
  }

  // ========== PARTIAL ROWS ==========
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

  // ========== SUMMARY ==========
  out.summary.furnitureWip = out.furnitureWip.length;
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
      o.NoFurnitureWIP,
      ISNULL(fw.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.HotStampingOutputLabelFWIP o WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = o.NoFurnitureWIP
    WHERE o.NoProduksi = @no
    ORDER BY o.NoFurnitureWIP DESC;
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
    FROM dbo.HotStampingOutputRejectV2 o WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj WITH (NOLOCK)
      ON rj.NoReject = o.NoReject
    WHERE o.NoProduksi = @no
    ORDER BY o.NoReject DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
}

async function validateFwipLabel(labelCode) {
  const pool = await poolPromise;
  const raw = String(labelCode || "").trim();
  if (!raw) throw new Error("Label code is required");

  /* =========================================================
   * 1) FULL : FurnitureWIP.NoFurnitureWIP
   *    - valid jika DateUsage IS NULL
   * ========================================================= */
  {
    const req = pool.request();
    req.input("code", sql.VarChar(50), raw);

    const q = `
       ;WITH PartialSum AS (
        SELECT
          fwp.NoFurnitureWIP,
          SUM(ISNULL(fwp.Pcs, 0)) AS PcsPartial
        FROM dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
        GROUP BY fwp.NoFurnitureWIP
      )
      SELECT
        fw.NoFurnitureWIP,
        fw.DateCreate,
        fw.Jam,
        CAST(fw.Pcs - ISNULL(ps.PcsPartial, 0) AS int) AS Pcs,  -- ✅ sisa pcs
        fw.IDFurnitureWIP,
        fw.Berat,
        CASE WHEN ISNULL(ps.PcsPartial, 0) > 0 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS IsPartial,
        fw.DateUsage,
        fw.IdWarehouse,
        fw.IdWarna,
        fw.CreateBy,
        fw.DateTimeCreate,
        fw.Blok,
        fw.IdLokasi,
        ISNULL(ps.PcsPartial, 0) AS PcsPartial  -- opsional debug
      FROM dbo.FurnitureWIP fw WITH (NOLOCK)
      LEFT JOIN PartialSum ps
        ON ps.NoFurnitureWIP = fw.NoFurnitureWIP
      WHERE fw.NoFurnitureWIP = @code
        AND fw.DateUsage IS NULL
        AND (fw.Pcs - ISNULL(ps.PcsPartial, 0)) > 0;
    `;

    const rs = await req.query(q);
    const rows = rs.recordset || [];

    if (rows.length > 0) {
      return {
        found: true,
        count: rows.length,
        tableName: "FurnitureWIP",
        data: rows,
      };
    }
  }

  return {
    found: false,
    count: 0,
    tableName: "",
    data: [],
  };
}

/**
 * Single entry: create NEW partials + link them, and attach EXISTING inputs.
 * All in one transaction.
 *
 * Payload shape (arrays optional):
 * {
 *   // existing inputs to attach
 *   furnitureWip:           [{ noFurnitureWip }],
 *   cabinetMaterial:        [{ idCabinetMaterial, jumlah }],
 *
 *   // NEW partials to create + map
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
  return sharedInputService.upsertInputsAndPartials("hotStamping", no, body, {
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
  return sharedInputService.deleteInputsAndPartials("hotStamping", no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

module.exports = {
  getProduksiByDate,
  getAllProduksi,
  createHotStampingProduksi,
  updateHotStampingProduksi,
  deleteHotStampingProduksi,
  fetchInputs,
  fetchOutputs,
  fetchOutputsReject,
  validateFwipLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

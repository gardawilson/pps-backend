// services/bongkar-susun-service.js
const { sql, poolPromise } = require("../../core/config/db");
const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,
  formatYMD,
  loadDocDateOnlyFromConfig,
} = require("../../core/shared/tutup-transaksi-guard");
const sharedInputService = require("../../core/shared/produksi-input.service");
const { badReq, conflict } = require("../../core/utils/http-error");
const { applyAuditContext } = require("../../core/utils/db-audit-context");
const { generateNextCode } = require("../../core/utils/sequence-code-helper");
const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require("../../core/utils/jam-kerja-helper");

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

  request.input("date", sql.Date, date);

  const result = await request.query(query);
  return result.recordset;
}

/**
 * Paginated fetch for dbo.BongkarSusun_h
 * Columns:
 *  NoBongkarSusun, Tanggal, IdUsername, Note + Username (from MstUsername)
 */
async function getAllBongkarSusun(page = 1, pageSize = 20, search = "") {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || "").trim();

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
      h.NoBongkarSusun,
      h.Tanggal,
      h.IdUsername,
      u.Username,
      h.Note,

      -- (opsional utk FE)
      lc.LastClosedDate AS LastClosedDate,

      -- ✅ flag tutup transaksi
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.BongkarSusun_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstUsername u WITH (NOLOCK)
      ON u.IdUsername = h.IdUsername

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    ORDER BY h.Tanggal DESC, h.NoBongkarSusun DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input("search", sql.VarChar(100), searchTerm);
  dataReq.input("offset", sql.Int, offset);
  dataReq.input("limit", sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);

  return {
    data: dataRes.recordset || [],
    total,
  };
}

async function createBongkarSusun(payload, ctx) {
  const must = [];
  if (!payload?.tanggal) must.push("tanggal");
  if (!payload?.username) must.push("username");
  if (must.length) throw badReq(`Field wajib: ${must.join(", ")}`);

  // ===============================
  // Validasi audit context
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
    // Normalize tanggal + lock guard
    // ===============================
    const effectiveDate = resolveEffectiveDateForCreate(payload.tanggal);
    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: "create BongkarSusun",
      useLock: true,
    });

    // ===============================
    // Resolve username -> IdUsername
    // ===============================
    const rqUser = new sql.Request(tx);
    const userRes = await rqUser.input(
      "Username",
      sql.VarChar(100),
      String(payload.username).trim(),
    ).query(`
        SELECT TOP 1 IdUsername
        FROM dbo.MstUsername WITH (NOLOCK)
        WHERE Username = @Username;
      `);

    if (userRes.recordset.length === 0) {
      throw badReq(
        `Username "${payload.username}" tidak ditemukan di MstUsername`,
      );
    }
    const idUsername = userRes.recordset[0].IdUsername;

    // ===============================
    // Generate NoBongkarSusun unik
    // ===============================
    let noBongkarSusun = await generateNextCode(tx, {
      tableName: "dbo.BongkarSusun_h",
      columnName: "NoBongkarSusun",
      prefix: "BG.",
      width: 10,
    });

    // Double-check exist + lock untuk race condition
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck.input(
      "NoBongkarSusun",
      sql.VarChar(50),
      noBongkarSusun,
    ).query(`
        SELECT 1
        FROM dbo.BongkarSusun_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBongkarSusun = @NoBongkarSusun;
      `);

    if (exist.recordset.length) {
      noBongkarSusun = await generateNextCode(tx, {
        tableName: "dbo.BongkarSusun_h",
        columnName: "NoBongkarSusun",
        prefix: "BG.",
        width: 10,
      });
    }

    // ===============================
    // Insert header (tanpa OUTPUT)
    // ===============================
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("Tanggal", sql.Date, effectiveDate)
      .input("IdUsername", sql.Int, idUsername)
      .input("Note", sql.VarChar(255), payload.note ?? null);

    const insertSql = `
      INSERT INTO dbo.BongkarSusun_h (NoBongkarSusun, Tanggal, IdUsername, Note)
      VALUES (@NoBongkarSusun, @Tanggal, @IdUsername, @Note);
    `;
    await rqIns.query(insertSql);

    // ===============================
    // SELECT ulang header
    // ===============================
    const selRes = await new sql.Request(tx).input(
      "NoBongkarSusun",
      sql.VarChar(50),
      noBongkarSusun,
    ).query(`
        SELECT *
        FROM dbo.BongkarSusun_h
        WHERE NoBongkarSusun = @NoBongkarSusun;
      `);

    const header = selRes.recordset?.[0] || null;

    await tx.commit();
    return { header, audit };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw Object.assign(e, auditCtx); // attach audit context untuk controller
  }
}

// ===========================
//  UPDATE BongkarSusun_h
// ===========================

// ============================================================
// updateBongkarSusunCascade  —  revised (aligned to BJJual pattern)
// ============================================================

async function updateBongkarSusunCascade(
  noBongkarSusun,
  headerPayload,
  inputsPayloadOrNull,
  ctx, // ← NEW: audit context dari controller (actorId, actorUsername, requestId)
) {
  if (!noBongkarSusun) throw badReq("noBongkarSusun wajib diisi");

  // ===============================
  // Audit context  (sama persis BJJual)
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

  // ===============================
  // Transaction setup  (tx.begin SEBELUM try — sama BJJual)
  // ===============================
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  // ✅ FIX DI SINI
  const auditReq = new sql.Request(tx);
  await applyAuditContext(auditReq, auditCtx);

  try {
    // =====================================================
    // 0) Load old doc date + lock header row
    // =====================================================
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "bongkarSusun",
      codeValue: noBongkarSusun,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // =====================================================
    // 1) Handle date change  (simplified detect — sama BJJual)
    // =====================================================
    const isChangingDate = headerPayload?.tanggal !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!headerPayload.tanggal) throw badReq("tanggal tidak boleh kosong");
      newDocDateOnly = resolveEffectiveDateForCreate(headerPayload.tanggal);
    }

    // =====================================================
    // 2) Guard tutup transaksi
    // =====================================================
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: "update BongkarSusun (current date)",
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update BongkarSusun (new date)",
        useLock: true,
      });
    }

    // =====================================================
    // 3) Update header (kalau ada field)
    //    - fallback ke GET kalau payload kosong/null
    //      (dipertahankan karena inputs bisa diupdate sendiri)
    // =====================================================
    let headerUpdated = null;

    if (headerPayload && Object.keys(headerPayload).length) {
      const headerToUpdate = { ...headerPayload };
      if (isChangingDate) headerToUpdate.tanggal = newDocDateOnly;

      headerUpdated = await _updateHeaderWithTx(
        tx,
        noBongkarSusun,
        headerToUpdate,
      );
    } else {
      headerUpdated = await _getHeaderWithTx(tx, noBongkarSusun);
    }

    // =====================================================
    // 4) Upsert inputs (kalau ada)
    // =====================================================
    let attachmentsSummary = null;
    if (inputsPayloadOrNull) {
      attachmentsSummary = await upsertInputsWithExistingTx(
        tx,
        noBongkarSusun,
        inputsPayloadOrNull,
      );
    }

    // =====================================================
    // 5) Sync DateUsage jika tanggal berubah
    //    — pakai oldDocDateOnly sebagai guard filter
    //      (aligned ke BJJual: hanya update row yang
    //       DateUsage IS NULL OR = old date)
    // =====================================================
    if (isChangingDate) {
      await refreshDateUsageByInputsTx(
        tx,
        noBongkarSusun,
        oldDocDateOnly, // ← NEW param
        newDocDateOnly, // ← NEW param
      );
      // re-fetch header supaya Tanggal reflect yang baru
      headerUpdated = await _getHeaderWithTx(tx, noBongkarSusun);
    }

    await tx.commit();

    // =====================================================
    // Return  — tambahkan audit (sama BJJual)
    // =====================================================
    return {
      header: headerUpdated,
      inputs: attachmentsSummary,
      audit: auditCtx,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    // attach auditCtx ke error (sama BJJual)
    throw Object.assign(e, auditCtx);
  }
}

// ============================================================
// _getHeaderWithTx  —  unchanged
// ============================================================
async function _getHeaderWithTx(tx, no) {
  const rq = new sql.Request(tx);
  rq.input("No", sql.VarChar(50), no);
  const rs = await rq.query(
    `SELECT * FROM dbo.BongkarSusun_h WITH (NOLOCK) WHERE NoBongkarSusun=@No;`,
  );
  if (!rs.recordset.length) throw badReq("BongkarSusun tidak ditemukan");
  return rs.recordset[0];
}

// ============================================================
// _updateHeaderWithTx  —  unchanged (pattern sudah oke)
// ============================================================
async function _updateHeaderWithTx(tx, noBongkarSusun, payload) {
  const setClauses = [];
  const rq = new sql.Request(tx);

  rq.input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun);

  if (Object.prototype.hasOwnProperty.call(payload, "tanggal")) {
    setClauses.push("Tanggal=@Tanggal");
    rq.input("Tanggal", sql.Date, payload.tanggal);
  }

  if (Object.prototype.hasOwnProperty.call(payload, "note")) {
    setClauses.push("Note=@Note");
    rq.input("Note", sql.VarChar(255), payload.note ?? null);
  }

  if (!setClauses.length) {
    return _getHeaderWithTx(tx, noBongkarSusun);
  }

  await rq.query(`
    UPDATE dbo.BongkarSusun_h
    SET ${setClauses.join(", ")}
    WHERE NoBongkarSusun=@NoBongkarSusun;
  `);

  // ⬇️ SELECT ulang (AMAN + trigger tetap jalan)
  return _getHeaderWithTx(tx, noBongkarSusun);
}

// ============================================================
// refreshDateUsageByInputsTx  —  REVISED
//   - terima oldDocDateOnly + newDocDateOnly sebagai param
//   - setiap UPDATE pakai guard:
//       DateUsage IS NULL OR CONVERT(date, DateUsage) = @OldTanggal
//     → hanya row yang masih "milik" tanggal lama yang tergeser
//       (aligned ke BJJual pattern)
// ============================================================
async function refreshDateUsageByInputsTx(
  tx,
  no,
  oldDocDateOnly,
  newDocDateOnly,
) {
  const rq = new sql.Request(tx);
  rq.input("No", sql.VarChar(50), no);
  rq.input("OldTanggal", sql.Date, oldDocDateOnly);
  rq.input("NewTanggal", sql.Date, newDocDateOnly);

  await rq.query(`
    SET NOCOUNT ON;

    -- ─── BROKER ────────────────────────────────────────────
    UPDATE b
    SET   b.DateUsage = @NewTanggal
    FROM  dbo.Broker_d b
    INNER JOIN dbo.BongkarSusunInputBroker i
      ON  i.NoBroker = b.NoBroker
      AND i.NoSak    = b.NoSak
    WHERE i.NoBongkarSusun = @No
      AND (b.DateUsage IS NULL OR CONVERT(date, b.DateUsage) = @OldTanggal);

    -- ─── BAHAN BAKU ────────────────────────────────────────
    UPDATE d
    SET   d.DateUsage = @NewTanggal
    FROM  dbo.BahanBaku_d d
    INNER JOIN dbo.BongkarSusunInputBahanBaku i
      ON  i.NoBahanBaku = d.NoBahanBaku
      AND i.NoPallet    = d.NoPallet
      AND i.NoSak       = d.NoSak
    WHERE i.NoBongkarSusun = @No
      AND (d.DateUsage IS NULL OR CONVERT(date, d.DateUsage) = @OldTanggal);

    -- ─── WASHING ───────────────────────────────────────────
    UPDATE d
    SET   d.DateUsage = @NewTanggal
    FROM  dbo.Washing_d d
    INNER JOIN dbo.BongkarSusunInputWashing i
      ON  i.NoWashing = d.NoWashing
      AND i.NoSak     = d.NoSak
    WHERE i.NoBongkarSusun = @No
      AND (d.DateUsage IS NULL OR CONVERT(date, d.DateUsage) = @OldTanggal);

    -- ─── CRUSHER ───────────────────────────────────────────
    UPDATE c
    SET   c.DateUsage = @NewTanggal
    FROM  dbo.Crusher c
    INNER JOIN dbo.BongkarSusunInputCrusher i
      ON  i.NoCrusher = c.NoCrusher
    WHERE i.NoBongkarSusun = @No
      AND (c.DateUsage IS NULL OR CONVERT(date, c.DateUsage) = @OldTanggal);

    -- ─── GILINGAN ──────────────────────────────────────────
    UPDATE g
    SET   g.DateUsage = @NewTanggal
    FROM  dbo.Gilingan g
    INNER JOIN dbo.BongkarSusunInputGilingan i
      ON  i.NoGilingan = g.NoGilingan
    WHERE i.NoBongkarSusun = @No
      AND (g.DateUsage IS NULL OR CONVERT(date, g.DateUsage) = @OldTanggal);

    -- ─── MIXER ─────────────────────────────────────────────
    UPDATE d
    SET   d.DateUsage = @NewTanggal
    FROM  dbo.Mixer_d d
    INNER JOIN dbo.BongkarSusunInputMixer i
      ON  i.NoMixer = d.NoMixer
      AND i.NoSak   = d.NoSak
    WHERE i.NoBongkarSusun = @No
      AND (d.DateUsage IS NULL OR CONVERT(date, d.DateUsage) = @OldTanggal);

    -- ─── BONGGOLAN ─────────────────────────────────────────
    UPDATE b
    SET   b.DateUsage = @NewTanggal
    FROM  dbo.Bonggolan b
    INNER JOIN dbo.BongkarSusunInputBonggolan i
      ON  i.NoBonggolan = b.NoBonggolan
    WHERE i.NoBongkarSusun = @No
      AND (b.DateUsage IS NULL OR CONVERT(date, b.DateUsage) = @OldTanggal);

    -- ─── FURNITURE WIP ─────────────────────────────────────
    UPDATE f
    SET   f.DateUsage = @NewTanggal
    FROM  dbo.FurnitureWIP f
    INNER JOIN dbo.BongkarSusunInputFurnitureWIP i
      ON  i.NoFurnitureWIP = f.NoFurnitureWIP
    WHERE i.NoBongkarSusun = @No
      AND (f.DateUsage IS NULL OR CONVERT(date, f.DateUsage) = @OldTanggal);

    -- ─── BARANG JADI ───────────────────────────────────────
    UPDATE b
    SET   b.DateUsage = @NewTanggal
    FROM  dbo.BarangJadi b
    INNER JOIN dbo.BongkarSusunInputBarangJadi i
      ON  i.NoBJ = b.NoBJ
    WHERE i.NoBongkarSusun = @No
      AND (b.DateUsage IS NULL OR CONVERT(date, b.DateUsage) = @OldTanggal);
  `);
}

// ============================================================
// upsertInputsWithExistingTx  —  unchanged
// ============================================================
async function upsertInputsWithExistingTx(tx, noBongkarSusun, payload) {
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

  return await _insertInputsWithTx(tx, noBongkarSusun, body);
}

// ===========================
//  DELETE BongkarSusun_h
// ===========================
async function deleteBongkarSusun(noBongkarSusun, ctx) {
  if (!noBongkarSusun) throw badReq("noBongkarSusun wajib diisi");

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
      entityKey: "bongkarSusun",
      codeValue: noBongkarSusun,
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
      action: "delete BongkarSusun",
      useLock: true,
    });

    // ===============================
    // 2) CEK OUTPUT (BongkarSusun-specific)
    //    kalau ada label output → tolak delete
    // ===============================
    const rqOut = new sql.Request(tx);
    rqOut.input("No", sql.VarChar(50), noBongkarSusun);

    const out = await rqOut.query(`
      SET NOCOUNT ON;

      IF EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBahanBaku      WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBarangjadi     WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBonggolan      WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBroker         WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputCrusher        WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputFurnitureWIP   WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputGilingan       WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputMixer          WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputWashing        WITH (NOLOCK) WHERE NoBongkarSusun=@No)
      BEGIN
        SELECT CAST(1 AS bit) AS HasOutput;
        RETURN;
      END

      SELECT CAST(0 AS bit) AS HasOutput;
    `);

    const hasOutput = out.recordset?.[0]?.HasOutput === true;
    if (hasOutput) {
      throw badReq(
        "Nomor Bongkar Susun ini telah menerbitkan label, hapus labelnya kemudian coba kembali",
      );
    }

    // ===============================
    // 3) CASCADE DELETE INPUTS + RESET DATEUSAGE + DELETE HEADER
    // ===============================
    const rqDel = new sql.Request(tx);
    rqDel.input("No", sql.VarChar(50), noBongkarSusun);

    // apply audit context sebelum eksekusi
    await applyAuditContext(rqDel, auditCtx);

    const sqlDelete = `
      /* ===================================================
         BONGKAR SUSUN DELETE
         - reset DateUsage on all input source tables
         - delete all input mapping tables
         - delete header last
         =================================================== */

      /* A) BROKER */
      DECLARE @delBroker TABLE(NoBroker varchar(50), NoSak int);
      DELETE map
      OUTPUT DELETED.NoBroker, DELETED.NoSak INTO @delBroker(NoBroker, NoSak)
      FROM dbo.BongkarSusunInputBroker map
      WHERE map.NoBongkarSusun = @No;

      UPDATE d
      SET d.DateUsage = NULL
      FROM dbo.Broker_d d
      INNER JOIN @delBroker x ON x.NoBroker=d.NoBroker AND x.NoSak=d.NoSak;

      /* B) BAHAN BAKU */
      DECLARE @delBB TABLE(NoBahanBaku varchar(50), NoPallet int, NoSak int);
      DELETE map
      OUTPUT DELETED.NoBahanBaku, DELETED.NoPallet, DELETED.NoSak
        INTO @delBB(NoBahanBaku, NoPallet, NoSak)
      FROM dbo.BongkarSusunInputBahanBaku map
      WHERE map.NoBongkarSusun = @No;

      UPDATE d
      SET d.DateUsage = NULL
      FROM dbo.BahanBaku_d d
      INNER JOIN @delBB x
        ON x.NoBahanBaku=d.NoBahanBaku AND x.NoPallet=d.NoPallet AND x.NoSak=d.NoSak;

      /* C) WASHING */
      DECLARE @delW TABLE(NoWashing varchar(50), NoSak int);
      DELETE map
      OUTPUT DELETED.NoWashing, DELETED.NoSak INTO @delW(NoWashing, NoSak)
      FROM dbo.BongkarSusunInputWashing map
      WHERE map.NoBongkarSusun = @No;

      UPDATE d
      SET d.DateUsage = NULL
      FROM dbo.Washing_d d
      INNER JOIN @delW x ON x.NoWashing=d.NoWashing AND x.NoSak=d.NoSak;

      /* D) CRUSHER */
      DECLARE @delC TABLE(NoCrusher varchar(50));
      DELETE map
      OUTPUT DELETED.NoCrusher INTO @delC(NoCrusher)
      FROM dbo.BongkarSusunInputCrusher map
      WHERE map.NoBongkarSusun = @No;

      UPDATE c
      SET c.DateUsage = NULL
      FROM dbo.Crusher c
      INNER JOIN @delC x ON x.NoCrusher=c.NoCrusher;

      /* E) GILINGAN */
      DECLARE @delG TABLE(NoGilingan varchar(50));
      DELETE map
      OUTPUT DELETED.NoGilingan INTO @delG(NoGilingan)
      FROM dbo.BongkarSusunInputGilingan map
      WHERE map.NoBongkarSusun = @No;

      UPDATE g
      SET g.DateUsage = NULL
      FROM dbo.Gilingan g
      INNER JOIN @delG x ON x.NoGilingan=g.NoGilingan;

      /* F) MIXER */
      DECLARE @delM TABLE(NoMixer varchar(50), NoSak int);
      DELETE map
      OUTPUT DELETED.NoMixer, DELETED.NoSak INTO @delM(NoMixer, NoSak)
      FROM dbo.BongkarSusunInputMixer map
      WHERE map.NoBongkarSusun = @No;

      UPDATE d
      SET d.DateUsage = NULL
      FROM dbo.Mixer_d d
      INNER JOIN @delM x ON x.NoMixer=d.NoMixer AND x.NoSak=d.NoSak;

      /* G) BONGGOLAN */
      DECLARE @delBg TABLE(NoBonggolan varchar(50));
      DELETE map
      OUTPUT DELETED.NoBonggolan INTO @delBg(NoBonggolan)
      FROM dbo.BongkarSusunInputBonggolan map
      WHERE map.NoBongkarSusun = @No;

      UPDATE b
      SET b.DateUsage = NULL
      FROM dbo.Bonggolan b
      INNER JOIN @delBg x ON x.NoBonggolan=b.NoBonggolan;

      /* H) FURNITURE WIP */
      DECLARE @delFW TABLE(NoFurnitureWIP varchar(50));
      DELETE map
      OUTPUT DELETED.NoFurnitureWIP INTO @delFW(NoFurnitureWIP)
      FROM dbo.BongkarSusunInputFurnitureWIP map
      WHERE map.NoBongkarSusun = @No;

      UPDATE f
      SET f.DateUsage = NULL
      FROM dbo.FurnitureWIP f
      INNER JOIN @delFW x ON x.NoFurnitureWIP=f.NoFurnitureWIP;

      /* I) BARANG JADI */
      DECLARE @delBJ TABLE(NoBJ varchar(50));
      DELETE map
      OUTPUT DELETED.NoBJ INTO @delBJ(NoBJ)
      FROM dbo.BongkarSusunInputBarangJadi map
      WHERE map.NoBongkarSusun = @No;

      UPDATE b
      SET b.DateUsage = NULL
      FROM dbo.BarangJadi b
      INNER JOIN @delBJ x ON x.NoBJ=b.NoBJ;

      /* J) DELETE HEADER LAST */
      DELETE FROM dbo.BongkarSusun_h WHERE NoBongkarSusun = @No;
    `;

    await rqDel.query(sqlDelete);
    await tx.commit();

    return { success: true, audit: auditCtx };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}

    // attach audit context agar controller tetap bisa kirim meta.audit
    throw Object.assign(e, auditCtx);
  }
}

async function fetchInputs(noBongkarSusun) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input("no", sql.VarChar(50), noBongkarSusun);

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

        fw.IDFurnitureWIP AS IdJenis,              -- id jenis (mengacu ke master cabinet)
        mcw.Nama AS NamaJenis                      -- ✅ ambil nama dari master cabinet
      FROM dbo.BongkarSusunInputFurnitureWIP ifw WITH (NOLOCK)
      LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
        ON fw.NoFurnitureWIP = ifw.NoFurnitureWIP
      LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
        ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
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
      fw.IDFurnitureWIP AS IdJenis,
      mcw.Nama AS NamaJenis
    FROM dbo.FurnitureWIPPartial p WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = p.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mcw WITH (NOLOCK)
      ON mcw.IdCabinetWIP = fw.IDFurnitureWIP
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
  const bbPart = rs.recordsets?.[1] || [];
  const gilPart = rs.recordsets?.[2] || [];
  const mixPart = rs.recordsets?.[3] || [];
  const brkPart = rs.recordsets?.[4] || [];
  const bjPart = rs.recordsets?.[5] || [];
  const fwPart = rs.recordsets?.[6] || [];

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
      case "broker":
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case "crusher":
        out.crusher.push({ noCrusher: r.Ref1, ...base });
        break;
      case "bonggolan":
        out.bonggolan.push({ noBonggolan: r.Ref1, ...base });
        break;
      case "gilingan":
        out.gilingan.push({ noGilingan: r.Ref1, ...base });
        break;
      case "mixer":
        out.mixer.push({ noMixer: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case "furniture_wip":
        out.furnitureWip.push({ noFurnitureWIP: r.Ref1, ...base });
        break;
      case "barang_jadi":
        out.barangJadi.push({ noBJ: r.Ref1, ...base });
        break;
    }
  }

  // PARTIAL: BB
  for (const p of bbPart) {
    out.bb.push({
      noBBPartial: p.NoBBPartial,
      noBahanBaku: p.NoBahanBaku ?? null,
      noPallet: p.NoPallet ?? null,
      noSak: p.NoSak ?? null,
      pcs: null,
      berat: p.Berat ?? null,
      beratAct: null,
      isPartial: true,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Gilingan
  for (const p of gilPart) {
    out.gilingan.push({
      noGilinganPartial: p.NoGilinganPartial,
      noGilingan: p.NoGilingan ?? null,
      pcs: null,
      berat: p.Berat ?? null,
      beratAct: null,
      isPartial: true,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Mixer
  for (const p of mixPart) {
    out.mixer.push({
      noMixerPartial: p.NoMixerPartial,
      noMixer: p.NoMixer ?? null,
      noSak: p.NoSak ?? null,
      pcs: null,
      berat: p.Berat ?? null,
      beratAct: null,
      isPartial: true,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Broker
  for (const p of brkPart) {
    out.broker.push({
      noBrokerPartial: p.NoBrokerPartial,
      noBroker: p.NoBroker ?? null,
      noSak: p.NoSak ?? null,
      pcs: null,
      berat: p.Berat ?? null,
      beratAct: null,
      isPartial: true,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Barang Jadi
  for (const p of bjPart) {
    out.barangJadi.push({
      noBJPartial: p.NoBJPartial,
      noBJ: p.NoBJ ?? null,
      pcs: p.Pcs ?? null,
      berat: null,
      beratAct: null,
      isPartial: true,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  // PARTIAL: Furniture WIP
  for (const p of fwPart) {
    out.furnitureWip.push({
      noFurnitureWIPPartial: p.NoFurnitureWIPPartial,
      noFurnitureWIP: p.NoFurnitureWIP ?? null,
      pcs: p.Pcs ?? null,
      berat: null,
      beratAct: null,
      isPartial: true,
      idJenis: p.IdJenis ?? null,
      namaJenis: null, // kalau ada master furniture wip nanti kita isi
    });
  }

  // Summary
  for (const k of Object.keys(out.summary)) out.summary[k] = out[k].length;

  return out;
}

/**
 * Validate label khusus untuk Bongkar Susun
 * Bedanya dengan validateLabel biasa:
 * - Filter out items dengan isPartial = 1
 * - Hanya ambil items yang bisa dibongkar (non-partial only)
 */
async function validateLabelBongkarSusun(labelCode) {
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
  const p3 = raw.substring(0, 3).toUpperCase();
  if (p3 === "BF." || p3 === "BB." || p3 === "BA.") {
    prefix = p3;
  } else {
    prefix = raw.substring(0, 2).toUpperCase();
  }

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
    // =========================
    // A. BahanBaku_d
    // =========================
    case "A.": {
      tableName = "BahanBaku_d";
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
          Berat = CASE
                    WHEN ISNULL(NULLIF(d.BeratAct, 0), d.Berat) - ISNULL(pa.PartialBerat, 0) < 0
                      THEN 0
                    ELSE ISNULL(NULLIF(d.BeratAct, 0), d.Berat) - ISNULL(pa.PartialBerat, 0)
                  END,
          d.DateUsage,
          CAST(0 AS bit) AS IsPartial,  -- ✅ ALWAYS 0 for Bongkar Susun
          ph.IdJenisPlastik AS idJenis,
          jp.Jenis AS namaJenis
        FROM dbo.BahanBaku_d AS d WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoBahanBaku = d.NoBahanBaku
         AND pa.NoPallet = d.NoPallet
         AND pa.NoSak = d.NoSak
        LEFT JOIN dbo.BahanBakuPallet_h AS ph WITH (NOLOCK)
          ON ph.NoBahanBaku = d.NoBahanBaku
         AND ph.NoPallet = d.NoPallet
        LEFT JOIN dbo.MstJenisPlastik AS jp WITH (NOLOCK)
          ON jp.IdJenisPlastik = ph.IdJenisPlastik
        WHERE d.NoBahanBaku = @noBahanBaku
          AND d.NoPallet = @noPallet
          AND d.DateUsage IS NULL
          AND (d.IsPartial IS NULL OR d.IsPartial = 0)  -- ✅ FILTER: non-partial only
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
    // B. Washing_d (no isPartial check needed)
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
          jp.Jenis AS namaJenis,
          CAST(0 AS bit) AS IsPartial  -- ✅ ALWAYS 0
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
          d.NoBroker AS noBroker,
          d.NoSak AS noSak,
          CAST(d.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          d.DateUsage AS dateUsage,
          CAST(0 AS bit) AS isPartial,  -- ✅ ALWAYS 0 for Bongkar Susun
          h.IdJenisPlastik AS idJenis,
          jp.Jenis AS namaJenis
        FROM dbo.Broker_d AS d WITH (NOLOCK)
        LEFT JOIN PartialSum AS ps
          ON ps.NoBroker = d.NoBroker
         AND ps.NoSak = d.NoSak
        LEFT JOIN dbo.Broker_h AS h WITH (NOLOCK)
          ON h.NoBroker = d.NoBroker
        LEFT JOIN dbo.MstJenisPlastik AS jp WITH (NOLOCK)
          ON jp.IdJenisPlastik = h.IdJenisPlastik
        WHERE d.NoBroker = @labelCode
          AND d.DateUsage IS NULL
          AND (d.Berat - ISNULL(ps.BeratPartial, 0)) > 0
          AND ISNULL(ps.BeratPartial, 0) = 0  -- ✅ FILTER: belum pernah di-partial
        ORDER BY d.NoBroker, d.NoSak;
      `;
      return await run(raw);

    // =========================
    // M. Bonggolan (no isPartial check needed)
    // =========================
    case "M.":
      tableName = "Bonggolan";
      query = `
        SELECT
          b.NoBonggolan,
          b.DateCreate,
          b.IdBonggolan AS idJenis,
          mb.NamaBonggolan AS namaJenis,
          b.IdWarehouse,
          b.DateUsage,
          b.Berat,
          b.IdStatus,
          b.Blok,
          b.IdLokasi,
          b.CreateBy,
          b.DateTimeCreate,
          CAST(0 AS bit) AS IsPartial  -- ✅ ALWAYS 0
        FROM dbo.Bonggolan AS b WITH (NOLOCK)
        LEFT JOIN dbo.MstBonggolan AS mb WITH (NOLOCK)
          ON mb.IdBonggolan = b.IdBonggolan
        WHERE b.NoBonggolan = @labelCode
          AND b.DateUsage IS NULL
        ORDER BY b.NoBonggolan;
      `;
      return await run(raw);

    // =========================
    // F. Crusher (no isPartial check needed)
    // =========================
    case "F.":
      tableName = "Crusher";
      query = `
        SELECT
          c.NoCrusher,
          c.DateCreate,
          c.IdCrusher AS idJenis,
          mc.NamaCrusher AS namaJenis,
          c.IdWarehouse,
          c.DateUsage,
          c.Berat,
          c.IdStatus,
          c.Blok,
          c.IdLokasi,
          c.CreateBy,
          c.DateTimeCreate,
          CAST(0 AS bit) AS IsPartial  -- ✅ ALWAYS 0
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
          g.IdGilingan AS idJenis,
          mg.NamaGilingan AS namaJenis,
          g.DateUsage,
          Berat = CASE
                    WHEN g.Berat - ISNULL(pa.PartialBerat, 0) < 0 THEN 0
                    ELSE g.Berat - ISNULL(pa.PartialBerat, 0)
                  END,
          CAST(0 AS bit) AS IsPartial  -- ✅ ALWAYS 0 for Bongkar Susun
        FROM dbo.Gilingan AS g WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoGilingan = g.NoGilingan
        LEFT JOIN dbo.MstGilingan AS mg WITH (NOLOCK)
          ON mg.IdGilingan = g.IdGilingan
        WHERE g.NoGilingan = @labelCode
          AND g.DateUsage IS NULL
          AND (g.IsPartial IS NULL OR g.IsPartial = 0)  -- ✅ FILTER: non-partial only
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
          d.NoMixer AS noMixer,
          d.NoSak AS noSak,
          CAST(d.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          d.DateUsage AS dateUsage,
          CAST(0 AS bit) AS isPartial,  -- ✅ ALWAYS 0 for Bongkar Susun
          d.IdLokasi AS idLokasi,
          h.IdMixer AS idJenis,
          mm.Jenis AS namaJenis
        FROM dbo.Mixer_d AS d WITH (NOLOCK)
        LEFT JOIN PartialSum AS ps
          ON ps.NoMixer = d.NoMixer
         AND ps.NoSak = d.NoSak
        LEFT JOIN dbo.Mixer_h AS h WITH (NOLOCK)
          ON h.NoMixer = d.NoMixer
        LEFT JOIN dbo.MstMixer AS mm WITH (NOLOCK)
          ON mm.IdMixer = h.IdMixer
        WHERE d.NoMixer = @labelCode
          AND d.DateUsage IS NULL
          AND (d.Berat - ISNULL(ps.BeratPartial, 0)) > 0
          AND ISNULL(ps.BeratPartial, 0) = 0  -- ✅ FILTER: belum pernah di-partial
        ORDER BY d.NoMixer, d.NoSak;
      `;
      return await run(raw);

    // =========================
    // BB. FurnitureWIP
    // =========================
    case "BB.":
      tableName = "FurnitureWIP";
      query = `
        ;WITH PartialAgg AS (
          SELECT
            p.NoFurnitureWIP,
            SUM(ISNULL(p.Pcs, 0)) AS PcsPartial
          FROM dbo.FurnitureWIPPartial AS p WITH (NOLOCK)
          GROUP BY p.NoFurnitureWIP
        )
        SELECT
          f.NoFurnitureWIP AS noFurnitureWip,
          f.DateCreate AS dateCreate,
          f.Jam AS jam,
          Pcs = CASE
                  WHEN ISNULL(f.Pcs, 0) - ISNULL(pa.PcsPartial, 0) < 0 THEN 0
                  ELSE ISNULL(f.Pcs, 0) - ISNULL(pa.PcsPartial, 0)
                END,
          f.IDFurnitureWIP AS idJenis,
          mc.Nama AS namaJenis,
          f.Berat AS berat,
          f.DateUsage AS dateUsage,
          f.IdWarehouse AS idWarehouse,
          f.IdWarna AS idWarna,
          f.CreateBy AS createBy,
          f.DateTimeCreate AS dateTimeCreate,
          f.Blok AS blok,
          f.IdLokasi AS idLokasi,
          CAST(0 AS bit) AS isPartial  -- ✅ ALWAYS 0 for Bongkar Susun
        FROM dbo.FurnitureWIP AS f WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoFurnitureWIP = f.NoFurnitureWIP
        LEFT JOIN dbo.MstCabinetWIP AS mc WITH (NOLOCK)
          ON mc.IdCabinetWIP = f.IDFurnitureWIP
        WHERE f.NoFurnitureWIP = @labelCode
          AND f.DateUsage IS NULL
          AND (ISNULL(f.Pcs, 0) - ISNULL(pa.PcsPartial, 0)) > 0
          AND ISNULL(pa.PcsPartial, 0) = 0  -- ✅ FILTER: belum pernah di-partial
        ORDER BY f.NoFurnitureWIP;
      `;
      return await run(raw);

    // =========================
    // BA. BarangJadi
    // =========================
    case "BA.":
      tableName = "BarangJadi";
      query = `
        ;WITH PartialAgg AS (
          SELECT
            p.NoBJ,
            SUM(ISNULL(p.Pcs, 0)) AS PcsPartial
          FROM dbo.BarangJadiPartial AS p WITH (NOLOCK)
          GROUP BY p.NoBJ
        )
        SELECT
          b.NoBJ AS noBj,
          b.IdBJ AS idJenis,
          mb.NamaBJ AS namaJenis,
          b.DateCreate AS dateCreate,
          b.DateUsage AS dateUsage,
          b.Jam AS jam,
          Pcs = CASE
                  WHEN ISNULL(b.Pcs, 0) - ISNULL(pa.PcsPartial, 0) < 0 THEN 0
                  ELSE ISNULL(b.Pcs, 0) - ISNULL(pa.PcsPartial, 0)
                END,
          b.Berat AS berat,
          b.IdWarehouse AS idWarehouse,
          b.CreateBy AS createBy,
          b.DateTimeCreate AS dateTimeCreate,
          b.Blok AS blok,
          b.IdLokasi AS idLokasi,
          CAST(0 AS bit) AS isPartial  -- ✅ ALWAYS 0 for Bongkar Susun
        FROM dbo.BarangJadi AS b WITH (NOLOCK)
        LEFT JOIN PartialAgg AS pa
          ON pa.NoBJ = b.NoBJ
        LEFT JOIN dbo.MstBarangJadi AS mb WITH (NOLOCK)
          ON mb.IdBJ = b.IdBJ
        WHERE b.NoBJ = @labelCode
          AND b.DateUsage IS NULL
          AND (ISNULL(b.Pcs, 0) - ISNULL(pa.PcsPartial, 0)) > 0
          AND ISNULL(pa.PcsPartial, 0) = 0  -- ✅ FILTER: belum pernah di-partial
        ORDER BY b.NoBJ;
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
          r.IdReject AS idJenis,
          mr.NamaReject AS namaJenis,
          r.DateCreate,
          r.DateUsage,
          r.IdWarehouse,
          CAST(r.Berat - ISNULL(ps.BeratPartial, 0) AS DECIMAL(18,2)) AS berat,
          r.Jam,
          r.CreateBy,
          r.DateTimeCreate,
          r.Blok,
          r.IdLokasi,
          CAST(0 AS bit) AS isPartial  -- ✅ ALWAYS 0 for Bongkar Susun
        FROM dbo.RejectV2 AS r WITH (NOLOCK)
        LEFT JOIN PartialSum AS ps
          ON ps.NoReject = r.NoReject
        LEFT JOIN dbo.MstReject AS mr WITH (NOLOCK)
          ON mr.IdReject = r.IdReject
        WHERE r.NoReject = @labelCode
          AND r.DateUsage IS NULL
          AND (r.Berat - ISNULL(ps.BeratPartial, 0)) > 0
          AND ISNULL(ps.BeratPartial, 0) = 0  -- ✅ FILTER: belum pernah di-partial
        ORDER BY r.NoReject;
      `;
      return await run(raw);

    default:
      throw new Error(
        `Invalid prefix: ${prefix}. Valid prefixes: A., B., D., M., F., V., H., BB., BA., BF.`,
      );
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

async function upsertInputs(noProduksi, payload, ctx) {
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
  return sharedInputService.upsertInputsAndPartials("bongkarSusun", no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

async function deleteInputs(noProduksi, payload, ctx) {
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
  return sharedInputService.deleteInputsAndPartials("bongkarSusun", no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

module.exports = {
  getByDate,
  getAllBongkarSusun,
  createBongkarSusun,
  updateBongkarSusunCascade,
  deleteBongkarSusun,
  fetchInputs,
  validateLabelBongkarSusun,
  upsertInputs,
  deleteInputs,
};

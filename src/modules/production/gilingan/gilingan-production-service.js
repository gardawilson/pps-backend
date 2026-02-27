// services/gilingan-production-service.js
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

  request.input("date", sql.Date, date);
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
    FROM dbo.GilinganProduksi_h h WITH (NOLOCK)
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
      CONVERT(VARCHAR(8), h.HourEnd,   108) AS HourEnd,

      -- (opsional utk FE)
      lc.LastClosedDate AS LastClosedDate,

      -- ✅ flag tutup transaksi (pakai kolom asli: h.Tanggal)
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.GilinganProduksi_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin    ms WITH (NOLOCK) ON ms.IdMesin    = h.IdMesin
    LEFT JOIN dbo.MstOperator op WITH (NOLOCK) ON op.IdOperator = h.IdOperator

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    -- rekomendasi: urut by tanggal + jam + no
    ORDER BY h.Tanggal DESC, h.Jam ASC, h.NoProduksi DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input("search", sql.VarChar(100), searchTerm);
  dataReq.input("offset", sql.Int, offset);
  dataReq.input("limit", sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

async function createGilinganProduksi(payload, ctx) {
  // ===============================
  // 0) Validasi payload basic
  // ===============================
  const body = payload && typeof payload === "object" ? payload : {};

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
  // 1) Validasi + normalisasi ctx (audit)
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
    // 2) Set SESSION_CONTEXT untuk trigger audit
    // =====================================================
    const auditReq = new sql.Request(tx);
    const audit = await applyAuditContext(auditReq, auditCtx);

    // =====================================================
    // 3) Guard tutup transaksi (CREATE = WRITE)
    // =====================================================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "create GilinganProduksi",
      useLock: true,
    });

    // =====================================================
    // 4) Generate NoProduksi via generateNextCode()
    //    Format: W.0000000001
    // =====================================================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.GilinganProduksi_h",
        columnName: "NoProduksi",
        prefix: "W.",
        width: 10,
      });

    let noProduksi = await gen();

    // anti-race (double check)
    const exist = await new sql.Request(tx).input(
      "NoProduksi",
      sql.VarChar(50),
      noProduksi,
    ).query(`
        SELECT 1
        FROM dbo.GilinganProduksi_h WITH (UPDLOCK, HOLDLOCK)
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
          FROM dbo.GilinganProduksi_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoProduksi = @NoProduksi
        `);

      if (exist2.recordset.length > 0) {
        throw conflict("Gagal generate NoProduksi unik, coba lagi.");
      }
      noProduksi = retry;
    }

    // =====================================================
    // 5) Insert header (OUTPUT ... INTO @out)
    // =====================================================
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoProduksi", sql.VarChar(50), noProduksi)
      .input("Tanggal", sql.Date, docDateOnly)
      .input("IdMesin", sql.Int, body.idMesin)
      .input("IdOperator", sql.Int, body.idOperator)
      .input("Shift", sql.Int, body.shift)
      .input("CreateBy", sql.VarChar(100), body.createBy) // controller overwrite
      .input("CheckBy1", sql.VarChar(100), body.checkBy1 ?? null)
      .input("CheckBy2", sql.VarChar(100), body.checkBy2 ?? null)
      .input("ApproveBy", sql.VarChar(100), body.approveBy ?? null)
      .input("JmlhAnggota", sql.Int, body.jmlhAnggota ?? null)
      .input("Hadir", sql.Int, body.hadir ?? null)
      .input("HourMeter", sql.Decimal(18, 2), body.hourMeter ?? null)
      .input("HourStart", sql.VarChar(20), body.hourStart ?? null)
      .input("HourEnd", sql.VarChar(20), body.hourEnd ?? null);

    const insertSql = `
      DECLARE @out TABLE (
        NoProduksi   varchar(50),
        Tanggal      date,
        IdMesin      int,
        IdOperator   int,
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

      INSERT INTO dbo.GilinganProduksi_h (
        NoProduksi,
        Tanggal,
        IdMesin,
        IdOperator,
        Shift,
        CreateBy,
        CheckBy1,
        CheckBy2,
        ApproveBy,
        JmlhAnggota,
        Hadir,
        HourMeter,
        HourStart,
        HourEnd
      )
      OUTPUT
        INSERTED.NoProduksi,
        INSERTED.Tanggal,
        INSERTED.IdMesin,
        INSERTED.IdOperator,
        INSERTED.Shift,
        INSERTED.CreateBy,
        INSERTED.CheckBy1,
        INSERTED.CheckBy2,
        INSERTED.ApproveBy,
        INSERTED.JmlhAnggota,
        INSERTED.Hadir,
        INSERTED.HourMeter,
        INSERTED.HourStart,
        INSERTED.HourEnd
      INTO @out
      VALUES (
        @NoProduksi,
        @Tanggal,
        @IdMesin,
        @IdOperator,
        @Shift,
        @CreateBy,
        @CheckBy1,
        @CheckBy2,
        @ApproveBy,
        @JmlhAnggota,
        @Hadir,
        @HourMeter,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd   IS NULL OR LTRIM(RTRIM(@HourEnd))   = '' THEN NULL ELSE CAST(@HourEnd   AS time(7)) END
      );

      SELECT * FROM @out;
    `;

    const insRes = await rqIns.query(insertSql);

    await tx.commit();

    return {
      header: insRes.recordset?.[0] || null,
      audit, // optional debug
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

/**
 * UPDATE header GilinganProduksi_h
 * - Tanpa kolom Jam
 * - Wajib kirim field utama, sama seperti create
 */
async function updateGilinganProduksi(noProduksi, payload, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) SET AUDIT CONTEXT (PERSIS CRUSHER / WASHING)
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
    // 1) LOAD DOC DATE (LOCK HEADER ROW)
    // =====================================================
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "gilinganProduksi",
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // =====================================================
    // 2) HANDLE TANGGAL
    // =====================================================
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq("tglProduksi tidak boleh kosong");
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // =====================================================
    // 3) GUARD TUTUP TRANSAKSI
    // =====================================================
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: "update GilinganProduksi (current date)",
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update GilinganProduksi (new date)",
        useLock: true,
      });
    }

    // =====================================================
    // 4) BUILD DYNAMIC SET
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

    if (sets.length === 0) throw badReq("No fields to update");

    rqUpd.input("NoProduksi", sql.VarChar(50), noProduksi);

    const sqlUpdate = `
      UPDATE dbo.GilinganProduksi_h
      SET ${sets.join(", ")}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.GilinganProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const resUpd = await rqUpd.query(sqlUpdate);
    const updatedHeader = resUpd.recordset?.[0] || null;

    // =====================================================
    // 5) SYNC DateUsage INPUT (KHUSUS GILINGAN)
    // =====================================================
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input("NoProduksi", sql.VarChar(50), noProduksi)
        .input("Tanggal", sql.Date, usageDate);

      await rqUsage.query(/* SQL SYNC DateUsage (tidak diubah) */);
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

async function deleteGilinganProduksi(noProduksi, ctx) {
  if (!noProduksi) throw badReq("noProduksi wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // =====================================================
    // 0) SET SESSION_CONTEXT (WAJIB untuk trigger audit)
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
    // 1) AMBIL docDateOnly DARI CONFIG (LOCK HEADER)
    // =====================================================
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "gilinganProduksi",
      codeValue: noProduksi,
      runner: tx,
      useLock: true, // DELETE = write
      throwIfNotFound: true,
    });

    // =====================================================
    // 2) GUARD TUTUP TRANSAKSI
    // =====================================================
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "delete GilinganProduksi",
      useLock: true,
    });

    // =====================================================
    // 3) DELETE INPUT + PARTIAL + RESET + DELETE HEADER
    // =====================================================
    const req = new sql.Request(tx);
    req.input("NoProduksi", sql.VarChar(50), noProduksi);

    const sqlDelete = `
      SET NOCOUNT ON;

      ---------------------------------------------------------
      -- TABLE VARIABLE UNTUK MENYIMPAN KEY YANG TERDAMPAK
      ---------------------------------------------------------
      DECLARE @BrokerKeys TABLE (NoBroker varchar(50), NoSak int);
      DECLARE @BrokerPartialKeys TABLE (NoBrokerPartial varchar(50));

      DECLARE @RejectKeys TABLE (NoReject varchar(50));
      DECLARE @RejectPartialKeys TABLE (NoRejectPartial varchar(50));

      ---------------------------------------------------------
      -- 1. BONGGOLAN
      ---------------------------------------------------------
      DELETE FROM dbo.GilinganProduksiInputBonggolan
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 2. BROKER (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @BrokerKeys (NoBroker, NoSak)
      SELECT DISTINCT b.NoBroker, b.NoSak
      FROM dbo.Broker_d b
      WHERE EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputBroker map
        WHERE map.NoProduksi = @NoProduksi
          AND map.NoBroker = b.NoBroker
          AND map.NoSak = b.NoSak
      )
      OR EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputBrokerPartial mp
        JOIN dbo.BrokerPartial bp
          ON bp.NoBrokerPartial = mp.NoBrokerPartial
        WHERE mp.NoProduksi = @NoProduksi
          AND bp.NoBroker = b.NoBroker
          AND bp.NoSak = b.NoSak
      );

      INSERT INTO @BrokerPartialKeys (NoBrokerPartial)
      SELECT DISTINCT mp.NoBrokerPartial
      FROM dbo.GilinganProduksiInputBrokerPartial mp
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.GilinganProduksiInputBrokerPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE bp
      FROM dbo.BrokerPartial bp
      JOIN @BrokerPartialKeys k
        ON k.NoBrokerPartial = bp.NoBrokerPartial
      WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputBrokerPartial mp2
        WHERE mp2.NoBrokerPartial = bp.NoBrokerPartial
      );

      DELETE FROM dbo.GilinganProduksiInputBroker
      WHERE NoProduksi = @NoProduksi;

      UPDATE b
      SET
        b.DateUsage = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputBroker mb
            WHERE mb.NoBroker = b.NoBroker
              AND mb.NoSak = b.NoSak
          ) THEN b.DateUsage
          WHEN EXISTS (
            SELECT 1
            FROM dbo.BrokerPartial bp2
            JOIN dbo.GilinganProduksiInputBrokerPartial mp2
              ON mp2.NoBrokerPartial = bp2.NoBrokerPartial
            WHERE bp2.NoBroker = b.NoBroker
              AND bp2.NoSak = b.NoSak
          ) THEN b.DateUsage
          ELSE NULL
        END,
        b.IsPartial = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.BrokerPartial bp3
            WHERE bp3.NoBroker = b.NoBroker
              AND bp3.NoSak = b.NoSak
          ) THEN 1 ELSE 0 END
      FROM dbo.Broker_d b
      JOIN @BrokerKeys k
        ON k.NoBroker = b.NoBroker
       AND k.NoSak = b.NoSak;

      ---------------------------------------------------------
      -- 3. CRUSHER (FULL ONLY)
      ---------------------------------------------------------
      UPDATE c
      SET c.DateUsage = CASE
        WHEN EXISTS (
          SELECT 1
          FROM dbo.GilinganProduksiInputCrusher m2
          WHERE m2.NoCrusher = c.NoCrusher
            AND m2.NoProduksi <> @NoProduksi
        ) THEN c.DateUsage
        ELSE NULL
      END
      FROM dbo.Crusher c
      JOIN dbo.GilinganProduksiInputCrusher map
        ON map.NoCrusher = c.NoCrusher
      WHERE map.NoProduksi = @NoProduksi;

      DELETE FROM dbo.GilinganProduksiInputCrusher
      WHERE NoProduksi = @NoProduksi;

      ---------------------------------------------------------
      -- 4. REJECT (FULL + PARTIAL)
      ---------------------------------------------------------
      INSERT INTO @RejectKeys (NoReject)
      SELECT DISTINCT r.NoReject
      FROM dbo.RejectV2 r
      WHERE EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputRejectV2 map
        WHERE map.NoProduksi = @NoProduksi
          AND map.NoReject = r.NoReject
      )
      OR EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputRejectV2Partial mp
        JOIN dbo.RejectV2Partial rp
          ON rp.NoRejectPartial = mp.NoRejectPartial
        WHERE mp.NoProduksi = @NoProduksi
          AND rp.NoReject = r.NoReject
      );

      INSERT INTO @RejectPartialKeys (NoRejectPartial)
      SELECT DISTINCT mp.NoRejectPartial
      FROM dbo.GilinganProduksiInputRejectV2Partial mp
      WHERE mp.NoProduksi = @NoProduksi;

      DELETE FROM dbo.GilinganProduksiInputRejectV2Partial
      WHERE NoProduksi = @NoProduksi;

      DELETE rp
      FROM dbo.RejectV2Partial rp
      JOIN @RejectPartialKeys k
        ON k.NoRejectPartial = rp.NoRejectPartial
      WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.GilinganProduksiInputRejectV2Partial mp2
        WHERE mp2.NoRejectPartial = rp.NoRejectPartial
      );

      DELETE FROM dbo.GilinganProduksiInputRejectV2
      WHERE NoProduksi = @NoProduksi;

      UPDATE r
      SET
        r.DateUsage = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.GilinganProduksiInputRejectV2 m2
            WHERE m2.NoReject = r.NoReject
              AND m2.NoProduksi <> @NoProduksi
          ) THEN r.DateUsage
          WHEN EXISTS (
            SELECT 1
            FROM dbo.RejectV2Partial rp2
            WHERE rp2.NoReject = r.NoReject
          ) THEN r.DateUsage
          ELSE NULL
        END,
        r.IsPartial = CASE
          WHEN EXISTS (
            SELECT 1
            FROM dbo.RejectV2Partial rp3
            WHERE rp3.NoReject = r.NoReject
          ) THEN 1 ELSE 0 END
      FROM dbo.RejectV2 r
      JOIN @RejectKeys k ON k.NoReject = r.NoReject;

      ---------------------------------------------------------
      -- 5. DELETE HEADER (TRIGGER AUDIT AKAN JALAN)
      ---------------------------------------------------------
      DELETE FROM dbo.GilinganProduksi_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const res = await req.query(sqlDelete);
    if (res.rowsAffected?.[res.rowsAffected.length - 1] === 0) {
      throw notFound(`NoProduksi tidak ditemukan: ${noProduksi}`);
    }

    await tx.commit();
    return { success: true };
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
    SELECT 
      'broker' AS Src,
      ib.NoProduksi,
      ib.NoBroker AS Ref1,
      ib.NoSak    AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      bd.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      bd.IsPartial AS IsPartial,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis,
      CAST(NULL AS datetime) AS DatetimeInput
    FROM dbo.GilinganProduksiInputBroker ib WITH (NOLOCK)
    LEFT JOIN dbo.Broker_d bd        WITH (NOLOCK)
      ON bd.NoBroker = ib.NoBroker AND bd.NoSak = ib.NoSak
    LEFT JOIN dbo.Broker_h bh        WITH (NOLOCK)
      ON bh.NoBroker = ib.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE ib.NoProduksi=@no

    UNION ALL
    SELECT
      'bonggolan' AS Src,
      ibg.NoProduksi,
      ibg.NoBonggolan AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,
      bg.Berat AS Berat,
      CAST(NULL AS decimal(18,3)) AS BeratAct,
      CAST(NULL AS bit) AS IsPartial,
      bg.IdBonggolan AS IdJenis,
      mbg.NamaBonggolan AS NamaJenis,
      ibg.DatetimeInput AS DatetimeInput
    FROM dbo.GilinganProduksiInputBonggolan ibg WITH (NOLOCK)
    /* TODO: sesuaikan tabel master/detail bonggolan anda */
    LEFT JOIN dbo.Bonggolan bg     WITH (NOLOCK) ON bg.NoBonggolan = ibg.NoBonggolan
    LEFT JOIN dbo.MstBonggolan mbg WITH (NOLOCK) ON mbg.IdBonggolan = bg.IdBonggolan
    WHERE ibg.NoProduksi=@no

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
      mc.NamaCrusher AS NamaJenis,
      ic.DatetimeInput AS DatetimeInput
    FROM dbo.GilinganProduksiInputCrusher ic WITH (NOLOCK)
    LEFT JOIN dbo.Crusher c     WITH (NOLOCK) ON c.NoCrusher = ic.NoCrusher
    LEFT JOIN dbo.MstCrusher mc WITH (NOLOCK) ON mc.IdCrusher = c.IdCrusher
    WHERE ic.NoProduksi=@no

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
      mr.NamaReject   AS NamaJenis,
      ir.DatetimeInput AS DatetimeInput
    FROM dbo.GilinganProduksiInputRejectV2 ir WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK) ON rj.NoReject = ir.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK) ON mr.IdReject = rj.IdReject
    WHERE ir.NoProduksi=@no
    ORDER BY Ref1 DESC, Ref2 ASC;

    /* ===================== [2] PARTIALS ===================== */

    /* Broker partial */
    SELECT
      pmap.NoBrokerPartial,
      pdet.NoBroker,
      pdet.NoSak,
      pdet.Berat,
      bh.IdJenisPlastik AS IdJenis,
      jp.Jenis          AS NamaJenis
    FROM dbo.GilinganProduksiInputBrokerPartial pmap WITH (NOLOCK)
    LEFT JOIN dbo.BrokerPartial pdet WITH (NOLOCK)
      ON pdet.NoBrokerPartial = pmap.NoBrokerPartial
    LEFT JOIN dbo.Broker_h bh WITH (NOLOCK)
      ON bh.NoBroker = pdet.NoBroker
    LEFT JOIN dbo.MstJenisPlastik jp WITH (NOLOCK)
      ON jp.IdJenisPlastik = bh.IdJenisPlastik
    WHERE pmap.NoProduksi = @no
    ORDER BY pmap.NoBrokerPartial DESC;

    /* Reject partial */
    SELECT
      rmap.NoRejectPartial,
      rdet.NoReject,
      rdet.Berat,
      rj.IdReject     AS IdJenis,
      mr.NamaReject   AS NamaJenis
    FROM dbo.GilinganProduksiInputRejectV2Partial rmap WITH (NOLOCK)
    LEFT JOIN dbo.RejectV2Partial rdet WITH (NOLOCK)
      ON rdet.NoRejectPartial = rmap.NoRejectPartial
    LEFT JOIN dbo.RejectV2 rj  WITH (NOLOCK)
      ON rj.NoReject = rdet.NoReject
    LEFT JOIN dbo.MstReject mr WITH (NOLOCK)
      ON mr.IdReject = rj.IdReject
    WHERE rmap.NoProduksi = @no
    ORDER BY rmap.NoRejectPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const brkPartial = rs.recordsets?.[1] || [];
  const rejPartial = rs.recordsets?.[2] || [];

  const out = {
    broker: [],
    bonggolan: [],
    crusher: [],
    reject: [],
    summary: { broker: 0, bonggolan: 0, crusher: 0, reject: 0 },
  };

  // MAIN rows
  for (const r of mainRows) {
    const base = {
      berat: r.Berat ?? null,
      beratAct: r.BeratAct ?? null,
      isPartial: r.IsPartial ?? null,
      idJenis: r.IdJenis ?? null,
      namaJenis: r.NamaJenis ?? null,
      datetimeInput: r.DatetimeInput ?? null,
    };

    switch (r.Src) {
      case "broker":
        out.broker.push({ noBroker: r.Ref1, noSak: r.Ref2, ...base });
        break;
      case "bonggolan":
        out.bonggolan.push({ noBonggolan: r.Ref1, ...base });
        break;
      case "crusher":
        out.crusher.push({ noCrusher: r.Ref1, ...base });
        break;
      case "reject":
        out.reject.push({ noReject: r.Ref1, ...base });
        break;
    }
  }

  // PARTIAL rows (merge into same bucket)
  for (const p of brkPartial) {
    out.broker.push({
      noBrokerPartial: p.NoBrokerPartial,
      noBroker: p.NoBroker ?? null,
      noSak: p.NoSak ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
    });
  }

  for (const p of rejPartial) {
    out.reject.push({
      noRejectPartial: p.NoRejectPartial,
      noReject: p.NoReject ?? null,
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
    SELECT DISTINCT
      o.NoProduksi,
      o.NoGilingan,
      ISNULL(g.HasBeenPrinted, 0) AS HasBeenPrinted
    FROM dbo.GilinganProduksiOutput o WITH (NOLOCK)
    LEFT JOIN dbo.Gilingan g WITH (NOLOCK)
      ON g.NoGilingan = o.NoGilingan
    WHERE o.NoProduksi = @no
    ORDER BY o.NoGilingan DESC;
  `;

  const rs = await req.query(q);
  return rs.recordset || [];
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
 * Payload shape:
 * {
 *   // existing inputs to attach
 *   broker:    [{ noBroker, noSak }],
 *   bonggolan: [{ noBonggolan, datetimeInput? }],
 *   crusher:   [{ noCrusher, datetimeInput? }],
 *   reject:    [{ noReject, datetimeInput? }],
 *
 *   // NEW partials to create + map
 *   brokerPartialNew: [{ noBroker, noSak, berat }],
 *   rejectPartialNew: [{ noReject, berat }]
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
    "gilinganProduksi",
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
    "gilinganProduksi",
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
  getProduksiByDate,
  getAllProduksi,
  createGilinganProduksi,
  updateGilinganProduksi,
  deleteGilinganProduksi,
  fetchInputs,
  fetchOutputs,
  validateLabel,
  upsertInputsAndPartials,
  deleteInputsAndPartials,
};

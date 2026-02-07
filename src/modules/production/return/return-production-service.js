// services/return-production-service.js
const { sql, poolPromise } = require("../../../core/config/db");
const {
  resolveEffectiveDateForCreate,
  assertNotLocked,
  loadDocDateOnlyFromConfig,
} = require("../../../core/shared/tutup-transaksi-guard");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require("../../../core/utils/jam-kerja-helper");
const { badReq } = require("../../../core/utils/http-error");

async function getAllReturns(
  page = 1,
  pageSize = 20,
  search = "",
  dateFrom = null,
  dateTo = null,
) {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = String(search || "").trim();

  // normalize date strings
  const df =
    typeof dateFrom === "string" && dateFrom.trim() ? dateFrom.trim() : null;
  const dt = typeof dateTo === "string" && dateTo.trim() ? dateTo.trim() : null;

  const whereClause = `
    WHERE 1=1
      AND (
        @search = ''
        OR h.NoRetur LIKE '%' + @search + '%'
        OR ISNULL(h.Invoice, '') LIKE '%' + @search + '%'
        OR ISNULL(h.NoBJSortir, '') LIKE '%' + @search + '%'
        OR ISNULL(p.NamaPembeli, '') LIKE '%' + @search + '%'
      )
      AND (@dateFrom IS NULL OR CONVERT(date, h.Tanggal) >= @dateFrom)
      AND (@dateTo   IS NULL OR CONVERT(date, h.Tanggal) <= @dateTo)
  `;

  // 1) Count
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.BJRetur_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstPembeli p WITH (NOLOCK)
      ON p.IdPembeli = h.IdPembeli
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input("search", sql.VarChar(100), searchTerm);
  countReq.input("dateFrom", sql.Date, df);
  countReq.input("dateTo", sql.Date, dt);

  const countRes = await countReq.query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) return { data: [], total: 0 };

  // 2) Data + lock flag
  const dataQry = `
    ;WITH LastClosed AS (
      SELECT TOP 1
        CONVERT(date, PeriodHarian) AS LastClosedDate
      FROM dbo.MstTutupTransaksiHarian WITH (NOLOCK)
      WHERE [Lock] = 1
      ORDER BY CONVERT(date, PeriodHarian) DESC, Id DESC
    )
    SELECT
      h.NoRetur,
      h.Invoice,
      h.Tanggal,
      h.IdPembeli,
      p.NamaPembeli,
      h.NoBJSortir,

      lc.LastClosedDate AS LastClosedDate,

      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.Tanggal) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.BJRetur_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstPembeli p WITH (NOLOCK)
      ON p.IdPembeli = h.IdPembeli

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    ORDER BY h.Tanggal DESC, h.NoRetur DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input("search", sql.VarChar(100), searchTerm);
  dataReq.input("dateFrom", sql.Date, df);
  dataReq.input("dateTo", sql.Date, dt);
  dataReq.input("offset", sql.Int, offset);
  dataReq.input("limit", sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}

async function getReturnsByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      h.NoRetur,
      h.Invoice,
      h.Tanggal,
      h.IdPembeli,
      p.NamaPembeli,
      h.NoBJSortir
    FROM [dbo].[BJRetur_h] h
    LEFT JOIN [dbo].[MstPembeli] p
      ON h.IdPembeli = p.IdPembeli
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.NoRetur ASC;
  `;

  request.input("date", sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}

async function createReturn(payload) {
  const must = [];
  if (!payload?.tanggal) must.push("tanggal");
  if (payload?.idPembeli == null) must.push("idPembeli");
  if (must.length) throw badReq(`Field wajib: ${must.join(", ")}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) normalize date + lock guard
    const effectiveDate = resolveEffectiveDateForCreate(payload.tanggal);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: "create BJRetur",
      useLock: true,
    });

    // 1) generate NoRetur
    // ✅ adjust prefix/width to your standard
    const no1 = await generateNextCode(tx, {
      tableName: "dbo.BJRetur_h",
      columnName: "NoRetur",
      prefix: "L.", // <--- change if needed
      width: 10,
    });

    // optional anti-race double check
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck.input("NoRetur", sql.VarChar(50), no1).query(`
        SELECT 1
        FROM dbo.BJRetur_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoRetur = @NoRetur
      `);

    const noRetur = exist.recordset.length
      ? await generateNextCode(tx, {
          tableName: "dbo.BJRetur_h",
          columnName: "NoRetur",
          prefix: "L.",
          width: 10,
        })
      : no1;

    // 2) insert header
    const rqIns = new sql.Request(tx);
    rqIns
      .input("NoRetur", sql.VarChar(50), noRetur)
      .input("Invoice", sql.VarChar(50), payload.invoice) // adjust length if needed
      .input("Tanggal", sql.Date, effectiveDate)
      .input("IdPembeli", sql.Int, payload.idPembeli)
      .input("NoBJSortir", sql.VarChar(50), payload.noBJSortir);

    // If your table has IdUsername, uncomment:
    // rqIns.input('IdUsername', sql.Int, payload.idUsername);

    const insertSql = `
      INSERT INTO dbo.BJRetur_h (
        NoRetur,
        Invoice,
        Tanggal,
        IdPembeli,
        NoBJSortir
        -- ,IdUsername
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoRetur,
        @Invoice,
        @Tanggal,
        @IdPembeli,
        @NoBJSortir
        -- ,@IdUsername
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

async function updateReturn(noRetur, payload) {
  if (!noRetur) throw badReq("noRetur wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock header + get old doc date
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "return", // ✅ ensure this exists in your config
      codeValue: noRetur, // ✅ NoRetur
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) determine date change
    const isChangingDate = payload?.tanggal !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tanggal) throw badReq("tanggal tidak boleh kosong");
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tanggal);
    }

    // 2) tutup transaksi guard (old and new if changed)
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: "update BJRetur (current date)",
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: "update BJRetur (new date)",
        useLock: true,
      });
    }

    // 3) dynamic SET (HEADER ONLY)
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push("Tanggal = @Tanggal");
      rqUpd.input("Tanggal", sql.Date, newDocDateOnly);
    }

    if (payload.invoice !== undefined) {
      const inv =
        payload.invoice === null ? null : String(payload.invoice || "").trim();
      sets.push("Invoice = @Invoice");
      rqUpd.input("Invoice", sql.VarChar(50), inv || null);
    }

    if (payload.idPembeli !== undefined) {
      if (payload.idPembeli == null)
        throw badReq("idPembeli tidak boleh kosong");
      sets.push("IdPembeli = @IdPembeli");
      rqUpd.input("IdPembeli", sql.Int, payload.idPembeli);
    }

    if (payload.noBJSortir !== undefined) {
      const nb =
        payload.noBJSortir === null
          ? null
          : String(payload.noBJSortir || "").trim();
      sets.push("NoBJSortir = @NoBJSortir");
      rqUpd.input("NoBJSortir", sql.VarChar(50), nb || null);
    }

    if (sets.length === 0) throw badReq("No fields to update");

    rqUpd.input("NoRetur", sql.VarChar(50), noRetur);

    const updateSql = `
      UPDATE dbo.BJRetur_h
      SET ${sets.join(", ")}
      WHERE NoRetur = @NoRetur;

      SELECT *
      FROM dbo.BJRetur_h
      WHERE NoRetur = @NoRetur;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;
    if (!updatedHeader) throw badReq(`NoRetur ${noRetur} tidak ditemukan`);

    // 4) if tanggal changed -> sync DateUsage for labels mapped by this NoRetur
    //    (details tables are used ONLY internally for sync)
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input("NoRetur", sql.VarChar(50), noRetur)
        .input("Tanggal", sql.Date, usageDate);

      await rqUsage.query(`
        /* =======================
           RETURN -> DateUsage Sync
           Rule: update only if DateUsage already exists (NOT NULL)
           ======================= */

        -- BARANG JADI (via BJReturBarangJadi_d)
        UPDATE bj
        SET bj.DateUsage = @Tanggal
        FROM dbo.BarangJadi AS bj
        WHERE bj.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.BJReturBarangJadi_d AS d
            WHERE d.NoRetur = @NoRetur
              AND d.NoBJ = bj.NoBJ
          );

        -- FURNITURE WIP (via BJReturFurnitureWIP_d)
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.BJReturFurnitureWIP_d AS d
            WHERE d.NoRetur = @NoRetur
              AND d.NoFurnitureWIP = fw.NoFurnitureWIP
          );
      `);
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

async function deleteReturn(noRetur) {
  if (!noRetur) throw badReq("noRetur wajib");

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) ambil docDateOnly dari config (lock header)
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: "return", // ✅ must exist in your config
      codeValue: noRetur, // ✅ NoRetur
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: "delete BJRetur",
      useLock: true,
    });

    // 2) cek detail input dulu (block delete if exists)
    const rqChk = new sql.Request(tx);
    rqChk.input("NoRetur", sql.VarChar(50), noRetur);

    const chkRes = await rqChk.query(`
      SELECT
        (SELECT COUNT(1) FROM dbo.BJReturBarangJadi_d WITH (NOLOCK) WHERE NoRetur = @NoRetur) AS CntBJ,
        (SELECT COUNT(1) FROM dbo.BJReturFurnitureWIP_d WITH (NOLOCK) WHERE NoRetur = @NoRetur) AS CntFWIP;
    `);

    const row = chkRes.recordset?.[0] || { CntBJ: 0, CntFWIP: 0 };
    const cntBJ = Number(row.CntBJ || 0);
    const cntFWIP = Number(row.CntFWIP || 0);

    if (cntBJ > 0 || cntFWIP > 0) {
      throw badReq(
        `Tidak dapat menghapus NoRetur ini karena masih memiliki detail input: ` +
          `Barang Jadi=${cntBJ}, FurnitureWIP=${cntFWIP}. ` +
          `Hapus detailnya terlebih dahulu.`,
      );
    }

    // 3) delete header (details already must be empty)
    const rqDel = new sql.Request(tx);
    rqDel.input("NoRetur", sql.VarChar(50), noRetur);

    await rqDel.query(`
      DELETE FROM dbo.BJRetur_h
      WHERE NoRetur = @NoRetur;
    `);

    await tx.commit();
    return { success: true };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

module.exports = {
  getAllReturns,
  getReturnsByDate,
  createReturn,
  updateReturn,
  deleteReturn,
};

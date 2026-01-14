// services/bj-jual-service.js
// services/bongkar-susun-service.js
const { sql, poolPromise } = require('../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
  loadDocDateOnlyFromConfig
} = require('../../core/shared/tutup-transaksi-guard');

const { generateNextCode } = require('../../core/utils/sequence-code-helper');
const { badReq } = require('../../core/utils/http-error');

const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
} = require('../../core/utils/jam-kerja-helper');


async function getAllBJJual(
  page = 1,
  pageSize = 20,
  search = '',
  dateFrom = null,
  dateTo = null
) {
  const pool = await poolPromise;

  const offset = (Math.max(page, 1) - 1) * Math.max(pageSize, 1);
  const s = String(search || '').trim();

  const rqCount = pool.request();
  const rqData = pool.request();

  // search
  rqCount.input('search', sql.VarChar(50), s);
  rqData.input('search', sql.VarChar(50), s);

  // optional dates (kalau null biarkan null)
  rqCount.input('dateFrom', sql.Date, dateFrom);
  rqCount.input('dateTo', sql.Date, dateTo);

  rqData.input('dateFrom', sql.Date, dateFrom);
  rqData.input('dateTo', sql.Date, dateTo);

  // paging
  rqData.input('offset', sql.Int, offset);
  rqData.input('pageSize', sql.Int, pageSize);

  const qWhere = `
    WHERE (@search = '' OR h.NoBJJual LIKE '%' + @search + '%')
      AND (@dateFrom IS NULL OR CONVERT(date, h.Tanggal) >= @dateFrom)
      AND (@dateTo   IS NULL OR CONVERT(date, h.Tanggal) <= @dateTo)
  `;

  const qCount = `
    SELECT COUNT(1) AS Total
    FROM dbo.BJJual_h h WITH (NOLOCK)
    ${qWhere};
  `;

  const qData = `
    SELECT
      h.NoBJJual,
      h.Tanggal,
      h.IdPembeli,
      p.NamaPembeli,
      h.Remark
    FROM dbo.BJJual_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstPembeli p WITH (NOLOCK)
      ON h.IdPembeli = p.IdPembeli
    ${qWhere}
    ORDER BY h.Tanggal DESC, h.NoBJJual DESC
    OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY;
  `;

  const countRes = await rqCount.query(qCount);
  const total = countRes.recordset?.[0]?.Total ?? 0;

  const dataRes = await rqData.query(qData);
  const data = dataRes.recordset || [];

  return { data, total };
}

async function createBJJual(payload) {
  const must = [];
  if (!payload?.tanggal) must.push('tanggal');
  if (payload?.idPembeli == null) must.push('idPembeli');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) normalize date + lock guard (same pattern as packing)
    const effectiveDate = resolveEffectiveDateForCreate(payload.tanggal);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create BJJual',
      useLock: true,
    });

    // 1) generate NoBJJual (choose your prefix!)
    // Example: BJ.0000000123 (adjust to your real format)
    const no1 = await generateNextCode(tx, {
      tableName: 'dbo.BJJual_h',
      columnName: 'NoBJJual',
      prefix: 'K.',
      width: 10,
    });

    // optional anti-race double check
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBJJual', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.BJJual_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJJual = @NoBJJual
      `);

    const noBJJual = exist.recordset.length
      ? await generateNextCode(tx, {
          tableName: 'dbo.BJJual_h',
          columnName: 'NoBJJual',
          prefix: 'K.',
          width: 10,
        })
      : no1;

    // 2) insert header
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoBJJual', sql.VarChar(50), noBJJual)
      .input('Tanggal', sql.Date, effectiveDate)
      .input('IdPembeli', sql.Int, payload.idPembeli)
      .input('Remark', sql.VarChar(255), payload.remark ?? null);

    const insertSql = `
      INSERT INTO dbo.BJJual_h (
        NoBJJual, Tanggal, IdPembeli, Remark
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoBJJual, @Tanggal, @IdPembeli, @Remark
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


async function updateBJJual(noBJJual, payload) {
  if (!noBJJual) throw badReq('noBJJual wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock header + ambil tanggal lama
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'bjJual',      // ✅ pastikan ada di config entityKey
      codeValue: noBJJual,      // ✅ NoBJJual
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) kalau user kirim tanggal -> berarti mau ubah tanggal
    const isChangingDate = payload?.tanggal !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tanggal) throw badReq('tanggal tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tanggal);
    }

    // 2) guard tutup transaksi
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: 'update BJJual (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update BJJual (new date)',
        useLock: true,
      });
    }

    // 3) dynamic SET
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('Tanggal = @Tanggal');
      rqUpd.input('Tanggal', sql.Date, newDocDateOnly);
    }

    if (payload.idPembeli !== undefined) {
      if (payload.idPembeli == null) throw badReq('idPembeli tidak boleh kosong');
      sets.push('IdPembeli = @IdPembeli');
      rqUpd.input('IdPembeli', sql.Int, payload.idPembeli);
    }

    if (payload.remark !== undefined) {
      sets.push('Remark = @Remark');
      rqUpd.input('Remark', sql.VarChar(255), payload.remark ?? null);
    }

    if (sets.length === 0) throw badReq('No fields to update');

    rqUpd.input('NoBJJual', sql.VarChar(50), noBJJual);

    const updateSql = `
      UPDATE dbo.BJJual_h
      SET ${sets.join(', ')}
      WHERE NoBJJual = @NoBJJual;

      SELECT *
      FROM dbo.BJJual_h
      WHERE NoBJJual = @NoBJJual;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // 4) kalau tanggal berubah -> sync DateUsage untuk input label
    if (isChangingDate && updatedHeader) {
      const newUsageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoBJJual', sql.VarChar(50), noBJJual)
        .input('OldTanggal', sql.Date, oldDocDateOnly)
        .input('NewTanggal', sql.Date, newUsageDate);

const sqlUpdateUsage = `
  /* =======================
     BJ JUAL -> DateUsage Sync
     ======================= */

  /* A) BARANG JADI (FULL) */
  UPDATE bj
  SET bj.DateUsage = @NewTanggal
  FROM dbo.BarangJadi bj
  WHERE EXISTS (
    SELECT 1
    FROM dbo.BJJual_dLabelBarangJadi map
    WHERE map.NoBJJual = @NoBJJual
      AND map.NoBJ = bj.NoBJ
  )
  AND (bj.DateUsage IS NULL OR CONVERT(date, bj.DateUsage) = @OldTanggal);

  /* B) BARANG JADI (PARTIAL)
     Asumsi: NoBJPartial = BarangJadi.NoBJ (IsPartial=1)  ✅ kalau beda tabel, nanti kita ubah joinnya
  */
  UPDATE bj
  SET bj.DateUsage = @NewTanggal
  FROM dbo.BarangJadi bj
  WHERE EXISTS (
    SELECT 1
    FROM dbo.BJJual_dLabelBarangJadiPartial mp
    WHERE mp.NoBJJual = @NoBJJual
      AND mp.NoBJPartial = bj.NoBJ
  )
  AND (bj.DateUsage IS NULL OR CONVERT(date, bj.DateUsage) = @OldTanggal);

  /* C) FURNITURE WIP (FULL) */
  UPDATE fw
  SET fw.DateUsage = @NewTanggal
  FROM dbo.FurnitureWIP fw
  WHERE EXISTS (
    SELECT 1
    FROM dbo.BJJual_dLabelFurnitureWIP mf
    WHERE mf.NoBJJual = @NoBJJual
      AND mf.NoFurnitureWIP = fw.NoFurnitureWIP
  )
  AND (fw.DateUsage IS NULL OR CONVERT(date, fw.DateUsage) = @OldTanggal);

  /* D) FURNITURE WIP (PARTIAL) */
  UPDATE fw
  SET fw.DateUsage = @NewTanggal
  FROM dbo.FurnitureWIP AS fw
  WHERE EXISTS (
    SELECT 1
    FROM dbo.BJJual_dLabelFurnitureWIPPartial AS mp
    JOIN dbo.FurnitureWIPPartial AS fwp
      ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
    WHERE mp.NoBJJual = @NoBJJual
      AND fwp.NoFurnitureWIP = fw.NoFurnitureWIP
  )
  AND (fw.DateUsage IS NULL OR CONVERT(date, fw.DateUsage) = @OldTanggal);
`;


      await rqUsage.query(sqlUpdateUsage);
    }

    await tx.commit();
    return { header: updatedHeader };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}


async function deleteBJJual(noBJJual) {
  if (!noBJJual) throw badReq('noBJJual wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock header + get doc date (untuk guard)
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'bjJual',     // ✅ pastikan ada di config
      codeValue: noBJJual,     // ✅ NoBJJual
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete BJJual',
      useLock: true,
    });

    // 2) delete mappings + reset usage + delete header
    const req = new sql.Request(tx);
    req.input('NoBJJual', sql.VarChar(50), noBJJual);

    const sqlDelete = `
      /* ===================================================
         BJ JUAL DELETE
         - reset DateUsage on BarangJadi + FurnitureWIP
         - delete mapping tables + cabinet material
         - delete header last
         =================================================== */

      DECLARE @BJKeys TABLE (NoBJ varchar(50) PRIMARY KEY);
      DECLARE @FWIPKeys TABLE (NoFurnitureWIP varchar(50) PRIMARY KEY);

      /* =======================
         A) collect BJ keys (FULL)
         ======================= */
      INSERT INTO @BJKeys (NoBJ)
      SELECT DISTINCT d.NoBJ
      FROM dbo.BJJual_dLabelBarangJadi d
      WHERE d.NoBJJual = @NoBJJual
        AND d.NoBJ IS NOT NULL;

      /* =======================
         B) collect BJ keys (PARTIAL)
         NOTE: asumsi NoBJPartial = BarangJadi.NoBJ (IsPartial=1)
         ======================= */
      INSERT INTO @BJKeys (NoBJ)
      SELECT DISTINCT p.NoBJPartial
      FROM dbo.BJJual_dLabelBarangJadiPartial p
      WHERE p.NoBJJual = @NoBJJual
        AND p.NoBJPartial IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM @BJKeys k WHERE k.NoBJ = p.NoBJPartial);

      /* =======================
         C) collect FWIP keys (FULL)
         ======================= */
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT f.NoFurnitureWIP
      FROM dbo.BJJual_dLabelFurnitureWIP f
      WHERE f.NoBJJual = @NoBJJual
        AND f.NoFurnitureWIP IS NOT NULL;

      /* =======================
         D) collect FWIP keys (PARTIAL -> FurnitureWIPPartial)
         ======================= */
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.BJJual_dLabelFurnitureWIPPartial fp
      JOIN dbo.FurnitureWIPPartial fwp
        ON fwp.NoFurnitureWIPPartial = fp.NoFurnitureWIPPartial
      WHERE fp.NoBJJual = @NoBJJual
        AND fwp.NoFurnitureWIP IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM @FWIPKeys k WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP);

      /* =======================
         E) reset DateUsage BarangJadi
         ======================= */
      UPDATE bj
      SET bj.DateUsage = NULL
      FROM dbo.BarangJadi bj
      JOIN @BJKeys k
        ON k.NoBJ = bj.NoBJ;

      /* =======================
         F) reset DateUsage FurnitureWIP + recalc IsPartial
         ======================= */
      UPDATE fw
      SET fw.DateUsage = NULL,
          fw.IsPartial = CASE
            WHEN EXISTS (
              SELECT 1
              FROM dbo.FurnitureWIPPartial p
              WHERE p.NoFurnitureWIP = fw.NoFurnitureWIP
            ) THEN 1 ELSE 0 END
      FROM dbo.FurnitureWIP fw
      JOIN @FWIPKeys k
        ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      /* =======================
         G) delete BJ Jual inputs (detail tables)
         ======================= */
      DELETE FROM dbo.BJJualCabinetMaterial_d
      WHERE NoBJJual = @NoBJJual;

      DELETE FROM dbo.BJJual_dLabelBarangJadiPartial
      WHERE NoBJJual = @NoBJJual;

      DELETE FROM dbo.BJJual_dLabelBarangJadi
      WHERE NoBJJual = @NoBJJual;

      DELETE FROM dbo.BJJual_dLabelFurnitureWIPPartial
      WHERE NoBJJual = @NoBJJual;

      DELETE FROM dbo.BJJual_dLabelFurnitureWIP
      WHERE NoBJJual = @NoBJJual;

      /* =======================
         H) delete header last
         ======================= */
      DELETE FROM dbo.BJJual_h
      WHERE NoBJJual = @NoBJJual;
    `;

    await req.query(sqlDelete);

    await tx.commit();
    return { success: true };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}

async function fetchInputs(noBJJual) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noBJJual);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */

    -- Barang Jadi FULL
    SELECT
      'bj' AS Src,
      map.NoBJJual,
      map.NoBJ AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      bj.Berat,
      bj.Pcs,
      bj.IsPartial,
      bj.IdBJ               AS IdJenis,
      mbj.NamaBJ            AS NamaJenis,
      uom.NamaUOM           AS NamaUOM,
      bj.DateTimeCreate     AS DatetimeInput
    FROM dbo.BJJual_dLabelBarangJadi map WITH (NOLOCK)
    LEFT JOIN dbo.BarangJadi bj WITH (NOLOCK)
      ON bj.NoBJ = map.NoBJ
    LEFT JOIN dbo.MstBarangJadi mbj WITH (NOLOCK)
      ON mbj.IdBJ = bj.IdBJ
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mbj.IdUOM
    WHERE map.NoBJJual = @no

    UNION ALL

    -- FurnitureWIP FULL
    SELECT
      'fwip' AS Src,
      map.NoBJJual,
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
    FROM dbo.BJJual_dLabelFurnitureWIP map WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = map.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = fw.IDFurnitureWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE map.NoBJJual = @no

    UNION ALL

    -- Cabinet Material (BJ Jual)
    SELECT
      'material' AS Src,
      cm.NoBJJual,
      CAST(cm.IdCabinetMaterial AS varchar(50)) AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      CAST(NULL AS decimal(18,3)) AS Berat,
      CAST(cm.Pcs AS int)         AS Pcs,
      CAST(NULL AS bit)           AS IsPartial,
      CAST(NULL AS int)           AS IdJenis,
      mm.Nama                     AS NamaJenis,
      uom.NamaUOM                 AS NamaUOM,
      CAST(NULL AS datetime)      AS DatetimeInput
    FROM dbo.BJJualCabinetMaterial_d cm WITH (NOLOCK)
    LEFT JOIN dbo.MstCabinetMaterial mm WITH (NOLOCK)
      ON mm.IdCabinetMaterial = cm.IdCabinetMaterial
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mm.IdUOM
    WHERE cm.NoBJJual = @no

    ORDER BY Ref1 DESC, Ref2 ASC;

    /* ===================== [2] PARTIAL BJ ===================== */
    SELECT
      mp.NoBJPartial,
      bjp.NoBJ,
      bjp.Pcs            AS PcsPartial,

      bj.Pcs             AS PcsHeader,
      bj.Berat,
      bj.IdBJ            AS IdJenis,
      mbj.NamaBJ         AS NamaJenis,
      uom.NamaUOM        AS NamaUOM,
      bj.DateTimeCreate  AS DatetimeInput
    FROM dbo.BJJual_dLabelBarangJadiPartial mp WITH (NOLOCK)
    LEFT JOIN dbo.BarangJadiPartial bjp WITH (NOLOCK)
      ON bjp.NoBJPartial = mp.NoBJPartial
    LEFT JOIN dbo.BarangJadi bj WITH (NOLOCK)
      ON bj.NoBJ = bjp.NoBJ
    LEFT JOIN dbo.MstBarangJadi mbj WITH (NOLOCK)
      ON mbj.IdBJ = bj.IdBJ
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mbj.IdUOM
    WHERE mp.NoBJJual = @no
    ORDER BY mp.NoBJPartial DESC;

    /* ===================== [3] PARTIAL FWIP ===================== */
    SELECT
      mp.NoFurnitureWIPPartial,
      fwp.NoFurnitureWIP,
      fwp.Pcs            AS PcsPartial,
      fw.Pcs             AS PcsHeader,
      fw.Berat,
      fw.IDFurnitureWIP  AS IdJenis,
      mw.Nama            AS NamaJenis,
      uom.NamaUOM        AS NamaUOM
    FROM dbo.BJJual_dLabelFurnitureWIPPartial mp WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIPPartial fwp WITH (NOLOCK)
      ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = fwp.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = fw.IDFurnitureWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE mp.NoBJJual = @no
    ORDER BY mp.NoFurnitureWIPPartial DESC;
  `;

  const rs = await req.query(q);

  const mainRows = rs.recordsets?.[0] || [];
  const bjPartialRows = rs.recordsets?.[1] || [];
  const fwipPartialRows = rs.recordsets?.[2] || [];

  const out = {
    barangJadi: [],
    furnitureWip: [],
    cabinetMaterial: [],
    summary: { barangJadi: 0, furnitureWip: 0, cabinetMaterial: 0 },
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
      case 'bj':
        out.barangJadi.push({
          noBJ: r.Ref1,
          ...base,
        });
        break;

      case 'fwip':
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          ...base,
        });
        break;

      case 'material':
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1, // string cast (konsisten)
          pcs: r.Pcs ?? null,        // kalau mau samakan dgn packing: rename ke "jumlah"
          ...base,
        });
        break;
    }
  }

  // PARTIAL BJ (merge into barangJadi bucket)
  for (const p of bjPartialRows) {
    out.barangJadi.push({
      noBJPartial: p.NoBJPartial,     // BL...
      noBJ: p.NoBJ ?? null,           // BA... (header)
      pcs: p.PcsPartial ?? null,      // pcs partial (BarangJadiPartial)
      pcsHeader: p.PcsHeader ?? null, // pcs header (BarangJadi)
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
      namaUom: p.NamaUOM ?? null,
      isPartial: true,
      isPartialRow: true,
      datetimeInput: p.DatetimeInput ?? null,
    });
  }

  // PARTIAL FWIP (merge into furnitureWip bucket)
  for (const p of fwipPartialRows) {
    out.furnitureWip.push({
      noFurnitureWipPartial: p.NoFurnitureWIPPartial,
      noFurnitureWip: p.NoFurnitureWIP ?? null,
      pcs: p.PcsPartial ?? null,
      pcsHeader: p.PcsHeader ?? null,
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
      namaUom: p.NamaUOM ?? null,
      isPartial: true,
      isPartialRow: true,
    });
  }

  out.summary.barangJadi = out.barangJadi.length;
  out.summary.furnitureWip = out.furnitureWip.length;
  out.summary.cabinetMaterial = out.cabinetMaterial.length;

  return out;
}


/**
 * Payload shape (arrays optional):
 * {
 *   barangJadi:            [{ noBJ }],
 *   furnitureWip:          [{ noFurnitureWip }],
 *   cabinetMaterial:       [{ idCabinetMaterial, pcs }],
 *
 *   barangJadiPartialNew:  [{ noBJ, pcs }],
 *   furnitureWipPartialNew:[{ noFurnitureWip, pcs }]
 * }
 */
async function upsertInputsAndPartials(noBJJual, payload) {
  if (!noBJJual) throw badReq('noBJJual wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    barangJadi: norm(payload?.barangJadi),
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),

    barangJadiPartialNew: norm(payload?.barangJadiPartialNew),
    furnitureWipPartialNew: norm(payload?.furnitureWipPartialNew),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) lock header & get doc date
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'bjJual', // ✅ must match config key tutup-transaksi
      codeValue: noBJJual,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'upsert BJ Jual inputs/partials',
      useLock: true,
    });

    // 2) create partial + mapping
    const partials = await _insertPartialsWithTx(tx, noBJJual, {
      barangJadiPartialNew: body.barangJadiPartialNew,
      furnitureWipPartialNew: body.furnitureWipPartialNew,
    });

    // 3) attach existing inputs
    const bjAttach = await _insertBarangJadiWithTx(tx, noBJJual, {
      barangJadi: body.barangJadi,
    });

    const fwipAttach = await _insertFurnitureWipWithTx(tx, noBJJual, {
      furnitureWip: body.furnitureWip,
    });

    const matAttach = await _insertCabinetMaterialWithTx(tx, noBJJual, {
      cabinetMaterial: body.cabinetMaterial,
    });

    const attachments = {
      barangJadi: bjAttach.barangJadi,
      furnitureWip: fwipAttach.furnitureWip,
      cabinetMaterial: matAttach.cabinetMaterial,
    };

    await tx.commit();

    // ===== summary =====
    const totalInserted = Object.values(attachments).reduce(
      (sum, x) => sum + (x.inserted || 0),
      0
    );
    const totalUpdated = Object.values(attachments).reduce(
      (sum, x) => sum + (x.updated || 0),
      0
    );
    const totalSkipped = Object.values(attachments).reduce(
      (sum, x) => sum + (x.skipped || 0),
      0
    );
    const totalInvalid = Object.values(attachments).reduce(
      (sum, x) => sum + (x.invalid || 0),
      0
    );

    const totalPartialsCreated = Object.values(partials.summary || {}).reduce(
      (sum, item) => sum + (item.created || 0),
      0
    );

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess =
      totalInserted + totalUpdated === 0 && totalPartialsCreated === 0;

    const response = {
      noBJJual,
      summary: {
        totalInserted,
        totalUpdated,
        totalSkipped,
        totalInvalid,
        totalPartialsCreated,
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
    } catch (_) {}
    throw err;
  }
}

/* =====================
   Details builders
===================== */

function _buildInputDetails(attachments, requestBody) {
  const details = [];

  const sections = [
    { key: 'barangJadi', label: 'Barang Jadi' },
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result =
      attachments[section.key] || { inserted: 0, updated: 0, skipped: 0, invalid: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      inserted: result.inserted,
      updated: result.updated,
      skipped: result.skipped,
      invalid: result.invalid,
      status:
        result.invalid > 0 ? 'error' : result.skipped > 0 ? 'warning' : 'success',
      message: _buildSectionMessage(section.label, result),
    });
  }

  return details;
}

function _buildPartialDetails(partials, requestBody) {
  const details = [];

  const sections = [
    { key: 'barangJadiPartialNew', label: 'Barang Jadi Partial' },
    { key: 'furnitureWipPartialNew', label: 'Furniture WIP Partial' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const created = partials.summary?.[section.key]?.created || 0;

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      created,
      status: created === requestedCount ? 'success' : created > 0 ? 'warning' : 'error',
      message: `${created} dari ${requestedCount} ${section.label} berhasil dibuat`,
      codes: partials.createdLists?.[section.key] || [],
    });
  }

  return details;
}

function _buildSectionMessage(label, result) {
  const parts = [];
  if (result.inserted > 0) parts.push(`${result.inserted} berhasil ditambahkan`);
  if (result.updated > 0) parts.push(`${result.updated} berhasil diperbarui`);
  if (result.skipped > 0) parts.push(`${result.skipped} sudah ada (dilewati)`);
  if (result.invalid > 0) parts.push(`${result.invalid} tidak valid`);
  return parts.length ? `${label}: ${parts.join(', ')}` : `Tidak ada ${label} yang diproses`;
}

/* =====================
   SQL helpers (BJ Jual tables)
===================== */

async function _insertPartialsWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Created int);

  DECLARE @createdBJP TABLE(NoBJPartial varchar(50));
  DECLARE @createdFWP TABLE(NoFurnitureWIPPartial varchar(50));

  /* =======================
     A) Barang Jadi Partial New (BL.##########)
     ======================= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.barangJadiPartialNew'))
  BEGIN
    DECLARE @nextBJP int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoBJPartial, 10) AS int))
      FROM dbo.BarangJadiPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoBJPartial LIKE 'BL.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noBJ,
        pcs,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.barangJadiPartialNew')
      WITH (
        noBJ varchar(50) '$.noBJ',
        pcs int '$.pcs'
      )
      WHERE NULLIF(noBJ,'') IS NOT NULL
        AND ISNULL(pcs,0) > 0
        AND EXISTS (
          SELECT 1 FROM dbo.BarangJadi b WITH (NOLOCK)
          WHERE b.NoBJ = noBJ
            AND b.DateUsage IS NULL
        )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('BL.', RIGHT(REPLICATE('0',10) + CAST(@nextBJP + rn AS varchar(10)), 10)),
        noBJ,
        pcs
      FROM src
    )
    INSERT INTO dbo.BarangJadiPartial (NoBJPartial, NoBJ, Pcs)
    OUTPUT INSERTED.NoBJPartial INTO @createdBJP(NoBJPartial)
    SELECT NewNo, noBJ, pcs
    FROM numbered;

    INSERT INTO dbo.BJJual_dLabelBarangJadiPartial (NoBJJual, NoBJPartial)
    SELECT @no, c.NoBJPartial
    FROM @createdBJP c
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.BJJual_dLabelBarangJadiPartial x WITH (NOLOCK)
      WHERE x.NoBJJual=@no AND x.NoBJPartial=c.NoBJPartial
    );

    DECLARE @insBJP int = @@ROWCOUNT;

    IF @insBJP > 0
    BEGIN
      UPDATE b
      SET b.IsPartial = 1,
          b.DateUsage = @tgl
      FROM dbo.BarangJadi b
      WHERE EXISTS (
        SELECT 1
        FROM dbo.BarangJadiPartial bp WITH (NOLOCK)
        JOIN @createdBJP n ON n.NoBJPartial = bp.NoBJPartial
        WHERE bp.NoBJ = b.NoBJ
      );
    END

    INSERT INTO @out SELECT 'barangJadiPartialNew', @insBJP;
  END

  /* =======================
     B) Furniture WIP Partial New (BC.##########)
     ======================= */
  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew'))
  BEGIN
    DECLARE @nextFWP int = ISNULL((
      SELECT MAX(TRY_CAST(RIGHT(NoFurnitureWIPPartial, 10) AS int))
      FROM dbo.FurnitureWIPPartial WITH (UPDLOCK, HOLDLOCK)
      WHERE NoFurnitureWIPPartial LIKE 'BC.%'
    ), 0);

    ;WITH src AS (
      SELECT
        noFurnitureWip,
        pcs,
        ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn
      FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew')
      WITH (
        noFurnitureWip varchar(50) '$.noFurnitureWip',
        pcs int '$.pcs'
      )
      WHERE NULLIF(noFurnitureWip, '') IS NOT NULL
        AND ISNULL(pcs, 0) > 0
        AND EXISTS (
          SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK)
          WHERE f.NoFurnitureWIP = noFurnitureWip
            AND f.DateUsage IS NULL
        )
    ),
    numbered AS (
      SELECT
        NewNo = CONCAT('BC.', RIGHT(REPLICATE('0',10) + CAST(@nextFWP + rn AS varchar(10)), 10)),
        noFurnitureWip,
        pcs
      FROM src
    )
    INSERT INTO dbo.FurnitureWIPPartial (NoFurnitureWIPPartial, NoFurnitureWIP, Pcs)
    OUTPUT INSERTED.NoFurnitureWIPPartial INTO @createdFWP(NoFurnitureWIPPartial)
    SELECT NewNo, noFurnitureWip, pcs
    FROM numbered;

    INSERT INTO dbo.BJJual_dLabelFurnitureWIPPartial (NoBJJual, NoFurnitureWIPPartial)
    SELECT @no, c.NoFurnitureWIPPartial
    FROM @createdFWP c
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.BJJual_dLabelFurnitureWIPPartial x WITH (NOLOCK)
      WHERE x.NoBJJual=@no AND x.NoFurnitureWIPPartial=c.NoFurnitureWIPPartial
    );

    DECLARE @insFWP int = @@ROWCOUNT;

    IF @insFWP > 0
    BEGIN
      UPDATE f
      SET f.IsPartial = 1,
          f.DateUsage = @tgl
      FROM dbo.FurnitureWIP f
      WHERE EXISTS (
        SELECT 1
        FROM dbo.FurnitureWIPPartial fp WITH (NOLOCK)
        JOIN @createdFWP n ON n.NoFurnitureWIPPartial = fp.NoFurnitureWIPPartial
        WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP
      );
    END

    INSERT INTO @out SELECT 'furnitureWipPartialNew', @insFWP;
  END

  SELECT Section, Created FROM @out;
  SELECT NoBJPartial FROM @createdBJP;
  SELECT NoFurnitureWIPPartial FROM @createdFWP;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  const createdLists = {
    barangJadiPartialNew: (rs.recordsets?.[1] || []).map((r) => r.NoBJPartial),
    furnitureWipPartialNew: (rs.recordsets?.[2] || []).map((r) => r.NoFurnitureWIPPartial),
  };

  return { summary, createdLists };
}

async function _insertBarangJadiWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @bjIns int=0, @bjSkp int=0, @bjInv int=0;

  DECLARE @reqBJ TABLE(NoBJ varchar(50));
  DECLARE @alreadyMapped TABLE(NoBJ varchar(50));
  DECLARE @eligibleNotMapped TABLE(NoBJ varchar(50));
  DECLARE @invalid TABLE(NoBJ varchar(50));
  DECLARE @insBJ TABLE(NoBJ varchar(50));

  INSERT INTO @reqBJ(NoBJ)
  SELECT DISTINCT noBJ
  FROM OPENJSON(@jsInputs, '$.barangJadi')
  WITH ( noBJ varchar(50) '$.noBJ' )
  WHERE NULLIF(noBJ,'') IS NOT NULL;

  INSERT INTO @alreadyMapped(NoBJ)
  SELECT r.NoBJ
  FROM @reqBJ r
  WHERE EXISTS (
    SELECT 1 FROM dbo.BJJual_dLabelBarangJadi x WITH (NOLOCK)
    WHERE x.NoBJJual=@no AND x.NoBJ=r.NoBJ
  );

  INSERT INTO @eligibleNotMapped(NoBJ)
  SELECT r.NoBJ
  FROM @reqBJ r
  WHERE NOT EXISTS (SELECT 1 FROM @alreadyMapped a WHERE a.NoBJ=r.NoBJ)
    AND EXISTS (
      SELECT 1 FROM dbo.BarangJadi b WITH (NOLOCK)
      WHERE b.NoBJ=r.NoBJ
        AND b.DateUsage IS NULL
    );

  INSERT INTO @invalid(NoBJ)
  SELECT r.NoBJ
  FROM @reqBJ r
  WHERE NOT EXISTS (SELECT 1 FROM @alreadyMapped a WHERE a.NoBJ=r.NoBJ)
    AND NOT EXISTS (SELECT 1 FROM @eligibleNotMapped e WHERE e.NoBJ=r.NoBJ);

  INSERT INTO dbo.BJJual_dLabelBarangJadi (NoBJJual, NoBJ)
  OUTPUT INSERTED.NoBJ INTO @insBJ(NoBJ)
  SELECT @no, e.NoBJ
  FROM @eligibleNotMapped e;

  SET @bjIns = @@ROWCOUNT;

  IF @bjIns > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = @tgl
    FROM dbo.BarangJadi b
    JOIN @insBJ i ON i.NoBJ=b.NoBJ;
  END

  SELECT @bjSkp = COUNT(*) FROM @alreadyMapped;
  SELECT @bjInv = COUNT(*) FROM @invalid;

  SELECT
    @bjIns AS Inserted,
    0 AS Updated,
    @bjSkp AS Skipped,
    @bjInv AS Invalid;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    barangJadi: {
      inserted: row.Inserted || 0,
      updated: row.Updated || 0,
      skipped: row.Skipped || 0,
      invalid: row.Invalid || 0,
    },
  };
}

async function _insertFurnitureWipWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @fwIns int=0, @fwSkp int=0, @fwInv int=0;

  DECLARE @reqFW TABLE(NoFurnitureWip varchar(50));
  DECLARE @alreadyMapped TABLE(NoFurnitureWip varchar(50));
  DECLARE @eligibleNotMapped TABLE(NoFurnitureWip varchar(50));
  DECLARE @invalid TABLE(NoFurnitureWip varchar(50));
  DECLARE @insFW TABLE(NoFurnitureWIP varchar(50));

  INSERT INTO @reqFW(NoFurnitureWip)
  SELECT DISTINCT noFurnitureWip
  FROM OPENJSON(@jsInputs, '$.furnitureWip')
  WITH ( noFurnitureWip varchar(50) '$.noFurnitureWip' )
  WHERE NULLIF(noFurnitureWip,'') IS NOT NULL;

  INSERT INTO @alreadyMapped(NoFurnitureWip)
  SELECT r.NoFurnitureWip
  FROM @reqFW r
  WHERE EXISTS (
    SELECT 1 FROM dbo.BJJual_dLabelFurnitureWIP x WITH (NOLOCK)
    WHERE x.NoBJJual=@no AND x.NoFurnitureWIP=r.NoFurnitureWip
  );

  INSERT INTO @eligibleNotMapped(NoFurnitureWip)
  SELECT r.NoFurnitureWip
  FROM @reqFW r
  WHERE NOT EXISTS (SELECT 1 FROM @alreadyMapped a WHERE a.NoFurnitureWip=r.NoFurnitureWip)
    AND EXISTS (
      SELECT 1 FROM dbo.FurnitureWIP f WITH (NOLOCK)
      WHERE f.NoFurnitureWIP=r.NoFurnitureWip
        AND f.DateUsage IS NULL
    );

  INSERT INTO @invalid(NoFurnitureWip)
  SELECT r.NoFurnitureWip
  FROM @reqFW r
  WHERE NOT EXISTS (SELECT 1 FROM @alreadyMapped a WHERE a.NoFurnitureWip=r.NoFurnitureWip)
    AND NOT EXISTS (SELECT 1 FROM @eligibleNotMapped e WHERE e.NoFurnitureWip=r.NoFurnitureWip);

  INSERT INTO dbo.BJJual_dLabelFurnitureWIP (NoBJJual, NoFurnitureWIP)
  OUTPUT INSERTED.NoFurnitureWIP INTO @insFW(NoFurnitureWIP)
  SELECT @no, e.NoFurnitureWip
  FROM @eligibleNotMapped e;

  SET @fwIns = @@ROWCOUNT;

  IF @fwIns > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = @tgl
    FROM dbo.FurnitureWIP f
    JOIN @insFW i ON i.NoFurnitureWIP=f.NoFurnitureWIP;
  END

  SELECT @fwSkp = COUNT(*) FROM @alreadyMapped;
  SELECT @fwInv = COUNT(*) FROM @invalid;

  SELECT
    @fwIns AS Inserted,
    0 AS Updated,
    @fwSkp AS Skipped,
    @fwInv AS Invalid;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    furnitureWip: {
      inserted: row.Inserted || 0,
      updated: row.Updated || 0,
      skipped: row.Skipped || 0,
      invalid: row.Invalid || 0,
    },
  };
}

async function _insertCabinetMaterialWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @mIns int=0, @mUpd int=0, @mInv int=0;

  DECLARE @MatSrc TABLE(IdCabinetMaterial int, Pcs int);

  INSERT INTO @MatSrc(IdCabinetMaterial, Pcs)
  SELECT IdCabinetMaterial, SUM(ISNULL(Pcs,0)) AS Pcs
  FROM OPENJSON(@jsInputs, '$.cabinetMaterial')
  WITH (
    IdCabinetMaterial int '$.idCabinetMaterial',
    Pcs int '$.jumlah'
  )
  WHERE IdCabinetMaterial IS NOT NULL
  GROUP BY IdCabinetMaterial;

  SELECT @mInv = COUNT(*)
  FROM @MatSrc s
  WHERE s.Pcs <= 0
     OR NOT EXISTS (
        SELECT 1
        FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
        WHERE m.IdCabinetMaterial=s.IdCabinetMaterial AND m.Enable=1
     );

  UPDATE tgt
  SET tgt.Pcs = src.Pcs
  FROM dbo.BJJualCabinetMaterial_d tgt
  JOIN @MatSrc src ON src.IdCabinetMaterial=tgt.IdCabinetMaterial
  WHERE tgt.NoBJJual=@no
    AND src.Pcs > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    );

  SET @mUpd = @@ROWCOUNT;

  INSERT INTO dbo.BJJualCabinetMaterial_d (NoBJJual, IdCabinetMaterial, Pcs)
  SELECT @no, src.IdCabinetMaterial, src.Pcs
  FROM @MatSrc src
  WHERE src.Pcs > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    )
    AND NOT EXISTS (
      SELECT 1 FROM dbo.BJJualCabinetMaterial_d x WITH (NOLOCK)
      WHERE x.NoBJJual=@no AND x.IdCabinetMaterial=src.IdCabinetMaterial
    );

  SET @mIns = @@ROWCOUNT;

  SELECT
    @mIns AS Inserted,
    @mUpd AS Updated,
    0 AS Skipped,
    @mInv AS Invalid;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    cabinetMaterial: {
      inserted: row.Inserted || 0,
      updated: row.Updated || 0,
      skipped: row.Skipped || 0,
      invalid: row.Invalid || 0,
    },
  };
}


async function deleteInputsAndPartials(noBJJual, payload) {
  if (!noBJJual) throw badReq('noBJJual wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    barangJadi: norm(payload?.barangJadi),
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    barangJadiPartial: norm(payload?.barangJadiPartial),
    furnitureWipPartial: norm(payload?.furnitureWipPartial),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) lock header & get doc date
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'bjJual',        // ✅ samakan dengan config tutup-transaksi
      codeValue: noBJJual,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete BJ Jual inputs/partials',
      useLock: true,
    });

    // 2) delete each section
    const bjPartialRes = await _deleteBarangJadiPartialsWithTx(tx, noBJJual, {
      barangJadiPartial: body.barangJadiPartial,
    });

    const fwPartialRes = await _deleteFurnitureWipPartialsWithTx(tx, noBJJual, {
      furnitureWipPartial: body.furnitureWipPartial,
    });

    const bjRes = await _deleteBarangJadiWithTx(tx, noBJJual, {
      barangJadi: body.barangJadi,
    });

    const fwRes = await _deleteFurnitureWipWithTx(tx, noBJJual, {
      furnitureWip: body.furnitureWip,
    });

    const matRes = await _deleteCabinetMaterialWithTx(tx, noBJJual, {
      cabinetMaterial: body.cabinetMaterial,
    });

    await tx.commit();

    const summary = {
      barangJadi: bjRes?.barangJadi ?? { deleted: 0, notFound: 0 },
      furnitureWip: fwRes?.furnitureWip ?? { deleted: 0, notFound: 0 },
      cabinetMaterial: matRes?.cabinetMaterial ?? { deleted: 0, notFound: 0 },

      barangJadiPartial:
        bjPartialRes?.summary?.barangJadiPartial ?? { deleted: 0, notFound: 0 },

      furnitureWipPartial:
        fwPartialRes?.summary?.furnitureWipPartial ?? { deleted: 0, notFound: 0 },
    };

    const totalDeleted =
      (summary.barangJadi.deleted || 0) +
      (summary.furnitureWip.deleted || 0) +
      (summary.cabinetMaterial.deleted || 0) +
      (summary.barangJadiPartial.deleted || 0) +
      (summary.furnitureWipPartial.deleted || 0);

    const totalNotFound =
      (summary.barangJadi.notFound || 0) +
      (summary.furnitureWip.notFound || 0) +
      (summary.cabinetMaterial.notFound || 0) +
      (summary.barangJadiPartial.notFound || 0) +
      (summary.furnitureWipPartial.notFound || 0);

    return {
      success: totalDeleted > 0,
      hasWarnings: totalNotFound > 0,
      data: {
        noBJJual,
        summary: {
          totalDeleted,
          totalNotFound,
          bySection: summary,
        },
      },
    };
  } catch (err) {
    try { await tx.rollback(); } catch (_) {}
    throw err;
  }
}


async function _deleteBarangJadiPartialsWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);
  DECLARE @bjpDeleted int = 0, @bjpNotFound int = 0;

  -- count deletable mappings
  SELECT @bjpDeleted = COUNT(*)
  FROM dbo.BJJual_dLabelBarangJadiPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.barangJadiPartial')
    WITH (noBJPartial varchar(50) '$.noBJPartial') j
    ON map.NoBJPartial = j.noBJPartial
  WHERE map.NoBJJual = @no;

  -- collect affected parent NoBJ
  DECLARE @deletedParents TABLE (NoBJ varchar(50));

  INSERT INTO @deletedParents (NoBJ)
  SELECT DISTINCT bp.NoBJ
  FROM dbo.BarangJadiPartial bp
  INNER JOIN dbo.BJJual_dLabelBarangJadiPartial map
    ON bp.NoBJPartial = map.NoBJPartial
  INNER JOIN OPENJSON(@jsPartials, '$.barangJadiPartial')
    WITH (noBJPartial varchar(50) '$.noBJPartial') j
    ON map.NoBJPartial = j.noBJPartial
  WHERE map.NoBJJual = @no;

  -- delete mapping
  DELETE map
  FROM dbo.BJJual_dLabelBarangJadiPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.barangJadiPartial')
    WITH (noBJPartial varchar(50) '$.noBJPartial') j
    ON map.NoBJPartial = j.noBJPartial
  WHERE map.NoBJJual = @no;

  -- delete partial rows
  DELETE bp
  FROM dbo.BarangJadiPartial bp
  INNER JOIN OPENJSON(@jsPartials, '$.barangJadiPartial')
    WITH (noBJPartial varchar(50) '$.noBJPartial') j
    ON bp.NoBJPartial = j.noBJPartial;

  -- recompute parent BarangJadi flags
  IF @bjpDeleted > 0
  BEGIN
    -- still has partials
    UPDATE b
    SET b.DateUsage = NULL,
        b.IsPartial = 1
    FROM dbo.BarangJadi b
    INNER JOIN @deletedParents del ON del.NoBJ = b.NoBJ
    WHERE EXISTS (SELECT 1 FROM dbo.BarangJadiPartial bp WHERE bp.NoBJ = b.NoBJ);

    -- no more partials
    UPDATE b
    SET b.DateUsage = NULL,
        b.IsPartial = 0
    FROM dbo.BarangJadi b
    INNER JOIN @deletedParents del ON del.NoBJ = b.NoBJ
    WHERE NOT EXISTS (SELECT 1 FROM dbo.BarangJadiPartial bp WHERE bp.NoBJ = b.NoBJ);
  END;

  DECLARE @reqCnt int;
  SELECT @reqCnt = COUNT(*) FROM OPENJSON(@jsPartials, '$.barangJadiPartial');

  SET @bjpNotFound = @reqCnt - @bjpDeleted;

  INSERT INTO @out SELECT 'barangJadiPartial', @bjpDeleted, @bjpNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return { summary };
}


async function _deleteFurnitureWipPartialsWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  DECLARE @fwpDeleted int = 0, @fwpNotFound int = 0;

  SELECT @fwpDeleted = COUNT(*)
  FROM dbo.BJJual_dLabelFurnitureWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoBJJual = @no;

  DECLARE @deletedParents TABLE (NoFurnitureWIP varchar(50));

  INSERT INTO @deletedParents (NoFurnitureWIP)
  SELECT DISTINCT fp.NoFurnitureWIP
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN dbo.BJJual_dLabelFurnitureWIPPartial map
    ON fp.NoFurnitureWIPPartial = map.NoFurnitureWIPPartial
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoBJJual = @no;

  DELETE map
  FROM dbo.BJJual_dLabelFurnitureWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoBJJual = @no;

  DELETE fp
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON fp.NoFurnitureWIPPartial = j.noFurnitureWipPartial;

  IF @fwpDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = NULL,
        f.IsPartial = 1
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedParents del ON del.NoFurnitureWIP = f.NoFurnitureWIP
    WHERE EXISTS (SELECT 1 FROM dbo.FurnitureWIPPartial fp WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP);

    UPDATE f
    SET f.DateUsage = NULL,
        f.IsPartial = 0
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedParents del ON del.NoFurnitureWIP = f.NoFurnitureWIP
    WHERE NOT EXISTS (SELECT 1 FROM dbo.FurnitureWIPPartial fp WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP);
  END;

  DECLARE @reqCnt int;
  SELECT @reqCnt = COUNT(*) FROM OPENJSON(@jsPartials, '$.furnitureWipPartial');

  SET @fwpNotFound = @reqCnt - @fwpDeleted;

  INSERT INTO @out SELECT 'furnitureWipPartial', @fwpDeleted, @fwpNotFound;

  SELECT Section, Deleted, NotFound FROM @out ORDER BY Section;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordset || []) {
    summary[row.Section] = { deleted: row.Deleted, notFound: row.NotFound };
  }
  return { summary };
}


async function _deleteBarangJadiWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @bjDeleted int = 0, @bjNotFound int = 0;

  SELECT @bjDeleted = COUNT(*)
  FROM dbo.BJJual_dLabelBarangJadi map
  INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
    WITH (noBJ varchar(50) '$.noBJ') j
    ON map.NoBJ = j.noBJ
  WHERE map.NoBJJual = @no;

  IF @bjDeleted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = NULL
    FROM dbo.BarangJadi b
    INNER JOIN dbo.BJJual_dLabelBarangJadi map ON b.NoBJ = map.NoBJ
    INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
      WITH (noBJ varchar(50) '$.noBJ') j
      ON map.NoBJ = j.noBJ
    WHERE map.NoBJJual = @no;
  END

  DELETE map
  FROM dbo.BJJual_dLabelBarangJadi map
  INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
    WITH (noBJ varchar(50) '$.noBJ') j
    ON map.NoBJ = j.noBJ
  WHERE map.NoBJJual = @no;

  DECLARE @reqCnt int;
  SELECT @reqCnt = COUNT(*) FROM OPENJSON(@jsInputs, '$.barangJadi');

  SET @bjNotFound = @reqCnt - @bjDeleted;

  SELECT @bjDeleted AS Deleted, @bjNotFound AS NotFound;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    barangJadi: {
      deleted: row.Deleted || 0,
      notFound: row.NotFound || 0,
    },
  };
}


async function _deleteFurnitureWipWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @fwDeleted int = 0, @fwNotFound int = 0;

  SELECT @fwDeleted = COUNT(*)
  FROM dbo.BJJual_dLabelFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoBJJual = @no;

  IF @fwDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = NULL
    FROM dbo.FurnitureWIP f
    INNER JOIN dbo.BJJual_dLabelFurnitureWIP map ON f.NoFurnitureWIP = map.NoFurnitureWIP
    INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
      WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
      ON map.NoFurnitureWIP = j.noFurnitureWip
    WHERE map.NoBJJual = @no;
  END

  DELETE map
  FROM dbo.BJJual_dLabelFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoBJJual = @no;

  DECLARE @reqCnt int;
  SELECT @reqCnt = COUNT(*) FROM OPENJSON(@jsInputs, '$.furnitureWip');

  SET @fwNotFound = @reqCnt - @fwDeleted;

  SELECT @fwDeleted AS Deleted, @fwNotFound AS NotFound;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    furnitureWip: {
      deleted: row.Deleted || 0,
      notFound: row.NotFound || 0,
    },
  };
}


async function _deleteCabinetMaterialWithTx(tx, noBJJual, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJJual);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.BJJual_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJJual = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJJual_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @matDeleted int = 0, @matNotFound int = 0;

  SELECT @matDeleted = COUNT(*)
  FROM dbo.BJJualCabinetMaterial_d map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetMaterial = j.idCabinetMaterial
  WHERE map.NoBJJual = @no;

  DELETE map
  FROM dbo.BJJualCabinetMaterial_d map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetMaterial = j.idCabinetMaterial
  WHERE map.NoBJJual = @no;

  DECLARE @reqCnt int;
  SELECT @reqCnt = COUNT(*) FROM OPENJSON(@jsInputs, '$.cabinetMaterial');

  SET @matNotFound = @reqCnt - @matDeleted;

  SELECT @matDeleted AS Deleted, @matNotFound AS NotFound;
  `;

  const rs = await req.query(SQL);
  const row = rs.recordset?.[0] || {};

  return {
    cabinetMaterial: {
      deleted: row.Deleted || 0,
      notFound: row.NotFound || 0,
    },
  };
}



module.exports = {
  getAllBJJual,
  createBJJual,
  updateBJJual,
  deleteBJJual,
  fetchInputs,
  upsertInputsAndPartials,
  deleteInputsAndPartials
};

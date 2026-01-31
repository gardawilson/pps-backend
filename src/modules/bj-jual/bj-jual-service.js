// services/bj-jual-service.js
const { sql, poolPromise } = require('../../core/config/db');
const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,     
  formatYMD,
  loadDocDateOnlyFromConfig
} = require('../../core/shared/tutup-transaksi-guard');
const { generateNextCode } = require('../../core/utils/sequence-code-helper');
const sharedInputService = require('../../core/shared/produksi-input.service');
const { badReq, conflict } = require('../../core/utils/http-error')


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
async function upsertInputsAndPartials(noProduksi, payload, ctx) {
  const no = String(noProduksi || '').trim();
  if (!no) throw badReq('noProduksi wajib diisi');

  const body = payload && typeof payload === 'object' ? payload : {};

  // ✅ ctx wajib (audit)
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
  }

  const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';

  // requestId wajib string (kalau kosong, nanti di applyAuditContext dibuat fallback juga)
  const requestId = String(ctx?.requestId || '').trim();

  // ✅ forward ctx yang sudah dinormalisasi ke shared service
  return sharedInputService.upsertInputsAndPartials('bjJual', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

async function deleteInputsAndPartials(noProduksi, payload, ctx) {
  const no = String(noProduksi || '').trim();
  if (!no) throw badReq('noProduksi wajib diisi');

  const body = payload && typeof payload === 'object' ? payload : {};

  // ✅ Validate audit context
  const actorIdNum = Number(ctx?.actorId);
  if (!Number.isFinite(actorIdNum) || actorIdNum <= 0) {
    throw badReq('ctx.actorId wajib. Controller harus inject dari token.');
  }

  const actorUsername = String(ctx?.actorUsername || '').trim() || 'system';
  const requestId = String(ctx?.requestId || '').trim();

  // ✅ Forward to shared service
  return sharedInputService.deleteInputsAndPartials('bjJual', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
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

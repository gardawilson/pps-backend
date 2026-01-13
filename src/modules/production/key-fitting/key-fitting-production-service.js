// services/key-fitting-production-service.js
const { sql, poolPromise } = require('../../../core/config/db');

const {
  resolveEffectiveDateForCreate,
  assertNotLocked,
  loadDocDateOnlyFromConfig,
} = require('../../../core/shared/tutup-transaksi-guard');

const { generateNextCode } = require('../../../core/utils/sequence-code-helper');
const {
  parseJamToInt,
  calcJamKerjaFromStartEnd,
  badReq,
} = require('../../../core/utils/jam-kerja-helper');

// =====================================================
// GET ALL (paged + search)
// =====================================================
async function getAllProduksi(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const offset = (Math.max(page, 1) - 1) * Math.max(pageSize, 1);
  const s = String(search || '').trim();

  const rqCount = pool.request();
  const rqData = pool.request();

  rqCount.input('search', sql.VarChar(50), s);
  rqData.input('search', sql.VarChar(50), s);
  rqData.input('offset', sql.Int, offset);
  rqData.input('pageSize', sql.Int, pageSize);

  const qCount = `
    SELECT COUNT(1) AS Total
    FROM dbo.PasangKunci_h h WITH (NOLOCK)
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
    FROM dbo.PasangKunci_h h WITH (NOLOCK)
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
    FROM dbo.PasangKunci_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstMesin m WITH (NOLOCK)
      ON h.IdMesin = m.IdMesin
    LEFT JOIN dbo.MstOperator o WITH (NOLOCK)
      ON h.IdOperator = o.IdOperator
    WHERE CONVERT(date, h.Tanggal) = @date
    ORDER BY h.JamKerja ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset || [];
}

// =====================================================
// CREATE PasangKunci_h
// =====================================================
async function createKeyFittingProduksi(payload) {
  const must = [];
  if (!payload?.tglProduksi) must.push('tglProduksi');
  if (payload?.idMesin == null) must.push('idMesin');
  if (payload?.idOperator == null) must.push('idOperator');
  if (payload?.shift == null) must.push('shift');
  if (!payload?.hourStart) must.push('hourStart');
  if (!payload?.hourEnd) must.push('hourEnd');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) normalize date + guard tutup transaksi
    const effectiveDate = resolveEffectiveDateForCreate(payload.tglProduksi);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create PasangKunci',
      useLock: true,
    });

    // 1) generate NoProduksi BI.0000000001 (generic helper)
    const no1 = await generateNextCode(tx, {
      tableName: 'dbo.PasangKunci_h',
      columnName: 'NoProduksi',
      prefix: 'BI.',
      width: 10,
    });

    // optional anti-race double check (keep your style)
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoProduksi', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.PasangKunci_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoProduksi = @NoProduksi
      `);

    const noProduksi = exist.recordset.length
      ? await generateNextCode(tx, {
          tableName: 'dbo.PasangKunci_h',
          columnName: 'NoProduksi',
          prefix: 'BI.',
          width: 10,
        })
      : no1;

    // 2) jam kerja
    let jamKerjaInt = null;
    if (payload.jamKerja !== null && payload.jamKerja !== undefined && payload.jamKerja !== '') {
      jamKerjaInt = parseJamToInt(payload.jamKerja);
    } else {
      jamKerjaInt = calcJamKerjaFromStartEnd(payload.hourStart, payload.hourEnd);
    }

    // 3) insert header
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .input('Tanggal', sql.Date, effectiveDate)
      .input('IdMesin', sql.Int, payload.idMesin)
      .input('IdOperator', sql.Int, payload.idOperator)
      .input('Shift', sql.Int, payload.shift)
      .input('JamKerja', sql.Int, jamKerjaInt)
      .input('CreateBy', sql.VarChar(100), payload.createBy)
      .input('CheckBy1', sql.VarChar(100), payload.checkBy1 ?? null)
      .input('CheckBy2', sql.VarChar(100), payload.checkBy2 ?? null)
      .input('ApproveBy', sql.VarChar(100), payload.approveBy ?? null)
      .input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null)
      .input('HourStart', sql.VarChar(20), payload.hourStart ?? null)
      .input('HourEnd', sql.VarChar(20), payload.hourEnd ?? null);

    const insertSql = `
      INSERT INTO dbo.PasangKunci_h (
        NoProduksi, Tanggal, IdMesin, IdOperator, Shift, JamKerja,
        CreateBy, CheckBy1, CheckBy2, ApproveBy, HourMeter,
        HourStart, HourEnd
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoProduksi, @Tanggal, @IdMesin, @IdOperator, @Shift,
        @JamKerja,
        @CreateBy, @CheckBy1, @CheckBy2, @ApproveBy, @HourMeter,
        CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL ELSE CAST(@HourStart AS time(7)) END,
        CASE WHEN @HourEnd   IS NULL OR LTRIM(RTRIM(@HourEnd))   = '' THEN NULL ELSE CAST(@HourEnd   AS time(7)) END
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

// =====================================================
// UPDATE PasangKunci_h (dynamic) + sync DateUsage jika Tanggal berubah
// =====================================================
async function updateKeyFittingProduksi(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock header + ambil tanggal lama dari config
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'keyFitting',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) jika user kirim tglProduksi -> new date
    const isChangingDate = payload?.tglProduksi !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglProduksi) throw badReq('tglProduksi tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglProduksi);
    }

    // 2) guard tutup transaksi
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: 'update PasangKunci (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update PasangKunci (new date)',
        useLock: true,
      });
    }

    // 3) build set dinamis
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('Tanggal = @Tanggal');
      rqUpd.input('Tanggal', sql.Date, newDocDateOnly);
    }

    if (payload.idMesin !== undefined) {
      sets.push('IdMesin = @IdMesin');
      rqUpd.input('IdMesin', sql.Int, payload.idMesin);
    }

    if (payload.idOperator !== undefined) {
      sets.push('IdOperator = @IdOperator');
      rqUpd.input('IdOperator', sql.Int, payload.idOperator);
    }

    if (payload.shift !== undefined) {
      sets.push('Shift = @Shift');
      rqUpd.input('Shift', sql.Int, payload.shift);
    }

    if (payload.jamKerja !== undefined) {
      const jamKerjaInt = payload.jamKerja === null ? null : parseJamToInt(payload.jamKerja);
      sets.push('JamKerja = @JamKerja');
      rqUpd.input('JamKerja', sql.Int, jamKerjaInt);
    }

    if (payload.checkBy1 !== undefined) {
      sets.push('CheckBy1 = @CheckBy1');
      rqUpd.input('CheckBy1', sql.VarChar(100), payload.checkBy1 ?? null);
    }

    if (payload.checkBy2 !== undefined) {
      sets.push('CheckBy2 = @CheckBy2');
      rqUpd.input('CheckBy2', sql.VarChar(100), payload.checkBy2 ?? null);
    }

    if (payload.approveBy !== undefined) {
      sets.push('ApproveBy = @ApproveBy');
      rqUpd.input('ApproveBy', sql.VarChar(100), payload.approveBy ?? null);
    }

    if (payload.hourMeter !== undefined) {
      sets.push('HourMeter = @HourMeter');
      rqUpd.input('HourMeter', sql.Decimal(18, 2), payload.hourMeter ?? null);
    }

    if (payload.hourStart !== undefined) {
      sets.push(`
        HourStart =
          CASE WHEN @HourStart IS NULL OR LTRIM(RTRIM(@HourStart)) = '' THEN NULL
               ELSE CAST(@HourStart AS time(7)) END
      `);
      rqUpd.input('HourStart', sql.VarChar(20), payload.hourStart ?? null);
    }

    if (payload.hourEnd !== undefined) {
      sets.push(`
        HourEnd =
          CASE WHEN @HourEnd IS NULL OR LTRIM(RTRIM(@HourEnd)) = '' THEN NULL
               ELSE CAST(@HourEnd AS time(7)) END
      `);
      rqUpd.input('HourEnd', sql.VarChar(20), payload.hourEnd ?? null);
    }

    if (sets.length === 0) throw badReq('No fields to update');

    rqUpd.input('NoProduksi', sql.VarChar(50), noProduksi);

    const updateSql = `
      UPDATE dbo.PasangKunci_h
      SET ${sets.join(', ')}
      WHERE NoProduksi = @NoProduksi;

      SELECT *
      FROM dbo.PasangKunci_h
      WHERE NoProduksi = @NoProduksi;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // 4) jika tanggal berubah -> sync DateUsage
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.Tanggal);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoProduksi', sql.VarChar(50), noProduksi)
        .input('Tanggal', sql.Date, usageDate);

      const sqlUpdateUsage = `
        -- FULL
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.PasangKunciInputLabelFWIP AS map
            WHERE map.NoProduksi     = @NoProduksi
              AND map.NoFurnitureWIP = fw.NoFurnitureWIP
          );

        -- PARTIAL (via FurnitureWIPPartial)
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.PasangKunciInputLabelFWIPPartial AS mp
            JOIN dbo.FurnitureWIPPartial AS fwp
              ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
            WHERE mp.NoProduksi = @NoProduksi
              AND fwp.NoFurnitureWIP = fw.NoFurnitureWIP
          );
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

// =====================================================
// DELETE PasangKunci (cek output dulu) + reset DateUsage
// =====================================================
async function deleteKeyFittingProduksi(noProduksi) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'keyFitting',
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete PasangKunci',
      useLock: true,
    });

    // cek output
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut
      .input('NoProduksi', sql.VarChar(50), noProduksi)
      .query(`
        SELECT
          SUM(CASE WHEN Src = 'FWIP'   THEN Cnt ELSE 0 END) AS CntOutputFWIP,
          SUM(CASE WHEN Src = 'REJECT' THEN Cnt ELSE 0 END) AS CntOutputReject
        FROM (
          SELECT 'FWIP' AS Src, COUNT(1) AS Cnt
          FROM dbo.PasangKunciOutputLabelFWIP WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi

          UNION ALL

          SELECT 'REJECT' AS Src, COUNT(1) AS Cnt
          FROM dbo.PasangKunciOutputRejectV2 WITH (NOLOCK)
          WHERE NoProduksi = @NoProduksi
        ) X;
      `);

    const row = outRes.recordset?.[0] || { CntOutputFWIP: 0, CntOutputReject: 0 };
    const hasOutputFWIP = (row.CntOutputFWIP || 0) > 0;
    const hasOutputReject = (row.CntOutputReject || 0) > 0;

    if (hasOutputFWIP || hasOutputReject) {
      throw badReq('Tidak dapat menghapus Nomor Produksi ini karena sudah memiliki data output.');
    }

    // delete inputs + reset
    const req = new sql.Request(tx);
    req.input('NoProduksi', sql.VarChar(50), noProduksi);

    const sqlDelete = `
      DECLARE @FWIPKeys TABLE (NoFurnitureWIP varchar(50) PRIMARY KEY);

      -- keys FULL
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT map.NoFurnitureWIP
      FROM dbo.PasangKunciInputLabelFWIP AS map
      WHERE map.NoProduksi = @NoProduksi
        AND map.NoFurnitureWIP IS NOT NULL;

      -- keys PARTIAL
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT fwp.NoFurnitureWIP
      FROM dbo.PasangKunciInputLabelFWIPPartial AS mp
      JOIN dbo.FurnitureWIPPartial AS fwp
        ON fwp.NoFurnitureWIPPartial = mp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi
        AND fwp.NoFurnitureWIP IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM @FWIPKeys k WHERE k.NoFurnitureWIP = fwp.NoFurnitureWIP);

      -- delete material input
      DELETE FROM dbo.PasangKunciInputMaterial
      WHERE NoProduksi = @NoProduksi;

      -- delete partial rows
      DELETE fwp
      FROM dbo.FurnitureWIPPartial AS fwp
      JOIN dbo.PasangKunciInputLabelFWIPPartial AS mp
        ON mp.NoFurnitureWIPPartial = fwp.NoFurnitureWIPPartial
      WHERE mp.NoProduksi = @NoProduksi;

      -- delete mappings
      DELETE FROM dbo.PasangKunciInputLabelFWIPPartial
      WHERE NoProduksi = @NoProduksi;

      DELETE FROM dbo.PasangKunciInputLabelFWIP
      WHERE NoProduksi = @NoProduksi;

      -- reset dateusage + recalc isPartial
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

      -- delete header last
      DELETE FROM dbo.PasangKunci_h
      WHERE NoProduksi = @NoProduksi;
    `;

    await req.query(sqlDelete);

    await tx.commit();
    return { success: true };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}


async function fetchInputs(noProduksi) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noProduksi);

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
    FROM dbo.PasangKunciInputLabelFWIP map WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = map.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = fw.IDFurnitureWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE map.NoProduksi = @no

    UNION ALL

    -- Cabinet Material
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
      CAST(NULL AS datetime) AS DatetimeInput
    FROM dbo.PasangKunciInputMaterial im WITH (NOLOCK)
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
    FROM dbo.PasangKunciInputLabelFWIPPartial mp WITH (NOLOCK)
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

  const out = {
    furnitureWip: [],
    cabinetMaterial: [],
    summary: { furnitureWip: 0, cabinetMaterial: 0 },
  };

  // MAIN (seperti gilingan: base object)
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
      case 'fwip':
        out.furnitureWip.push({ noFurnitureWip: r.Ref1, ...base });
        break;

      case 'material':
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1, // string cast
          jumlah: r.Pcs ?? null,
          ...base,
        });
        break;
    }
  }

  // PARTIALS (merge into SAME bucket seperti gilingan broker/reject)
  for (const p of fwipPartial) {
    out.furnitureWip.push({
      noFurnitureWipPartial: p.NoFurnitureWIPPartial, // ✅ nomor partial wajib ada
      noFurnitureWip: p.NoFurnitureWIP ?? null,       // header
      pcs: p.PcsPartial ?? null,                      // pcs partial
      pcsHeader: p.PcsHeader ?? null,                 // opsional
      berat: p.Berat ?? null,
      idJenis: p.IdJenis ?? null,
      namaJenis: p.NamaJenis ?? null,
      namaUom: p.NamaUOM ?? null,
      isPartial: true,         // optional marker
      isPartialRow: true,      // optional marker (mirip VM kamu)
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
async function upsertInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    furnitureWipPartialNew: norm(payload?.furnitureWipPartialNew),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) ambil tanggal header dari config + lock header row
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'keyFitting',   // ✅ sesuaikan dengan config kamu
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'upsert KeyFitting inputs/partials',
      useLock: true,
    });

    // 2) create partial + mapping
    const partials = await _insertPartialsWithTx(tx, noProduksi, {
      furnitureWipPartialNew: body.furnitureWipPartialNew,
    });

    // 3) attach existing inputs
    const fwipAttach = await _insertFurnitureWipWithTx(tx, noProduksi, {
      furnitureWip: body.furnitureWip,
    });

    const matAttach = await _insertCabinetMaterialWithTx(tx, noProduksi, {
      cabinetMaterial: body.cabinetMaterial,
    });

    const attachments = {
      furnitureWip: fwipAttach.furnitureWip,
      cabinetMaterial: matAttach.cabinetMaterial,
    };

    await tx.commit();

    // ===== response summary (sama pola HotStamp) =====
    const totalInserted = Object.values(attachments).reduce((sum, x) => sum + (x.inserted || 0), 0);
    const totalUpdated  = Object.values(attachments).reduce((sum, x) => sum + (x.updated  || 0), 0);
    const totalSkipped  = Object.values(attachments).reduce((sum, x) => sum + (x.skipped  || 0), 0);
    const totalInvalid  = Object.values(attachments).reduce((sum, x) => sum + (x.invalid  || 0), 0);

    const totalPartialsCreated = Object.values(partials.summary || {}).reduce(
      (sum, item) => sum + (item.created || 0),
      0
    );

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = (totalInserted + totalUpdated) === 0 && totalPartialsCreated === 0;

    const response = {
      noProduksi,
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
    try { await tx.rollback(); } catch {}
    throw err;
  }
}

/* =====================
   Details builders (sama)
===================== */

function _buildInputDetails(attachments, requestBody) {
  const details = [];

  const sections = [
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = attachments[section.key] || { inserted: 0, updated: 0, skipped: 0, invalid: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      inserted: result.inserted,
      updated: result.updated,
      skipped: result.skipped,
      invalid: result.invalid,
      status: result.invalid > 0 ? 'error' : result.skipped > 0 ? 'warning' : 'success',
      message: _buildSectionMessage(section.label, result),
    });
  }

  return details;
}

function _buildPartialDetails(partials, requestBody) {
  const details = [];

  const sections = [{ key: 'furnitureWipPartialNew', label: 'Furniture WIP Partial' }];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const created = partials.summary?.[section.key]?.created || 0;

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      created,
      status: created === requestedCount ? 'success' : 'error',
      message: `${created} dari ${requestedCount} ${section.label} berhasil dibuat`,
      codes: partials.createdLists?.[section.key] || [],
    });
  }

  return details;
}

function _buildSectionMessage(label, result) {
  const parts = [];
  if (result.inserted > 0) parts.push(`${result.inserted} berhasil ditambahkan`);
  if (result.updated > 0)  parts.push(`${result.updated} berhasil diperbarui`);
  if (result.skipped > 0)  parts.push(`${result.skipped} sudah ada (dilewati)`);
  if (result.invalid > 0)  parts.push(`${result.invalid} tidak valid`);
  return parts.length ? `${label}: ${parts.join(', ')}` : `Tidak ada ${label} yang diproses`;
}

/* =====================
   SQL helpers (ganti tabel jadi PasangKunci*)
===================== */

async function _insertPartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.PasangKunci_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header PasangKunci_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Created int);
  DECLARE @createdFWP TABLE(NoFurnitureWIPPartial varchar(50));

  IF EXISTS (SELECT 1 FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew'))
  BEGIN
    -- Prefix partial: pakai 'BC.' seperti hotstamp (FurnitureWIPPartial global)
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

    -- map partial ke KeyFitting
    INSERT INTO dbo.PasangKunciInputLabelFWIPPartial (NoProduksi, NoFurnitureWIPPartial)
    SELECT @no, c.NoFurnitureWIPPartial
    FROM @createdFWP c
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.PasangKunciInputLabelFWIPPartial x WITH (NOLOCK)
      WHERE x.NoProduksi=@no AND x.NoFurnitureWIPPartial=c.NoFurnitureWIPPartial
    );

    -- update parent: IsPartial=1 dan DateUsage kalau pcs habis
    DECLARE @ins int = @@ROWCOUNT;

    IF @ins > 0
    BEGIN
      ;WITH existingPartials AS (
        SELECT fp.NoFurnitureWIP, SUM(ISNULL(fp.Pcs, 0)) AS TotalPcsPartialExisting
        FROM dbo.FurnitureWIPPartial fp WITH (NOLOCK)
        WHERE fp.NoFurnitureWIPPartial NOT IN (SELECT NoFurnitureWIPPartial FROM @createdFWP)
        GROUP BY fp.NoFurnitureWIP
      ),
      newPartials AS (
        SELECT noFurnitureWip, SUM(pcs) AS TotalPcsPartialNew
        FROM OPENJSON(@jsPartials, '$.furnitureWipPartialNew')
        WITH (
          noFurnitureWip varchar(50) '$.noFurnitureWip',
          pcs int '$.pcs'
        )
        GROUP BY noFurnitureWip
      )
      UPDATE f
      SET
        f.IsPartial = 1,
        f.DateUsage = CASE
          WHEN (f.Pcs - ISNULL(ep.TotalPcsPartialExisting, 0) - ISNULL(np.TotalPcsPartialNew, 0)) <= 0
          THEN @tgl
          ELSE f.DateUsage
        END
      FROM dbo.FurnitureWIP f
      LEFT JOIN existingPartials ep ON ep.NoFurnitureWIP = f.NoFurnitureWIP
      INNER JOIN newPartials np ON np.noFurnitureWip = f.NoFurnitureWIP;
    END

    INSERT INTO @out SELECT 'furnitureWipPartialNew', @ins;
  END

  SELECT Section, Created FROM @out;
  SELECT NoFurnitureWIPPartial FROM @createdFWP;
  `;

  const rs = await req.query(SQL);

  const summary = {};
  for (const row of rs.recordsets?.[0] || []) {
    summary[row.Section] = { created: row.Created };
  }

  const createdLists = {
    furnitureWipPartialNew: (rs.recordsets?.[1] || []).map((r) => r.NoFurnitureWIPPartial),
  };

  return { summary, createdLists };
}

async function _insertFurnitureWipWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.PasangKunci_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header PasangKunci_h tidak ditemukan / Tanggal NULL', 16, 1);
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
    SELECT 1 FROM dbo.PasangKunciInputLabelFWIP x WITH (NOLOCK)
    WHERE x.NoProduksi=@no AND x.NoFurnitureWIP=r.NoFurnitureWip
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

  INSERT INTO dbo.PasangKunciInputLabelFWIP (NoProduksi, NoFurnitureWIP)
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

async function _insertCabinetMaterialWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.PasangKunci_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header PasangKunci_h tidak ditemukan / Tanggal NULL', 16, 1);
    RETURN;
  END;

  DECLARE @mIns int=0, @mUpd int=0, @mInv int=0;

  DECLARE @MatSrc TABLE(IdCabinetMaterial int, Jumlah int);

  INSERT INTO @MatSrc(IdCabinetMaterial, Jumlah)
  SELECT IdCabinetMaterial, SUM(ISNULL(Jumlah,0)) AS Jumlah
  FROM OPENJSON(@jsInputs, '$.cabinetMaterial')
  WITH (
    IdCabinetMaterial int '$.idCabinetMaterial',
    Jumlah int '$.jumlah'
  )
  WHERE IdCabinetMaterial IS NOT NULL
  GROUP BY IdCabinetMaterial;

  SELECT @mInv = COUNT(*)
  FROM @MatSrc s
  WHERE s.Jumlah <= 0
     OR NOT EXISTS (
        SELECT 1
        FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
        WHERE m.IdCabinetMaterial=s.IdCabinetMaterial AND m.Enable=1
     );

  UPDATE tgt
  SET tgt.Jumlah = src.Jumlah
  FROM dbo.PasangKunciInputMaterial tgt
  JOIN @MatSrc src ON src.IdCabinetMaterial=tgt.IdCabinetMaterial
  WHERE tgt.NoProduksi=@no
    AND src.Jumlah > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    );

  SET @mUpd = @@ROWCOUNT;

  INSERT INTO dbo.PasangKunciInputMaterial (NoProduksi, IdCabinetMaterial, Jumlah)
  SELECT @no, src.IdCabinetMaterial, src.Jumlah
  FROM @MatSrc src
  WHERE src.Jumlah > 0
    AND EXISTS (
      SELECT 1 FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      WHERE m.IdCabinetMaterial=src.IdCabinetMaterial AND m.Enable=1
    )
    AND NOT EXISTS (
      SELECT 1 FROM dbo.PasangKunciInputMaterial x WITH (NOLOCK)
      WHERE x.NoProduksi=@no AND x.IdCabinetMaterial=src.IdCabinetMaterial
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

/**
 * Payload shape:
 * {
 *   furnitureWip: [{ noFurnitureWip }],
 *   cabinetMaterial: [{ idCabinetMaterial }],
 *   furnitureWipPartial: [{ noFurnitureWipPartial }]
 * }
 */
async function deleteInputsAndPartials(noProduksi, payload) {
  if (!noProduksi) throw badReq('noProduksi wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    furnitureWipPartial: norm(payload?.furnitureWipPartial),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) lock header + ambil docDateOnly
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'keyFitting', // ✅ sesuaikan nama config kamu
      codeValue: noProduksi,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete KeyFitting inputs/partials',
      useLock: true,
    });

    // 2) delete partials
    const partialsResult = await _deletePartialsWithTx(tx, noProduksi, {
      furnitureWipPartial: body.furnitureWipPartial,
    });

    // 3) delete inputs
    const fwipResult = await _deleteFurnitureWipWithTx(tx, noProduksi, {
      furnitureWip: body.furnitureWip,
    });

    const matResult = await _deleteCabinetMaterialWithTx(tx, noProduksi, {
      cabinetMaterial: body.cabinetMaterial,
    });

    const inputsResult = {
      furnitureWip: fwipResult.furnitureWip,
      cabinetMaterial: matResult.cabinetMaterial,
    };

    await tx.commit();

    const totalDeleted = Object.values(inputsResult).reduce((s, x) => s + (x.deleted || 0), 0);
    const totalNotFound = Object.values(inputsResult).reduce((s, x) => s + (x.notFound || 0), 0);

    const totalPartialsDeleted = Object.values(partialsResult.summary).reduce(
      (s, x) => s + (x.deleted || 0),
      0
    );
    const totalPartialsNotFound = Object.values(partialsResult.summary).reduce(
      (s, x) => s + (x.notFound || 0),
      0
    );

    const hasNotFound = totalNotFound > 0 || totalPartialsNotFound > 0;
    const hasNoSuccess = totalDeleted === 0 && totalPartialsDeleted === 0;

    const response = {
      noProduksi,
      summary: {
        totalDeleted,
        totalNotFound,
        totalPartialsDeleted,
        totalPartialsNotFound,
      },
      details: {
        inputs: _buildDeleteInputDetails(inputsResult, body),
        partials: _buildDeletePartialDetails(partialsResult, body),
      },
    };

    return {
      success: !hasNoSuccess,
      hasWarnings: hasNotFound,
      data: response,
    };
  } catch (err) {
    try { await tx.rollback(); } catch {}
    throw err;
  }
}

/* =====================
   Detail builders (sama)
===================== */
function _buildDeleteInputDetails(results, requestBody) {
  const details = [];
  const sections = [
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'cabinetMaterial', label: 'Cabinet Material' },
  ];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = results[section.key] || { deleted: 0, notFound: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      deleted: result.deleted,
      notFound: result.notFound,
      status: result.notFound > 0 ? 'warning' : 'success',
      message: `${section.label}: ${result.deleted} berhasil dihapus${
        result.notFound > 0 ? `, ${result.notFound} tidak ditemukan` : ''
      }`,
    });
  }
  return details;
}

function _buildDeletePartialDetails(partialsResult, requestBody) {
  const details = [];
  const sections = [{ key: 'furnitureWipPartial', label: 'Furniture WIP Partial' }];

  for (const section of sections) {
    const requestedCount = requestBody[section.key]?.length || 0;
    if (requestedCount === 0) continue;

    const result = partialsResult.summary[section.key] || { deleted: 0, notFound: 0 };

    details.push({
      section: section.key,
      label: section.label,
      requested: requestedCount,
      deleted: result.deleted,
      notFound: result.notFound,
      status: result.notFound > 0 ? 'warning' : 'success',
      message: `${section.label}: ${result.deleted} berhasil dihapus${
        result.notFound > 0 ? `, ${result.notFound} tidak ditemukan` : ''
      }`,
    });
  }
  return details;
}

/* =====================
   SQL DELETE helpers (ganti tabel)
===================== */

async function _deletePartialsWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsPartials', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.PasangKunci_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header PasangKunci_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @out TABLE(Section sysname, Deleted int, NotFound int);

  DECLARE @fwpDeleted int = 0, @fwpNotFound int = 0;

  SELECT @fwpDeleted = COUNT(*)
  FROM dbo.PasangKunciInputLabelFWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;

  DECLARE @deletedFWPPartials TABLE (NoFurnitureWIP varchar(50));

  INSERT INTO @deletedFWPPartials (NoFurnitureWIP)
  SELECT DISTINCT fp.NoFurnitureWIP
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN dbo.PasangKunciInputLabelFWIPPartial map
    ON fp.NoFurnitureWIPPartial = map.NoFurnitureWIPPartial
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;

  DELETE map
  FROM dbo.PasangKunciInputLabelFWIPPartial map
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON map.NoFurnitureWIPPartial = j.noFurnitureWipPartial
  WHERE map.NoProduksi = @no;

  DELETE fp
  FROM dbo.FurnitureWIPPartial fp
  INNER JOIN OPENJSON(@jsPartials, '$.furnitureWipPartial')
    WITH (noFurnitureWipPartial varchar(50) '$.noFurnitureWipPartial') j
    ON fp.NoFurnitureWIPPartial = j.noFurnitureWipPartial;

  IF @fwpDeleted > 0
  BEGIN
    UPDATE f
    SET
      f.DateUsage = NULL,
      f.IsPartial = 1
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedFWPPartials del ON f.NoFurnitureWIP = del.NoFurnitureWIP
    WHERE EXISTS (
      SELECT 1 FROM dbo.FurnitureWIPPartial fp
      WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP
    );

    UPDATE f
    SET
      f.DateUsage = NULL,
      f.IsPartial = 0
    FROM dbo.FurnitureWIP f
    INNER JOIN @deletedFWPPartials del ON f.NoFurnitureWIP = del.NoFurnitureWIP
    WHERE NOT EXISTS (
      SELECT 1 FROM dbo.FurnitureWIPPartial fp
      WHERE fp.NoFurnitureWIP = f.NoFurnitureWIP
    );
  END;

  DECLARE @fwpRequested int;
  SELECT @fwpRequested = COUNT(*) FROM OPENJSON(@jsPartials, '$.furnitureWipPartial');

  SET @fwpNotFound = @fwpRequested - @fwpDeleted;

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

async function _deleteFurnitureWipWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.PasangKunci_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header PasangKunci_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @fwDeleted int = 0, @fwNotFound int = 0;

  SELECT @fwDeleted = COUNT(*)
  FROM dbo.PasangKunciInputLabelFWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoProduksi = @no;

  IF @fwDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = NULL
    FROM dbo.FurnitureWIP f
    INNER JOIN dbo.PasangKunciInputLabelFWIP map ON f.NoFurnitureWIP = map.NoFurnitureWIP
    INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
      WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
      ON map.NoFurnitureWIP = j.noFurnitureWip
    WHERE map.NoProduksi = @no;
  END;

  DELETE map
  FROM dbo.PasangKunciInputLabelFWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoProduksi = @no;

  DECLARE @fwRequested int;
  SELECT @fwRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.furnitureWip');

  SET @fwNotFound = @fwRequested - @fwDeleted;

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

async function _deleteCabinetMaterialWithTx(tx, noProduksi, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noProduksi);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.Tanggal AS datetime)
  FROM dbo.PasangKunci_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoProduksi = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header PasangKunci_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @matDeleted int = 0, @matNotFound int = 0;

  SELECT @matDeleted = COUNT(*)
  FROM dbo.PasangKunciInputMaterial map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetMaterial = j.idCabinetMaterial
  WHERE map.NoProduksi = @no;

  DELETE map
  FROM dbo.PasangKunciInputMaterial map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetMaterial = j.idCabinetMaterial
  WHERE map.NoProduksi = @no;

  DECLARE @matRequested int;
  SELECT @matRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.cabinetMaterial');

  SET @matNotFound = @matRequested - @matDeleted;

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

module.exports = { getAllProduksi, getProductionByDate, createKeyFittingProduksi, updateKeyFittingProduksi, deleteKeyFittingProduksi, fetchInputs, upsertInputsAndPartials, deleteInputsAndPartials };

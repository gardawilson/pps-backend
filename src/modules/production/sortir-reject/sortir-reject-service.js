// services/sortir-reject-service.js
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
} = require('../../../core/utils/jam-kerja-helper');
const sharedInputService = require('../../../core/shared/produksi-input.service');
const { badReq, conflict } = require('../../../core/utils/http-error');



async function getAllSortirReject(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;

  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoBJSortir LIKE '%' + @search + '%')
  `;

  // 1) Count (lightweight)
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.BJSortirReject_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);

  const countRes = await countReq.query(countQry);
  const total = countRes.recordset?.[0]?.total || 0;

  if (total === 0) return { data: [], total: 0 };

  // 2) Data + Flag Tutup Transaksi + JOIN username + JOIN warehouse
  const dataQry = `
    ;WITH LastClosed AS (
      SELECT TOP 1
        CONVERT(date, PeriodHarian) AS LastClosedDate
      FROM dbo.MstTutupTransaksiHarian WITH (NOLOCK)
      WHERE [Lock] = 1
      ORDER BY CONVERT(date, PeriodHarian) DESC, Id DESC
    )
    SELECT
      h.NoBJSortir,
      h.TglBJSortir,
      h.IdWarehouse,
      h.IdUsername,

      -- join mst username
      u.Username,

      -- join mst warehouse
      w.NamaWarehouse,

      -- (opsional utk frontend)
      lc.LastClosedDate AS LastClosedDate,

      -- flag tutup transaksi
      CASE
        WHEN lc.LastClosedDate IS NOT NULL
         AND CONVERT(date, h.TglBJSortir) <= lc.LastClosedDate
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
      END AS IsLocked

    FROM dbo.BJSortirReject_h h WITH (NOLOCK)

    LEFT JOIN dbo.MstUsername u WITH (NOLOCK)
      ON u.IdUsername = h.IdUsername

    LEFT JOIN dbo.MstWarehouse w WITH (NOLOCK)
      ON w.IdWarehouse = h.IdWarehouse
      -- kalau mau hanya yang aktif:
      -- AND w.[Enable] = 1

    OUTER APPLY (
      SELECT TOP 1 LastClosedDate
      FROM LastClosed
    ) lc

    ${whereClause}

    ORDER BY h.TglBJSortir DESC, h.NoBJSortir DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, ps);

  const dataRes = await dataReq.query(dataQry);
  return { data: dataRes.recordset || [], total };
}



async function getSortirRejectByDate(date) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      h.NoBJSortir,
      h.TglBJSortir,
      h.IdUsername
    FROM [dbo].[BJSortirReject_h] h
    WHERE CONVERT(date, h.TglBJSortir) = @date
    ORDER BY h.NoBJSortir ASC;
  `;

  request.input('date', sql.Date, date);
  const result = await request.query(query);
  return result.recordset;
}


async function createSortirReject(payload) {
  const must = [];
  if (!payload?.tglBJSortir) must.push('tglBJSortir');
  if (payload?.idWarehouse == null) must.push('idWarehouse');
  if (payload?.idUsername == null) must.push('idUsername');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) normalize date + lock guard
    const effectiveDate = resolveEffectiveDateForCreate(payload.tglBJSortir);

    await assertNotLocked({
      date: effectiveDate,
      runner: tx,
      action: 'create BJSortirReject',
      useLock: true,
    });

    // 1) generate NoBJSortir
    // 🔧 sesuaikan prefix sesuai standar kamu
    const no1 = await generateNextCode(tx, {
      tableName: 'dbo.BJSortirReject_h',
      columnName: 'NoBJSortir',
      prefix: 'J.',
      width: 10,
    });

    // optional anti-race double check
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBJSortir', sql.VarChar(50), no1)
      .query(`
        SELECT 1
        FROM dbo.BJSortirReject_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBJSortir = @NoBJSortir
      `);

    const noBJSortir = exist.recordset.length
      ? await generateNextCode(tx, {
          tableName: 'dbo.BJSortirReject_h',
          columnName: 'NoBJSortir',
          prefix: 'J.',
          width: 10,
        })
      : no1;

    // 2) insert header
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoBJSortir', sql.VarChar(50), noBJSortir)
      .input('TglBJSortir', sql.Date, effectiveDate)
      .input('IdWarehouse', sql.Int, payload.idWarehouse) // ✅ INT
      .input('IdUsername', sql.Int, payload.idUsername);  // ✅ INT

    const insertSql = `
      INSERT INTO dbo.BJSortirReject_h (
        NoBJSortir,
        TglBJSortir,
        IdWarehouse,
        IdUsername
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoBJSortir,
        @TglBJSortir,
        @IdWarehouse,
        @IdUsername
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


async function updateSortirReject(noBJSortir, payload) {
  if (!noBJSortir) throw badReq('noBJSortir wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) lock header + ambil tanggal lama dari config
    const { docDateOnly: oldDocDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'sortirReject', // ✅ must exist in your config
      codeValue: noBJSortir,       // ✅ NoBJSortir
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) if user sends tglBJSortir -> new date
    const isChangingDate = payload?.tglBJSortir !== undefined;
    let newDocDateOnly = null;

    if (isChangingDate) {
      if (!payload.tglBJSortir) throw badReq('tglBJSortir tidak boleh kosong');
      newDocDateOnly = resolveEffectiveDateForCreate(payload.tglBJSortir);
    }

    // 2) guard tutup transaksi
    await assertNotLocked({
      date: oldDocDateOnly,
      runner: tx,
      action: 'update BJSortirReject (current date)',
      useLock: true,
    });

    if (isChangingDate) {
      await assertNotLocked({
        date: newDocDateOnly,
        runner: tx,
        action: 'update BJSortirReject (new date)',
        useLock: true,
      });
    }

    // 3) build dynamic SET (HEADER ONLY)
    const sets = [];
    const rqUpd = new sql.Request(tx);

    if (isChangingDate) {
      sets.push('TglBJSortir = @TglBJSortir');
      rqUpd.input('TglBJSortir', sql.Date, newDocDateOnly);
    }

    if (payload.idWarehouse !== undefined) {
      if (payload.idWarehouse == null) throw badReq('idWarehouse tidak boleh kosong');
      sets.push('IdWarehouse = @IdWarehouse');
      rqUpd.input('IdWarehouse', sql.Int, payload.idWarehouse);
    }

    if (sets.length === 0) throw badReq('No fields to update');

    rqUpd.input('NoBJSortir', sql.VarChar(50), noBJSortir);

    const updateSql = `
      UPDATE dbo.BJSortirReject_h
      SET ${sets.join(', ')}
      WHERE NoBJSortir = @NoBJSortir;

      SELECT *
      FROM dbo.BJSortirReject_h
      WHERE NoBJSortir = @NoBJSortir;
    `;

    const updRes = await rqUpd.query(updateSql);
    const updatedHeader = updRes.recordset?.[0] || null;

    // 4) if tanggal berubah -> sync DateUsage untuk semua label yg dipakai dokumen ini
    if (isChangingDate && updatedHeader) {
      const usageDate = resolveEffectiveDateForCreate(updatedHeader.TglBJSortir);

      const rqUsage = new sql.Request(tx);
      rqUsage
        .input('NoBJSortir', sql.VarChar(50), noBJSortir)
        .input('Tanggal', sql.Date, usageDate);

      const sqlUpdateUsage = `
        /* =======================
           SORTIR REJECT -> DateUsage Sync
           Rule: update hanya jika DateUsage sudah ada (NOT NULL)
           ======================= */

        -- BARANG JADI (via BJSortirRejectInputLabelBarangJadi)
        UPDATE bj
        SET bj.DateUsage = @Tanggal
        FROM dbo.BarangJadi AS bj
        WHERE bj.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.BJSortirRejectInputLabelBarangJadi AS map
            WHERE map.NoBJSortir = @NoBJSortir
              AND map.NoBJ = bj.NoBJ
          );

        -- FURNITURE WIP (via BJSortirRejectInputLabelFurnitureWIP)
        UPDATE fw
        SET fw.DateUsage = @Tanggal
        FROM dbo.FurnitureWIP AS fw
        WHERE fw.DateUsage IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM dbo.BJSortirRejectInputLabelFurnitureWIP AS map
            WHERE map.NoBJSortir = @NoBJSortir
              AND map.NoFurnitureWIP = fw.NoFurnitureWIP
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



async function deleteSortirReject(noBJSortir) {
  if (!noBJSortir) throw badReq('noBJSortir wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 0) ambil docDateOnly dari config (lock header)
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'sortirReject',
      codeValue: noBJSortir,
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'delete BJSortirReject',
      useLock: true,
    });

    // 2) cek output dulu (Reject output)
    const rqOut = new sql.Request(tx);
    const outRes = await rqOut
      .input('NoBJSortir', sql.VarChar(50), noBJSortir)
      .query(`
        SELECT COUNT(1) AS CntOutputReject
        FROM dbo.BJSortirRejectOutputLabelReject WITH (NOLOCK)
        WHERE NoBJSortir = @NoBJSortir;
      `);

    const row = outRes.recordset?.[0] || { CntOutputReject: 0 };
    const hasOutputReject = (row.CntOutputReject || 0) > 0;

    if (hasOutputReject) {
      throw badReq(
        'Tidak dapat menghapus NoBJSortir ini karena sudah memiliki data output (Label Reject).'
      );
    }

    // 3) delete input + reset dateusage + delete header
    const req = new sql.Request(tx);
    req.input('NoBJSortir', sql.VarChar(50), noBJSortir);

    const sqlDelete = `
      DECLARE @BJKeys TABLE (NoBJ varchar(50) PRIMARY KEY);
      DECLARE @FWIPKeys TABLE (NoFurnitureWIP varchar(50) PRIMARY KEY);

      /* =======================
         A) collect BJ keys
         ======================= */
      INSERT INTO @BJKeys (NoBJ)
      SELECT DISTINCT map.NoBJ
      FROM dbo.BJSortirRejectInputLabelBarangJadi AS map
      WHERE map.NoBJSortir = @NoBJSortir
        AND map.NoBJ IS NOT NULL;

      /* =======================
         B) collect FWIP keys
         ======================= */
      INSERT INTO @FWIPKeys (NoFurnitureWIP)
      SELECT DISTINCT map.NoFurnitureWIP
      FROM dbo.BJSortirRejectInputLabelFurnitureWIP AS map
      WHERE map.NoBJSortir = @NoBJSortir
        AND map.NoFurnitureWIP IS NOT NULL;

      /* =======================
         C) delete mappings (input)
         ======================= */
      DELETE FROM dbo.BJSortirRejectInputLabelBarangJadi
      WHERE NoBJSortir = @NoBJSortir;

      DELETE FROM dbo.BJSortirRejectInputLabelFurnitureWIP
      WHERE NoBJSortir = @NoBJSortir;

      /* =======================
         D) reset DateUsage
         ======================= */
      UPDATE bj
      SET bj.DateUsage = NULL
      FROM dbo.BarangJadi AS bj
      JOIN @BJKeys AS k
        ON k.NoBJ = bj.NoBJ;

      UPDATE fw
      SET fw.DateUsage = NULL
      FROM dbo.FurnitureWIP AS fw
      JOIN @FWIPKeys AS k
        ON k.NoFurnitureWIP = fw.NoFurnitureWIP;

      /* =======================
         E) delete header last
         ======================= */
      DELETE FROM dbo.BJSortirReject_h
      WHERE NoBJSortir = @NoBJSortir;
    `;

    await req.query(sqlDelete);

    await tx.commit();
    return { success: true };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
}


/**
 * ✅ GET Inputs for BJSortirReject
 * Output shape meniru Packing:
 * {
 *   furnitureWip: [...],
 *   cabinetMaterial: [...],
 *   barangJadi: [...],
 *   summary: { furnitureWip: n, cabinetMaterial: n, barangJadi: n }
 * }
 */
async function fetchInputs(noBJSortir) {
  const pool = await poolPromise;
  const req = pool.request();
  req.input('no', sql.VarChar(50), noBJSortir);

  const q = `
    /* ===================== [1] MAIN INPUTS (UNION) ===================== */

    -- FurnitureWIP FULL (BB...)
    SELECT
      'fwip' AS Src,
      map.NoBJSortir,
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
    FROM dbo.BJSortirRejectInputLabelFurnitureWIP map WITH (NOLOCK)
    LEFT JOIN dbo.FurnitureWIP fw WITH (NOLOCK)
      ON fw.NoFurnitureWIP = map.NoFurnitureWIP
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = fw.IDFurnitureWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE map.NoBJSortir = @no

    UNION ALL

    -- CabinetWIP input (tapi kita map ke bucket cabinetMaterial agar sama seperti packing)
    SELECT
      'material' AS Src,
      c.NoBJSortir,
      CAST(c.IdCabinetWIP AS varchar(50)) AS Ref1,  -- nanti jadi idCabinetMaterial (string)
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      CAST(NULL AS decimal(18,3)) AS Berat,
      CAST(c.Pcs AS int)          AS Pcs,           -- nanti jadi jumlah
      CAST(NULL AS bit)           AS IsPartial,
      c.IdCabinetWIP              AS IdJenis,
      mw.Nama                     AS NamaJenis,
      uom.NamaUOM                 AS NamaUOM,
      CAST(NULL AS datetime)      AS DatetimeInput
    FROM dbo.BJSortirRejectInputCabinetWIP c WITH (NOLOCK)
    LEFT JOIN dbo.MstCabinetWIP mw WITH (NOLOCK)
      ON mw.IdCabinetWIP = c.IdCabinetWIP
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mw.IdUOM
    WHERE c.NoBJSortir = @no

    UNION ALL

    -- Barang Jadi labels (BA...)
    SELECT
      'bj' AS Src,
      map.NoBJSortir,
      map.NoBJ AS Ref1,
      CAST(NULL AS varchar(50)) AS Ref2,
      CAST(NULL AS varchar(50)) AS Ref3,

      bj.Berat,
      bj.Pcs,
      bj.IsPartial,
      bj.IdBJ                AS IdJenis,
      mbj.NamaBJ             AS NamaJenis,
      uom.NamaUOM            AS NamaUOM,
      CAST(NULL AS datetime) AS DatetimeInput
    FROM dbo.BJSortirRejectInputLabelBarangJadi map WITH (NOLOCK)
    LEFT JOIN dbo.BarangJadi bj WITH (NOLOCK)
      ON bj.NoBJ = map.NoBJ
    LEFT JOIN dbo.MstBarangJadi mbj WITH (NOLOCK)
      ON mbj.IdBJ = bj.IdBJ
    LEFT JOIN dbo.MstUOM uom WITH (NOLOCK)
      ON uom.IdUOM = mbj.IdUOM
    WHERE map.NoBJSortir = @no


    ORDER BY Src ASC, Ref1 DESC, Ref2 ASC;
  `;

  const rs = await req.query(q);
  const mainRows = rs.recordset || [];

  const out = {
    furnitureWip: [],
    cabinetMaterial: [],
    barangJadi: [],
    summary: { furnitureWip: 0, cabinetMaterial: 0, barangJadi: 0 },
  };

  // MAIN rows (imitate packing mapping style)
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
        out.furnitureWip.push({
          noFurnitureWip: r.Ref1,
          ...base,
        });
        break;

      case 'material':
        // meniru packing: idCabinetMaterial + jumlah
        out.cabinetMaterial.push({
          idCabinetMaterial: r.Ref1, // string cast (konsisten seperti packing)
          jumlah: r.Pcs ?? null,     // Pcs -> jumlah
          ...base,
        });
        break;

      case 'bj':
        // bucket baru, tapi field-nya tetap "packing-ish"
        out.barangJadi.push({
          noBJ: r.Ref1,
          ...base,
        });
        break;
    }
  }

  out.summary.furnitureWip = out.furnitureWip.length;
  out.summary.cabinetMaterial = out.cabinetMaterial.length;
  out.summary.barangJadi = out.barangJadi.length;

  return out;
}


/**
 * Payload shape (arrays optional):
 * {
 *   furnitureWip:     [{ noFurnitureWip }],
 *   cabinetMaterial:  [{ idCabinetMaterial, jumlah }],   // di DB: BJSortirRejectInputCabinetWIP(IdCabinetWIP, Pcs)
 *   barangJadi:       [{ noBJ }]
 * }
 */
async function upsertInputs(noProduksi, payload, ctx) {
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
  return sharedInputService.upsertInputsAndPartials('sortirReject', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}

async function deleteInputs(noProduksi, payload, ctx) {
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
  return sharedInputService.deleteInputsAndPartials('sortirReject', no, body, {
    actorId: Math.trunc(actorIdNum),
    actorUsername,
    requestId,
  });
}



module.exports = {
  getAllSortirReject,
  getSortirRejectByDate,
  createSortirReject,
  updateSortirReject,
  deleteSortirReject,
  fetchInputs,
  upsertInputs,
  deleteInputs
  
};

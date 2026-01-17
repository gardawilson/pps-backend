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
const { badReq } = require('../../../core/utils/http-error');



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
async function upsertInputs(noBJSortir, payload) {
  if (!noBJSortir) throw badReq('noBJSortir wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    barangJadi: norm(payload?.barangJadi),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) lock header & get doc date
    const { docDateOnly } = await loadDocDateOnlyFromConfig({
      entityKey: 'sortirReject', // ✅ must match config key
      codeValue: noBJSortir,       // ✅ NoBJSortir
      runner: tx,
      useLock: true,
      throwIfNotFound: true,
    });

    // 1) guard tutup transaksi
    await assertNotLocked({
      date: docDateOnly,
      runner: tx,
      action: 'upsert SortirReject inputs',
      useLock: true,
    });

    // 2) attach inputs
    const fwipAttach = await _insertFurnitureWipWithTx(tx, noBJSortir, {
      furnitureWip: body.furnitureWip,
    });

    const bjAttach = await _insertBarangJadiWithTx(tx, noBJSortir, {
      barangJadi: body.barangJadi,
    });

    const cabAttach = await _insertCabinetWipWithTx(tx, noBJSortir, {
      cabinetMaterial: body.cabinetMaterial,
    });

    const attachments = {
      furnitureWip: fwipAttach.furnitureWip,
      barangJadi: bjAttach.barangJadi,
      cabinetMaterial: cabAttach.cabinetMaterial,
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

    const hasInvalid = totalInvalid > 0;
    const hasNoSuccess = totalInserted + totalUpdated === 0;

    const response = {
      noBJSortir,
      summary: {
        totalInserted,
        totalUpdated,
        totalSkipped,
        totalInvalid,
      },
      details: {
        inputs: _buildInputDetails(attachments, body),
      },
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
   Details builders (packing style)
===================== */

function _buildInputDetails(attachments, requestBody) {
  const details = [];

  const sections = [
    { key: 'furnitureWip', label: 'Furniture WIP' },
    { key: 'barangJadi', label: 'Barang Jadi' },
    { key: 'cabinetMaterial', label: 'Cabinet WIP' }, // label tampilan boleh “Cabinet WIP”
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

function _buildSectionMessage(label, result) {
  const parts = [];
  if (result.inserted > 0) parts.push(`${result.inserted} berhasil ditambahkan`);
  if (result.updated > 0) parts.push(`${result.updated} berhasil diperbarui`);
  if (result.skipped > 0) parts.push(`${result.skipped} sudah ada (dilewati)`);
  if (result.invalid > 0) parts.push(`${result.invalid} tidak valid`);
  return parts.length ? `${label}: ${parts.join(', ')}` : `Tidak ada ${label} yang diproses`;
}

/* =====================
   SQL helpers (SortirReject tables)
===================== */

async function _insertFurnitureWipWithTx(tx, noBJSortir, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJSortir);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.TglBJSortir AS datetime)
  FROM dbo.BJSortirReject_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJSortir = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJSortirReject_h tidak ditemukan / TglBJSortir NULL', 16, 1);
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
    SELECT 1 FROM dbo.BJSortirRejectInputLabelFurnitureWIP x WITH (NOLOCK)
    WHERE x.NoBJSortir=@no AND x.NoFurnitureWIP=r.NoFurnitureWip
  );

  -- eligible: FWIP ada dan DateUsage masih NULL (belum dipakai)
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

  INSERT INTO dbo.BJSortirRejectInputLabelFurnitureWIP (NoBJSortir, NoFurnitureWIP)
  OUTPUT INSERTED.NoFurnitureWIP INTO @insFW(NoFurnitureWIP)
  SELECT @no, e.NoFurnitureWip
  FROM @eligibleNotMapped e;

  SET @fwIns = @@ROWCOUNT;

  -- set DateUsage ke tanggal dokumen
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

async function _insertBarangJadiWithTx(tx, noBJSortir, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJSortir);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.TglBJSortir AS datetime)
  FROM dbo.BJSortirReject_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJSortir = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJSortirReject_h tidak ditemukan / TglBJSortir NULL', 16, 1);
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
    SELECT 1 FROM dbo.BJSortirRejectInputLabelBarangJadi x WITH (NOLOCK)
    WHERE x.NoBJSortir=@no AND x.NoBJ=r.NoBJ
  );

  -- eligible: BJ exists & DateUsage is NULL (assumption tabel BarangJadi punya DateUsage)
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

  INSERT INTO dbo.BJSortirRejectInputLabelBarangJadi (NoBJSortir, NoBJ)
  OUTPUT INSERTED.NoBJ INTO @insBJ(NoBJ)
  SELECT @no, e.NoBJ
  FROM @eligibleNotMapped e;

  SET @bjIns = @@ROWCOUNT;

  -- set DateUsage BJ ke tanggal dokumen
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

/**
 * NOTE:
 * Table kamu: BJSortirRejectInputCabinetWIP(NoBJSortir, IdCabinetWIP, Pcs)
 * Tapi kita expose payload sebagai cabinetMaterial[{idCabinetMaterial, jumlah}]
 * supaya Flutter reuse pola Packing.
 */
async function _insertCabinetWipWithTx(tx, noBJSortir, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJSortir);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @cIns int=0, @cUpd int=0, @cInv int=0;

  DECLARE @Src TABLE(IdCabinetWIP int, Pcs int);

  INSERT INTO @Src(IdCabinetWIP, Pcs)
  SELECT IdCabinetMaterial, SUM(ISNULL(Jumlah,0)) AS Pcs
  FROM OPENJSON(@jsInputs, '$.cabinetMaterial')
  WITH (
    IdCabinetMaterial int '$.idCabinetMaterial',
    Jumlah int '$.jumlah'
  )
  WHERE IdCabinetMaterial IS NOT NULL
  GROUP BY IdCabinetMaterial;

  -- invalid: pcs <= 0 atau master tidak ada / tidak enable
  SELECT @cInv = COUNT(*)
  FROM @Src s
  WHERE s.Pcs <= 0
     OR NOT EXISTS (
        SELECT 1
        FROM dbo.MstCabinetWIP m WITH (NOLOCK)
        WHERE m.IdCabinetWIP=s.IdCabinetWIP
          AND (m.Enable=1 OR m.Enable IS NULL) -- sesuaikan jika ada kolom Enable
     );

  -- update existing
  UPDATE tgt
  SET tgt.Pcs = src.Pcs
  FROM dbo.BJSortirRejectInputCabinetWIP tgt
  JOIN @Src src ON src.IdCabinetWIP=tgt.IdCabinetWIP
  WHERE tgt.NoBJSortir=@no
    AND src.Pcs > 0
    AND EXISTS (
      SELECT 1
      FROM dbo.MstCabinetWIP m WITH (NOLOCK)
      WHERE m.IdCabinetWIP=src.IdCabinetWIP
        AND (m.Enable=1 OR m.Enable IS NULL)
    );

  SET @cUpd = @@ROWCOUNT;

  -- insert new
  INSERT INTO dbo.BJSortirRejectInputCabinetWIP (NoBJSortir, IdCabinetWIP, Pcs)
  SELECT @no, src.IdCabinetWIP, src.Pcs
  FROM @Src src
  WHERE src.Pcs > 0
    AND EXISTS (
      SELECT 1
      FROM dbo.MstCabinetWIP m WITH (NOLOCK)
      WHERE m.IdCabinetWIP=src.IdCabinetWIP
        AND (m.Enable=1 OR m.Enable IS NULL)
    )
    AND NOT EXISTS (
      SELECT 1
      FROM dbo.BJSortirRejectInputCabinetWIP x WITH (NOLOCK)
      WHERE x.NoBJSortir=@no AND x.IdCabinetWIP=src.IdCabinetWIP
    );

  SET @cIns = @@ROWCOUNT;

  SELECT
    @cIns AS Inserted,
    @cUpd AS Updated,
    0 AS Skipped,
    @cInv AS Invalid;
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


async function deleteInputs(noBJSortir, payload) {
  if (!noBJSortir) throw badReq('noBJSortir wajib');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const norm = (a) => (Array.isArray(a) ? a : []);

  const body = {
    furnitureWip: norm(payload?.furnitureWip),
    cabinetMaterial: norm(payload?.cabinetMaterial),
    barangJadi: norm(payload?.barangJadi),
  };

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) lock header & get doc date
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
      action: 'delete SortirReject inputs',
      useLock: true,
    });

    // 2) delete each section
    const fwRes = await _deleteFurnitureWipWithTx(tx, noBJSortir, {
      furnitureWip: body.furnitureWip,
    });

    const bjRes = await _deleteBarangJadiWithTx(tx, noBJSortir, {
      barangJadi: body.barangJadi,
    });

    const cabRes = await _deleteCabinetWipWithTx(tx, noBJSortir, {
      cabinetMaterial: body.cabinetMaterial,
    });

    await tx.commit();

    const summary = {
      furnitureWip: fwRes?.furnitureWip ?? { deleted: 0, notFound: 0 },
      cabinetMaterial: cabRes?.cabinetMaterial ?? { deleted: 0, notFound: 0 },
      barangJadi: bjRes?.barangJadi ?? { deleted: 0, notFound: 0 },
    };

    const totalDeleted =
      (summary.furnitureWip.deleted || 0) +
      (summary.cabinetMaterial.deleted || 0) +
      (summary.barangJadi.deleted || 0);

    const totalNotFound =
      (summary.furnitureWip.notFound || 0) +
      (summary.cabinetMaterial.notFound || 0) +
      (summary.barangJadi.notFound || 0);

    return {
      success: totalDeleted > 0,
      hasWarnings: totalNotFound > 0,
      data: {
        noBJSortir,
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


async function _deleteFurnitureWipWithTx(tx, noBJSortir, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJSortir);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.TglBJSortir AS datetime)
  FROM dbo.BJSortirReject_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJSortir = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJSortirReject_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @fwDeleted int = 0, @fwNotFound int = 0;

  SELECT @fwDeleted = COUNT(*)
  FROM dbo.BJSortirRejectInputLabelFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoBJSortir = @no;

  IF @fwDeleted > 0
  BEGIN
    UPDATE f
    SET f.DateUsage = NULL
    FROM dbo.FurnitureWIP f
    INNER JOIN dbo.BJSortirRejectInputLabelFurnitureWIP map
      ON f.NoFurnitureWIP = map.NoFurnitureWIP
    INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
      WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
      ON map.NoFurnitureWIP = j.noFurnitureWip
    WHERE map.NoBJSortir = @no;
  END

  DELETE map
  FROM dbo.BJSortirRejectInputLabelFurnitureWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.furnitureWip')
    WITH (noFurnitureWip varchar(50) '$.noFurnitureWip') j
    ON map.NoFurnitureWIP = j.noFurnitureWip
  WHERE map.NoBJSortir = @no;

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

async function _deleteBarangJadiWithTx(tx, noBJSortir, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJSortir);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.TglBJSortir AS datetime)
  FROM dbo.BJSortirReject_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJSortir = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJSortirReject_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @bjDeleted int = 0, @bjNotFound int = 0;

  SELECT @bjDeleted = COUNT(*)
  FROM dbo.BJSortirRejectInputLabelBarangJadi map
  INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
    WITH (noBJ varchar(50) '$.noBJ') j
    ON map.NoBJ = j.noBJ
  WHERE map.NoBJSortir = @no;

  IF @bjDeleted > 0
  BEGIN
    UPDATE b
    SET b.DateUsage = NULL
    FROM dbo.BarangJadi b
    INNER JOIN dbo.BJSortirRejectInputLabelBarangJadi map
      ON b.NoBJ = map.NoBJ
    INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
      WITH (noBJ varchar(50) '$.noBJ') j
      ON map.NoBJ = j.noBJ
    WHERE map.NoBJSortir = @no;
  END

  DELETE map
  FROM dbo.BJSortirRejectInputLabelBarangJadi map
  INNER JOIN OPENJSON(@jsInputs, '$.barangJadi')
    WITH (noBJ varchar(50) '$.noBJ') j
    ON map.NoBJ = j.noBJ
  WHERE map.NoBJSortir = @no;

  DECLARE @bjRequested int;
  SELECT @bjRequested = COUNT(*) FROM OPENJSON(@jsInputs, '$.barangJadi');

  SET @bjNotFound = @bjRequested - @bjDeleted;

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

async function _deleteCabinetWipWithTx(tx, noBJSortir, lists) {
  const req = new sql.Request(tx);
  req.input('no', sql.VarChar(50), noBJSortir);
  req.input('jsInputs', sql.NVarChar(sql.MAX), JSON.stringify(lists));

  const SQL = `
  SET NOCOUNT ON;

  DECLARE @tgl datetime;
  SELECT @tgl = CAST(h.TglBJSortir AS datetime)
  FROM dbo.BJSortirReject_h h WITH (UPDLOCK, HOLDLOCK)
  WHERE h.NoBJSortir = @no;

  IF @tgl IS NULL
  BEGIN
    RAISERROR('Header BJSortirReject_h tidak ditemukan', 16, 1);
    RETURN;
  END;

  DECLARE @matDeleted int = 0, @matNotFound int = 0;

  SELECT @matDeleted = COUNT(*)
  FROM dbo.BJSortirRejectInputCabinetWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetWIP = j.idCabinetMaterial
  WHERE map.NoBJSortir = @no;

  DELETE map
  FROM dbo.BJSortirRejectInputCabinetWIP map
  INNER JOIN OPENJSON(@jsInputs, '$.cabinetMaterial')
    WITH (idCabinetMaterial int '$.idCabinetMaterial') j
    ON map.IdCabinetWIP = j.idCabinetMaterial
  WHERE map.NoBJSortir = @no;

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

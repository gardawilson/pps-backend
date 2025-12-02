// services/labels/furniture-wip-service.js
const { sql, poolPromise } = require('../../../core/config/db');

exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama AS NamaFurnitureWIP,

      -- 🔹 Pcs sudah dikurangi partial (jika IsPartial = 1)
      CASE 
        WHEN f.IsPartial = 1 THEN
          CASE
            WHEN ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0) < 0 
              THEN 0
            ELSE ISNULL(f.Pcs, 0) - ISNULL(MAX(fp.TotalPartialPcs), 0)
          END
        ELSE ISNULL(f.Pcs, 0)
      END AS Pcs,

      ISNULL(f.Berat, 0) AS Berat,

      f.IsPartial,
      f.IdWarna,
      f.Blok,
      f.IdLokasi

    FROM [dbo].[FurnitureWIP] f
    -- 🔹 Aggregate partial per NoFurnitureWIP
    LEFT JOIN (
      SELECT
        NoFurnitureWIP,
        SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
      FROM [dbo].[FurnitureWIPPartial]
      GROUP BY NoFurnitureWIP
    ) fp
      ON fp.NoFurnitureWIP = f.NoFurnitureWIP

    -- 🔗 Master nama furniture WIP
    LEFT JOIN [dbo].[MstCabinetWIP] cw
      ON cw.IdCabinetWIP = f.IdFurnitureWIP

    WHERE 1=1
      AND f.DateUsage IS NULL
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search
             )`
          : ''
      }
    GROUP BY
      f.NoFurnitureWIP,
      f.DateCreate,
      f.IdFurnitureWIP,
      cw.Nama,
      f.Pcs,
      f.Berat,
      f.IsPartial,
      f.IdWarna,
      f.Blok,
      f.IdLokasi
    ORDER BY f.NoFurnitureWIP DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT f.NoFurnitureWIP) AS total
    FROM [dbo].[FurnitureWIP] f
    LEFT JOIN [dbo].[MstCabinetWIP] cw
      ON cw.IdCabinetWIP = f.IdFurnitureWIP
    WHERE 1=1
      AND f.DateUsage IS NULL
      ${
        search
          ? `AND (
               f.NoFurnitureWIP LIKE @search
               OR f.Blok LIKE @search
               OR CONVERT(VARCHAR(20), f.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), f.IdFurnitureWIP) LIKE @search
               OR ISNULL(cw.Nama,'') LIKE @search
             )`
          : ''
      }
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);
  if (search) {
    request.input('search', sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};




function padLeft(num, width) {
    const s = String(num);
    return s.length >= width ? s : '0'.repeat(width - s.length) + s;
  }
  
  // Generate next NoFurnitureWIP: e.g. 'FW.0000000002'
  // ⚠️ GANTI prefix 'FW.' kalau format NoFurnitureWIP kamu beda.
  async function generateNextNoFurnitureWip(
    tx,
    { prefix = 'BB.', width = 10 } = {}
  ) {
    const rq = new sql.Request(tx);
    const q = `
      SELECT TOP 1 f.NoFurnitureWIP
      FROM [dbo].[FurnitureWIP] AS f WITH (UPDLOCK, HOLDLOCK)
      WHERE f.NoFurnitureWIP LIKE @prefix + '%'
      ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(f.NoFurnitureWIP, LEN(@prefix) + 1, 50)) DESC,
               f.NoFurnitureWIP DESC;
    `;
    const r = await rq.input('prefix', sql.VarChar, prefix).query(q);
  
    let lastNum = 0;
    if (r.recordset.length > 0) {
      const last = r.recordset[0].NoFurnitureWIP;
      const numericPart = last.substring(prefix.length);
      lastNum = parseInt(numericPart, 10) || 0;
    }
    const next = lastNum + 1;
    return prefix + padLeft(next, width);
  }
  
  exports.createFurnitureWip = async (payload) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    const header = payload?.header || {};
    const outputCode = (payload?.outputCode || '').toString().trim();
  
    // ---- validation helper
    const badReq = (msg) => {
      const e = new Error(msg);
      e.statusCode = 400;
      return e;
    };
  
    if (!header.IdFurnitureWIP) {
      throw badReq('IdFurnitureWIP is required');
    }
  
    // Wajib link ke salah satu sumber label (prefix-based)
    if (!outputCode) {
      throw badReq('outputCode is required (BH., BI., BG., L., etc.)');
    }
  
    let outputType = null;
    let mappingTable = null;
  
 // Prefix rules (updated)
 if (outputCode.startsWith('BH.')) {
    outputType = 'HOTSTAMPING';
    mappingTable = 'HotStampingOutputLabelFWIP';
  } else if (outputCode.startsWith('BI.')) {
    outputType = 'PASANG_KUNCI';
    mappingTable = 'PasangKunciOutputLabelFWIP';
  } else if (outputCode.startsWith('BG.')) {
    outputType = 'BONGKAR_SUSUN';
    mappingTable = 'BongkarSusunOutputFurnitureWIP';
  } else if (outputCode.startsWith('L.')) {
    outputType = 'RETUR';
    mappingTable = 'BJReturFurnitureWIP_d';
  } else if (outputCode.startsWith('BJ.')) {            // 🔹 NEW
    outputType = 'SPANNER';
    mappingTable = 'SpannerOutputLabelFWIP';
  } else if (outputCode.startsWith('S.')) {             // 🔹 NEW
    outputType = 'INJECT';
    mappingTable = 'InjectProduksiOutputFurnitureWIP';
  } else {
    throw badReq(
      'outputCode prefix not recognized (supported: BH., BI., BG., L., BJ., S.)'
    );
  }
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // 1) Generate NoFurnitureWIP
      const generatedNo = await generateNextNoFurnitureWip(tx, {
        prefix: 'BB.', // GANTI kalau prefix NoFurnitureWIP kamu berbeda
        width: 10,
      });
  
      // Double-check uniqueness (sangat jarang kepakai)
      const rqCheck = new sql.Request(tx);
      const exist = await rqCheck
        .input('NoFurnitureWIP', sql.VarChar, generatedNo)
        .query(`
          SELECT 1
          FROM [dbo].[FurnitureWIP] WITH (UPDLOCK, HOLDLOCK)
          WHERE NoFurnitureWIP = @NoFurnitureWIP
        `);
  
      const noFurnitureWip =
        exist.recordset.length > 0
          ? await generateNextNoFurnitureWip(tx, { prefix: 'BB.', width: 10 })
          : generatedNo;
  
      // 2) Insert header ke dbo.FurnitureWIP
      const nowDateOnly = header.DateCreate || null; // null -> GETDATE() (date only)
      const insertHeaderSql = `
        INSERT INTO [dbo].[FurnitureWIP] (
          NoFurnitureWIP,
          DateCreate,
          Pcs,
          IdFurnitureWIP,
          Berat,
          IsPartial,
          DateUsage,
          IdWarna,
          CreateBy,
          DateTimeCreate,
          Blok,
          IdLokasi
        )
        VALUES (
          @NoFurnitureWIP,
          ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
          @Pcs,
          @IdFurnitureWIP,
          @Berat,
          @IsPartial,
          NULL,
          @IdWarna,
          @CreateBy,
          GETDATE(),
          @Blok,
          @IdLokasi
        );
      `;
  
      const rqHeader = new sql.Request(tx);
  
      // normalize IdLokasi
      const rawIdLokasi = header.IdLokasi;
      let idLokasiVal = null;
      if (rawIdLokasi !== undefined && rawIdLokasi !== null) {
        idLokasiVal = String(rawIdLokasi).trim();
        if (idLokasiVal.length === 0) {
          idLokasiVal = null;
        }
      }
  
      rqHeader
        .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
        .input('Pcs', sql.Decimal(18, 3), header.Pcs ?? null)
        .input('IdFurnitureWIP', sql.Int, header.IdFurnitureWIP)
        .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
        .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
        .input('IdWarna', sql.Int, header.IdWarna ?? null)
        .input('CreateBy', sql.VarChar, header.CreateBy ?? null)
        .input('Blok', sql.VarChar, header.Blok ?? null)
        .input('IdLokasi', sql.VarChar, idLokasiVal);
  
      if (nowDateOnly) {
        rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
      }
  
      await rqHeader.query(insertHeaderSql);
  
    // 3) Insert mapping berdasarkan outputType / mappingTable
    if (mappingTable === 'HotStampingOutputLabelFWIP') {
        const q = `
          INSERT INTO [dbo].[HotStampingOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
          VALUES (@OutputCode, @NoFurnitureWIP);
        `;
        await new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
          .query(q);
  
      } else if (mappingTable === 'PasangKunciOutputLabelFWIP') {
        const q = `
          INSERT INTO [dbo].[PasangKunciOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
          VALUES (@OutputCode, @NoFurnitureWIP);
        `;
        await new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
          .query(q);
  
      } else if (mappingTable === 'BongkarSusunOutputFurnitureWIP') {
        const q = `
          INSERT INTO [dbo].[BongkarSusunOutputFurnitureWIP] (NoBongkarSusun, NoFurnitureWIP)
          VALUES (@OutputCode, @NoFurnitureWIP);
        `;
        await new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
          .query(q);
  
      } else if (mappingTable === 'BJReturFurnitureWIP_d') {
        const q = `
          INSERT INTO [dbo].[BJReturFurnitureWIP_d] (NoRetur, NoFurnitureWIP)
          VALUES (@OutputCode, @NoFurnitureWIP);
        `;
        await new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
          .query(q);
  
      } else if (mappingTable === 'SpannerOutputLabelFWIP') {   // 🔹 NEW
        const q = `
          INSERT INTO [dbo].[SpannerOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
          VALUES (@OutputCode, @NoFurnitureWIP);
        `;
        await new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
          .query(q);
  
      } else if (mappingTable === 'InjectProduksiOutputFurnitureWIP') { // 🔹 NEW
        const q = `
          INSERT INTO [dbo].[InjectProduksiOutputFurnitureWIP] (NoProduksi, NoFurnitureWIP)
          VALUES (@OutputCode, @NoFurnitureWIP);
        `;
        await new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
          .query(q);
      }
  
  
      await tx.commit();
  
      return {
        header: {
          NoFurnitureWIP: noFurnitureWip,
          DateCreate: nowDateOnly || 'GETDATE()',
          Pcs: header.Pcs ?? null,
          IdFurnitureWIP: header.IdFurnitureWIP,
          Berat: header.Berat ?? null,
          IsPartial: header.IsPartial ?? 0,
          IdWarna: header.IdWarna ?? null,
          CreateBy: header.CreateBy ?? null,
          Blok: header.Blok ?? null,
          IdLokasi: header.IdLokasi ?? null,
        },
        output: {
          code: outputCode,
          type: outputType,   // 'HOTSTAMPING' / 'PASANG_KUNCI' / 'BONGKAR_SUSUN' / 'RETUR'
          mappingTable,       // table used
        },
      };
    } catch (e) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw e;
    }
  };




  const hasOwn = (obj, key) =>
    Object.prototype.hasOwnProperty.call(obj || {}, key);
  
  /**
   * Hapus semua mapping FurnitureWIP ke proses manapun
   */
  async function deleteAllMappings(tx, noFurnitureWip) {
    await new sql.Request(tx)
      .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
      .query(`
        DELETE FROM [dbo].[HotStampingOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[PasangKunciOutputLabelFWIP]           WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[BongkarSusunOutputFurnitureWIP]       WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[BJReturFurnitureWIP_d]                WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[SpannerOutputLabelFWIP]               WHERE NoFurnitureWIP = @NoFurnitureWIP;
        DELETE FROM [dbo].[InjectProduksiOutputFurnitureWIP]     WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `);
  }
  
  exports.updateFurnitureWip = async (noFurnitureWip, payload) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    const header = payload?.header || {};
    const hasOutputCodeField = hasOwn(payload, 'outputCode');
    const outputCode = (payload?.outputCode || '').toString().trim();
  
    const badReq = (msg) => {
      const e = new Error(msg);
      e.statusCode = 400;
      return e;
    };
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // 1) Ambil data existing
      const rqExisting = new sql.Request(tx);
      const existingRes = await rqExisting
        .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
        .query(`
          SELECT TOP 1
            NoFurnitureWIP,
            DateCreate,
            Pcs,
            IdFurnitureWIP,
            Berat,
            IsPartial,
            DateUsage,
            IdWarna,
            CreateBy,
            DateTimeCreate,
            Blok,
            IdLokasi
          FROM [dbo].[FurnitureWIP] WITH (UPDLOCK, HOLDLOCK)
          WHERE NoFurnitureWIP = @NoFurnitureWIP;
        `);
  
      if (existingRes.recordset.length === 0) {
        const e = new Error('Furniture WIP not found');
        e.statusCode = 404;
        throw e;
      }
  
      const current = existingRes.recordset[0];
  
      // 2) Merge field (partial update)
      const merged = {
        IdFurnitureWIP:
          header.IdFurnitureWIP ?? current.IdFurnitureWIP, // default ke existing
        Pcs: hasOwn(header, 'Pcs') ? header.Pcs : current.Pcs,
        Berat: hasOwn(header, 'Berat') ? header.Berat : current.Berat,
        IsPartial: hasOwn(header, 'IsPartial')
          ? header.IsPartial
          : current.IsPartial,
        IdWarna: hasOwn(header, 'IdWarna') ? header.IdWarna : current.IdWarna,
        Blok: hasOwn(header, 'Blok') ? header.Blok : current.Blok,
        IdLokasi: hasOwn(header, 'IdLokasi') ? header.IdLokasi : current.IdLokasi,
        DateCreate: hasOwn(header, 'DateCreate')
          ? header.DateCreate
          : current.DateCreate,
        CreateBy: hasOwn(header, 'CreateBy')
          ? header.CreateBy
          : current.CreateBy,
      };
  
      if (!merged.IdFurnitureWIP) {
        throw badReq('IdFurnitureWIP cannot be empty');
      }
  
      // normalize IdLokasi
      let idLokasiVal = merged.IdLokasi;
      if (idLokasiVal !== undefined && idLokasiVal !== null) {
        idLokasiVal = String(idLokasiVal).trim();
        if (idLokasiVal.length === 0) {
          idLokasiVal = null;
        }
      }
  
      // 3) UPDATE header
      const rqUpdate = new sql.Request(tx);
      rqUpdate
        .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
        .input('IdFurnitureWIP', sql.Int, merged.IdFurnitureWIP)
        .input('Pcs', sql.Decimal(18, 3), merged.Pcs ?? null)
        .input('Berat', sql.Decimal(18, 3), merged.Berat ?? null)
        .input('IsPartial', sql.Bit, merged.IsPartial ?? 0)
        .input('IdWarna', sql.Int, merged.IdWarna ?? null)
        .input('Blok', sql.VarChar, merged.Blok ?? null)
        .input('IdLokasi', sql.VarChar, idLokasiVal)
        .input('CreateBy', sql.VarChar, merged.CreateBy ?? null);
  
      if (merged.DateCreate) {
        rqUpdate.input('DateCreate', sql.Date, new Date(merged.DateCreate));
      }
  
      const updateSql = `
        UPDATE [dbo].[FurnitureWIP]
        SET
          IdFurnitureWIP = @IdFurnitureWIP,
          Pcs = @Pcs,
          Berat = @Berat,
          IsPartial = @IsPartial,
          IdWarna = @IdWarna,
          Blok = @Blok,
          IdLokasi = @IdLokasi,
          CreateBy = @CreateBy
          ${merged.DateCreate ? ', DateCreate = @DateCreate' : ''}
        WHERE NoFurnitureWIP = @NoFurnitureWIP;
      `;
      await rqUpdate.query(updateSql);
  
      // 4) Mapping update (optional, hanya kalau field outputCode ada)
      let outputType = null;
      let mappingTable = null;
  
      if (hasOutputCodeField) {
        // kalau dikirim tapi kosong -> hapus mapping
        if (!outputCode) {
          await deleteAllMappings(tx, noFurnitureWip);
        } else {
          // Tentukan mapping table dari prefix
          if (outputCode.startsWith('BH.')) {
            outputType = 'HOTSTAMPING';
            mappingTable = 'HotStampingOutputLabelFWIP';
          } else if (outputCode.startsWith('BI.')) {
            outputType = 'PASANG_KUNCI';
            mappingTable = 'PasangKunciOutputLabelFWIP';
          } else if (outputCode.startsWith('BG.')) {
            outputType = 'BONGKAR_SUSUN';
            mappingTable = 'BongkarSusunOutputFurnitureWIP';
          } else if (outputCode.startsWith('L.')) {
            outputType = 'RETUR';
            mappingTable = 'BJReturFurnitureWIP_d';
          } else if (outputCode.startsWith('BJ.')) {
            outputType = 'SPANNER';
            mappingTable = 'SpannerOutputLabelFWIP';
          } else if (outputCode.startsWith('S.')) {
            outputType = 'INJECT';
            mappingTable = 'InjectProduksiOutputFurnitureWIP';
          } else {
            throw badReq(
              'outputCode prefix not recognized (supported: BH., BI., BG., L., BJ., S.)'
            );
          }
  
          // Hapus semua mapping lama untuk NoFurnitureWIP ini
          await deleteAllMappings(tx, noFurnitureWip);
  
          // Insert mapping baru
          if (mappingTable === 'HotStampingOutputLabelFWIP') {
            await new sql.Request(tx)
              .input('OutputCode', sql.VarChar, outputCode)
              .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
              .query(`
                INSERT INTO [dbo].[HotStampingOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
                VALUES (@OutputCode, @NoFurnitureWIP);
              `);
          } else if (mappingTable === 'PasangKunciOutputLabelFWIP') {
            await new sql.Request(tx)
              .input('OutputCode', sql.VarChar, outputCode)
              .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
              .query(`
                INSERT INTO [dbo].[PasangKunciOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
                VALUES (@OutputCode, @NoFurnitureWIP);
              `);
          } else if (mappingTable === 'BongkarSusunOutputFurnitureWIP') {
            await new sql.Request(tx)
              .input('OutputCode', sql.VarChar, outputCode)
              .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
              .query(`
                INSERT INTO [dbo].[BongkarSusunOutputFurnitureWIP] (NoBongkarSusun, NoFurnitureWIP)
                VALUES (@OutputCode, @NoFurnitureWIP);
              `);
          } else if (mappingTable === 'BJReturFurnitureWIP_d') {
            await new sql.Request(tx)
              .input('OutputCode', sql.VarChar, outputCode)
              .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
              .query(`
                INSERT INTO [dbo].[BJReturFurnitureWIP_d] (NoRetur, NoFurnitureWIP)
                VALUES (@OutputCode, @NoFurnitureWIP);
              `);
          } else if (mappingTable === 'SpannerOutputLabelFWIP') {
            await new sql.Request(tx)
              .input('OutputCode', sql.VarChar, outputCode)
              .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
              .query(`
                INSERT INTO [dbo].[SpannerOutputLabelFWIP] (NoProduksi, NoFurnitureWIP)
                VALUES (@OutputCode, @NoFurnitureWIP);
              `);
          } else if (mappingTable === 'InjectProduksiOutputFurnitureWIP') {
            await new sql.Request(tx)
              .input('OutputCode', sql.VarChar, outputCode)
              .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
              .query(`
                INSERT INTO [dbo].[InjectProduksiOutputFurnitureWIP] (NoProduksi, NoFurnitureWIP)
                VALUES (@OutputCode, @NoFurnitureWIP);
              `);
          }
        }
      }
  
      await tx.commit();
  
      return {
        header: {
          NoFurnitureWIP: noFurnitureWip,
          DateCreate: merged.DateCreate,
          Pcs: merged.Pcs,
          IdFurnitureWIP: merged.IdFurnitureWIP,
          Berat: merged.Berat,
          IsPartial: merged.IsPartial,
          IdWarna: merged.IdWarna,
          CreateBy: merged.CreateBy,
          Blok: merged.Blok,
          IdLokasi: merged.IdLokasi,
        },
        output: hasOutputCodeField
          ? {
              code: outputCode || null,
              type: outputType,   // bisa null kalau mapping dihapus
              mappingTable,       // bisa null kalau mapping dihapus
            }
          : undefined, // kalau tidak kirim outputCode, tidak return block output
      };
    } catch (err) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw err;
    }
  };




  exports.deleteFurnitureWip = async (noFurnitureWip) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    // helper error 404
    const notFound = () => {
      const e = new Error('Furniture WIP not found');
      e.statusCode = 404;
      return e;
    };
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // 1) Cek apakah header ada
      const rqCheck = new sql.Request(tx);
      const checkRes = await rqCheck
        .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
        .query(`
          SELECT TOP 1 NoFurnitureWIP
          FROM [dbo].[FurnitureWIP] WITH (UPDLOCK, HOLDLOCK)
          WHERE NoFurnitureWIP = @NoFurnitureWIP;
        `);
  
      if (checkRes.recordset.length === 0) {
        throw notFound();
      }
  
      // 2) Hapus semua mapping (BH/BI/BG/L/BJ/S)
      await deleteAllMappings(tx, noFurnitureWip);
  
      // 3) Hapus partial (kalau ada)
      await new sql.Request(tx)
        .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
        .query(`
          DELETE FROM [dbo].[FurnitureWIPPartial]
          WHERE NoFurnitureWIP = @NoFurnitureWIP;
        `);
  
      // 4) Hapus header
      await new sql.Request(tx)
        .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip)
        .query(`
          DELETE FROM [dbo].[FurnitureWIP]
          WHERE NoFurnitureWIP = @NoFurnitureWIP;
        `);
  
      await tx.commit();
  
      return {
        noFurnitureWip,
        deleted: true,
      };
    } catch (err) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw err;
    }
  };
  


/**
 * Ambil info partial FurnitureWIP per NoFurnitureWIP.
 *
 * Tabel yang dipakai:
 * - dbo.FurnitureWIPPartial                      (Base partial, Pcs)
 * - dbo.InjectProduksiInputFurnitureWIPPartial   (konsumsi partial -> NoProduksi)
 * - dbo.InjectProduksi_h                         (header produksi inject)
 * - dbo.MstMesin                                 (nama mesin)
 */
exports.getPartialInfoByFurnitureWip = async (noFurnitureWip) => {
    const pool = await poolPromise;
  
    const req = pool
      .request()
      .input('NoFurnitureWIP', sql.VarChar, noFurnitureWip);
  
    const query = `
      ;WITH BasePartial AS (
        SELECT
          fwp.NoFurnitureWIPPartial,
          fwp.NoFurnitureWIP,
          fwp.Pcs
        FROM dbo.FurnitureWIPPartial fwp
        WHERE fwp.NoFurnitureWIP = @NoFurnitureWIP
      ),
      Consumed AS (
        SELECT
          ip.NoFurnitureWIPPartial,
          'INJECT' AS SourceType,
          ip.NoProduksi
        FROM dbo.InjectProduksiInputFurnitureWIPPartial ip
      )
      SELECT
        bp.NoFurnitureWIPPartial,
        bp.NoFurnitureWIP,
        bp.Pcs,                  -- partial pcs
  
        c.SourceType,            -- 'INJECT' / NULL
        c.NoProduksi,
  
        iph.TglProduksi,
        iph.IdMesin,
        iph.IdOperator,
        iph.Jam,
        iph.Shift,
  
        mm.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoFurnitureWIPPartial = bp.NoFurnitureWIPPartial
  
      LEFT JOIN dbo.InjectProduksi_h iph
        ON iph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MstMesin mm
        ON mm.IdMesin = iph.IdMesin
  
      ORDER BY
        bp.NoFurnitureWIPPartial ASC,
        c.NoProduksi ASC;
    `;
  
    const result = await req.query(query);
  
    // total partial pcs (unique per NoFurnitureWIPPartial)
    const seen = new Set();
    let totalPartialPcs = 0;
  
    for (const row of result.recordset) {
      const key = row.NoFurnitureWIPPartial;
      if (!seen.has(key)) {
        seen.add(key);
        const pcs =
          typeof row.Pcs === 'number'
            ? row.Pcs
            : Number(row.Pcs) || 0;
        totalPartialPcs += pcs;
      }
    }
  
    const formatDate = (date) => {
      if (!date) return null;
      const d = new Date(date);
      const pad = (n) => (n < 10 ? '0' + n : '' + n);
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    };
  
    const rows = result.recordset.map((r) => ({
      NoFurnitureWIPPartial: r.NoFurnitureWIPPartial,
      NoFurnitureWIP: r.NoFurnitureWIP,
      Pcs: r.Pcs,
  
      SourceType: r.SourceType || null,     // 'INJECT' | null
      NoProduksi: r.NoProduksi || null,
  
      TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
      IdMesin: r.IdMesin || null,
      NamaMesin: r.NamaMesin || null,
      IdOperator: r.IdOperator || null,
      Jam: r.Jam || null,
      Shift: r.Shift || null,
    }));
  
    return { totalPartialPcs, rows };
  };




  // === hanya untuk unit test ===
exports._test = {
    padLeft,
    generateNextNoFurnitureWip,
  };
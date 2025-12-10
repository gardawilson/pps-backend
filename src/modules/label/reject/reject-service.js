// services/labels/reject-service.js
const { sql, poolPromise } = require('../../../core/config/db');

exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    WITH RejectPartialAgg AS (
      SELECT
        NoReject,
        SUM(ISNULL(Berat, 0)) AS TotalPartialBerat
      FROM [PPS_TEST3].[dbo].[RejectV2Partial]
      GROUP BY NoReject
    )
    SELECT
      r.NoReject,
      r.DateCreate,
      r.IdReject,
      r.IdWarehouse,

      ------------------------------------------------------------------
      -- BERAT HEADER - TOTAL PARTIAL = BERAT NET
      ------------------------------------------------------------------
      CASE 
        WHEN ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0) < 0 
          THEN 0
        ELSE ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0)
      END AS Berat,
      -- Kalau nanti mau dipakai di FE, bisa expose juga:
      -- ISNULL(rp.TotalPartialBerat, 0) AS TotalPartialBerat,

      r.IsPartial,
      r.Blok,
      r.IdLokasi,

      ------------------------------------------------------------------
      -- NAMA JENIS REJECT
      ------------------------------------------------------------------
      MAX(mr.NamaReject) AS NamaReject,

      ------------------------------------------------------------------
      -- TIPE SUMBER: INJECT / HOT_STAMPING / SPANNER / BJ_SORTIR
      ------------------------------------------------------------------
      CASE
        WHEN MAX(injr.NoProduksi) IS NOT NULL THEN 'INJECT'
        WHEN MAX(hsr.NoProduksi) IS NOT NULL THEN 'HOT_STAMPING'
        WHEN MAX(spr.NoProduksi) IS NOT NULL THEN 'SPANNER'
        WHEN MAX(bjr.NoBJSortir) IS NOT NULL THEN 'BJ_SORTIR'
        ELSE NULL
      END AS OutputType,

      ------------------------------------------------------------------
      -- KODE SUMBER: NoProduksi / NoBJSortir (untuk BJ_SORTIR)
      ------------------------------------------------------------------
      MAX(
        COALESCE(
          injr.NoProduksi,
          hsr.NoProduksi,
          spr.NoProduksi,
          bjr.NoBJSortir
        )
      ) AS OutputCode,

      ------------------------------------------------------------------
      -- NAMA MESIN / 'BJ Sortir'
      ------------------------------------------------------------------
      MAX(
        COALESCE(
          mInject.NamaMesin,
          mHot.NamaMesin,
          mSpan.NamaMesin,
          CASE 
            WHEN bjr.NoBJSortir IS NOT NULL THEN 'BJ Sortir'
          END
        )
      ) AS OutputNamaMesin

    FROM [dbo].[RejectV2] r

    ------------------------------------------------------------------
    -- MASTER JENIS REJECT
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[MstReject] mr
      ON mr.IdReject = r.IdReject
    -- kalau DB utama kamu PPS dan master ada di PPS_TEST3:
    -- LEFT JOIN [PPS_TEST3].[dbo].[MstReject] mr
    --   ON mr.IdReject = r.IdReject

    ------------------------------------------------------------------
    -- AGGREGATE PARTIAL REJECT
    ------------------------------------------------------------------
    LEFT JOIN RejectPartialAgg rp
      ON rp.NoReject = r.NoReject

    ------------------------------------------------------------------
    -- MAPPING: INJECT → MESIN
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[InjectProduksiOutputRejectV2] injr
      ON injr.NoReject = r.NoReject
    LEFT JOIN [dbo].[InjectProduksi_h] injh
      ON injh.NoProduksi = injr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInject
      ON mInject.IdMesin = injh.IdMesin

    ------------------------------------------------------------------
    -- MAPPING: HOT STAMPING → MESIN
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[HotStampingOutputRejectV2] hsr
      ON hsr.NoReject = r.NoReject
    LEFT JOIN [dbo].[HotStamping_h] hsh
      ON hsh.NoProduksi = hsr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHot
      ON mHot.IdMesin = hsh.IdMesin

    ------------------------------------------------------------------
    -- MAPPING: SPANNER → MESIN
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[SpannerOutputRejectV2] spr
      ON spr.NoReject = r.NoReject
    LEFT JOIN [dbo].[Spanner_h] sph
      ON sph.NoProduksi = spr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSpan
      ON mSpan.IdMesin = sph.IdMesin

    ------------------------------------------------------------------
    -- MAPPING: BJ SORTIR (bukan dari mesin)
    ------------------------------------------------------------------
    LEFT JOIN [dbo].[BJSortirRejectOutputLabelReject] bjr
      ON bjr.NoReject = r.NoReject

    WHERE 1 = 1
      AND r.DateUsage IS NULL
      ${
        search
          ? `AND (
               r.NoReject LIKE @search
               OR r.Blok LIKE @search
               OR CONVERT(VARCHAR(20), r.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), r.IdReject) LIKE @search

               -- cari berdasarkan kode sumber
               OR ISNULL(injr.NoProduksi,'')   LIKE @search
               OR ISNULL(hsr.NoProduksi,'')    LIKE @search
               OR ISNULL(spr.NoProduksi,'')    LIKE @search
               OR ISNULL(bjr.NoBJSortir,'')    LIKE @search

               -- cari berdasarkan nama mesin
               OR ISNULL(mInject.NamaMesin,'') LIKE @search
               OR ISNULL(mHot.NamaMesin,'')    LIKE @search
               OR ISNULL(mSpan.NamaMesin,'')   LIKE @search

               -- cari berdasarkan nama jenis reject
               OR ISNULL(mr.NamaReject,'')     LIKE @search
             )`
          : ''
      }
    GROUP BY
      r.NoReject,
      r.DateCreate,
      r.IdReject,
      r.IdWarehouse,
      r.Berat,
      r.IsPartial,
      r.Blok,
      r.IdLokasi,
      rp.TotalPartialBerat
    ORDER BY r.NoReject DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT r.NoReject) AS total
    FROM [dbo].[RejectV2] r

    LEFT JOIN [dbo].[MstReject] mr
      ON mr.IdReject = r.IdReject
    -- atau [PPS_TEST3].[dbo].[MstReject] mr, kalau beda DB

    LEFT JOIN [dbo].[InjectProduksiOutputRejectV2] injr
      ON injr.NoReject = r.NoReject
    LEFT JOIN [dbo].[InjectProduksi_h] injh
      ON injh.NoProduksi = injr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mInject
      ON mInject.IdMesin = injh.IdMesin

    LEFT JOIN [dbo].[HotStampingOutputRejectV2] hsr
      ON hsr.NoReject = r.NoReject
    LEFT JOIN [dbo].[HotStamping_h] hsh
      ON hsh.NoProduksi = hsr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mHot
      ON mHot.IdMesin = hsh.IdMesin

    LEFT JOIN [dbo].[SpannerOutputRejectV2] spr
      ON spr.NoReject = r.NoReject
    LEFT JOIN [dbo].[Spanner_h] sph
      ON sph.NoProduksi = spr.NoProduksi
    LEFT JOIN [dbo].[MstMesin] mSpan
      ON mSpan.IdMesin = sph.IdMesin

    LEFT JOIN [dbo].[BJSortirRejectOutputLabelReject] bjr
      ON bjr.NoReject = r.NoReject

    WHERE 1 = 1
      AND r.DateUsage IS NULL
      ${
        search
          ? `AND (
               r.NoReject LIKE @search
               OR r.Blok LIKE @search
               OR CONVERT(VARCHAR(20), r.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), r.IdReject) LIKE @search
               OR ISNULL(injr.NoProduksi,'')   LIKE @search
               OR ISNULL(hsr.NoProduksi,'')    LIKE @search
               OR ISNULL(spr.NoProduksi,'')    LIKE @search
               OR ISNULL(bjr.NoBJSortir,'')    LIKE @search
               OR ISNULL(mInject.NamaMesin,'') LIKE @search
               OR ISNULL(mHot.NamaMesin,'')    LIKE @search
               OR ISNULL(mSpan.NamaMesin,'')   LIKE @search
               OR ISNULL(mr.NamaReject,'')     LIKE @search
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



// helper pad left
function padLeft(num, width) {
    const s = String(num);
    return s.length >= width ? s : '0'.repeat(width - s.length) + s;
  }
  
  // Generate next NoReject: misalnya 'R.0000000002' (GANTI prefix kalau beda)
  async function generateNextNoReject(
    tx,
    { prefix = 'BF.', width = 10 } = {}
  ) {
    const rq = new sql.Request(tx);
    const q = `
      SELECT TOP 1 r.NoReject
      FROM [dbo].[RejectV2] AS r WITH (UPDLOCK, HOLDLOCK)
      WHERE r.NoReject LIKE @prefix + '%'
      ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(r.NoReject, LEN(@prefix) + 1, 50)) DESC,
               r.NoReject DESC;
    `;
    const r = await rq.input('prefix', sql.VarChar, prefix).query(q);
  
    let lastNum = 0;
    if (r.recordset.length > 0) {
      const last = r.recordset[0].NoReject;
      const numericPart = last.substring(prefix.length);
      lastNum = parseInt(numericPart, 10) || 0;
    }
    const next = lastNum + 1;
    return prefix + padLeft(next, width);
  }


  function resolveOutputByPrefix(outputCode, badReq) {
    let outputType = null;
    let mappingTable = null;
  
    if (outputCode.startsWith('S.')) {
      outputType = 'INJECT';
      mappingTable = 'InjectProduksiOutputRejectV2';
    } else if (outputCode.startsWith('BH.')) {
      outputType = 'HOT_STAMPING';
      mappingTable = 'HotStampingOutputRejectV2';
    } else if (outputCode.startsWith('BI.')) {
      outputType = 'PASANG_KUNCI';
      mappingTable = 'PasangKunciOutputRejectV2';
    } else if (outputCode.startsWith('BJ.')) {
      outputType = 'SPANNER';
      mappingTable = 'SpannerOutputRejectV2';
    } else if (outputCode.startsWith('J.')) {
      outputType = 'BJ_SORTIR';
      mappingTable = 'BJSortirRejectOutputLabelReject';
    } else {
      throw badReq(
        'outputCode prefix not recognized (supported: S., BH., BI., BJ., J.)'
      );
    }
  
    return { outputType, mappingTable };
  }

  
  /**
 * Helper: insert 1 row RejectV2 + mapping ke table output
 */
async function insertSingleReject({
    tx,
    header,
    idReject,
    outputCode,
    outputType,
    mappingTable,
  }) {
    // 1) Generate NoReject
    const generatedNo = await generateNextNoReject(tx, {
      prefix: 'BF.',  // 🔁 ganti kalau prefix NoReject beda
      width: 10,
    });
  
    // Double-check uniqueness (rare)
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoReject', sql.VarChar, generatedNo)
      .query(`
        SELECT 1
        FROM [dbo].[RejectV2] WITH (UPDLOCK, HOLDLOCK)
        WHERE NoReject = @NoReject
      `);
  
    const noReject =
      exist.recordset.length > 0
        ? await generateNextNoReject(tx, { prefix: 'BF.', width: 10 })
        : generatedNo;
  
    // 2) Insert header ke dbo.RejectV2
    const nowDateOnly = header.DateCreate || null; // null -> GETDATE() (date only)
    const insertHeaderSql = `
      INSERT INTO [dbo].[RejectV2] (
        NoReject,
        IdReject,
        DateCreate,
        DateUsage,
        IdWarehouse,
        Berat,
        Jam,
        CreateBy,
        DateTimeCreate,
        IsPartial,
        Blok,
        IdLokasi
      )
      VALUES (
        @NoReject,
        @IdReject,
        ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
        NULL,
        @IdWarehouse,
        @Berat,
        @Jam,
        @CreateBy,
        GETDATE(),
        @IsPartial,
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
      .input('NoReject', sql.VarChar, noReject)
      .input('IdReject', sql.Int, idReject)
      .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
      .input('Jam', sql.VarChar, header.Jam ?? null)
      .input('IdWarehouse', sql.Int, header.IdWarehouse ?? null)
      .input('IsPartial', sql.Bit, header.IsPartial ?? 0)
      .input('CreateBy', sql.VarChar, header.CreateBy ?? null)
      .input('Blok', sql.VarChar, header.Blok ?? null)
      .input('IdLokasi', sql.VarChar, idLokasiVal);
  
    if (nowDateOnly) {
      rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
    }
  
    await rqHeader.query(insertHeaderSql);
  
    // 3) Insert mapping berdasarkan mappingTable
    const rqMap = new sql.Request(tx)
      .input('OutputCode', sql.VarChar, outputCode)
      .input('NoReject', sql.VarChar, noReject);
  
    if (mappingTable === 'InjectProduksiOutputRejectV2') {
      const q = `
        INSERT INTO [dbo].[InjectProduksiOutputRejectV2] (NoProduksi, NoReject)
        VALUES (@OutputCode, @NoReject);
      `;
      await rqMap.query(q);
    } else if (mappingTable === 'HotStampingOutputRejectV2') {
      const q = `
        INSERT INTO [dbo].[HotStampingOutputRejectV2] (NoProduksi, NoReject)
        VALUES (@OutputCode, @NoReject);
      `;
      await rqMap.query(q);
    } else if (mappingTable === 'PasangKunciOutputRejectV2') {
      const q = `
        INSERT INTO [dbo].[PasangKunciOutputRejectV2] (NoProduksi, NoReject)
        VALUES (@OutputCode, @NoReject);
      `;
      await rqMap.query(q);
    } else if (mappingTable === 'SpannerOutputRejectV2') {
      const q = `
        INSERT INTO [dbo].[SpannerOutputRejectV2] (NoProduksi, NoReject)
        VALUES (@OutputCode, @NoReject);
      `;
      await rqMap.query(q);
    } else if (mappingTable === 'BJSortirRejectOutputLabelReject') {
      const q = `
        INSERT INTO [dbo].[BJSortirRejectOutputLabelReject] (NoBJSortir, NoReject)
        VALUES (@OutputCode, @NoReject);
      `;
      await rqMap.query(q);
    }
  
    // Return header shape yang dikirim ke controller
    return {
      NoReject: noReject,
      DateCreate: nowDateOnly || 'GETDATE()',
      IdReject: idReject,
      IdWarehouse: header.IdWarehouse ?? null,
      Berat: header.Berat ?? null,
      Jam: header.Jam ?? null,
      IsPartial: header.IsPartial ?? 0,
      CreateBy: header.CreateBy ?? null,
      Blok: header.Blok ?? null,
      IdLokasi: header.IdLokasi ?? null,
      OutputCode: outputCode,
      OutputType: outputType,
    };
  }

  
  exports.createReject = async (payload) => {
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
  
    // Wajib link ke salah satu sumber label (prefix-based)
    if (!outputCode) {
      throw badReq('outputCode is required (S., BH., BI., BJ., J., etc.)');
    }
  
    // Prefix rules → mappingTable
    const { outputType, mappingTable } = resolveOutputByPrefix(outputCode, badReq);
  
    // Di Reject TIDAK ada mode khusus Inject multi, jadi IdReject selalu wajib
    if (!header.IdReject) {
      throw badReq('IdReject is required');
    }
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // Single create
      const createdHeader = await insertSingleReject({
        tx,
        header,
        idReject: header.IdReject,
        outputCode,
        outputType,
        mappingTable,
      });
  
      const result = {
        headers: [createdHeader],
        output: {
          code: outputCode,
          type: outputType,
          mappingTable,
          isMulti: false,
          count: 1,
        },
      };
  
      await tx.commit();
      return result;
    } catch (e) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw e;
    }
  };
  


  /**
 * UPDATE RejectV2 + opsional ganti mapping output
 */
exports.updateReject = async (noReject, payload) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    const header = payload?.header || {};
    const rawOutputCode = payload?.outputCode;
  
    // helper error 400
    const badReq = (msg) => {
      const e = new Error(msg);
      e.statusCode = 400;
      return e;
    };
  
    // Kalau outputCode dikirim tapi kosong → error
    if (rawOutputCode !== undefined && String(rawOutputCode).trim() === '') {
      throw badReq('outputCode cannot be empty string');
    }
  
    // Normalisasi outputCode (bisa undefined = tidak ganti mapping)
    const outputCode = rawOutputCode !== undefined
      ? String(rawOutputCode).trim()
      : undefined;
  
    let outputType = null;
    let mappingTable = null;
  
    if (outputCode !== undefined) {
      // Kalau mau ganti mapping, harus prefix valid
      const resolved = resolveOutputByPrefix(outputCode, badReq);
      outputType = resolved.outputType;
      mappingTable = resolved.mappingTable;
    }
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // 1) Ambil row existing (lock)
      const rqCur = new sql.Request(tx);
      const curRes = await rqCur
        .input('NoReject', sql.VarChar, noReject)
        .query(`
          SELECT TOP 1
            NoReject,
            IdReject,
            DateCreate,
            IdWarehouse,
            Berat,
            IsPartial,
            Blok,
            IdLokasi
          FROM [dbo].[RejectV2] WITH (UPDLOCK, HOLDLOCK)
          WHERE NoReject = @NoReject
            AND DateUsage IS NULL;
        `);
  
      if (curRes.recordset.length === 0) {
        const e = new Error(`Reject with NoReject=${noReject} not found or already used`);
        e.statusCode = 404;
        throw e;
      }
  
      const current = curRes.recordset[0];
  
      // 2) Gabungkan nilai baru dengan existing
      const finalIdReject =
        header.IdReject !== undefined ? header.IdReject : current.IdReject;
      const finalBerat =
        header.Berat !== undefined ? header.Berat : current.Berat;
      const finalDateCreate =
        header.DateCreate !== undefined
          ? new Date(header.DateCreate)
          : current.DateCreate;
      const finalIsPartial =
        header.IsPartial !== undefined ? header.IsPartial : current.IsPartial;
      const finalIdWarehouse =
        header.IdWarehouse !== undefined ? header.IdWarehouse : current.IdWarehouse;
      const finalBlok =
        header.Blok !== undefined ? header.Blok : current.Blok;
  
      // Normalisasi IdLokasi
      let finalIdLokasi;
      if (header.IdLokasi !== undefined) {
        const raw = header.IdLokasi;
        if (raw === null || String(raw).trim() === '') {
          finalIdLokasi = null;
        } else {
          finalIdLokasi = String(raw).trim();
        }
      } else {
        finalIdLokasi = current.IdLokasi;
      }
  
      // Validasi minimal: IdReject tidak boleh null
      if (finalIdReject == null) {
        throw badReq('IdReject cannot be null');
      }
  
      // 3) UPDATE RejectV2
      const rqUpd = new sql.Request(tx);
      rqUpd
        .input('NoReject', sql.VarChar, noReject)
        .input('IdReject', sql.Int, finalIdReject)
        .input('DateCreate', sql.Date, finalDateCreate)
        .input('IdWarehouse', sql.Int, finalIdWarehouse ?? null)
        .input('Berat', sql.Decimal(18, 3), finalBerat ?? null)
        .input('IsPartial', sql.Bit, finalIsPartial ?? 0)
        .input('Blok', sql.VarChar, finalBlok ?? null)
        .input('IdLokasi', sql.Int, finalIdLokasi ?? null)
  
      await rqUpd.query(`
        UPDATE [dbo].[RejectV2]
        SET
          IdReject    = @IdReject,
          DateCreate  = @DateCreate,
          IdWarehouse = @IdWarehouse,
          Berat       = @Berat,
          IsPartial   = @IsPartial,
          Blok        = @Blok,
          IdLokasi    = @IdLokasi
        WHERE NoReject = @NoReject;
      `);
  
      // 4) Kalau user kirim outputCode → ganti mapping
      if (outputCode !== undefined) {
        // Hapus mapping lama dari semua tabel, lalu insert ke tabel baru
        const rqDel = new sql.Request(tx).input('NoReject', sql.VarChar, noReject);
        await rqDel.query(`
          DELETE FROM [dbo].[InjectProduksiOutputRejectV2]         WHERE NoReject = @NoReject;
          DELETE FROM [dbo].[HotStampingOutputRejectV2]            WHERE NoReject = @NoReject;
          DELETE FROM [dbo].[PasangKunciOutputRejectV2]            WHERE NoReject = @NoReject;
          DELETE FROM [dbo].[SpannerOutputRejectV2]                WHERE NoReject = @NoReject;
          DELETE FROM [dbo].[BJSortirRejectOutputLabelReject]      WHERE NoReject = @NoReject;
        `);
  
        const rqMap = new sql.Request(tx)
          .input('OutputCode', sql.VarChar, outputCode)
          .input('NoReject', sql.VarChar, noReject);
  
        if (mappingTable === 'InjectProduksiOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO [dbo].[InjectProduksiOutputRejectV2] (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'HotStampingOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO [dbo].[HotStampingOutputRejectV2] (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'PasangKunciOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO [dbo].[PasangKunciOutputRejectV2] (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'SpannerOutputRejectV2') {
          await rqMap.query(`
            INSERT INTO [dbo].[SpannerOutputRejectV2] (NoProduksi, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        } else if (mappingTable === 'BJSortirRejectOutputLabelReject') {
          await rqMap.query(`
            INSERT INTO [dbo].[BJSortirRejectOutputLabelReject] (NoBJSortir, NoReject)
            VALUES (@OutputCode, @NoReject);
          `);
        }
      }
  
      await tx.commit();
  
      // Bentuk response (mirip createReject, tapi tanpa generate NoReject)
      return {
        header: {
          NoReject: noReject,
          DateCreate: finalDateCreate,
          IdReject: finalIdReject,
          IdWarehouse: finalIdWarehouse,
          Berat: finalBerat,
          IsPartial: finalIsPartial,
          Blok: finalBlok,
          IdLokasi: finalIdLokasi,
        },
        output: {
          code: outputCode ?? null,
          type: outputType ?? null,
          mappingTable: mappingTable ?? null,
          isMulti: false,
          count: 1,
        },
      };
    } catch (e) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw e;
    }
  };


  /**
 * DELETE RejectV2 + semua mapping output
 */
exports.deleteReject = async (noReject) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // 1) Pastikan data ada & belum dipakai (DateUsage IS NULL)
      const rqCheck = new sql.Request(tx);
      const checkRes = await rqCheck
        .input('NoReject', sql.VarChar, noReject)
        .query(`
          SELECT TOP 1
            NoReject,
            DateUsage
          FROM [dbo].[RejectV2] WITH (UPDLOCK, HOLDLOCK)
          WHERE NoReject = @NoReject;
        `);
  
      if (checkRes.recordset.length === 0) {
        const e = new Error(`Reject with NoReject=${noReject} not found`);
        e.statusCode = 404;
        throw e;
      }
  
      const row = checkRes.recordset[0];
      if (row.DateUsage !== null) {
        const e = new Error(
          `Reject ${noReject} cannot be deleted because it has already been used (DateUsage is not NULL)`
        );
        e.statusCode = 400;
        throw e;
      }
  
      // 2) Hapus mapping dari semua tabel output
      const rqDelMap = new sql.Request(tx).input('NoReject', sql.VarChar, noReject);
  
      const delMapRes = await rqDelMap.query(`
        DELETE FROM [dbo].[InjectProduksiOutputRejectV2]         WHERE NoReject = @NoReject;
        DELETE FROM [dbo].[HotStampingOutputRejectV2]            WHERE NoReject = @NoReject;
        DELETE FROM [dbo].[PasangKunciOutputRejectV2]            WHERE NoReject = @NoReject;
        DELETE FROM [dbo].[SpannerOutputRejectV2]                WHERE NoReject = @NoReject;
        DELETE FROM [dbo].[BJSortirRejectOutputLabelReject]      WHERE NoReject = @NoReject;
      `);
  
      // 3) Hapus row utama dari RejectV2
      const rqDelMain = new sql.Request(tx).input('NoReject', sql.VarChar, noReject);
      const delMainRes = await rqDelMain.query(`
        DELETE FROM [dbo].[RejectV2]
        WHERE NoReject = @NoReject;
      `);
  
      await tx.commit();
  
      return {
        NoReject: noReject,
        deleted: true,
        // optional: info tambahan kalau mau
        // rowsAffected: {
        //   mappings: delMapRes.rowsAffected,
        //   main: delMainRes.rowsAffected,
        // },
      };
    } catch (e) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw e;
    }
  };


  /**
 * Ambil info partial Reject per NoReject.
 *
 * Tabel yang dipakai:
 * - dbo.RejectV2Partial                 (base partial, Berat)
 * - dbo.BrokerProduksiInputRejectPartial (konsumsi partial -> NoProduksi)
 * - dbo.BrokerProduksi_h                (header broker -> IdMesin, TglProduksi, Jam, Shift, ...)
 * - dbo.MstMesin                        (nama mesin)
 */
exports.getPartialInfoByReject = async (noReject) => {
    const pool = await poolPromise;
  
    const req = pool
      .request()
      .input('NoReject', sql.VarChar, noReject);
  
    const query = `
      ;WITH BasePartial AS (
        SELECT
          rp.NoRejectPartial,
          rp.NoReject,
          rp.Berat
        FROM dbo.RejectV2Partial rp
        WHERE rp.NoReject = @NoReject
      ),
      Consumed AS (
        SELECT
          bpir.NoRejectPartial,
          'BROKER' AS SourceType,
          bpir.NoProduksi
        FROM dbo.BrokerProduksiInputRejectPartial bpir
      )
      SELECT
        bp.NoRejectPartial,
        bp.NoReject,
        bp.Berat,            -- partial berat
  
        c.SourceType,        -- 'BROKER' / NULL
        c.NoProduksi,
  
        bh.TglProduksi,
        bh.IdMesin,
        bh.Jam,
        bh.Shift,
  
        m.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoRejectPartial = bp.NoRejectPartial
  
      LEFT JOIN dbo.BrokerProduksi_h bh
        ON bh.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MstMesin m
        ON m.IdMesin = bh.IdMesin
  
      ORDER BY
        bp.NoRejectPartial ASC,
        c.NoProduksi ASC;
    `;
  
    const result = await req.query(query);
  
    // total partial berat (unique per NoRejectPartial)
    const seen = new Set();
    let totalPartialBerat = 0;
  
    for (const row of result.recordset) {
      const key = row.NoRejectPartial;
      if (!seen.has(key)) {
        seen.add(key);
        const berat =
          typeof row.Berat === 'number'
            ? row.Berat
            : Number(row.Berat) || 0;
        totalPartialBerat += berat;
      }
    }
  
    const formatDate = (date) => {
      if (!date) return null;
      const d = new Date(date);
      const pad = (n) => (n < 10 ? '0' + n : '' + n);
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    };
  
    const rows = result.recordset.map((r) => ({
      NoRejectPartial: r.NoRejectPartial,
      NoReject: r.NoReject,
      Berat: r.Berat,
  
      SourceType: r.SourceType || null,       // 'BROKER' | null
      NoProduksi: r.NoProduksi || null,
  
      TanggalProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
      IdMesin: r.IdMesin || null,
      NamaMesin: r.NamaMesin || null,
      JamProduksi: r.Jam || null,
      Shift: r.Shift || null,
    }));
  
    return { totalPartialBerat, rows };
  };
  
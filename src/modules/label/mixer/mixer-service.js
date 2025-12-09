// controllers/mixer-service.js (atau di folder yang sama dengan broker-service)
const { sql, poolPromise } = require('../../../core/config/db');

// GET all header Mixer dengan pagination & search (mirror of Broker.getAll)
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const offset = (page - 1) * limit;

  // ==== optional search clause ====
  // Cari di:
  // - NoMixer
  // - NamaMixer (mx.Jenis)
  // - Blok / IdLokasi
  // - Output MixerProduksi (NoProduksi / NamaMesin)
  // - Output BongkarSusun (NoBongkarSusun)
  // - Output InjectProduksiMixer (NoProduksi / NamaMesin)
  const searchClause = search
    ? `
      AND (
        h.NoMixer LIKE @search
        OR mx.Jenis LIKE @search
        OR h.Blok LIKE @search
        OR CAST(h.IdLokasi AS VARCHAR(20)) LIKE @search

        -- MixerProduksiOutput
        OR EXISTS (
          SELECT 1
          FROM dbo.MixerProduksiOutput mpo
          INNER JOIN dbo.MixerProduksi_h mph
            ON mph.NoProduksi = mpo.NoProduksi
          LEFT JOIN dbo.MstMesin m
            ON m.IdMesin = mph.IdMesin
          WHERE mpo.NoMixer = h.NoMixer
            AND (
              mpo.NoProduksi LIKE @search
              OR m.NamaMesin LIKE @search
            )
        )

        -- BongkarSusunOutputMixer
        OR EXISTS (
          SELECT 1
          FROM dbo.BongkarSusunOutputMixer bsom
          WHERE bsom.NoMixer = h.NoMixer
            AND bsom.NoBongkarSusun LIKE @search
        )

        -- InjectProduksiOutputMixer
        OR EXISTS (
          SELECT 1
          FROM dbo.InjectProduksiOutputMixer ipom
          INNER JOIN dbo.InjectProduksi_h iph
            ON iph.NoProduksi = ipom.NoProduksi
          LEFT JOIN dbo.MstMesin mi
            ON mi.IdMesin = iph.IdMesin
          WHERE ipom.NoMixer = h.NoMixer
            AND (
              ipom.NoProduksi LIKE @search
              OR mi.NamaMesin LIKE @search
            )
        )
      )
    `
    : '';

  const baseQuery = `
    SELECT
      h.NoMixer,
      h.DateCreate,
      h.IdMixer,
      mx.Jenis AS NamaMixer,
      h.IdStatus,
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,

      h.Moisture,
      h.MaxMeltTemp,
      h.MinMeltTemp,
      h.MFI,
      h.Moisture2,
      h.Moisture3,
      h.Blok,
      h.IdLokasi,

      -- 🔹 Output generik (MixerProduksi / BongkarSusun / Inject)
      outInfo.OutputType,
      outInfo.OutputCode,
      outInfo.OutputNamaMesin
    FROM dbo.Mixer_h h
    INNER JOIN dbo.MstMixer mx
      ON mx.IdMixer = h.IdMixer

    -- 🔹 Ambil 1 output per NoMixer (prioritas: MixerProduksi, Inject, Bongkar Susun)
    OUTER APPLY (
      SELECT TOP (1)
        src.OutputType,
        src.OutputCode,
        src.OutputNamaMesin
      FROM (
        -- 1) Output: MixerProduksi
        SELECT 
          'MIXER_PRODUKSI' AS OutputType,
          mpo.NoProduksi   AS OutputCode,
          m.NamaMesin      AS OutputNamaMesin,
          1                AS Priority
        FROM dbo.MixerProduksiOutput mpo
        INNER JOIN dbo.MixerProduksi_h mph
          ON mph.NoProduksi = mpo.NoProduksi
        LEFT JOIN dbo.MstMesin m
          ON m.IdMesin = mph.IdMesin
        WHERE mpo.NoMixer = h.NoMixer

        UNION ALL

        -- 2) Output: InjectProduksiOutputMixer
        SELECT
          'INJECT_PRODUKSI'        AS OutputType,
          ipom.NoProduksi          AS OutputCode,
          mi.NamaMesin             AS OutputNamaMesin,
          2                        AS Priority
        FROM dbo.InjectProduksiOutputMixer ipom
        INNER JOIN dbo.InjectProduksi_h iph
          ON iph.NoProduksi = ipom.NoProduksi
        LEFT JOIN dbo.MstMesin mi
          ON mi.IdMesin = iph.IdMesin
        WHERE ipom.NoMixer = h.NoMixer

        UNION ALL

        -- 3) Output: Bongkar Susun Mixer
        SELECT
          'BONGKAR_SUSUN'          AS OutputType,
          bsom.NoBongkarSusun      AS OutputCode,
          'Bongkar Susun'          AS OutputNamaMesin,
          3                        AS Priority
        FROM dbo.BongkarSusunOutputMixer bsom
        WHERE bsom.NoMixer = h.NoMixer
      ) AS src
      WHERE src.OutputCode IS NOT NULL
      ORDER BY src.Priority, src.OutputCode
    ) AS outInfo

    WHERE 1 = 1
      ${searchClause}
      AND EXISTS (
        SELECT 1
        FROM dbo.Mixer_d d2
        WHERE d2.NoMixer = h.NoMixer
          AND d2.DateUsage IS NULL
      )

    ORDER BY h.NoMixer DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoMixer) AS total
    FROM dbo.Mixer_h h
    INNER JOIN dbo.MstMixer mx
      ON mx.IdMixer = h.IdMixer
    WHERE 1 = 1
      ${searchClause}
      AND EXISTS (
        SELECT 1
        FROM dbo.Mixer_d d2
        WHERE d2.NoMixer = h.NoMixer
          AND d2.DateUsage IS NULL
      );
  `;

  // ==== eksekusi query data ====
  const reqData = pool.request();
  reqData.input('offset', sql.Int, offset);
  reqData.input('limit', sql.Int, limit);
  if (search) {
    reqData.input('search', sql.VarChar, `%${search}%`);
  }
  const dataResult = await reqData.query(baseQuery);

  // ==== eksekusi query count ====
  const reqCount = pool.request();
  if (search) {
    reqCount.input('search', sql.VarChar, `%${search}%`);
  }
  const countResult = await reqCount.query(countQuery);

  const data = dataResult.recordset?.map((item) => ({ ...item })) ?? [];
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};



// GET details by NoMixer (tanpa IdLokasi)
exports.getMixerDetailByNoMixer = async (nomixer) => {
    const pool = await poolPromise;
  
    const result = await pool
      .request()
      .input('NoMixer', sql.VarChar, nomixer)
      .query(`
        SELECT
          d.NoMixer,
          d.NoSak,
          -- Jika IsPartial = 1, maka Berat dikurangi total dari MixerPartial
          CASE 
            WHEN d.IsPartial = 1 THEN 
              d.Berat - ISNULL((
                SELECT SUM(p.Berat)
                FROM dbo.MixerPartial p
                WHERE p.NoMixer = d.NoMixer
                  AND p.NoSak   = d.NoSak
              ), 0)
            ELSE d.Berat
          END AS Berat,
          d.DateUsage,
          d.IsPartial
          -- ⬆️ IdLokasi dihapus dari SELECT
        FROM dbo.Mixer_d d
        WHERE d.NoMixer = @NoMixer
          AND d.DateUsage IS NULL
        ORDER BY d.NoSak;
      `);
  
    // Optional: format tanggal biar rapi (sama seperti broker)
    const formatDateTime = (date) => {
      if (!date) return null;
      const d = new Date(date);
      const pad = (n) => (n < 10 ? '0' + n : String(n));
      return (
        `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
        `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
      );
    };
  
    return result.recordset.map((item) => ({
      ...item,
      ...(item.DateUsage && { DateUsage: formatDateTime(item.DateUsage) }),
    }));
  };


  function padLeft(num, width) {
    const s = String(num);
    return s.length >= width ? s : '0'.repeat(width - s.length) + s;
  }
  
  /**
   * Generate next NoMixer like: H.0000000001
   */
  async function generateNextNoMixer(tx, { prefix = 'H.', width = 10 } = {}) {
    const rq = new sql.Request(tx);
    const q = `
        SELECT TOP 1 h.NoMixer
        FROM dbo.Mixer_h AS h WITH (UPDLOCK, HOLDLOCK)
        WHERE h.NoMixer LIKE @prefix + '%'
        ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(h.NoMixer, LEN(@prefix) + 1, 50)) DESC,
                 h.NoMixer DESC
      `;
    const r = await rq.input('prefix', sql.VarChar, prefix).query(q);
  
    let lastNum = 0;
    if (r.recordset.length > 0) {
      const last = r.recordset[0].NoMixer;               // e.g. "H.0000000001"
      const numericPart = last.substring(prefix.length); // "0000000001"
      lastNum = parseInt(numericPart, 10) || 0;
    }
    const next = lastNum + 1;
    return prefix + padLeft(next, width);                // e.g. "H.0000000002"
  }
  
  /**
   * Create Mixer header + details + optional outputs (outputCode)
   *
   * outputCode:
   * - "BG.******" → BongkarSusunOutputMixer
   * - "I.******"  → MixerProduksiOutput
   * - "S.******"  → InjectProduksiOutputMixer  ⬅️ (inject production output)
   */
  exports.createMixerCascade = async (payload) => {
    const pool = await poolPromise;
    const tx = new sql.Transaction(pool);
  
    const header = payload?.header || {};
    const details = Array.isArray(payload?.details) ? payload.details : [];
  
    const badReq = (msg) => {
      const e = new Error(msg);
      e.statusCode = 400;
      return e;
    };
  
    // ---- basic validation
    if (!header.IdMixer) throw badReq('IdMixer is required');
    if (!header.CreateBy) throw badReq('CreateBy is required');
    if (!Array.isArray(details) || details.length === 0) {
      throw badReq('Details must contain at least 1 item');
    }
  
    // ---- outputCode → NoProduksi / NoBongkarSusun / NoInjectProduksi
    const rawOutputCode = payload?.outputCode?.toString().trim() || '';
  
    let NoProduksi = null;        // untuk MixerProduksiOutput
    let NoBongkarSusun = null;    // untuk BongkarSusunOutputMixer
    let NoInjectProduksi = null;  // untuk InjectProduksiOutputMixer
    let outputKind = null;        // 'PRODUKSI' | 'BONGKAR' | 'INJECT' | null
  
    if (rawOutputCode) {
      const upper = rawOutputCode.toUpperCase();
  
      if (upper.startsWith('BG.')) {
        // BG.* → BongkarSusunOutputMixer
        NoBongkarSusun = rawOutputCode;
        outputKind = 'BONGKAR';
      } else if (upper.startsWith('I.')) {
        // I.* → MixerProduksiOutput
        NoProduksi = rawOutputCode;
        outputKind = 'PRODUKSI';
      } else if (upper.startsWith('S.')) {
        // S.* → InjectProduksiOutputMixer (inject production output)
        NoInjectProduksi = rawOutputCode;
        outputKind = 'INJECT';
      } else {
        throw badReq('outputCode must start with "BG.", "I." or "S."');
      }
    }
  
    try {
      await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
  
      // 1) Generate NoMixer
      const generatedNo = await generateNextNoMixer(tx, { prefix: 'H.', width: 10 });
  
      // Double-check uniqueness (very defensive, like your broker code)
      const rqCheck = new sql.Request(tx);
      const exist = await rqCheck
        .input('NoMixer', sql.VarChar, generatedNo)
        .query(`
          SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoMixer = @NoMixer
        `);
  
      header.NoMixer = (exist.recordset.length > 0)
        ? await generateNextNoMixer(tx, { prefix: 'H.', width: 10 })
        : generatedNo;
  
      // 2) Insert Mixer_h (header)
      const nowDateOnly = header.DateCreate || null; // if null → GETDATE()
      const nowDateTime = new Date();                // DateTimeCreate
  
      const insertHeaderSql = `
          INSERT INTO dbo.Mixer_h (
            NoMixer, IdMixer, DateCreate, IdStatus, CreateBy, DateTimeCreate,
            Moisture, MaxMeltTemp, MinMeltTemp, MFI,
            Moisture2, Moisture3,
            Blok, IdLokasi
          ) VALUES (
            @NoMixer, @IdMixer,
            ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
            @IdStatus, @CreateBy, @DateTimeCreate,
            @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI,
            @Moisture2, @Moisture3,
            @Blok, @IdLokasi
          )
        `;
  
      const rqHeader = new sql.Request(tx);
      rqHeader
        .input('NoMixer', sql.VarChar, header.NoMixer)
        .input('IdMixer', sql.Int, header.IdMixer)
        .input('IdStatus', sql.Int, header.IdStatus ?? 1) // default PASS=1
        .input('CreateBy', sql.VarChar, header.CreateBy)
        .input('DateTimeCreate', sql.DateTime, nowDateTime)
        .input('Moisture', sql.Decimal(10, 3), header.Moisture ?? null)
        .input('MaxMeltTemp', sql.Decimal(10, 3), header.MaxMeltTemp ?? null)
        .input('MinMeltTemp', sql.Decimal(10, 3), header.MinMeltTemp ?? null)
        .input('MFI', sql.Decimal(10, 3), header.MFI ?? null)
        .input('Moisture2', sql.Decimal(10, 3), header.Moisture2 ?? null)
        .input('Moisture3', sql.Decimal(10, 3), header.Moisture3 ?? null)
        .input('Blok', sql.VarChar, header.Blok ?? null)
        .input(
          'IdLokasi',
          sql.Int,
          header.IdLokasi != null ? Number(header.IdLokasi) : null
        );
  
      if (nowDateOnly) {
        rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
      }
  
      await rqHeader.query(insertHeaderSql);
  
      // 3) Insert Mixer_d (details)
      const insertDetailSql = `
        INSERT INTO dbo.Mixer_d (
          NoMixer, NoSak, Berat, DateUsage, IsPartial
        ) VALUES (
          @NoMixer, @NoSak, @Berat, NULL, 0
        )
      `;
  
      let detailCount = 0;
      for (const d of details) {
        const rqDet = new sql.Request(tx);
  
        await rqDet
          .input('NoMixer', sql.VarChar, header.NoMixer)
          .input('NoSak', sql.Int, d.NoSak)
          .input('Berat', sql.Decimal(18, 3), d.Berat ?? 0)
          .query(insertDetailSql);
  
        detailCount++;
      }
  
      // 4) Optional outputs (based on outputCode)
      let outputTarget = null;
      let outputCount = 0;
  
      if (NoProduksi) {
        // Insert into MixerProduksiOutput
        const insertProdSql = `
          INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
          VALUES (@NoProduksi, @NoMixer, @NoSak)
        `;
        for (const d of details) {
          const rqProd = new sql.Request(tx);
          await rqProd
            .input('NoProduksi', sql.VarChar, NoProduksi)
            .input('NoMixer', sql.VarChar, header.NoMixer)
            .input('NoSak', sql.Int, d.NoSak)
            .query(insertProdSql);
          outputCount++;
        }
        outputTarget = 'MixerProduksiOutput';
      } else if (NoBongkarSusun) {
        // Insert into BongkarSusunOutputMixer
        const insertBsoSql = `
          INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
          VALUES (@NoBongkarSusun, @NoMixer, @NoSak)
        `;
        for (const d of details) {
          const rqBso = new sql.Request(tx);
          await rqBso
            .input('NoBongkarSusun', sql.VarChar, NoBongkarSusun)
            .input('NoMixer', sql.VarChar, header.NoMixer)
            .input('NoSak', sql.Int, d.NoSak)
            .query(insertBsoSql);
          outputCount++;
        }
        outputTarget = 'BongkarSusunOutputMixer';
      } else if (NoInjectProduksi) {
        // Insert into InjectProduksiOutputMixer  ⬅️ inject production output
        const insertInjectSql = `
          INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
          VALUES (@NoProduksi, @NoMixer, @NoSak)
        `;
        for (const d of details) {
          const rqInject = new sql.Request(tx);
          await rqInject
            .input('NoProduksi', sql.VarChar, NoInjectProduksi)
            .input('NoMixer', sql.VarChar, header.NoMixer)
            .input('NoSak', sql.Int, d.NoSak)
            .query(insertInjectSql);
          outputCount++;
        }
        outputTarget = 'InjectProduksiOutputMixer';
      }
  
      await tx.commit();
  
      return {
        header: {
          NoMixer: header.NoMixer,
          IdMixer: header.IdMixer,
          IdStatus: header.IdStatus ?? 1,
          CreateBy: header.CreateBy,
          DateCreate: nowDateOnly || 'GETDATE()',
          DateTimeCreate: nowDateTime,
          Moisture: header.Moisture ?? null,
          MaxMeltTemp: header.MaxMeltTemp ?? null,
          MinMeltTemp: header.MinMeltTemp ?? null,
          MFI: header.MFI ?? null,
          Moisture2: header.Moisture2 ?? null,
          Moisture3: header.Moisture3 ?? null,
          Blok: header.Blok ?? null,
          IdLokasi: header.IdLokasi ?? null,
        },
        counts: {
          detailsInserted: detailCount,
          outputInserted: outputCount,
        },
        outputKind,   // 'PRODUKSI' | 'BONGKAR' | 'INJECT' | null
        outputTarget, // nama tabel output yang kena insert
        outputCode: rawOutputCode || null,
      };
    } catch (e) {
      try {
        await tx.rollback();
      } catch (_) {}
      throw e;
    }
  };
  


exports.updateMixerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoMixer = payload?.NoMixer?.toString().trim();
  if (!NoMixer) {
    const e = new Error('NoMixer (path) is required');
    e.statusCode = 400;
    throw e;
  }

  const header = payload?.header || {};
  // details = null → do not touch details
  const details = Array.isArray(payload?.details) ? payload.details : null;

  // ----- outputCode handling (BG.* / I.* / S.*) -----
  const hasOutputCodeField = Object.prototype.hasOwnProperty.call(
    payload,
    'outputCode'
  );

  let rawOutputCode = null;

  // Untuk 3 jenis output:
  let NoProduksiMixer = null;   // I.*  -> MixerProduksiOutput
  let NoBongkarSusun = null;    // BG.* -> BongkarSusunOutputMixer
  let NoProduksiInject = null;  // S.*  -> InjectProduksiOutputMixer

  // Info jenis mapping (opsional, hanya untuk return)
  let outputKind = null; // 'MIXER_PRODUKSI' | 'BONGKAR' | 'INJECT' | null

  if (hasOutputCodeField) {
    rawOutputCode = payload.outputCode?.toString().trim() || '';

    if (rawOutputCode) {
      const upper = rawOutputCode.toUpperCase();

      if (upper.startsWith('BG.')) {
        NoBongkarSusun = rawOutputCode;
        outputKind = 'BONGKAR';
      } else if (upper.startsWith('I.')) {
        NoProduksiMixer = rawOutputCode;
        outputKind = 'MIXER_PRODUKSI';
      } else if (upper.startsWith('S.')) {
        NoProduksiInject = rawOutputCode;
        outputKind = 'INJECT';
      } else {
        const e = new Error(
          'outputCode must start with "BG.", "I." or "S." if provided'
        );
        e.statusCode = 400;
        throw e;
      }
    } else {
      // field ada tapi kosong/null -> clear outputs saja
      rawOutputCode = '';
    }
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) Pastikan header ada
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(
        `SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK) WHERE NoMixer = @NoMixer`
      );

    if (exist.recordset.length === 0) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    // 1) Dynamic header update
    const setParts = [];
    const reqHeader = new sql.Request(tx).input('NoMixer', sql.VarChar, NoMixer);

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    // IdMixer
    setIf('IdMixer', 'IdMixer', sql.Int, header.IdMixer);

    // DateCreate
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null) {
        // null means reset to today (like Broker)
        setParts.push('DateCreate = CONVERT(date, GETDATE())');
      } else {
        setIf('DateCreate', 'DateCreate', sql.Date, new Date(header.DateCreate));
      }
    }

    // basic numeric fields
    setIf('IdStatus', 'IdStatus', sql.Int, header.IdStatus);
    setIf(
      'Moisture',
      'Moisture',
      sql.Decimal(10, 3),
      header.Moisture ?? null
    );
    setIf(
      'MaxMeltTemp',
      'MaxMeltTemp',
      sql.Decimal(10, 3),
      header.MaxMeltTemp ?? null
    );
    setIf(
      'MinMeltTemp',
      'MinMeltTemp',
      sql.Decimal(10, 3),
      header.MinMeltTemp ?? null
    );
    setIf('MFI', 'MFI', sql.Decimal(10, 3), header.MFI ?? null);
    setIf(
      'Moisture2',
      'Moisture2',
      sql.Decimal(10, 3),
      header.Moisture2 ?? null
    );
    setIf(
      'Moisture3',
      'Moisture3',
      sql.Decimal(10, 3),
      header.Moisture3 ?? null
    );
    setIf('Blok', 'Blok', sql.VarChar, header.Blok ?? null);

    // IdLokasi is INT
    if (header.IdLokasi !== undefined) {
      setParts.push('IdLokasi = @IdLokasi');
      reqHeader.input(
        'IdLokasi',
        sql.Int,
        header.IdLokasi != null ? Number(header.IdLokasi) : null
      );
    }

    // Optional audit (contoh, kalau mau)
    // if (payload.UpdateBy) {
    //   setParts.push('UpdateBy = @UpdateBy');
    //   setParts.push('DateTimeUpdate = @DateTimeUpdate');
    //   reqHeader.input('UpdateBy', sql.VarChar, payload.UpdateBy);
    //   reqHeader.input('DateTimeUpdate', sql.DateTime, new Date());
    // }

    if (setParts.length > 0) {
      const sqlUpdateHeader = `
        UPDATE dbo.Mixer_h SET ${setParts.join(', ')}
        WHERE NoMixer = @NoMixer
      `;
      await reqHeader.query(sqlUpdateHeader);
    }

    // 2) Replace details (only if details is provided)
    let detailsAffected = 0;
    if (details) {
      // delete only rows that are still unused
      await new sql.Request(tx)
        .input('NoMixer', sql.VarChar, NoMixer)
        .query(`
          DELETE FROM dbo.Mixer_d
          WHERE NoMixer = @NoMixer AND DateUsage IS NULL
        `);

      const insertDetailSql = `
        INSERT INTO dbo.Mixer_d (
          NoMixer, NoSak, Berat, DateUsage, IsPartial
        ) VALUES (
          @NoMixer, @NoSak, @Berat, NULL, 0
        )
      `;

      for (const d of details) {
        const rqDet = new sql.Request(tx);
        await rqDet
          .input('NoMixer', sql.VarChar, NoMixer)
          .input('NoSak', sql.Int, d.NoSak)
          .input('Berat', sql.Decimal(18, 3), d.Berat ?? 0)
          .query(insertDetailSql);
        detailsAffected++;
      }
    }

    // 3) Outputs (MixerProduksiOutput / BongkarSusunOutputMixer / InjectProduksiOutputMixer)
    let outputTarget = null;
    let outputCount = 0;

    if (hasOutputCodeField) {
      // Clear SEMUA mapping output dulu
      await new sql.Request(tx)
        .input('NoMixer', sql.VarChar, NoMixer)
        .query(`DELETE FROM dbo.MixerProduksiOutput WHERE NoMixer = @NoMixer`);

      await new sql.Request(tx)
        .input('NoMixer', sql.VarChar, NoMixer)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputMixer WHERE NoMixer = @NoMixer`
        );

      await new sql.Request(tx)
        .input('NoMixer', sql.VarChar, NoMixer)
        .query(
          `DELETE FROM dbo.InjectProduksiOutputMixer WHERE NoMixer = @NoMixer`
        );

      // Kalau rawOutputCode kosong → hanya clear outputs, selesai
      if (rawOutputCode) {
        // Ambil details aktif (DateUsage IS NULL)
        const dets = await new sql.Request(tx)
          .input('NoMixer', sql.VarChar, NoMixer)
          .query(`
            SELECT NoSak
            FROM dbo.Mixer_d
            WHERE NoMixer = @NoMixer AND DateUsage IS NULL
            ORDER BY NoSak
          `);

        if (NoProduksiMixer) {
          // 3a) Mapping ke MixerProduksiOutput (I.*)
          const insertProdSql = `
            INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
            VALUES (@NoProduksi, @NoMixer, @NoSak)
          `;
          for (const row of dets.recordset) {
            await new sql.Request(tx)
              .input('NoProduksi', sql.VarChar, NoProduksiMixer)
              .input('NoMixer', sql.VarChar, NoMixer)
              .input('NoSak', sql.Int, row.NoSak)
              .query(insertProdSql);
            outputCount++;
          }
          outputTarget = 'MixerProduksiOutput';
        } else if (NoBongkarSusun) {
          // 3b) Mapping ke BongkarSusunOutputMixer (BG.*)
          const insertBsoSql = `
            INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
            VALUES (@NoBongkarSusun, @NoMixer, @NoSak)
          `;
          for (const row of dets.recordset) {
            await new sql.Request(tx)
              .input('NoBongkarSusun', sql.VarChar, NoBongkarSusun)
              .input('NoMixer', sql.VarChar, NoMixer)
              .input('NoSak', sql.Int, row.NoSak)
              .query(insertBsoSql);
            outputCount++;
          }
          outputTarget = 'BongkarSusunOutputMixer';
        } else if (NoProduksiInject) {
          // 3c) Mapping ke InjectProduksiOutputMixer (S.*)
          const insertInjectSql = `
            INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
            VALUES (@NoProduksi, @NoMixer, @NoSak)
          `;
          for (const row of dets.recordset) {
            await new sql.Request(tx)
              .input('NoProduksi', sql.VarChar, NoProduksiInject)
              .input('NoMixer', sql.VarChar, NoMixer)
              .input('NoSak', sql.Int, row.NoSak)
              .query(insertInjectSql);
            outputCount++;
          }
          outputTarget = 'InjectProduksiOutputMixer';
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoMixer,
        ...header,
      },
      counts: {
        detailsAffected,
        outputInserted: outputCount,
      },
      outputTarget,
      outputKind,
      outputCode: hasOutputCodeField ? rawOutputCode : undefined,
      note: details
        ? 'Details with DateUsage IS NULL were replaced according to payload.'
        : 'Details were not modified.',
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

  

// Delete 1 Mixer header + outputs + details + partials (safe)
exports.deleteMixerCascade = async (nomixer) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoMixer = (nomixer || '').toString().trim();
  if (!NoMixer) {
    const e = new Error('NoMixer is required');
    e.statusCode = 400;
    throw e;
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // 0) Ensure header exists + lock it
    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        SELECT 1
        FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoMixer = @NoMixer
      `);

    if (exist.recordset.length === 0) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    // 1) Block if any detail is already used (DateUsage IS NOT NULL)
    const rqUsed = new sql.Request(tx);
    const used = await rqUsed
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        SELECT TOP 1 1
        FROM dbo.Mixer_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoMixer = @NoMixer AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      const e = new Error(
        'Cannot delete: some details are already used (DateUsage IS NOT NULL).'
      );
      e.statusCode = 409;
      throw e;
    }

    // 2) Delete outputs first (avoid FK problems)

    // 2a) MixerProduksiOutput
    const delMixerProduksiOutput = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE FROM dbo.MixerProduksiOutput
        WHERE NoMixer = @NoMixer
      `);

    // 2b) BongkarSusunOutputMixer
    const delBongkarSusunOutput = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE FROM dbo.BongkarSusunOutputMixer
        WHERE NoMixer = @NoMixer
      `);

    // 2c) InjectProduksiOutputMixer  ⬅️ TAMBAHAN BARU
    const delInjectOutput = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE FROM dbo.InjectProduksiOutputMixer
        WHERE NoMixer = @NoMixer
      `);

    // 3) Delete partial INPUT usages that reference MixerPartial for this NoMixer
    const delBrokerPartialInput = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE bip
        FROM dbo.BrokerProduksiInputMixerPartial AS bip
        INNER JOIN dbo.MixerPartial AS mp
          ON mp.NoMixerPartial = bip.NoMixerPartial
        WHERE mp.NoMixer = @NoMixer
      `);

    const delInjectPartialInput = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE iip
        FROM dbo.InjectProduksiInputMixerPartial AS iip
        INNER JOIN dbo.MixerPartial AS mp
          ON mp.NoMixerPartial = iip.NoMixerPartial
        WHERE mp.NoMixer = @NoMixer
      `);

    const delMixerPartialInput = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE mip
        FROM dbo.MixerProduksiInputMixerPartial AS mip
        INNER JOIN dbo.MixerPartial AS mp
          ON mp.NoMixerPartial = mip.NoMixerPartial
        WHERE mp.NoMixer = @NoMixer
      `);

    // 4) Delete partial rows themselves
    const delPartial = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE FROM dbo.MixerPartial
        WHERE NoMixer = @NoMixer
      `);

    // 5) Delete details (only those not used)
    const delDet = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE FROM dbo.Mixer_d
        WHERE NoMixer = @NoMixer AND DateUsage IS NULL
      `);

    // 6) Delete header
    const delHead = await new sql.Request(tx)
      .input('NoMixer', sql.VarChar, NoMixer)
      .query(`
        DELETE FROM dbo.Mixer_h
        WHERE NoMixer = @NoMixer
      `);

    await tx.commit();

    return {
      NoMixer,
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: {
          mixerProduksiOutput:
            delMixerProduksiOutput.rowsAffected?.[0] ?? 0,
          bongkarSusunOutputMixer:
            delBongkarSusunOutput.rowsAffected?.[0] ?? 0,
          injectProduksiOutputMixer:
            delInjectOutput.rowsAffected?.[0] ?? 0,
        },
        partials: {
          mixerPartial: delPartial.rowsAffected?.[0] ?? 0,
          brokerInputPartial: delBrokerPartialInput.rowsAffected?.[0] ?? 0,
          injectInputPartial: delInjectPartialInput.rowsAffected?.[0] ?? 0,
          mixerInputPartial: delMixerPartialInput.rowsAffected?.[0] ?? 0,
        },
      },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}

    // Map FK constraint error, same pattern as Broker
    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || 'Delete failed due to foreign key constraint.';
    }

    throw e;
  }
};


  // mixer-service.js
exports.getPartialInfoByMixerAndSak = async (nomixer, nosak) => {
    const pool = await poolPromise;
  
    const req = pool
      .request()
      .input('NoMixer', sql.VarChar, nomixer)
      .input('NoSak', sql.Int, nosak);
  
    const query = `
      ;WITH BasePartial AS (
        SELECT
          mp.NoMixerPartial,
          mp.NoMixer,
          mp.NoSak,
          mp.Berat
        FROM dbo.MixerPartial mp
        WHERE mp.NoMixer = @NoMixer
          AND mp.NoSak   = @NoSak
      ),
      Consumed AS (
        SELECT
          b.NoMixerPartial,
          'BROKER' AS SourceType,
          b.NoProduksi
        FROM dbo.BrokerProduksiInputMixerPartial b
  
        UNION ALL
  
        SELECT
          i.NoMixerPartial,
          'INJECT' AS SourceType,
          i.NoProduksi
        FROM dbo.InjectProduksiInputMixerPartial i
  
        UNION ALL
  
        SELECT
          m.NoMixerPartial,
          'MIXER' AS SourceType,
          m.NoProduksi
        FROM dbo.MixerProduksiInputMixerPartial m
      )
      SELECT
        bp.NoMixerPartial,
        bp.NoMixer,
        bp.NoSak,
        bp.Berat,                 -- partial weight
  
        c.SourceType,             -- BROKER / INJECT / MIXER / NULL
        c.NoProduksi,
  
        -- unified production fields from the 3 header tables
        COALESCE(bph.TglProduksi, iph.TglProduksi, mph.TglProduksi) AS TglProduksi,
        COALESCE(bph.IdMesin,     iph.IdMesin,     mph.IdMesin)     AS IdMesin,
        COALESCE(bph.IdOperator,  iph.IdOperator,  mph.IdOperator)  AS IdOperator,
        COALESCE(bph.Jam,         iph.Jam,         mph.Jam)         AS Jam,
        COALESCE(bph.Shift,       iph.Shift,       mph.Shift)       AS Shift,
  
        mm.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoMixerPartial = bp.NoMixerPartial
  
      -- Production headers for each flow
      LEFT JOIN dbo.BrokerProduksi_h bph
        ON c.SourceType = 'BROKER'
       AND bph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.InjectProduksi_h iph
        ON c.SourceType = 'INJECT'
       AND iph.NoProduksi = c.NoProduksi
  
      LEFT JOIN dbo.MixerProduksi_h mph
        ON c.SourceType = 'MIXER'
       AND mph.NoProduksi = c.NoProduksi
  
      -- Machine
      LEFT JOIN dbo.MstMesin mm
        ON mm.IdMesin = COALESCE(bph.IdMesin, iph.IdMesin, mph.IdMesin)
  
      ORDER BY
        bp.NoMixerPartial ASC,
        c.SourceType ASC,
        c.NoProduksi ASC;
    `;
  
    const result = await req.query(query);
  
    // total partial weight (unique per NoMixerPartial)
    const seen = new Set();
    let totalPartialWeight = 0;
  
    for (const row of result.recordset) {
      const key = row.NoMixerPartial;
      if (!seen.has(key)) {
        seen.add(key);
        const w =
          typeof row.Berat === 'number'
            ? row.Berat
            : Number(row.Berat) || 0;
        totalPartialWeight += w;
      }
    }
  
    const formatDate = (date) => {
      if (!date) return null;
      const d = new Date(date);
      const pad = (n) => (n < 10 ? '0' + n : '' + n);
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
    };
  
    const rows = result.recordset.map((r) => ({
      NoMixerPartial: r.NoMixerPartial,
      NoMixer: r.NoMixer,
      NoSak: r.NoSak,
      Berat: r.Berat,
  
      SourceType: r.SourceType || null,      // 'BROKER' | 'INJECT' | 'MIXER' | null
      NoProduksi: r.NoProduksi || null,
  
      TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
      IdMesin: r.IdMesin || null,
      NamaMesin: r.NamaMesin || null,
      IdOperator: r.IdOperator || null,
      Jam: r.Jam || null,
      Shift: r.Shift || null,
    }));
  
    return { totalPartialWeight, rows };
  };
  
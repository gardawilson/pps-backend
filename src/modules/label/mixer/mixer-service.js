// controllers/mixer-service.js (atau di folder yang sama dengan broker-service)
const { sql, poolPromise } = require("../../../core/config/db");
const {
  getBlokLokasiFromKodeProduksi,
} = require("../../../core/shared/mesin-location-helper");

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,
  formatYMD,
} = require("../../../core/shared/tutup-transaksi-guard");

const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");

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
    : "";

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
      ISNULL(CAST(h.HasBeenPrinted AS int), 0) AS HasBeenPrinted,
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
  reqData.input("offset", sql.Int, offset);
  reqData.input("limit", sql.Int, limit);
  if (search) {
    reqData.input("search", sql.VarChar, `%${search}%`);
  }
  const dataResult = await reqData.query(baseQuery);

  // ==== eksekusi query count ====
  const reqCount = pool.request();
  if (search) {
    reqCount.input("search", sql.VarChar, `%${search}%`);
  }
  const countResult = await reqCount.query(countQuery);

  const data = dataResult.recordset?.map((item) => ({ ...item })) ?? [];
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};

// GET details by NoMixer (tanpa IdLokasi)
exports.getMixerDetailByNoMixer = async (nomixer) => {
  const pool = await poolPromise;

  const result = await pool.request().input("NoMixer", sql.VarChar, nomixer)
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
        ORDER BY d.NoSak;
      `);

  // Optional: format tanggal biar rapi (sama seperti broker)
  const formatDateTime = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? "0" + n : String(n));
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

exports.createMixerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : [];

  // =====================================================
  // Validasi dasar
  // =====================================================
  if (!header.IdMixer) throw badReq("IdMixer wajib diisi");
  if (!header.CreateBy) throw badReq("CreateBy wajib diisi"); // controller overwrite dari token
  if (!Array.isArray(details) || details.length === 0)
    throw badReq("Details wajib berisi minimal 1 item");

  // =====================================================
  // [AUDIT] actorId + requestId (dari controller)
  // =====================================================
  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;

  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  // =====================================================
  // outputCode parsing
  // =====================================================
  const rawOutputCode = payload?.outputCode?.toString().trim() || "";

  let NoProduksi = null; // MixerProduksiOutput
  let NoBongkarSusun = null; // BongkarSusunOutputMixer
  let NoInjectProduksi = null; // InjectProduksiOutputMixer
  let outputKind = null; // 'PRODUKSI' | 'BONGKAR' | 'INJECT' | null

  if (rawOutputCode) {
    const upper = rawOutputCode.toUpperCase();
    if (upper.startsWith("BG.")) {
      NoBongkarSusun = rawOutputCode;
      outputKind = "BONGKAR";
    } else if (upper.startsWith("I.")) {
      NoProduksi = rawOutputCode;
      outputKind = "PRODUKSI";
    } else if (upper.startsWith("S.")) {
      NoInjectProduksi = rawOutputCode;
      outputKind = "INJECT";
    } else {
      throw badReq('outputCode harus diawali "BG.", "I." atau "S."');
    }
  }

  // =====================================================
  // [DETAILS] normalize + validate sekali (NO INSERT LOOP)
  // Mixer_d: (NoMixer, NoSak, Berat, DateUsage, IsPartial)
  // =====================================================
  const normalizedDetails = details.map((d) => {
    const noSak = Number(d?.NoSak);
    if (!Number.isFinite(noSak) || noSak <= 0) {
      throw badReq(`NoSak tidak valid: ${d?.NoSak}`);
    }

    const berat = d?.Berat == null ? 0 : Number(d.Berat);
    if (!Number.isFinite(berat) || berat < 0) {
      throw badReq(
        `Berat tidak valid pada NoSak ${Math.trunc(noSak)}: ${d?.Berat}`,
      );
    }

    const isPartialRaw = d?.IsPartial;
    const isPartial =
      isPartialRaw === true ||
      isPartialRaw === 1 ||
      String(isPartialRaw).trim() === "1"
        ? 1
        : 0;

    return {
      NoSak: Math.trunc(noSak),
      Berat: berat,
      IsPartial: isPartial,
    };
  });

  // optional: cegah NoSak duplikat
  {
    const set = new Set();
    for (const x of normalizedDetails) {
      const k = String(x.NoSak);
      if (set.has(k)) throw badReq(`NoSak duplikat di payload: ${x.NoSak}`);
      set.add(k);
    }
  }

  const detailsJson = JSON.stringify(normalizedDetails);

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =====================================================
    // [AUDIT CTX] Set actor_id + request_id untuk trigger audit
    // =====================================================
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (CREATE)
    // ===============================
    const effectiveDateCreate = resolveEffectiveDateForCreate(
      header.DateCreate,
    ); // date-only
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: "create mixer",
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari kode produksi/bongkar/inject (kalau header kosong)
    // ===============================
    const needBlok = header.Blok == null || String(header.Blok).trim() === "";
    const needLokasi = header.IdLokasi == null;

    if ((needBlok || needLokasi) && rawOutputCode) {
      const lokasi = await getBlokLokasiFromKodeProduksi({
        kode: rawOutputCode,
        runner: tx,
      });
      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    // ===============================
    // 1) Generate NoMixer (PAKAI generateNextCode)
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.Mixer_h",
        columnName: "NoMixer",
        prefix: "H.",
        width: 10,
      });

    const generatedNo = await gen();

    // 2) Double-check belum dipakai (lock)
    const exist = await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), generatedNo)
      .query(
        `SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK) WHERE NoMixer = @NoMixer`,
      );

    if (exist.recordset.length > 0) {
      const retryNo = await gen();
      const exist2 = await new sql.Request(tx)
        .input("NoMixer", sql.VarChar(50), retryNo)
        .query(
          `SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK) WHERE NoMixer = @NoMixer`,
        );

      if (exist2.recordset.length > 0)
        throw conflict("Gagal generate NoMixer unik, coba lagi.");
      header.NoMixer = retryNo;
    } else {
      header.NoMixer = generatedNo;
    }

    // normalize IdLokasi
    let idLokasiVal = null;
    if (header.IdLokasi !== undefined && header.IdLokasi !== null) {
      const s = String(header.IdLokasi).trim();
      if (s !== "" && s !== "-") {
        const n = Number(s);
        if (!Number.isFinite(n)) throw badReq("IdLokasi harus angka");
        idLokasiVal = Math.trunc(n);
      }
    }

    // ===============================
    // 3) Insert header
    // ===============================
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO dbo.Mixer_h (
        NoMixer, IdMixer, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Moisture, MaxMeltTemp, MinMeltTemp, MFI,
        Moisture2, Moisture3,
        Blok, IdLokasi
      )
      VALUES (
        @NoMixer, @IdMixer, @DateCreate, @IdStatus, @CreateBy, @DateTimeCreate,
        @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI,
        @Moisture2, @Moisture3,
        @Blok, @IdLokasi
      );
    `;

    await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), header.NoMixer)
      .input("IdMixer", sql.Int, header.IdMixer)
      .input("DateCreate", sql.Date, effectiveDateCreate)
      .input("IdStatus", sql.Int, header.IdStatus ?? 1)
      .input("CreateBy", sql.VarChar(50), header.CreateBy)
      .input("DateTimeCreate", sql.DateTime, nowDateTime)
      .input("Moisture", sql.Decimal(10, 3), header.Moisture ?? null)
      .input("MaxMeltTemp", sql.Decimal(10, 3), header.MaxMeltTemp ?? null)
      .input("MinMeltTemp", sql.Decimal(10, 3), header.MinMeltTemp ?? null)
      .input("MFI", sql.Decimal(10, 3), header.MFI ?? null)
      .input("Moisture2", sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input("Moisture3", sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input("Blok", sql.VarChar(50), header.Blok ?? null)
      .input("IdLokasi", sql.Int, idLokasiVal)
      .query(insertHeaderSql);

    // ===============================
    // 4) Insert details (BULK) — OPENJSON
    // ===============================
    const insertDetailsBulkSql = `
      INSERT INTO dbo.Mixer_d (NoMixer, NoSak, Berat, DateUsage, IsPartial)
      SELECT
        @NoMixer,
        j.NoSak,
        j.Berat,
        NULL,
        j.IsPartial
      FROM OPENJSON(@DetailsJson)
      WITH (
        NoSak int '$.NoSak',
        Berat decimal(18,3) '$.Berat',
        IsPartial int '$.IsPartial'
      ) AS j;
    `;

    await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), header.NoMixer)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
      .query(insertDetailsBulkSql);

    const detailCount = normalizedDetails.length;

    // ===============================
    // 5) Optional output (BULK) — OPENJSON
    // ===============================
    let outputTarget = null;
    let outputCount = 0;

    if (NoProduksi) {
      const q = `
        INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
        SELECT @NoProduksi, @NoMixer, j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;
      await new sql.Request(tx)
        .input("NoProduksi", sql.VarChar(50), NoProduksi)
        .input("NoMixer", sql.VarChar(50), header.NoMixer)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(q);

      outputCount = detailCount;
      outputTarget = "MixerProduksiOutput";
    } else if (NoBongkarSusun) {
      const q = `
        INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
        SELECT @NoBongkarSusun, @NoMixer, j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
        .input("NoMixer", sql.VarChar(50), header.NoMixer)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(q);

      outputCount = detailCount;
      outputTarget = "BongkarSusunOutputMixer";
    } else if (NoInjectProduksi) {
      const q = `
        INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
        SELECT @NoProduksi, @NoMixer, j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;
      await new sql.Request(tx)
        .input("NoProduksi", sql.VarChar(50), NoInjectProduksi)
        .input("NoMixer", sql.VarChar(50), header.NoMixer)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(q);

      outputCount = detailCount;
      outputTarget = "InjectProduksiOutputMixer";
    }

    await tx.commit();

    return {
      header: {
        NoMixer: header.NoMixer,
        IdMixer: header.IdMixer,
        IdStatus: header.IdStatus ?? 1,
        CreateBy: header.CreateBy,
        DateCreate: formatYMD(effectiveDateCreate),
        DateTimeCreate: nowDateTime,
        Moisture: header.Moisture ?? null,
        MaxMeltTemp: header.MaxMeltTemp ?? null,
        MinMeltTemp: header.MinMeltTemp ?? null,
        MFI: header.MFI ?? null,
        Moisture2: header.Moisture2 ?? null,
        Moisture3: header.Moisture3 ?? null,
        Blok: header.Blok ?? null,
        IdLokasi: idLokasiVal,
      },
      counts: { detailsInserted: detailCount, outputInserted: outputCount },
      outputKind,
      outputTarget,
      outputCode: rawOutputCode || null,
      audit: { actorId, requestId },
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
    const e = new Error("NoMixer (path) is required");
    e.statusCode = 400;
    throw e;
  }

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : null;

  const hasOutputCodeField = Object.prototype.hasOwnProperty.call(
    payload,
    "outputCode",
  );
  const hasDetailsField = Object.prototype.hasOwnProperty.call(
    payload,
    "details",
  ); // ✅ penting: bedakan null vs tidak dikirim

  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };

  // =====================================================
  // [AUDIT] actorId + requestId (wajib, sama seperti create)
  // =====================================================
  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );
  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  // =====================================================
  // outputCode parsing (kalau field outputCode ada)
  // =====================================================
  let rawOutputCode = null;
  let NoProduksiMixer = null;
  let NoBongkarSusun = null;
  let NoProduksiInject = null;
  let outputKind = null;

  if (hasOutputCodeField) {
    rawOutputCode = payload.outputCode?.toString().trim() || "";

    if (rawOutputCode) {
      const upper = rawOutputCode.toUpperCase();

      if (upper.startsWith("BG.")) {
        NoBongkarSusun = rawOutputCode;
        outputKind = "BONGKAR";
      } else if (upper.startsWith("I.")) {
        NoProduksiMixer = rawOutputCode;
        outputKind = "MIXER_PRODUKSI";
      } else if (upper.startsWith("S.")) {
        NoProduksiInject = rawOutputCode;
        outputKind = "INJECT";
      } else {
        const e = new Error(
          'outputCode must start with "BG.", "I." or "S." if provided',
        );
        e.statusCode = 400;
        throw e;
      }
    } else {
      rawOutputCode = ""; // explicit clear output
    }
  }

  // =====================================================
  // Normalize details sekali (kalau details dikirim)
  // Mixer_d: (NoMixer, NoSak, Berat, DateUsage, IsPartial)
  // =====================================================
  let normalizedDetails = null;
  let detailsJson = null;

  if (hasDetailsField) {
    // kalau user kirim details tapi bukan array / empty -> boleh kamu tentukan:
    // di sini saya treat: harus array & minimal 1 item
    if (!Array.isArray(details) || details.length === 0)
      throw badReq("Details wajib berisi minimal 1 item");

    normalizedDetails = details.map((d) => {
      const noSak = Number(d?.NoSak);
      if (!Number.isFinite(noSak) || noSak <= 0)
        throw badReq(`NoSak tidak valid: ${d?.NoSak}`);

      const berat = d?.Berat == null ? 0 : Number(d.Berat);
      if (!Number.isFinite(berat) || berat < 0)
        throw badReq(
          `Berat tidak valid pada NoSak ${Math.trunc(noSak)}: ${d?.Berat}`,
        );

      const isPartialRaw = d?.IsPartial;
      const isPartial =
        isPartialRaw === true ||
        isPartialRaw === 1 ||
        String(isPartialRaw).trim() === "1"
          ? 1
          : 0;

      return {
        NoSak: Math.trunc(noSak),
        Berat: berat,
        IsPartial: isPartial,
      };
    });

    // cegah NoSak duplikat
    {
      const set = new Set();
      for (const x of normalizedDetails) {
        const k = String(x.NoSak);
        if (set.has(k)) throw badReq(`NoSak duplikat di payload: ${x.NoSak}`);
        set.add(k);
      }
    }

    detailsJson = JSON.stringify(normalizedDetails);
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =====================================================
    // [AUDIT CTX] Set actor_id + request_id untuk trigger audit
    // =====================================================
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 0) Pastikan header ada + lock + ambil DateCreate
    // ===============================
    const head = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        SELECT TOP 1 NoMixer, CONVERT(date, DateCreate) AS DateCreate
        FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoMixer = @NoMixer
      `);

    if (head.recordset.length === 0) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    const existingDateCreate = head.recordset[0]?.DateCreate
      ? toDateOnly(head.recordset[0].DateCreate)
      : null;

    // ===============================
    // 0b) TUTUP TRANSAKSI CHECK (UPDATE) - selalu cek tanggal existing
    // ===============================
    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: "update mixer",
      useLock: true,
    });

    // ===============================
    // 1) Dynamic header update
    // ===============================
    const setParts = [];
    const reqHeader = new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    );

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf("IdMixer", "IdMixer", sql.Int, header.IdMixer);

    // DateCreate: jika diubah, cek lock tanggal baru juga
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null || header.DateCreate === "") {
        const utcToday = toDateOnly(new Date());

        await assertNotLocked({
          date: utcToday,
          runner: tx,
          action: "update mixer (DateCreate reset)",
          useLock: true,
        });

        setParts.push("DateCreate = @DateCreate");
        reqHeader.input("DateCreate", sql.Date, utcToday);
      } else {
        const d = toDateOnly(header.DateCreate);
        if (!d) {
          const e = new Error("Invalid DateCreate");
          e.statusCode = 400;
          e.meta = { field: "DateCreate", value: header.DateCreate };
          throw e;
        }

        await assertNotLocked({
          date: d,
          runner: tx,
          action: "update mixer (DateCreate)",
          useLock: true,
        });

        setParts.push("DateCreate = @DateCreate");
        reqHeader.input("DateCreate", sql.Date, d);
      }
    }

    setIf("IdStatus", "IdStatus", sql.Int, header.IdStatus);
    setIf("Moisture", "Moisture", sql.Decimal(10, 3), header.Moisture ?? null);
    setIf(
      "MaxMeltTemp",
      "MaxMeltTemp",
      sql.Decimal(10, 3),
      header.MaxMeltTemp ?? null,
    );
    setIf(
      "MinMeltTemp",
      "MinMeltTemp",
      sql.Decimal(10, 3),
      header.MinMeltTemp ?? null,
    );
    setIf("MFI", "MFI", sql.Decimal(10, 3), header.MFI ?? null);
    setIf(
      "Moisture2",
      "Moisture2",
      sql.Decimal(10, 3),
      header.Moisture2 ?? null,
    );
    setIf(
      "Moisture3",
      "Moisture3",
      sql.Decimal(10, 3),
      header.Moisture3 ?? null,
    );
    setIf("Blok", "Blok", sql.VarChar(50), header.Blok ?? null);

    if (header.IdLokasi !== undefined) {
      setParts.push("IdLokasi = @IdLokasi");
      reqHeader.input(
        "IdLokasi",
        sql.Int,
        header.IdLokasi != null && String(header.IdLokasi).trim() !== ""
          ? Math.trunc(Number(header.IdLokasi))
          : null,
      );
    }

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.Mixer_h SET ${setParts.join(", ")}
        WHERE NoMixer = @NoMixer
      `);
    }

    // ===============================
    // 2) Replace details (ONLY if field details is provided)
    // ===============================
    let detailsAffected = 0;

    if (hasDetailsField) {
      // hapus detail aktif dulu
      await new sql.Request(tx).input("NoMixer", sql.VarChar(50), NoMixer)
        .query(`
          DELETE FROM dbo.Mixer_d
          WHERE NoMixer = @NoMixer AND DateUsage IS NULL
        `);

      // insert bulk OPENJSON
      const insertDetailsBulkSql = `
        INSERT INTO dbo.Mixer_d (NoMixer, NoSak, Berat, DateUsage, IsPartial)
        SELECT
          @NoMixer,
          j.NoSak,
          j.Berat,
          NULL,
          j.IsPartial
        FROM OPENJSON(@DetailsJson)
        WITH (
          NoSak int '$.NoSak',
          Berat decimal(18,3) '$.Berat',
          IsPartial int '$.IsPartial'
        ) AS j;
      `;

      await new sql.Request(tx)
        .input("NoMixer", sql.VarChar(50), NoMixer)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(insertDetailsBulkSql);

      detailsAffected = normalizedDetails.length;
    }

    // ===============================
    // 3) Outputs handling (ONLY if field outputCode is provided)
    // ===============================
    let outputTarget = null;
    let outputCount = 0;

    if (hasOutputCodeField) {
      // bersihkan semua relasi output untuk NoMixer ini
      await new sql.Request(tx).input("NoMixer", sql.VarChar(50), NoMixer)
        .query(`
          DELETE FROM dbo.MixerProduksiOutput WHERE NoMixer = @NoMixer;
          DELETE FROM dbo.BongkarSusunOutputMixer WHERE NoMixer = @NoMixer;
          DELETE FROM dbo.InjectProduksiOutputMixer WHERE NoMixer = @NoMixer;
        `);

      // jika outputCode kosong => berarti "clear output"
      if (rawOutputCode) {
        // sumber NoSak:
        // - kalau details dikirim, pakai detailsJson (lebih konsisten)
        // - kalau details tidak dikirim, ambil dari Mixer_d existing
        let sourceMode = null;

        if (hasDetailsField) {
          sourceMode = "JSON";
        } else {
          sourceMode = "DB";
        }

        if (NoProduksiMixer) {
          if (sourceMode === "JSON") {
            const q = `
              INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
              SELECT @NoProduksi, @NoMixer, j.NoSak
              FROM OPENJSON(@DetailsJson)
              WITH (NoSak int '$.NoSak') AS j;
            `;
            await new sql.Request(tx)
              .input("NoProduksi", sql.VarChar(50), NoProduksiMixer)
              .input("NoMixer", sql.VarChar(50), NoMixer)
              .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
              .query(q);

            outputCount = hasDetailsField ? normalizedDetails.length : 0;
          } else {
            const q = `
              INSERT INTO dbo.MixerProduksiOutput (NoProduksi, NoMixer, NoSak)
              SELECT @NoProduksi, @NoMixer, d.NoSak
              FROM dbo.Mixer_d d
              WHERE d.NoMixer=@NoMixer AND d.DateUsage IS NULL;
            `;
            const r = await new sql.Request(tx)
              .input("NoProduksi", sql.VarChar(50), NoProduksiMixer)
              .input("NoMixer", sql.VarChar(50), NoMixer)
              .query(q);

            outputCount = r.rowsAffected?.[0] || 0;
          }

          outputTarget = "MixerProduksiOutput";
        } else if (NoBongkarSusun) {
          if (sourceMode === "JSON") {
            const q = `
              INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
              SELECT @NoBongkarSusun, @NoMixer, j.NoSak
              FROM OPENJSON(@DetailsJson)
              WITH (NoSak int '$.NoSak') AS j;
            `;
            await new sql.Request(tx)
              .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
              .input("NoMixer", sql.VarChar(50), NoMixer)
              .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
              .query(q);

            outputCount = hasDetailsField ? normalizedDetails.length : 0;
          } else {
            const q = `
              INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
              SELECT @NoBongkarSusun, @NoMixer, d.NoSak
              FROM dbo.Mixer_d d
              WHERE d.NoMixer=@NoMixer AND d.DateUsage IS NULL;
            `;
            const r = await new sql.Request(tx)
              .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
              .input("NoMixer", sql.VarChar(50), NoMixer)
              .query(q);

            outputCount = r.rowsAffected?.[0] || 0;
          }

          outputTarget = "BongkarSusunOutputMixer";
        } else if (NoProduksiInject) {
          if (sourceMode === "JSON") {
            const q = `
              INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
              SELECT @NoProduksi, @NoMixer, j.NoSak
              FROM OPENJSON(@DetailsJson)
              WITH (NoSak int '$.NoSak') AS j;
            `;
            await new sql.Request(tx)
              .input("NoProduksi", sql.VarChar(50), NoProduksiInject)
              .input("NoMixer", sql.VarChar(50), NoMixer)
              .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
              .query(q);

            outputCount = hasDetailsField ? normalizedDetails.length : 0;
          } else {
            const q = `
              INSERT INTO dbo.InjectProduksiOutputMixer (NoProduksi, NoMixer, NoSak)
              SELECT @NoProduksi, @NoMixer, d.NoSak
              FROM dbo.Mixer_d d
              WHERE d.NoMixer=@NoMixer AND d.DateUsage IS NULL;
            `;
            const r = await new sql.Request(tx)
              .input("NoProduksi", sql.VarChar(50), NoProduksiInject)
              .input("NoMixer", sql.VarChar(50), NoMixer)
              .query(q);

            outputCount = r.rowsAffected?.[0] || 0;
          }

          outputTarget = "InjectProduksiOutputMixer";
        }
      }
    }

    await tx.commit();

    return {
      header: { NoMixer, ...header },
      counts: { detailsAffected, outputInserted: outputCount },
      outputTarget,
      outputKind,
      outputCode: hasOutputCodeField ? rawOutputCode : undefined,
      audit: { actorId, requestId },
      note: hasDetailsField
        ? "Details aktif (DateUsage IS NULL) diganti sesuai payload."
        : "Details tidak diubah.",
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.deleteMixerCascade = async (payload) => {
  // payload: { NoMixer, actorId, requestId }
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoMixer = (payload?.NoMixer || payload || "").toString().trim();
  if (!NoMixer) {
    const e = new Error("NoMixer is required");
    e.statusCode = 400;
    throw e;
  }

  // =====================================================
  // [AUDIT] actorId + requestId (wajib)
  // =====================================================
  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    // =====================================================
    // [AUDIT CTX] Set actor_id + request_id untuk trigger audit
    // =====================================================
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // ===============================
    // 0) Ensure header exists + lock it + ambil DateCreate
    // ===============================
    const head = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        SELECT TOP 1 NoMixer, CONVERT(date, DateCreate) AS DateCreate
        FROM dbo.Mixer_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoMixer = @NoMixer
      `);

    if (head.recordset.length === 0) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    const trxDate = head.recordset[0]?.DateCreate
      ? toDateOnly(head.recordset[0].DateCreate)
      : null;

    // ===============================
    // 0b) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: "delete mixer",
      useLock: true,
    });

    // ===============================
    // 1) Block if any detail is already used (DateUsage IS NOT NULL)
    // ===============================
    const used = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        SELECT TOP 1 1
        FROM dbo.Mixer_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoMixer = @NoMixer AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      const e = new Error(
        "Cannot delete: some details are already used (DateUsage IS NOT NULL).",
      );
      e.statusCode = 409;
      throw e;
    }

    // ===============================
    // 2) Delete outputs first (avoid FK problems)
    // ===============================
    const delMixerProduksiOutput = await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), NoMixer)
      .query(`DELETE FROM dbo.MixerProduksiOutput WHERE NoMixer = @NoMixer`);

    const delBongkarSusunOutput = await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), NoMixer)
      .query(
        `DELETE FROM dbo.BongkarSusunOutputMixer WHERE NoMixer = @NoMixer`,
      );

    const delInjectOutput = await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), NoMixer)
      .query(
        `DELETE FROM dbo.InjectProduksiOutputMixer WHERE NoMixer = @NoMixer`,
      );

    // ===============================
    // 3) Delete partial INPUT usages that reference MixerPartial for this NoMixer
    // ===============================
    const delBrokerPartialInput = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        DELETE bip
        FROM dbo.BrokerProduksiInputMixerPartial AS bip
        INNER JOIN dbo.MixerPartial AS mp
          ON mp.NoMixerPartial = bip.NoMixerPartial
        WHERE mp.NoMixer = @NoMixer
      `);

    const delInjectPartialInput = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        DELETE iip
        FROM dbo.InjectProduksiInputMixerPartial AS iip
        INNER JOIN dbo.MixerPartial AS mp
          ON mp.NoMixerPartial = iip.NoMixerPartial
        WHERE mp.NoMixer = @NoMixer
      `);

    const delMixerPartialInput = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        DELETE mip
        FROM dbo.MixerProduksiInputMixerPartial AS mip
        INNER JOIN dbo.MixerPartial AS mp
          ON mp.NoMixerPartial = mip.NoMixerPartial
        WHERE mp.NoMixer = @NoMixer
      `);

    // ===============================
    // 4) Delete partial rows themselves
    // ===============================
    const delPartial = await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), NoMixer)
      .query(`DELETE FROM dbo.MixerPartial WHERE NoMixer = @NoMixer`);

    // ===============================
    // 5) Delete details (only those not used)
    // ===============================
    const delDet = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        DELETE FROM dbo.Mixer_d
        WHERE NoMixer = @NoMixer AND DateUsage IS NULL
      `);

    // ===============================
    // 6) Delete header
    // ===============================
    const delHead = await new sql.Request(tx)
      .input("NoMixer", sql.VarChar(50), NoMixer)
      .query(`DELETE FROM dbo.Mixer_h WHERE NoMixer = @NoMixer`);

    if ((delHead.rowsAffected?.[0] ?? 0) === 0) {
      await tx.rollback();
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoMixer,
      audit: { actorId, requestId },
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: {
          mixerProduksiOutput: delMixerProduksiOutput.rowsAffected?.[0] ?? 0,
          bongkarSusunOutputMixer: delBongkarSusunOutput.rowsAffected?.[0] ?? 0,
          injectProduksiOutputMixer: delInjectOutput.rowsAffected?.[0] ?? 0,
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

    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || "Delete failed due to foreign key constraint.";
    }

    throw e;
  }
};

// mixer-service.js
exports.getPartialInfoByMixerAndSak = async (nomixer, nosak) => {
  const pool = await poolPromise;

  const req = pool
    .request()
    .input("NoMixer", sql.VarChar, nomixer)
    .input("NoSak", sql.Int, nosak);

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
        typeof row.Berat === "number" ? row.Berat : Number(row.Berat) || 0;
      totalPartialWeight += w;
    }
  }

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? "0" + n : "" + n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = result.recordset.map((r) => ({
    NoMixerPartial: r.NoMixerPartial,
    NoMixer: r.NoMixer,
    NoSak: r.NoSak,
    Berat: r.Berat,

    SourceType: r.SourceType || null, // 'BROKER' | 'INJECT' | 'MIXER' | null
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

exports.incrementHasBeenPrinted = async (payload) => {
  const NoMixer = String(payload?.NoMixer || "").trim();
  if (!NoMixer) throw badReq("NoMixer wajib diisi");

  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  if (!actorId) {
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );
  }

  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const rs = await new sql.Request(tx).input(
      "NoMixer",
      sql.VarChar(50),
      NoMixer,
    ).query(`
        DECLARE @out TABLE (
          NoMixer varchar(50),
          HasBeenPrinted int
        );

        UPDATE dbo.Mixer_h
        SET HasBeenPrinted = ISNULL(HasBeenPrinted, 0) + 1
        OUTPUT
          INSERTED.NoMixer,
          INSERTED.HasBeenPrinted
        INTO @out
        WHERE NoMixer = @NoMixer;

        SELECT NoMixer, HasBeenPrinted
        FROM @out;
      `);

    const row = rs.recordset?.[0] || null;
    if (!row) {
      const e = new Error(`NoMixer ${NoMixer} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoMixer: row.NoMixer,
      HasBeenPrinted: row.HasBeenPrinted,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

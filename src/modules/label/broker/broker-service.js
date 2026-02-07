// services/broker-service.js
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

// GET all header Broker with pagination & search (mirror of Washing.getAll)
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      h.NoBroker,
      h.DateCreate,
      h.IdJenisPlastik,
      jp.Jenis AS NamaJenisPlastik,
      h.IdWarehouse,
      w.NamaWarehouse,
      h.Blok,                   -- dari header
      h.IdLokasi,               -- dari header
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,

      -- kolom kualitas/notes
      h.Density,
      h.Moisture,
      h.MaxMeltTemp,
      h.MinMeltTemp,
      h.MFI,
      h.VisualNote,
      h.Density2,
      h.Density3,
      h.Moisture2,
      h.Moisture3,

      -- 🔎 Tambahan sesuai permintaan
      MAX(bpo.NoProduksi)         AS NoProduksi,        -- dari BrokerProduksiOutput
      MAX(m.NamaMesin)            AS NamaMesin,         -- via BrokerProduksi_h → MstMesin
      MAX(bsob.NoBongkarSusun)    AS NoBongkarSusun    -- dari BongkarSusunOutputBroker
    FROM Broker_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse    w  ON w.IdWarehouse     = h.IdWarehouse

    -- Header → Output Produksi (ambil NoProduksi)
    LEFT JOIN dbo.BrokerProduksiOutput bpo
      ON bpo.NoBroker = h.NoBroker
    -- Output → Header Produksi (ambil IdMesin)
    LEFT JOIN dbo.BrokerProduksi_h bp
      ON bp.NoProduksi = bpo.NoProduksi
    -- Mesin (ambil NamaMesin)
    LEFT JOIN dbo.MstMesin m
      ON m.IdMesin = bp.IdMesin

    -- Bongkar Susun (ambil NoBongkarSusun)
    LEFT JOIN dbo.BongkarSusunOutputBroker bsob
      ON bsob.NoBroker = h.NoBroker

    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBroker LIKE @search
               OR jp.Jenis LIKE @search
               OR w.NamaWarehouse LIKE @search
               OR bpo.NoProduksi LIKE @search
               OR m.NamaMesin LIKE @search
               OR bsob.NoBongkarSusun LIKE @search
             )`
          : ""
      }
      AND EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NULL
      )
    GROUP BY
      h.NoBroker, h.DateCreate, h.IdJenisPlastik, jp.Jenis,
      h.IdWarehouse, w.NamaWarehouse, h.IdStatus,
      h.Density, h.Moisture, h.MaxMeltTemp, h.MinMeltTemp, h.MFI, h.VisualNote,
      h.Density2, h.Density3, h.Moisture2, h.Moisture3,
      h.Blok, h.IdLokasi
    ORDER BY h.NoBroker DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoBroker) AS total
    FROM Broker_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse    w  ON w.IdWarehouse     = h.IdWarehouse
    LEFT JOIN dbo.BrokerProduksiOutput bpo
      ON bpo.NoBroker = h.NoBroker
    LEFT JOIN dbo.BrokerProduksi_h bp
      ON bp.NoProduksi = bpo.NoProduksi
    LEFT JOIN dbo.MstMesin m
      ON m.IdMesin = bp.IdMesin
    LEFT JOIN dbo.BongkarSusunOutputBroker bsob
      ON bsob.NoBroker = h.NoBroker
    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBroker LIKE @search
               OR jp.Jenis LIKE @search
               OR w.NamaWarehouse LIKE @search
               OR bpo.NoProduksi LIKE @search
               OR m.NamaMesin LIKE @search
               OR bsob.NoBongkarSusun LIKE @search
             )`
          : ""
      }
      AND EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NULL
      )
  `;

  request.input("offset", sql.Int, offset).input("limit", sql.Int, limit);
  if (search) request.input("search", sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map((item) => ({ ...item }));
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};

// GET details by NoBroker (mirror Washing.getWashingDetailByNoWashing)
exports.getBrokerDetailByNoBroker = async (nobroker) => {
  const pool = await poolPromise;

  const result = await pool.request().input("NoBroker", sql.VarChar, nobroker)
    .query(`
      SELECT
        d.NoBroker,
        d.NoSak,
        -- Jika IsPartial = 1, maka Berat dikurangi total dari BrokerPartial
        CASE 
          WHEN d.IsPartial = 1 THEN 
            d.Berat - ISNULL((
              SELECT SUM(p.Berat)
              FROM dbo.BrokerPartial p
              WHERE p.NoBroker = d.NoBroker
                AND p.NoSak = d.NoSak
            ), 0)
          ELSE d.Berat
        END AS Berat,
        d.DateUsage,
        d.IsPartial,
        d.IdLokasi
      FROM dbo.Broker_d d
      WHERE d.NoBroker = @NoBroker
      ORDER BY d.NoSak
    `);

  // Optional: format tanggal agar rapi di frontend
  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? "0" + n : n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  };

  return result.recordset.map((item) => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) }),
  }));
};

exports.createBrokerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : [];

  const NoProduksi = payload?.NoProduksi?.toString().trim() || null;
  const NoBongkarSusun = payload?.NoBongkarSusun?.toString().trim() || null;

  // ---- Validasi dasar
  if (!header.IdJenisPlastik) throw badReq("IdJenisPlastik wajib diisi");
  if (!header.IdWarehouse) throw badReq("IdWarehouse wajib diisi");
  if (!header.CreateBy) throw badReq("CreateBy wajib diisi"); // business field, controller harus overwrite dari token
  if (!Array.isArray(details) || details.length === 0)
    throw badReq("Details wajib berisi minimal 1 item");

  // Mutually exclusive check
  const hasProduksi = !!NoProduksi;
  const hasBongkar = !!NoBongkarSusun;
  if (hasProduksi && hasBongkar)
    throw badReq("NoProduksi dan NoBongkarSusun tidak boleh diisi bersamaan");

  // =====================================================
  // [AUDIT] Pakai actorId dari controller (token)
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
  // [DETAILS] normalize + validate sekali (NO INSERT LOOP)
  // Broker_d: (NoBroker, NoSak, Berat, DateUsage, IsPartial, IdLokasi)
  // - IdLokasi boleh "-" / "" / null => fallback header.IdLokasi / null
  // =====================================================
  const normalizedDetails = details.map((d) => {
    const noSak = Number(d?.NoSak);
    if (!Number.isFinite(noSak) || noSak <= 0) {
      throw badReq(`NoSak tidak valid: ${d?.NoSak}`);
    }

    const berat = d?.Berat == null ? 0 : Number(d.Berat);
    if (!Number.isFinite(berat) || berat < 0) {
      throw badReq(`Berat tidak valid pada NoSak ${noSak}: ${d?.Berat}`);
    }

    // IsPartial default 0/false
    const isPartialRaw = d?.IsPartial;
    const isPartial =
      isPartialRaw === true ||
      isPartialRaw === 1 ||
      String(isPartialRaw).trim() === "1"
        ? 1
        : 0;

    // IdLokasi fallback ke header.IdLokasi
    const rawLok = d?.IdLokasi;
    let idLokasi = null;

    if (rawLok === undefined || rawLok === null) {
      idLokasi = header.IdLokasi ?? null;
    } else {
      const s = String(rawLok).trim();
      if (s === "" || s === "-") {
        idLokasi = header.IdLokasi ?? null;
      } else {
        const n = Number(s);
        if (!Number.isFinite(n)) {
          throw badReq(
            `IdLokasi tidak valid pada NoSak ${Math.trunc(noSak)}: ${rawLok}`,
          );
        }
        idLokasi = Math.trunc(n);
      }
    }

    return {
      NoSak: Math.trunc(noSak),
      Berat: berat,
      IsPartial: isPartial,
      IdLokasi: idLokasi === null ? null : Math.trunc(Number(idLokasi)),
    };
  });

  // optional tapi recommended: cegah NoSak duplikat dalam payload
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
      action: "create broker",
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari sumber kode (produksi / bongkar susun)
    // ===============================
    const needBlok = header.Blok == null || String(header.Blok).trim() === "";
    const needLokasi = header.IdLokasi == null;

    if (needBlok || needLokasi) {
      const kodeRef = hasProduksi
        ? NoProduksi
        : hasBongkar
          ? NoBongkarSusun
          : null;

      let lokasi = null;
      if (kodeRef) {
        lokasi = await getBlokLokasiFromKodeProduksi({
          kode: kodeRef,
          runner: tx,
        });
      }

      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    // ===============================
    // 1) Generate NoBroker (PAKAI generateNextCode seperti washing)
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "Broker_h",
        columnName: "NoBroker",
        prefix: "D.",
        width: 10,
      });

    const generatedNo = await gen();

    // 2) Double-check belum dipakai (lock supaya konsisten)
    const exist = await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), generatedNo)
      .query(
        `SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK) WHERE NoBroker = @NoBroker`,
      );

    if (exist.recordset.length > 0) {
      const retryNo = await gen();
      const exist2 = await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), retryNo)
        .query(
          `SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK) WHERE NoBroker = @NoBroker`,
        );

      if (exist2.recordset.length > 0) {
        throw conflict("Gagal generate NoBroker unik, coba lagi.");
      }
      header.NoBroker = retryNo;
    } else {
      header.NoBroker = generatedNo;
    }

    // ===============================
    // 3) Insert header
    // ===============================
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO dbo.Broker_h (
        NoBroker, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Density, Moisture, MaxMeltTemp, MinMeltTemp, MFI, VisualNote,
        Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
      )
      VALUES (
        @NoBroker, @IdJenisPlastik, @IdWarehouse,
        @DateCreate,
        @IdStatus, @CreateBy, @DateTimeCreate,
        @Density, @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @VisualNote,
        @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `;

    await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), header.NoBroker)
      .input("IdJenisPlastik", sql.Int, header.IdJenisPlastik)
      .input("IdWarehouse", sql.Int, header.IdWarehouse)
      .input("DateCreate", sql.Date, effectiveDateCreate)
      .input("IdStatus", sql.Int, header.IdStatus ?? 1)
      .input("CreateBy", sql.VarChar(50), header.CreateBy) // overwritten by controller
      .input("DateTimeCreate", sql.DateTime, nowDateTime)
      .input("Density", sql.Decimal(10, 3), header.Density ?? null)
      .input("Moisture", sql.Decimal(10, 3), header.Moisture ?? null)
      .input("MaxMeltTemp", sql.Decimal(10, 3), header.MaxMeltTemp ?? null)
      .input("MinMeltTemp", sql.Decimal(10, 3), header.MinMeltTemp ?? null)
      .input("MFI", sql.Decimal(10, 3), header.MFI ?? null)
      .input("VisualNote", sql.VarChar(sql.MAX), header.VisualNote ?? null)
      .input("Density2", sql.Decimal(10, 3), header.Density2 ?? null)
      .input("Density3", sql.Decimal(10, 3), header.Density3 ?? null)
      .input("Moisture2", sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input("Moisture3", sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input("Blok", sql.VarChar(50), header.Blok ?? null)
      .input("IdLokasi", sql.Int, header.IdLokasi ?? null)
      .query(insertHeaderSql);

    // ===============================
    // 4) Insert details (BULK) — OPENJSON
    // ===============================
    const insertDetailsBulkSql = `
      INSERT INTO dbo.Broker_d (NoBroker, NoSak, Berat, DateUsage, IsPartial, IdLokasi)
      SELECT
        @NoBroker,
        j.NoSak,
        j.Berat,
        NULL,
        j.IsPartial,
        j.IdLokasi
      FROM OPENJSON(@DetailsJson)
      WITH (
        NoSak int '$.NoSak',
        Berat decimal(18,3) '$.Berat',
        IsPartial int '$.IsPartial',
        IdLokasi int '$.IdLokasi'
      ) AS j;
    `;

    await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), header.NoBroker)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
      .query(insertDetailsBulkSql);

    const detailCount = normalizedDetails.length;

    // ===============================
    // 5) Conditional output (BULK) — OPENJSON
    // ===============================
    let outputTarget = null;
    let outputCount = 0;

    if (hasProduksi) {
      const insertBpoBulkSql = `
        INSERT INTO dbo.BrokerProduksiOutput (NoProduksi, NoBroker, NoSak)
        SELECT
          @NoProduksi,
          @NoBroker,
          j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;

      await new sql.Request(tx)
        .input("NoProduksi", sql.VarChar(50), NoProduksi)
        .input("NoBroker", sql.VarChar(50), header.NoBroker)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(insertBpoBulkSql);

      outputCount = detailCount;
      outputTarget = "BrokerProduksiOutput";
    } else if (hasBongkar) {
      const insertBsoBulkSql = `
        INSERT INTO dbo.BongkarSusunOutputBroker (NoBongkarSusun, NoBroker, NoSak)
        SELECT
          @NoBongkarSusun,
          @NoBroker,
          j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
        .input("NoBroker", sql.VarChar(50), header.NoBroker)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(insertBsoBulkSql);

      outputCount = detailCount;
      outputTarget = "BongkarSusunOutputBroker";
    }

    await tx.commit();

    return {
      header: {
        NoBroker: header.NoBroker,
        IdJenisPlastik: header.IdJenisPlastik,
        IdWarehouse: header.IdWarehouse,
        IdStatus: header.IdStatus ?? 1,
        CreateBy: header.CreateBy,
        DateCreate: formatYMD(effectiveDateCreate),
        DateTimeCreate: nowDateTime,
        Density: header.Density ?? null,
        Moisture: header.Moisture ?? null,
        MaxMeltTemp: header.MaxMeltTemp ?? null,
        MinMeltTemp: header.MinMeltTemp ?? null,
        MFI: header.MFI ?? null,
        VisualNote: header.VisualNote ?? null,
        Density2: header.Density2 ?? null,
        Density3: header.Density3 ?? null,
        Moisture2: header.Moisture2 ?? null,
        Moisture3: header.Moisture3 ?? null,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
      },
      counts: {
        detailsInserted: detailCount,
        outputInserted: outputCount,
      },
      outputTarget,
      audit: { actorId, requestId }, // ✅ id
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.updateBrokerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBroker = payload?.NoBroker?.toString().trim();
  if (!NoBroker) throw badReq("NoBroker (path) wajib diisi");

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : null; // null => tidak sentuh details

  const NoProduksi = payload?.NoProduksi?.toString().trim() || null;
  const NoBongkarSusun = payload?.NoBongkarSusun?.toString().trim() || null;

  const hasProduksi = !!NoProduksi;
  const hasBongkar = !!NoBongkarSusun;
  if (hasProduksi && hasBongkar) {
    throw badReq("NoProduksi dan NoBongkarSusun tidak boleh diisi bersamaan");
  }

  // =====================================================
  // [AUDIT] actorId + requestId (ID only)
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
  // [DETAILS] normalize + validate untuk bulk insert (kalau details dikirim)
  // - IdLokasi boleh "-" / "" / null => dianggap kosong => fallback header.IdLokasi / null
  // - IsPartial default 0
  // =====================================================
  let normalizedDetails = null;
  let detailsJson = null;

  if (details) {
    normalizedDetails = details.map((d) => {
      const noSak = Number(d?.NoSak);
      if (!Number.isFinite(noSak) || noSak <= 0) {
        throw badReq(`NoSak tidak valid: ${d?.NoSak}`);
      }

      const berat = d?.Berat == null ? 0 : Number(d.Berat);
      if (!Number.isFinite(berat) || berat < 0) {
        throw badReq(`Berat tidak valid pada NoSak ${noSak}: ${d?.Berat}`);
      }

      const isPartialRaw = d?.IsPartial;
      const isPartial =
        isPartialRaw === true ||
        isPartialRaw === 1 ||
        String(isPartialRaw).trim() === "1"
          ? 1
          : 0;

      const rawLok = d?.IdLokasi;
      let idLokasi = null;

      if (rawLok === undefined || rawLok === null) {
        idLokasi = header.IdLokasi ?? null;
      } else {
        const s = String(rawLok).trim();
        if (s === "" || s === "-") {
          idLokasi = header.IdLokasi ?? null;
        } else {
          const n = Number(s);
          if (!Number.isFinite(n)) {
            throw badReq(
              `IdLokasi tidak valid pada NoSak ${Math.trunc(noSak)}: ${rawLok}`,
            );
          }
          idLokasi = Math.trunc(n);
        }
      }

      return {
        NoSak: Math.trunc(noSak),
        Berat: berat,
        IsPartial: isPartial,
        IdLokasi: idLokasi === null ? null : Math.trunc(Number(idLokasi)),
      };
    });

    // optional: cegah NoSak duplikat
    const set = new Set();
    for (const x of normalizedDetails) {
      const k = String(x.NoSak);
      if (set.has(k)) throw badReq(`NoSak duplikat di payload: ${x.NoSak}`);
      set.add(k);
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

    // 0) Pastikan header exist + ambil DateCreate existing (LOCK)
    const exist = await new sql.Request(tx).input(
      "NoBroker",
      sql.VarChar(50),
      NoBroker,
    ).query(`
        SELECT TOP 1 NoBroker, DateCreate
        FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker
      `);

    if (exist.recordset.length === 0) {
      const e = new Error(`NoBroker ${NoBroker} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    const existingDateCreate = exist.recordset[0]?.DateCreate;
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (UPDATE)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `update broker ${NoBroker}`,
      useLock: true,
    });

    // Jika client kirim DateCreate baru, cek juga
    let newDateOnly = null;
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null)
        throw badReq("DateCreate tidak boleh null pada UPDATE.");
      newDateOnly = toDateOnly(header.DateCreate);
      if (!newDateOnly) throw badReq("DateCreate tidak valid.");

      await assertNotLocked({
        date: newDateOnly,
        runner: tx,
        action: `update broker ${NoBroker} (change DateCreate)`,
        useLock: true,
      });
    }

    // 1) Update header (partial/dynamic)
    const setParts = [];
    const reqHeader = new sql.Request(tx).input(
      "NoBroker",
      sql.VarChar(50),
      NoBroker,
    );

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf("IdJenisPlastik", "IdJenisPlastik", sql.Int, header.IdJenisPlastik);
    setIf("IdWarehouse", "IdWarehouse", sql.Int, header.IdWarehouse);

    if (header.DateCreate !== undefined) {
      setIf("DateCreate", "DateCreate", sql.Date, newDateOnly);
    }

    setIf("IdStatus", "IdStatus", sql.Int, header.IdStatus);
    setIf("Density", "Density", sql.Decimal(10, 3), header.Density ?? null);
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
      "VisualNote",
      "VisualNote",
      sql.VarChar(sql.MAX),
      header.VisualNote ?? null,
    );
    setIf("Density2", "Density2", sql.Decimal(10, 3), header.Density2 ?? null);
    setIf("Density3", "Density3", sql.Decimal(10, 3), header.Density3 ?? null);
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
    // kalau mau bisa diedit, buka 2 baris ini:
    // setIf('Blok', 'Blok', sql.VarChar(50), header.Blok ?? null);
    // setIf('IdLokasi', 'IdLokasi', sql.Int, header.IdLokasi ?? null);

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.Broker_h
        SET ${setParts.join(", ")}
        WHERE NoBroker = @NoBroker
      `);
    }

    // =====================================================
    // [IMPORTANT FIX] Jika details akan diganti, output yg bergantung HARUS dihapus dulu
    // =====================================================
    if (details) {
      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), NoBroker)
        .query(
          `DELETE FROM dbo.BrokerProduksiOutput WHERE NoBroker = @NoBroker`,
        );

      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), NoBroker)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBroker = @NoBroker`,
        );
    }

    // 2) Replace details (DateUsage IS NULL) — BULK (kalau dikirim)
    let detailAffected = 0;

    if (details) {
      await new sql.Request(tx).input("NoBroker", sql.VarChar(50), NoBroker)
        .query(`
          DELETE FROM dbo.Broker_d
          WHERE NoBroker = @NoBroker AND DateUsage IS NULL
        `);

      const insertDetailsBulkSql = `
        INSERT INTO dbo.Broker_d (NoBroker, NoSak, Berat, DateUsage, IsPartial, IdLokasi)
        SELECT
          @NoBroker,
          j.NoSak,
          j.Berat,
          NULL,
          j.IsPartial,
          j.IdLokasi
        FROM OPENJSON(@DetailsJson)
        WITH (
          NoSak int '$.NoSak',
          Berat decimal(18,3) '$.Berat',
          IsPartial int '$.IsPartial',
          IdLokasi int '$.IdLokasi'
        ) AS j;
      `;

      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), NoBroker)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(insertDetailsBulkSql);

      detailAffected = normalizedDetails.length;
    }

    // 3) Conditional outputs (bulk juga)
    let outputTarget = null;
    let outputCount = 0;

    const sentAnyOutputField =
      Object.prototype.hasOwnProperty.call(payload, "NoProduksi") ||
      Object.prototype.hasOwnProperty.call(payload, "NoBongkarSusun");

    if (sentAnyOutputField) {
      // reset outputs (idempotent)
      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), NoBroker)
        .query(
          `DELETE FROM dbo.BrokerProduksiOutput WHERE NoBroker = @NoBroker`,
        );

      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), NoBroker)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBroker = @NoBroker`,
        );

      // Ambil NoSak sumber:
      let noSakJson = null;

      if (details) {
        noSakJson = JSON.stringify(
          normalizedDetails.map((x) => ({ NoSak: x.NoSak })),
        );
      } else {
        const dets = await new sql.Request(tx).input(
          "NoBroker",
          sql.VarChar(50),
          NoBroker,
        ).query(`
            SELECT NoSak
            FROM dbo.Broker_d
            WHERE NoBroker = @NoBroker AND DateUsage IS NULL
            ORDER BY NoSak
          `);

        noSakJson = JSON.stringify(
          dets.recordset.map((r) => ({ NoSak: r.NoSak })),
        );
      }

      const parsed = JSON.parse(noSakJson);
      const noSakCount = Array.isArray(parsed) ? parsed.length : 0;

      if (hasProduksi) {
        const insertBpoBulkSql = `
          INSERT INTO dbo.BrokerProduksiOutput (NoProduksi, NoBroker, NoSak)
          SELECT
            @NoProduksi,
            @NoBroker,
            j.NoSak
          FROM OPENJSON(@NoSakJson)
          WITH (NoSak int '$.NoSak') AS j;
        `;

        await new sql.Request(tx)
          .input("NoProduksi", sql.VarChar(50), NoProduksi)
          .input("NoBroker", sql.VarChar(50), NoBroker)
          .input("NoSakJson", sql.NVarChar(sql.MAX), noSakJson)
          .query(insertBpoBulkSql);

        outputCount = noSakCount;
        outputTarget = "BrokerProduksiOutput";
      } else if (hasBongkar) {
        const insertBsoBulkSql = `
          INSERT INTO dbo.BongkarSusunOutputBroker (NoBongkarSusun, NoBroker, NoSak)
          SELECT
            @NoBongkarSusun,
            @NoBroker,
            j.NoSak
          FROM OPENJSON(@NoSakJson)
          WITH (NoSak int '$.NoSak') AS j;
        `;

        await new sql.Request(tx)
          .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
          .input("NoBroker", sql.VarChar(50), NoBroker)
          .input("NoSakJson", sql.NVarChar(sql.MAX), noSakJson)
          .query(insertBsoBulkSql);

        outputCount = noSakCount;
        outputTarget = "BongkarSusunOutputBroker";
      }
    }

    await tx.commit();

    return {
      header: {
        NoBroker,
        ...header,
        existingDateCreate: formatYMD(existingDateOnly),
        ...(newDateOnly ? { newDateCreate: formatYMD(newDateOnly) } : {}),
      },
      counts: {
        detailsAffected: detailAffected,
        outputInserted: outputCount,
      },
      outputTarget,
      audit: { actorId, requestId }, // ✅ ID only
      note: details
        ? "Details (yang DateUsage IS NULL) diganti sesuai payload (bulk). Output dependent direset dulu untuk menghindari FK."
        : "Details tidak diubah.",
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

// Delete 1 header + outputs + details (safe)
// Delete 1 Broker header + outputs + details + partials (safe)
exports.deleteBrokerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  // payload bisa string (legacy) atau object
  const NoBroker =
    typeof payload === "string"
      ? String(payload || "").trim()
      : String(payload?.NoBroker || payload?.nobroker || "").trim();

  if (!NoBroker) throw badReq("NoBroker wajib diisi");

  // =====================================================
  // [AUDIT] actorId + requestId (ID only) - sama seperti washing
  // =====================================================
  const actorIdNum =
    typeof payload === "object" ? Number(payload?.actorId) : NaN;
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0
      ? Math.trunc(actorIdNum)
      : null;

  const requestId =
    typeof payload === "object"
      ? String(
          payload?.requestId ||
            `${Date.now()}-${Math.random().toString(16).slice(2)}`,
        )
      : String(`${Date.now()}-${Math.random().toString(16).slice(2)}`);

  // delete sebaiknya wajib actorId (biar audit tidak jatuh ke SUSER_SNAME())
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

    // 0) pastikan header exist + lock + ambil DateCreate existing
    const headRes = await new sql.Request(tx).input(
      "NoBroker",
      sql.VarChar(50),
      NoBroker,
    ).query(`
        SELECT TOP 1 NoBroker, DateCreate
        FROM dbo.Broker_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker
      `);

    if (headRes.recordset.length === 0) {
      const e = new Error(`NoBroker ${NoBroker} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    const existingDateCreate = headRes.recordset[0]?.DateCreate;
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `delete broker ${NoBroker}`,
      useLock: true,
    });

    // ===============================
    // [B] Block if any detail is already used
    // ===============================
    const used = await new sql.Request(tx).input(
      "NoBroker",
      sql.VarChar(50),
      NoBroker,
    ).query(`
        SELECT TOP 1 1
        FROM dbo.Broker_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBroker = @NoBroker AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      throw conflict(
        "Tidak bisa hapus: terdapat detail yang sudah terpakai (DateUsage IS NOT NULL).",
      );
    }

    // ===============================
    // [C] Delete outputs first (avoid FK)
    // ===============================
    const delBpo = await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), NoBroker)
      .query(`DELETE FROM dbo.BrokerProduksiOutput WHERE NoBroker = @NoBroker`);

    const delBso = await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), NoBroker)
      .query(
        `DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBroker = @NoBroker`,
      );

    // ===============================
    // [D] Delete partial-input usages that reference BrokerPartial for this NoBroker
    // (hapus child dulu baru parent BrokerPartial)
    // ===============================
    const delBrokerInputPartial = await new sql.Request(tx).input(
      "NoBroker",
      sql.VarChar(50),
      NoBroker,
    ).query(`
        DELETE bip
        FROM dbo.BrokerProduksiInputBrokerPartial AS bip
        INNER JOIN dbo.BrokerPartial AS bp
          ON bp.NoBrokerPartial = bip.NoBrokerPartial
        WHERE bp.NoBroker = @NoBroker
      `);

    const delMixerInputPartial = await new sql.Request(tx).input(
      "NoBroker",
      sql.VarChar(50),
      NoBroker,
    ).query(`
        DELETE mip
        FROM dbo.MixerProduksiInputBrokerPartial AS mip
        INNER JOIN dbo.BrokerPartial AS bp
          ON bp.NoBrokerPartial = mip.NoBrokerPartial
        WHERE bp.NoBroker = @NoBroker
      `);

    // ===============================
    // [E] Delete partial rows themselves
    // ===============================
    const delPartial = await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), NoBroker)
      .query(`DELETE FROM dbo.BrokerPartial WHERE NoBroker = @NoBroker`);

    // ===============================
    // [F] Delete details (only the ones not used)
    // ===============================
    const delDet = await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), NoBroker)
      .query(
        `DELETE FROM dbo.Broker_d WHERE NoBroker = @NoBroker AND DateUsage IS NULL`,
      );

    // ===============================
    // [G] Delete header
    // ===============================
    const delHead = await new sql.Request(tx)
      .input("NoBroker", sql.VarChar(50), NoBroker)
      .query(`DELETE FROM dbo.Broker_h WHERE NoBroker = @NoBroker`);

    await tx.commit();

    return {
      NoBroker,
      docDateCreate: formatYMD(existingDateOnly),
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: {
          BrokerProduksiOutput: delBpo.rowsAffected?.[0] ?? 0,
          BongkarSusunOutputBroker: delBso.rowsAffected?.[0] ?? 0,
        },
        partials: {
          BrokerPartial: delPartial.rowsAffected?.[0] ?? 0,
          BrokerProduksiInputBrokerPartial:
            delBrokerInputPartial.rowsAffected?.[0] ?? 0,
          MixerProduksiInputBrokerPartial:
            delMixerInputPartial.rowsAffected?.[0] ?? 0,
        },
      },
      audit: { actorId, requestId }, // ✅ ID only
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}

    // mapping FK error jika ada constraint lain di DB
    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || "Gagal hapus karena constraint referensi (FK).";
    }
    throw e;
  }
};

exports.getPartialInfoByBrokerAndSak = async (nobroker, nosak) => {
  const pool = await poolPromise;
  const req = pool
    .request()
    .input("NoBroker", sql.VarChar, nobroker)
    .input("NoSak", sql.Int, nosak);

  const query = `
    SELECT
      p.NoBrokerPartial,
      p.NoBroker,
      p.NoSak,
      p.Berat,                          -- partial weight
      mpi.NoProduksi,                   -- produksi number (if exists)
      mph.TglProduksi,                  -- production date
      mph.IdMesin,                      -- machine id
      mm.NamaMesin,                     -- machine name from MstMesin
      mph.IdOperator,
      mph.Jam,
      mph.Shift
    FROM dbo.BrokerPartial p
    LEFT JOIN dbo.MixerProduksiInputBrokerPartial mpi
      ON mpi.NoBrokerPartial = p.NoBrokerPartial
    LEFT JOIN dbo.MixerProduksi_h mph
      ON mph.NoProduksi = mpi.NoProduksi
    LEFT JOIN dbo.MstMesin mm
      ON mph.IdMesin = mm.IdMesin
    WHERE p.NoBroker = @NoBroker
      AND p.NoSak = @NoSak
    ORDER BY p.NoBrokerPartial ASC
  `;

  const result = await req.query(query);

  // Compute total partial weightd
  const totalPartialWeight = result.recordset.reduce((sum, row) => {
    const w =
      typeof row.Berat === "number" ? row.Berat : Number(row.Berat) || 0;
    return sum + w;
  }, 0);

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? "0" + n : "" + n);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = result.recordset.map((r) => ({
    NoBrokerPartial: r.NoBrokerPartial,
    NoBroker: r.NoBroker,
    NoSak: r.NoSak,
    Berat: r.Berat,
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

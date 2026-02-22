// services/label-washing-service.js
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
const { normalizeDecimalField } = require("../../../core/utils/number-utils");

// GET all header with pagination & search
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT 
      h.NoWashing,
      h.DateCreate,
      h.IdJenisPlastik,
      jp.Jenis AS NamaJenisPlastik,
      h.IdWarehouse,
      w.NamaWarehouse,
      h.Blok,                    -- ✅ ambil langsung dari header
      h.IdLokasi,                -- ✅ ambil langsung dari header
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,
      h.Density,
      h.Density2,
      h.Density3,
      h.Moisture,
      h.Moisture2,
      h.Moisture3,

      -- ambil NoProduksi & NamaMesin
      MAX(wpo.NoProduksi) AS NoProduksi,
      MAX(m.NamaMesin) AS NamaMesin,
      -- ambil NoBongkarSusun
      MAX(bso.NoBongkarSusun) AS NoBongkarSusun
    FROM Washing_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    LEFT JOIN Washing_d d ON h.NoWashing = d.NoWashing
    LEFT JOIN WashingProduksiOutput wpo ON wpo.NoWashing = h.NoWashing
    LEFT JOIN WashingProduksi_h wph ON wph.NoProduksi = wpo.NoProduksi
    LEFT JOIN MstMesin m ON m.IdMesin = wph.IdMesin
    LEFT JOIN BongkarSusunOutputWashing bso ON bso.NoWashing = h.NoWashing
    WHERE 1=1
      ${search ? `AND (h.NoWashing LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ""}
      AND EXISTS (SELECT 1 FROM Washing_d d2 WHERE d2.NoWashing = h.NoWashing AND d2.DateUsage IS NULL)
    GROUP BY 
      h.NoWashing, h.DateCreate, h.IdJenisPlastik, jp.Jenis, 
      h.IdWarehouse, w.NamaWarehouse, h.IdStatus, 
      h.Density, h.Density2, h.Density3, h.Moisture, h.Moisture2, h.Moisture3, h.Blok, h.IdLokasi
    ORDER BY h.NoWashing DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoWashing) as total
    FROM Washing_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    WHERE 1=1
      ${search ? `AND (h.NoWashing LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ""}
      AND EXISTS (SELECT 1 FROM Washing_d d2 WHERE d2.NoWashing = h.NoWashing AND d2.DateUsage IS NULL)
  `;

  request.input("offset", sql.Int, offset).input("limit", sql.Int, limit);
  if (search) request.input("search", sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map((item) => ({
    ...item,
  }));

  const total = countResult.recordset[0].total;

  return { data, total };
};

// GET details by NoWashing
exports.getWashingDetailByNoWashing = async (nowashing) => {
  const pool = await poolPromise;
  const result = await pool.request().input("NoWashing", sql.VarChar, nowashing)
    .query(`
      SELECT *
      FROM Washing_d
      WHERE NoWashing = @NoWashing
      ORDER BY NoSak
    `);

  return result.recordset.map((item) => ({
    ...item,
  }));
};

exports.createWashingCascade = async (payload) => {
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

    return { NoSak: Math.trunc(noSak), Berat: berat };
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
      action: "create washing",
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
    // 1) Generate NoWashing (PAKAI generateNextCode)
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "Washing_h",
        columnName: "NoWashing",
        prefix: "B.",
        width: 10,
      });

    const generatedNo = await gen();

    // 2) Double-check belum dipakai (lock supaya konsisten)
    const exist = await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), generatedNo)
      .query(
        `SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`,
      );

    if (exist.recordset.length > 0) {
      const retryNo = await gen();
      const exist2 = await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), retryNo)
        .query(
          `SELECT 1 FROM Washing_h WITH (UPDLOCK, HOLDLOCK) WHERE NoWashing = @NoWashing`,
        );

      if (exist2.recordset.length > 0) {
        throw conflict("Gagal generate NoWashing unik, coba lagi.");
      }
      header.NoWashing = retryNo;
    } else {
      header.NoWashing = generatedNo;
    }

    // ===============================
    // 3) Insert header
    // ===============================
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO Washing_h (
        NoWashing, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
        Density, Moisture, Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
      )
      VALUES (
        @NoWashing, @IdJenisPlastik, @IdWarehouse,
        @DateCreate,
        @IdStatus, @CreateBy, @DateTimeCreate,
        @Density, @Moisture, @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
      )
    `;

    await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), header.NoWashing)
      .input("IdJenisPlastik", sql.Int, header.IdJenisPlastik)
      .input("IdWarehouse", sql.Int, header.IdWarehouse)
      .input("DateCreate", sql.Date, effectiveDateCreate)
      .input("IdStatus", sql.Int, header.IdStatus ?? 1)
      .input("CreateBy", sql.VarChar(50), header.CreateBy) // overwritten by controller
      .input("DateTimeCreate", sql.DateTime, nowDateTime)
      .input("Density", sql.Decimal(10, 3), header.Density ?? null)
      .input("Moisture", sql.Decimal(10, 3), header.Moisture ?? null)
      .input("Density2", sql.Decimal(10, 3), header.Density2 ?? null)
      .input("Density3", sql.Decimal(10, 3), header.Density3 ?? null)
      .input("Moisture2", sql.Decimal(10, 3), header.Moisture2 ?? null)
      .input("Moisture3", sql.Decimal(10, 3), header.Moisture3 ?? null)
      .input("Blok", sql.VarChar(50), header.Blok ?? null)
      .input("IdLokasi", sql.Int, header.IdLokasi ?? null)
      .query(insertHeaderSql);

    // ===============================
    // 4) Insert details (BULK)
    // ===============================
    const insertDetailsBulkSql = `
      INSERT INTO Washing_d (NoWashing, NoSak, Berat, DateUsage)
      SELECT
        @NoWashing,
        j.NoSak,
        j.Berat,
        NULL
      FROM OPENJSON(@DetailsJson)
      WITH (
        NoSak int '$.NoSak',
        Berat decimal(18,3) '$.Berat'
      ) AS j;
    `;

    await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), header.NoWashing)
      .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
      .query(insertDetailsBulkSql);

    const detailCount = normalizedDetails.length;

    // ===============================
    // 5) Conditional output (BULK)
    // ===============================
    let outputTarget = null;
    let outputCount = 0;

    if (hasProduksi) {
      const insertWpoBulkSql = `
        INSERT INTO WashingProduksiOutput (NoProduksi, NoWashing, NoSak)
        SELECT
          @NoProduksi,
          @NoWashing,
          j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;

      await new sql.Request(tx)
        .input("NoProduksi", sql.VarChar(50), NoProduksi)
        .input("NoWashing", sql.VarChar(50), header.NoWashing)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(insertWpoBulkSql);

      outputCount = detailCount;
      outputTarget = "WashingProduksiOutput";
    } else if (hasBongkar) {
      const insertBsoBulkSql = `
        INSERT INTO BongkarSusunOutputWashing (NoBongkarSusun, NoWashing, NoSak)
        SELECT
          @NoBongkarSusun,
          @NoWashing,
          j.NoSak
        FROM OPENJSON(@DetailsJson)
        WITH (NoSak int '$.NoSak') AS j;
      `;

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
        .input("NoWashing", sql.VarChar(50), header.NoWashing)
        .input("DetailsJson", sql.NVarChar(sql.MAX), detailsJson)
        .query(insertBsoBulkSql);

      outputCount = detailCount;
      outputTarget = "BongkarSusunOutputWashing";
    }

    await tx.commit();

    return {
      header: {
        NoWashing: header.NoWashing,
        IdJenisPlastik: header.IdJenisPlastik,
        IdWarehouse: header.IdWarehouse,
        IdStatus: header.IdStatus ?? 1,
        CreateBy: header.CreateBy,
        DateCreate: formatYMD(effectiveDateCreate),
        DateTimeCreate: nowDateTime,
        Density: header.Density ?? null,
        Moisture: header.Moisture ?? null,
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
      audit: { actorId, requestId }, // ✅ sekarang id
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.updateWashingCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoWashing = payload?.NoWashing?.toString().trim();
  if (!NoWashing) throw badReq("NoWashing (path) wajib diisi");

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

      if (idLokasi !== null && !Number.isFinite(Number(idLokasi))) {
        throw badReq(
          `IdLokasi tidak valid pada NoSak ${Math.trunc(noSak)}: ${rawLok}`,
        );
      }

      return {
        NoSak: Math.trunc(noSak),
        Berat: berat,
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
      "NoWashing",
      sql.VarChar(50),
      NoWashing,
    ).query(`
        SELECT TOP 1 NoWashing, DateCreate
        FROM dbo.Washing_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing
      `);

    if (exist.recordset.length === 0) {
      const e = new Error(`NoWashing ${NoWashing} tidak ditemukan`);
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
      action: `update washing ${NoWashing}`,
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
        action: `update washing ${NoWashing} (change DateCreate)`,
        useLock: true,
      });
    }

    // 1) Update header (partial/dynamic)
    const setParts = [];
    const reqHeader = new sql.Request(tx).input(
      "NoWashing",
      sql.VarChar(50),
      NoWashing,
    );

    const { createSetIf } = require("../../../core/utils/update-diff-helper");
    const setIf = createSetIf(reqHeader, setParts);

    // use shared normalizeDecimalField from utils

    setIf("IdJenisPlastik", "IdJenisPlastik", sql.Int, header.IdJenisPlastik);
    setIf("IdWarehouse", "IdWarehouse", sql.Int, header.IdWarehouse);

    if (header.DateCreate !== undefined) {
      setIf("DateCreate", "DateCreate", sql.Date, newDateOnly);
    }

    setIf("IdStatus", "IdStatus", sql.Int, header.IdStatus);
    setIf(
      "Density",
      "Density",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Density, "Density"),
    );
    setIf(
      "Moisture",
      "Moisture",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Moisture, "Moisture"),
    );
    setIf(
      "Density2",
      "Density2",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Density2, "Density2"),
    );
    setIf(
      "Density3",
      "Density3",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Density3, "Density3"),
    );
    setIf(
      "Moisture2",
      "Moisture2",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Moisture2, "Moisture2"),
    );
    setIf(
      "Moisture3",
      "Moisture3",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Moisture3, "Moisture3"),
    );
    // setIf('Blok', 'Blok', sql.VarChar(50), header.Blok ?? null);
    // setIf('IdLokasi', 'IdLokasi', sql.Int, header.IdLokasi ?? null);

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.Washing_h
        SET ${setParts.join(", ")}
        WHERE NoWashing = @NoWashing
      `);
    }

    // =====================================================
    // [IMPORTANT FIX] Jika details akan diganti, output yang bergantung HARUS dihapus dulu
    // =====================================================
    if (details) {
      await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), NoWashing)
        .query(
          `DELETE FROM dbo.WashingProduksiOutput WHERE NoWashing = @NoWashing`,
        );

      await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), NoWashing)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`,
        );
    }

    // 2) Replace details (DateUsage IS NULL) — BULK (kalau dikirim)
    let detailAffected = 0;

    if (details) {
      await new sql.Request(tx).input("NoWashing", sql.VarChar(50), NoWashing)
        .query(`
          DELETE FROM dbo.Washing_d
          WHERE NoWashing = @NoWashing AND DateUsage IS NULL
        `);

      const insertDetailsBulkSql = `
        INSERT INTO dbo.Washing_d (NoWashing, NoSak, Berat, DateUsage, IdLokasi)
        SELECT
          @NoWashing,
          j.NoSak,
          j.Berat,
          NULL,
          j.IdLokasi
        FROM OPENJSON(@DetailsJson)
        WITH (
          NoSak int '$.NoSak',
          Berat decimal(18,3) '$.Berat',
          IdLokasi int '$.IdLokasi'
        ) AS j;
      `;

      await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), NoWashing)
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
        .input("NoWashing", sql.VarChar(50), NoWashing)
        .query(
          `DELETE FROM dbo.WashingProduksiOutput WHERE NoWashing = @NoWashing`,
        );

      await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), NoWashing)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`,
        );

      // Ambil NoSak sumber:
      let noSakJson = null;

      if (details) {
        noSakJson = JSON.stringify(
          normalizedDetails.map((x) => ({ NoSak: x.NoSak })),
        );
      } else {
        const dets = await new sql.Request(tx).input(
          "NoWashing",
          sql.VarChar(50),
          NoWashing,
        ).query(`
            SELECT NoSak
            FROM dbo.Washing_d
            WHERE NoWashing = @NoWashing AND DateUsage IS NULL
            ORDER BY NoSak
          `);

        noSakJson = JSON.stringify(
          dets.recordset.map((r) => ({ NoSak: r.NoSak })),
        );
      }

      const parsed = JSON.parse(noSakJson);
      const noSakCount = Array.isArray(parsed) ? parsed.length : 0;

      if (hasProduksi) {
        const insertWpoBulkSql = `
          INSERT INTO dbo.WashingProduksiOutput (NoProduksi, NoWashing, NoSak)
          SELECT
            @NoProduksi,
            @NoWashing,
            j.NoSak
          FROM OPENJSON(@NoSakJson)
          WITH (NoSak int '$.NoSak') AS j;
        `;

        await new sql.Request(tx)
          .input("NoProduksi", sql.VarChar(50), NoProduksi)
          .input("NoWashing", sql.VarChar(50), NoWashing)
          .input("NoSakJson", sql.NVarChar(sql.MAX), noSakJson)
          .query(insertWpoBulkSql);

        outputCount = noSakCount;
        outputTarget = "WashingProduksiOutput";
      } else if (hasBongkar) {
        const insertBsoBulkSql = `
          INSERT INTO dbo.BongkarSusunOutputWashing (NoBongkarSusun, NoWashing, NoSak)
          SELECT
            @NoBongkarSusun,
            @NoWashing,
            j.NoSak
          FROM OPENJSON(@NoSakJson)
          WITH (NoSak int '$.NoSak') AS j;
        `;

        await new sql.Request(tx)
          .input("NoBongkarSusun", sql.VarChar(50), NoBongkarSusun)
          .input("NoWashing", sql.VarChar(50), NoWashing)
          .input("NoSakJson", sql.NVarChar(sql.MAX), noSakJson)
          .query(insertBsoBulkSql);

        outputCount = noSakCount;
        outputTarget = "BongkarSusunOutputWashing";
      }
    }

    await tx.commit();

    return {
      header: {
        NoWashing,
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

exports.deleteWashingCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  // payload bisa string (legacy) atau object
  const NoWashing =
    typeof payload === "string"
      ? String(payload || "").trim()
      : String(payload?.NoWashing || payload?.nowashing || "").trim();

  if (!NoWashing) throw badReq("NoWashing wajib diisi");

  // =====================================================
  // [AUDIT] actorId + requestId (ID only)
  // =====================================================
  // ✅ rekomendasi: wajib object supaya audit jelas
  const actorIdNum =
    typeof payload === "object" ? Number(payload?.actorId) : NaN;
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;

  const requestId =
    typeof payload === "object"
      ? String(
          payload?.requestId ||
            `${Date.now()}-${Math.random().toString(16).slice(2)}`,
        )
      : String(`${Date.now()}-${Math.random().toString(16).slice(2)}`);

  // 🔒 delete sebaiknya wajib actorId (kalau tidak, audit bisa jatuh ke SUSER_SNAME())
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

    // 0) pastikan exist + lock + ambil DateCreate existing
    const headRes = await new sql.Request(tx).input(
      "NoWashing",
      sql.VarChar(50),
      NoWashing,
    ).query(`
        SELECT TOP 1 NoWashing, DateCreate
        FROM dbo.Washing_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing
      `);

    if (headRes.recordset.length === 0) {
      const e = new Error(`NoWashing ${NoWashing} tidak ditemukan`);
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
      action: `delete washing ${NoWashing}`,
      useLock: true,
    });

    // 1) cek apakah ada detail terpakai
    const used = await new sql.Request(tx).input(
      "NoWashing",
      sql.VarChar(50),
      NoWashing,
    ).query(`
        SELECT TOP 1 1
        FROM dbo.Washing_d WITH (UPDLOCK, HOLDLOCK)
        WHERE NoWashing = @NoWashing AND DateUsage IS NOT NULL
      `);

    if (used.recordset.length > 0) {
      throw conflict(
        "Tidak bisa hapus: terdapat detail yang sudah terpakai (DateUsage IS NOT NULL).",
      );
    }

    // 2) hapus output dulu (hindari FK)
    const delWpo = await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), NoWashing)
      .query(
        `DELETE FROM dbo.WashingProduksiOutput WHERE NoWashing = @NoWashing`,
      );

    const delBso = await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), NoWashing)
      .query(
        `DELETE FROM dbo.BongkarSusunOutputWashing WHERE NoWashing = @NoWashing`,
      );

    // 3) hapus semua details (yg belum terpakai)
    const delDet = await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), NoWashing)
      .query(
        `DELETE FROM dbo.Washing_d WHERE NoWashing = @NoWashing AND DateUsage IS NULL`,
      );

    // 4) hapus header
    const delHead = await new sql.Request(tx)
      .input("NoWashing", sql.VarChar(50), NoWashing)
      .query(`DELETE FROM dbo.Washing_h WHERE NoWashing = @NoWashing`);

    await tx.commit();

    return {
      NoWashing,
      docDateCreate: formatYMD(existingDateOnly),
      deleted: {
        header: delHead.rowsAffected?.[0] ?? 0,
        details: delDet.rowsAffected?.[0] ?? 0,
        outputs: {
          WashingProduksiOutput: delWpo.rowsAffected?.[0] ?? 0,
          BongkarSusunOutputWashing: delBso.rowsAffected?.[0] ?? 0,
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

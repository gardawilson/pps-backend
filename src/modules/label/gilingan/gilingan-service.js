// services/labels/gilingan-service.js
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

exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
      SELECT
        g.NoGilingan,
        g.DateCreate,
        g.IdGilingan,
        mg.NamaGilingan,

        -- 🔹 Berat sudah dikurangi partial (jika IsPartial = 1)
        CASE 
          WHEN g.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(g.Berat, 0) - ISNULL(MAX(gp.TotalPartial), 0) < 0 
                THEN 0
              ELSE ISNULL(g.Berat, 0) - ISNULL(MAX(gp.TotalPartial), 0)
            END
          ELSE ISNULL(g.Berat, 0)
        END AS Berat,

        g.IsPartial,
        g.IdStatus,
        CASE 
          WHEN g.IdStatus = 1 THEN 'PASS'
          WHEN g.IdStatus = 0 THEN 'HOLD'
          ELSE ''
        END AS StatusText,
        MAX(ISNULL(CAST(g.HasBeenPrinted AS int), 0)) AS HasBeenPrinted,
        g.Blok,
        g.IdLokasi,

        -- 🔗 Production chain: GilinganProduksiOutput → GilinganProduksi_h → MstMesin
        MAX(gpo.NoProduksi)     AS GilinganNoProduksi,
        MAX(m.NamaMesin)        AS GilinganNamaMesin,

        -- 🔗 Bongkar Susun mapping
        MAX(bs.NoBongkarSusun)  AS NoBongkarSusun

      FROM [dbo].[Gilingan] g
      LEFT JOIN [dbo].[MstGilingan] mg
             ON mg.IdGilingan = g.IdGilingan

      -- 🔹 Aggregate partial per NoGilingan
      LEFT JOIN (
        SELECT
          NoGilingan,
          SUM(ISNULL(Berat, 0)) AS TotalPartial
        FROM [dbo].[GilinganPartial]
        GROUP BY NoGilingan
      ) gp
        ON gp.NoGilingan = g.NoGilingan

      -- Gilingan production chain
      LEFT JOIN [dbo].[GilinganProduksiOutput] gpo
             ON gpo.NoGilingan = g.NoGilingan
      LEFT JOIN [dbo].[GilinganProduksi_h] gh
             ON gh.NoProduksi = gpo.NoProduksi
      LEFT JOIN [dbo].[MstMesin] m
             ON m.IdMesin = gh.IdMesin

      -- Bongkar Susun mapping
      LEFT JOIN [dbo].[BongkarSusunOutputGilingan] bs
             ON bs.NoGilingan = g.NoGilingan

      WHERE 1=1
        AND g.DateUsage IS NULL
        ${
          search
            ? `AND (
                 g.NoGilingan LIKE @search
                 OR g.Blok LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdLokasi) LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdGilingan) LIKE @search
                 OR ISNULL(mg.NamaGilingan,'') LIKE @search
                 OR ISNULL(gpo.NoProduksi,'') LIKE @search
                 OR ISNULL(m.NamaMesin,'') LIKE @search
                 OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
               )`
            : ""
        }
      GROUP BY
        g.NoGilingan,
        g.DateCreate,
        g.IdGilingan,
        mg.NamaGilingan,
        g.Berat,
        g.IsPartial,
        g.IdStatus,
        g.Blok,
        g.IdLokasi
      ORDER BY g.NoGilingan DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
    `;

  const countQuery = `
      SELECT COUNT(DISTINCT g.NoGilingan) AS total
      FROM [dbo].[Gilingan] g
      LEFT JOIN [dbo].[MstGilingan] mg
             ON mg.IdGilingan = g.IdGilingan
      LEFT JOIN [dbo].[GilinganProduksiOutput] gpo
             ON gpo.NoGilingan = g.NoGilingan
      LEFT JOIN [dbo].[GilinganProduksi_h] gh
             ON gh.NoProduksi = gpo.NoProduksi
      LEFT JOIN [dbo].[MstMesin] m
             ON m.IdMesin = gh.IdMesin
      LEFT JOIN [dbo].[BongkarSusunOutputGilingan] bs
             ON bs.NoGilingan = g.NoGilingan
      WHERE 1=1
        AND g.DateUsage IS NULL
        ${
          search
            ? `AND (
                 g.NoGilingan LIKE @search
                 OR g.Blok LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdLokasi) LIKE @search
                 OR CONVERT(VARCHAR(20), g.IdGilingan) LIKE @search
                 OR ISNULL(mg.NamaGilingan,'') LIKE @search
                 OR ISNULL(gpo.NoProduksi,'') LIKE @search
                 OR ISNULL(m.NamaMesin,'') LIKE @search
                 OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
               )`
            : ""
        }
    `;

  request.input("offset", sql.Int, offset);
  request.input("limit", sql.Int, limit);
  if (search) {
    request.input("search", sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};

exports.createGilinganCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = (payload?.outputCode || "").toString().trim(); // '', 'W.****', 'BG.****'

  // ---- validation dasar
  const badReq = (msg) => {
    const e = new Error(msg);
    e.statusCode = 400;
    return e;
  };
  const conflict = (msg) => {
    const e = new Error(msg);
    e.statusCode = 409;
    return e;
  };

  if (!header.IdGilingan) throw badReq("IdGilingan wajib diisi");
  if (!header.CreateBy) throw badReq("CreateBy wajib diisi"); // controller overwrite dari token

  // Identify target from outputCode (optional, sama seperti crusher: boleh kosong)
  const hasOutput = outputCode.length > 0;
  let outputType = null; // 'PRODUKSI' | 'BONGKAR'
  if (hasOutput) {
    if (outputCode.startsWith("W.")) outputType = "PRODUKSI";
    else if (outputCode.startsWith("BG.")) outputType = "BONGKAR";
    else throw badReq("outputCode prefix tidak dikenali (pakai W. atau BG.)");
  }

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
      action: "create gilingan",
      useLock: true,
    });

    // ===============================
    // 0) Auto-isi Blok & IdLokasi dari kode (produksi / bongkar) kalau header belum isi
    // ===============================
    const needBlok = header.Blok == null || String(header.Blok).trim() === "";
    const needLokasi = header.IdLokasi == null;

    if (needBlok || needLokasi) {
      const kodeRef = hasOutput ? outputCode : null;

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
    // 1) Generate NoGilingan (PAKAI generateNextCode seperti crusher)
    // ===============================
    const gen = async () =>
      generateNextCode(tx, {
        tableName: "Gilingan",
        columnName: "NoGilingan",
        prefix: "V.",
        width: 10,
      });

    const generatedNo = await gen();

    // 2) Double-check belum dipakai (lock supaya konsisten)
    const exist = await new sql.Request(tx)
      .input("NoGilingan", sql.VarChar(50), generatedNo)
      .query(
        `SELECT 1 FROM dbo.Gilingan WITH (UPDLOCK, HOLDLOCK) WHERE NoGilingan = @NoGilingan`,
      );

    if (exist.recordset.length > 0) {
      const retryNo = await gen();
      const exist2 = await new sql.Request(tx)
        .input("NoGilingan", sql.VarChar(50), retryNo)
        .query(
          `SELECT 1 FROM dbo.Gilingan WITH (UPDLOCK, HOLDLOCK) WHERE NoGilingan = @NoGilingan`,
        );

      if (exist2.recordset.length > 0) {
        throw conflict("Gagal generate NoGilingan unik, coba lagi.");
      }
      header.NoGilingan = retryNo;
    } else {
      header.NoGilingan = generatedNo;
    }

    // ===============================
    // 3) Insert header (samakan pattern: pakai @DateTimeCreate dari app, bukan GETDATE())
    // ===============================
    const nowDateTime = new Date();

    const insertHeaderSql = `
      INSERT INTO dbo.Gilingan (
        NoGilingan, DateCreate, IdGilingan, DateUsage,
        Berat, IsPartial, IdStatus, Blok, IdLokasi,
        CreateBy, DateTimeCreate
      )
      VALUES (
        @NoGilingan, @DateCreate, @IdGilingan, NULL,
        @Berat, @IsPartial, @IdStatus, @Blok, @IdLokasi,
        @CreateBy, @DateTimeCreate
      );
    `;

    await new sql.Request(tx)
      .input("NoGilingan", sql.VarChar(50), header.NoGilingan)
      .input("DateCreate", sql.Date, effectiveDateCreate)
      .input("IdGilingan", sql.Int, header.IdGilingan)
      .input("Berat", sql.Decimal(18, 3), header.Berat ?? null)
      .input("IsPartial", sql.Bit, header.IsPartial ?? 0)
      .input("IdStatus", sql.Int, header.IdStatus ?? 1)
      .input("Blok", sql.VarChar(50), header.Blok ?? null)
      .input("IdLokasi", sql.Int, header.IdLokasi ?? null)
      .input("CreateBy", sql.VarChar(50), header.CreateBy) // overwritten by controller
      .input("DateTimeCreate", sql.DateTime, nowDateTime)
      .query(insertHeaderSql);

    // ===============================
    // 4) Optional mapping table based on outputCode prefix
    //    (samakan crusher: mapping dibuat setelah header insert)
    // ===============================
    let mappingTable = null;

    if (outputType === "PRODUKSI") {
      await new sql.Request(tx)
        .input("NoProduksi", sql.VarChar(50), outputCode)
        .input("NoGilingan", sql.VarChar(50), header.NoGilingan)
        .input("Berat", sql.Decimal(18, 3), header.Berat ?? null).query(`
          INSERT INTO dbo.GilinganProduksiOutput (NoProduksi, NoGilingan, Berat)
          VALUES (@NoProduksi, @NoGilingan, @Berat);
        `);

      mappingTable = "GilinganProduksiOutput";
    } else if (outputType === "BONGKAR") {
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), outputCode)
        .input("NoGilingan", sql.VarChar(50), header.NoGilingan).query(`
          INSERT INTO dbo.BongkarSusunOutputGilingan (NoBongkarSusun, NoGilingan)
          VALUES (@NoBongkarSusun, @NoGilingan);
        `);

      mappingTable = "BongkarSusunOutputGilingan";
    }

    await tx.commit();

    return {
      header: {
        NoGilingan: header.NoGilingan,
        DateCreate: formatYMD(effectiveDateCreate),
        IdGilingan: header.IdGilingan,
        Berat: header.Berat ?? null,
        IsPartial: header.IsPartial ?? 0,
        IdStatus: header.IdStatus ?? 1,
        Blok: header.Blok ?? null,
        IdLokasi: header.IdLokasi ?? null,
        CreateBy: header.CreateBy,
        DateTimeCreate: nowDateTime,
      },
      output: {
        code: outputCode || null,
        type: outputType,
        mappingTable,
      },
      audit: { actorId, requestId }, // ✅ sama seperti crusher/broker
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.updateGilinganCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoGilingan = payload?.NoGilingan?.toString().trim();
  if (!NoGilingan) throw badReq("NoGilingan (path) wajib diisi");

  const header = payload?.header || {};
  const outputCode = (payload?.outputCode || "").toString().trim(); // '' | 'W.****' | 'BG.****'

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

  // Identify outputType from outputCode (optional)
  const hasOutput = outputCode.length > 0;
  let outputType = null; // 'PRODUKSI' | 'BONGKAR' | null
  if (hasOutput) {
    if (outputCode.startsWith("W.")) outputType = "PRODUKSI";
    else if (outputCode.startsWith("BG.")) outputType = "BONGKAR";
    else throw badReq("outputCode prefix tidak dikenali (pakai W. atau BG.)");
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
      "NoGilingan",
      sql.VarChar(50),
      NoGilingan,
    ).query(`
        SELECT TOP 1 NoGilingan, DateCreate, DateUsage
        FROM dbo.Gilingan WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    if (exist.recordset.length === 0) {
      // ✅ FIX: jangan pakai \` dan \${} (itu invalid di JS)
      throw notFound(`NoGilingan ${NoGilingan} tidak ditemukan`);
    }

    const existingDateCreate = exist.recordset[0]?.DateCreate;
    const existingDateOnly = toDateOnly(existingDateCreate);

    // ===============================
    // [A] TUTUP TRANSAKSI CHECK (UPDATE)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `update gilingan ${NoGilingan}`,
      useLock: true,
    });

    // Jika client kirim DateCreate baru, cek juga
    let newDateCreateOnly = null;
    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null)
        throw badReq("DateCreate tidak boleh null pada UPDATE.");
      newDateCreateOnly = toDateOnly(header.DateCreate);
      if (!newDateCreateOnly) throw badReq("DateCreate tidak valid.");

      await assertNotLocked({
        date: newDateCreateOnly,
        runner: tx,
        action: `update gilingan ${NoGilingan} (change DateCreate)`,
        useLock: true,
      });
    }

    // Jika client kirim DateUsage, cek juga (null => allow clear)
    let newDateUsageOnly = null;
    if (header.DateUsage !== undefined) {
      if (header.DateUsage === null) {
        newDateUsageOnly = null;
      } else {
        newDateUsageOnly = toDateOnly(header.DateUsage);
        if (!newDateUsageOnly) throw badReq("DateUsage tidak valid.");

        await assertNotLocked({
          date: newDateUsageOnly,
          runner: tx,
          action: `update gilingan ${NoGilingan} (change DateUsage)`,
          useLock: true,
        });
      }
    }

    // ===============================
    // 1) Update header (partial/dynamic)
    // ===============================
    const setParts = [];
    const reqHeader = new sql.Request(tx).input(
      "NoGilingan",
      sql.VarChar(50),
      NoGilingan,
    );

    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        reqHeader.input(param, type, val);
      }
    };

    setIf("IdGilingan", "IdGilingan", sql.Int, header.IdGilingan);

    if (header.DateCreate !== undefined) {
      setIf("DateCreate", "DateCreate", sql.Date, newDateCreateOnly);
    }
    if (header.DateUsage !== undefined) {
      setIf("DateUsage", "DateUsage", sql.Date, newDateUsageOnly);
    }

    if (Object.prototype.hasOwnProperty.call(header, "Berat")) {
      const num = header.Berat === null ? null : Number(header.Berat);
      if (num !== null && (!Number.isFinite(num) || num < 0))
        throw badReq("Berat tidak valid.");
      setIf("Berat", "Berat", sql.Decimal(18, 3), num);
    }

    if (Object.prototype.hasOwnProperty.call(header, "IsPartial")) {
      const v = header.IsPartial === null ? null : header.IsPartial ? 1 : 0;
      setIf("IsPartial", "IsPartial", sql.Bit, v);
    }

    setIf("IdStatus", "IdStatus", sql.Int, header.IdStatus);
    setIf("Blok", "Blok", sql.VarChar(50), header.Blok);

    if (header.IdLokasi !== undefined) {
      if (header.IdLokasi === null || String(header.IdLokasi).trim() === "") {
        setIf("IdLokasi", "IdLokasi", sql.Int, null);
      } else {
        const n = Number(String(header.IdLokasi).trim());
        if (!Number.isFinite(n)) throw badReq("IdLokasi harus angka.");
        setIf("IdLokasi", "IdLokasi", sql.Int, n);
      }
    }

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.Gilingan
        SET ${setParts.join(", ")}
        WHERE NoGilingan = @NoGilingan
      `);
    }

    // ===============================
    // 2) Optional: Output mapping (idempotent)
    // ===============================
    const sentOutputField = Object.prototype.hasOwnProperty.call(
      payload,
      "outputCode",
    );

    let mappingTable = null;
    if (sentOutputField) {
      await new sql.Request(tx)
        .input("NoGilingan", sql.VarChar(50), NoGilingan)
        .query(
          `DELETE FROM dbo.GilinganProduksiOutput WHERE NoGilingan = @NoGilingan`,
        );

      await new sql.Request(tx)
        .input("NoGilingan", sql.VarChar(50), NoGilingan)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputGilingan WHERE NoGilingan = @NoGilingan`,
        );

      if (hasOutput) {
        if (outputType === "PRODUKSI") {
          await new sql.Request(tx)
            .input("NoProduksi", sql.VarChar(50), outputCode)
            .input("NoGilingan", sql.VarChar(50), NoGilingan)
            .input("Berat", sql.Decimal(18, 3), header.Berat ?? null).query(`
              INSERT INTO dbo.GilinganProduksiOutput (NoProduksi, NoGilingan, Berat)
              VALUES (@NoProduksi, @NoGilingan, @Berat);
            `);
          mappingTable = "GilinganProduksiOutput";
        } else if (outputType === "BONGKAR") {
          await new sql.Request(tx)
            .input("NoBongkarSusun", sql.VarChar(50), outputCode)
            .input("NoGilingan", sql.VarChar(50), NoGilingan).query(`
              INSERT INTO dbo.BongkarSusunOutputGilingan (NoBongkarSusun, NoGilingan)
              VALUES (@NoBongkarSusun, @NoGilingan);
            `);
          mappingTable = "BongkarSusunOutputGilingan";
        }
      }
    }

    await tx.commit();

    return {
      header: {
        NoGilingan,
        ...header,
        existingDateCreate: existingDateOnly
          ? formatYMD(existingDateOnly)
          : null,
        ...(newDateCreateOnly
          ? { newDateCreate: formatYMD(newDateCreateOnly) }
          : {}),
        ...(header.DateUsage !== undefined
          ? {
              newDateUsage: newDateUsageOnly
                ? formatYMD(newDateUsageOnly)
                : null,
            }
          : {}),
      },
      output: sentOutputField
        ? { code: outputCode || null, type: outputType, mappingTable }
        : undefined,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.deleteGilinganCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoGilingan = payload?.NoGilingan?.toString().trim();
  if (!NoGilingan) throw badReq("NoGilingan (path) wajib diisi");

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
    // 0) Lock + ambil DateCreate existing
    // ===============================
    const head = await new sql.Request(tx).input(
      "NoGilingan",
      sql.VarChar(50),
      NoGilingan,
    ).query(`
        SELECT TOP 1 NoGilingan, DateCreate
        FROM dbo.Gilingan WITH (UPDLOCK, HOLDLOCK)
        WHERE NoGilingan = @NoGilingan
      `);

    if (head.recordset.length === 0) {
      throw notFound(`NoGilingan ${NoGilingan} tidak ditemukan`);
    }

    const existingDateOnly = toDateOnly(head.recordset[0]?.DateCreate);

    // ===============================
    // 1) TUTUP TRANSAKSI CHECK (DELETE)
    // ===============================
    await assertNotLocked({
      date: existingDateOnly,
      runner: tx,
      action: `delete gilingan ${NoGilingan}`,
      useLock: true,
    });

    // ===============================
    // 2) delete PARTIALS first
    // ===============================
    await new sql.Request(tx).input("NoGilingan", sql.VarChar(50), NoGilingan)
      .query(`
        DELETE FROM dbo.GilinganPartial
        WHERE NoGilingan = @NoGilingan;
      `);

    // ===============================
    // 3) delete OUTPUT mappings (idempotent)
    // ===============================
    await new sql.Request(tx)
      .input("NoGilingan", sql.VarChar(50), NoGilingan)
      .query(
        `DELETE FROM dbo.BongkarSusunOutputGilingan WHERE NoGilingan = @NoGilingan`,
      );

    await new sql.Request(tx)
      .input("NoGilingan", sql.VarChar(50), NoGilingan)
      .query(
        `DELETE FROM dbo.GilinganProduksiOutput WHERE NoGilingan = @NoGilingan`,
      );

    // ===============================
    // 4) delete header
    // ===============================
    const result = await new sql.Request(tx).input(
      "NoGilingan",
      sql.VarChar(50),
      NoGilingan,
    ).query(`
        DELETE FROM dbo.Gilingan
        WHERE NoGilingan = @NoGilingan;
      `);

    if ((result.rowsAffected?.[0] ?? 0) === 0) {
      // harusnya tidak kejadian karena sudah lock+cek di atas, tapi aman
      throw notFound(`NoGilingan ${NoGilingan} tidak ditemukan`);
    }

    await tx.commit();

    return {
      deleted: true,
      NoGilingan,
      existingDateCreate: existingDateOnly ? formatYMD(existingDateOnly) : null,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.getPartialInfoByGilingan = async (nogilingan) => {
  const pool = await poolPromise;

  const req = pool.request().input("NoGilingan", sql.VarChar, nogilingan);

  const query = `
      ;WITH BasePartial AS (
        SELECT
          gp.NoGilinganPartial,
          gp.NoGilingan,
          gp.Berat
        FROM dbo.GilinganPartial gp
        WHERE gp.NoGilingan = @NoGilingan
      ),
      Consumed AS (
        SELECT
          b.NoGilinganPartial,
          'BROKER' AS SourceType,
          b.NoProduksi
        FROM dbo.BrokerProduksiInputGilinganPartial b
  
        UNION ALL
  
        SELECT
          i.NoGilinganPartial,
          'INJECT' AS SourceType,
          i.NoProduksi
        FROM dbo.InjectProduksiInputGilinganPartial i
  
        UNION ALL
  
        SELECT
          m.NoGilinganPartial,
          'MIXER' AS SourceType,
          m.NoProduksi
        FROM dbo.MixerProduksiInputGilinganPartial m
  
        UNION ALL
  
        SELECT
          w.NoGilinganPartial,
          'WASHING' AS SourceType,
          w.NoProduksi
        FROM dbo.WashingProduksiInputGilinganPartial w
      )
      SELECT
        bp.NoGilinganPartial,
        bp.NoGilingan,
        bp.Berat,                 -- partial weight
  
        c.SourceType,             -- BROKER / INJECT / MIXER / WASHING / NULL
        c.NoProduksi,
  
        -- unified production fields from the 4 header tables
        COALESCE(bph.TglProduksi, iph.TglProduksi, mph.TglProduksi, wph.TglProduksi) AS TglProduksi,
        COALESCE(bph.IdMesin,     iph.IdMesin,     mph.IdMesin,     wph.IdMesin)     AS IdMesin,
        COALESCE(bph.IdOperator,  iph.IdOperator,  mph.IdOperator,  wph.IdOperator)  AS IdOperator,
        COALESCE(bph.Jam,         iph.Jam,         mph.Jam,         wph.JamKerja)    AS Jam,
        COALESCE(bph.Shift,       iph.Shift,       mph.Shift,       wph.Shift)       AS Shift,
  
        mm.NamaMesin
      FROM BasePartial bp
      LEFT JOIN Consumed c
        ON c.NoGilinganPartial = bp.NoGilinganPartial
  
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
  
      LEFT JOIN dbo.WashingProduksi_h wph
        ON c.SourceType = 'WASHING'
       AND wph.NoProduksi = c.NoProduksi
  
      -- Machine name
      LEFT JOIN dbo.MstMesin mm
        ON mm.IdMesin = COALESCE(bph.IdMesin, iph.IdMesin, mph.IdMesin, wph.IdMesin)
  
      ORDER BY
        bp.NoGilinganPartial ASC,
        c.SourceType ASC,
        c.NoProduksi ASC;
    `;

  const result = await req.query(query);

  // total partial weight (unique per NoGilinganPartial)
  const seen = new Set();
  let totalPartialWeight = 0;

  for (const row of result.recordset) {
    const key = row.NoGilinganPartial;
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
    NoGilinganPartial: r.NoGilinganPartial,
    NoGilingan: r.NoGilingan,
    Berat: r.Berat,

    SourceType: r.SourceType || null, // 'BROKER' | 'INJECT' | 'MIXER' | 'WASHING' | null
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
  const NoGilingan = String(payload?.NoGilingan || "").trim();
  if (!NoGilingan) throw badReq("NoGilingan wajib diisi");

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
      "NoGilingan",
      sql.VarChar(50),
      NoGilingan,
    ).query(`
        DECLARE @out TABLE (
          NoGilingan varchar(50),
          HasBeenPrinted int
        );

        UPDATE dbo.Gilingan
        SET HasBeenPrinted = ISNULL(HasBeenPrinted, 0) + 1
        OUTPUT
          INSERTED.NoGilingan,
          INSERTED.HasBeenPrinted
        INTO @out
        WHERE NoGilingan = @NoGilingan;

        SELECT NoGilingan, HasBeenPrinted
        FROM @out;
      `);

    const row = rs.recordset?.[0] || null;
    if (!row) {
      const e = new Error(`NoGilingan ${NoGilingan} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoGilingan: row.NoGilingan,
      HasBeenPrinted: row.HasBeenPrinted,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

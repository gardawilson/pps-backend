// services/bahan-baku-service.js
const { sql, poolPromise } = require("../../../core/config/db");
const { badReq } = require("../../../core/utils/http-error");
const { normalizeDecimalField } = require("../../../core/utils/number-utils");
const { createSetIf } = require("../../../core/utils/update-diff-helper");

// GET all header BahanBaku with pagination & search
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      h.NoBahanBaku,
      h.IdSupplier,
      s.NmSupplier AS NamaSupplier,
      h.NoPlat,
      h.DateCreate,
      h.CreateBy,
      h.DateTimeCreate
    FROM dbo.BahanBaku_h h
    LEFT JOIN dbo.MstSupplier s
      ON s.IdSupplier = h.IdSupplier
    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBahanBaku LIKE @search
               OR h.NoPlat LIKE @search
               OR h.CreateBy LIKE @search
               OR CAST(h.IdSupplier AS varchar(50)) LIKE @search
               OR s.NmSupplier LIKE @search
             )`
          : ""
      }
    ORDER BY h.NoBahanBaku DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(1) AS total
    FROM dbo.BahanBaku_h h
    LEFT JOIN dbo.MstSupplier s
      ON s.IdSupplier = h.IdSupplier
    WHERE 1=1
      ${
        search
          ? `AND (
               h.NoBahanBaku LIKE @search
               OR h.NoPlat LIKE @search
               OR h.CreateBy LIKE @search
               OR CAST(h.IdSupplier AS varchar(50)) LIKE @search
               OR s.NmSupplier LIKE @search
             )`
          : ""
      }
  `;

  request.input("offset", sql.Int, offset);
  request.input("limit", sql.Int, limit);
  if (search) request.input("search", sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map((r) => ({ ...r }));
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};

exports.getPalletByNoBahanBaku = async (nobahanbaku) => {
  const pool = await poolPromise;

  const result = await pool
    .request()
    .input("NoBahanBaku", sql.VarChar, nobahanbaku).query(`
      SELECT
        p.NoBahanBaku,
        p.NoPallet,
        p.IdJenisPlastik,
        jp.Jenis AS NamaJenisPlastik,
        p.IdWarehouse,
        w.NamaWarehouse,
        p.Keterangan,
        p.IdStatus,
        CASE 
          WHEN p.IdStatus = 1 THEN 'PASS'
          WHEN p.IdStatus = 0 THEN 'HOLD'
          ELSE ''
        END AS StatusText,

        p.Moisture,
        p.MeltingIndex,
        p.Elasticity,
        p.Tenggelam,
        p.Density,
        p.Density2,
        p.Density3,
        ISNULL(CAST(p.HasBeenPrinted AS int), 0) AS HasBeenPrinted,

        p.Blok,
        p.IdLokasi,

        -- ✅ ACTUAL (tidak peduli DateUsage & IsPartial)
        ISNULL(dAgg.SakActual, 0)   AS SakActual,
        ISNULL(dAgg.BeratActual, 0) AS BeratActual,

        -- ✅ SISA (hanya DateUsage IS NULL, partial dikurangkan)
        ISNULL(dAgg.SakSisa, 0)     AS SakSisa,
        ISNULL(dAgg.BeratSisa, 0)   AS BeratSisa,

        -- ✅ Flag: IsEmpty = 1 jika semua detail sudah DateUsage terisi
        CAST(
          CASE
            WHEN ISNULL(dAgg.TotalDetail, 0) = 0 THEN 0
            WHEN ISNULL(dAgg.SakSisa, 0) = 0 THEN 1
            ELSE 0
          END
        AS bit) AS IsEmpty

      FROM dbo.BahanBakuPallet_h p
      LEFT JOIN dbo.MstJenisPlastik jp ON jp.IdJenisPlastik = p.IdJenisPlastik
      LEFT JOIN dbo.MstWarehouse w     ON w.IdWarehouse     = p.IdWarehouse

      OUTER APPLY (
        SELECT
          COUNT(1) AS TotalDetail,

          -- ACTUAL
          COUNT(1) AS SakActual,
          SUM(ISNULL(d.Berat, 0)) AS BeratActual,

          -- SISA
          SUM(CASE WHEN d.DateUsage IS NULL THEN 1 ELSE 0 END) AS SakSisa,

          SUM(
            CASE
              WHEN d.DateUsage IS NOT NULL THEN 0
              ELSE
                CASE
                  WHEN d.IsPartial = 1 THEN
                    CASE 
                      WHEN (ISNULL(d.Berat,0) - ISNULL(ps.PartialBerat,0)) < 0 THEN 0
                      ELSE (ISNULL(d.Berat,0) - ISNULL(ps.PartialBerat,0))
                    END
                  ELSE ISNULL(d.Berat,0)
                END
            END
          ) AS BeratSisa

        FROM dbo.BahanBaku_d d
        LEFT JOIN (
          SELECT
            NoBahanBaku,
            NoPallet,
            NoSak,
            SUM(Berat) AS PartialBerat
          FROM dbo.BahanBakuPartial
          GROUP BY NoBahanBaku, NoPallet, NoSak
        ) ps
          ON ps.NoBahanBaku = d.NoBahanBaku
         AND ps.NoPallet    = d.NoPallet
         AND ps.NoSak       = d.NoSak

        WHERE d.NoBahanBaku = p.NoBahanBaku
          AND d.NoPallet    = p.NoPallet
      ) dAgg

      WHERE p.NoBahanBaku = @NoBahanBaku
      ORDER BY p.NoPallet;
    `);

  const toInt = (v) =>
    typeof v === "number" ? v : parseInt(v ?? "0", 10) || 0;
  const toNum = (v) => (typeof v === "number" ? v : parseFloat(v ?? "0") || 0);

  return result.recordset.map((r) => ({
    ...r,
    IsEmpty: r.IsEmpty === true || r.IsEmpty === 1,
    HasBeenPrinted: toInt(r.HasBeenPrinted),

    SakActual: toInt(r.SakActual),
    SakSisa: toInt(r.SakSisa),

    BeratActual: toNum(r.BeratActual),
    BeratSisa: toNum(r.BeratSisa),
  }));
};

exports.getDetailByNoBahanBakuAndNoPallet = async ({
  nobahanbaku,
  nopallet,
}) => {
  const pool = await poolPromise;

  const result = await pool
    .request()
    .input("NoBahanBaku", sql.VarChar, nobahanbaku)
    .input("NoPallet", sql.VarChar, nopallet).query(`
      SELECT
        d.NoBahanBaku,
        d.NoPallet,
        d.NoSak,
        d.TimeCreate,

        -- Jika IsPartial = 1, maka Berat dikurangi total dari BahanBakuPartial
        CASE
          WHEN d.IsPartial = 1 THEN
            d.Berat - ISNULL((
              SELECT SUM(p.Berat)
              FROM dbo.BahanBakuPartial p
              WHERE p.NoBahanBaku = d.NoBahanBaku
                AND p.NoPallet    = d.NoPallet
                AND p.NoSak       = d.NoSak
            ), 0)
          ELSE d.Berat
        END AS Berat,

        d.BeratAct,
        d.DateUsage,
        d.IsLembab,
        d.IsPartial,
        d.IdLokasi
      FROM dbo.BahanBaku_d d
      WHERE d.NoBahanBaku = @NoBahanBaku
        AND d.NoPallet    = @NoPallet
      ORDER BY d.NoSak;
    `);

  // optional: rapikan DateUsage agar konsisten di FE (mirip broker)
  const formatDate = (date) => {
    if (!date) return null;
    const x = new Date(date);
    const pad = (n) => (n < 10 ? "0" + n : n);
    return `${x.getFullYear()}-${pad(x.getMonth() + 1)}-${pad(x.getDate())} ${pad(x.getHours())}:${pad(x.getMinutes())}:${pad(x.getSeconds())}`;
  };

  return result.recordset.map((item) => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) }),
  }));
};

exports.updateByNoBahanBakuAndNoPallet = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoBahanBaku = payload?.NoBahanBaku?.toString().trim();
  const NoPallet = payload?.NoPallet?.toString().trim();

  if (!NoBahanBaku) throw badReq("NoBahanBaku (path) wajib diisi");
  if (!NoPallet) throw badReq("NoPallet (path) wajib diisi");

  const header = payload?.header || {};

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

    // 0) Cek pallet exist & lock
    const exist = await new sql.Request(tx)
      .input("NoBahanBaku", sql.VarChar(50), NoBahanBaku)
      .input("NoPallet", sql.VarChar(50), NoPallet).query(`
        SELECT TOP 1 NoBahanBaku, NoPallet
        FROM dbo.BahanBakuPallet_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
      `);

    if (exist.recordset.length === 0) {
      const e = new Error(
        `Pallet tidak ditemukan untuk NoBahanBaku ${NoBahanBaku} dan NoPallet ${NoPallet}`,
      );
      e.statusCode = 404;
      throw e;
    }

    // 1) Update header (partial/dynamic)
    const setParts = [];
    const reqHeader = new sql.Request(tx)
      .input("NoBahanBaku", sql.VarChar(50), NoBahanBaku)
      .input("NoPallet", sql.VarChar(50), NoPallet);

    const setIf = createSetIf(reqHeader, setParts);

    // use shared normalizeDecimalField from utils

    setIf("IdJenisPlastik", "IdJenisPlastik", sql.Int, header.IdJenisPlastik);
    setIf("IdWarehouse", "IdWarehouse", sql.Int, header.IdWarehouse);
    setIf("IdStatus", "IdStatus", sql.Int, header.IdStatus);
    setIf("Keterangan", "Keterangan", sql.VarChar(sql.MAX), header.Keterangan);

    // Field numeric dengan normalisasi (terima '', '-', null, string angka, angka)
    setIf(
      "Moisture",
      "Moisture",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Moisture, "Moisture"),
    );
    setIf(
      "MeltingIndex",
      "MeltingIndex",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.MeltingIndex, "MeltingIndex"),
    );
    setIf(
      "Elasticity",
      "Elasticity",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Elasticity, "Elasticity"),
    );
    setIf(
      "Tenggelam",
      "Tenggelam",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Tenggelam, "Tenggelam"),
    );
    setIf(
      "Density",
      "Density",
      sql.Decimal(10, 3),
      normalizeDecimalField(header.Density, "Density"),
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

    if (setParts.length > 0) {
      await reqHeader.query(`
        UPDATE dbo.BahanBakuPallet_h
        SET ${setParts.join(", ")}
        WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
      `);
    }

    await tx.commit();

    return {
      header: {
        NoBahanBaku,
        NoPallet,
        ...header,
      },
      counts: {
        detailsAffected: 0,
      },
      audit: { actorId, requestId }, // ✅ ID only
      note: "Pallet berhasil diupdate",
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.incrementHasBeenPrinted = async (payload) => {
  const NoBahanBaku = String(payload?.NoBahanBaku || "").trim();
  const NoPallet = String(payload?.NoPallet || "").trim();

  if (!NoBahanBaku) throw badReq("NoBahanBaku wajib diisi");
  if (!NoPallet) throw badReq("NoPallet wajib diisi");

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

    const rs = await new sql.Request(tx)
      .input("NoBahanBaku", sql.VarChar(50), NoBahanBaku)
      .input("NoPallet", sql.VarChar(50), NoPallet).query(`
        DECLARE @out TABLE (
          NoBahanBaku varchar(50),
          NoPallet varchar(50),
          HasBeenPrinted int
        );

        UPDATE dbo.BahanBakuPallet_h
        SET HasBeenPrinted = ISNULL(HasBeenPrinted, 0) + 1
        OUTPUT
          INSERTED.NoBahanBaku,
          INSERTED.NoPallet,
          INSERTED.HasBeenPrinted
        INTO @out
        WHERE NoBahanBaku = @NoBahanBaku
          AND NoPallet = @NoPallet;

        SELECT NoBahanBaku, NoPallet, HasBeenPrinted
        FROM @out;
      `);

    const row = rs.recordset?.[0] || null;
    if (!row) {
      const e = new Error(
        `Pallet tidak ditemukan untuk NoBahanBaku ${NoBahanBaku} dan NoPallet ${NoPallet}`,
      );
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoBahanBaku: row.NoBahanBaku,
      NoPallet: row.NoPallet,
      HasBeenPrinted: Number(row.HasBeenPrinted) || 0,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

const { sql, poolPromise } = require("../../core/config/db");
const { badReq, conflict } = require("../../core/utils/http-error");
const { formatYMD } = require("../../core/shared/tutup-transaksi-guard");
const {
  detectCategory,
} = require("./sortir-reject-v2-category-registry");
const {
  getLabelInfoBarangJadi,
} = require("./handlers/get-label-info-barang-jadi.handler");
const {
  getLabelInfoFurnitureWip,
} = require("./handlers/get-label-info-furniture-wip.handler");
const {
  createSortirRejectBarangJadi,
} = require("./handlers/create-barang-jadi.handler");
const {
  createSortirRejectFurnitureWip,
} = require("./handlers/create-furniture-wip.handler");
const {
  createSortirRejectReject,
} = require("./handlers/create-reject.handler");

exports.getLabelInfo = async (labelCode) => {
  const code = String(labelCode || "").trim();
  const category = detectCategory(code);

  if (!category) {
    throw badReq(`Label code tidak dikenali: ${code}`);
  }

  if (category === "barangJadi") {
    return getLabelInfoBarangJadi(code);
  }
  if (category === "furnitureWip") {
    return getLabelInfoFurnitureWip(code);
  }

  throw badReq(`Kategori ${category} belum didukung`);
};

exports.getAll = async (page = 1, pageSize = 20, search = "") => {
  const pool = await poolPromise;
  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;
  const searchTerm = (search || "").trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoBJSortir LIKE '%' + @search + '%')
      AND (
        EXISTS (
          SELECT 1
          FROM dbo.BJSortirRejectOutputLabelReject rejectV2
          WHERE rejectV2.NoBJSortir = h.NoBJSortir
        )
        OR
        EXISTS (
          SELECT 1
          FROM dbo.BJSortirRejectOutputLabelBarangJadi bjv2
          WHERE bjv2.NoBJSortir = h.NoBJSortir
        )
        OR (
          EXISTS (
            SELECT 1
            FROM dbo.BJSortirRejectInputLabelFurnitureWIP fwv2
            WHERE fwv2.NoBJSortir = h.NoBJSortir
          )
          AND NOT EXISTS (
            SELECT 1
            FROM dbo.BJSortirRejectOutputLabelReject rejectOut
            WHERE rejectOut.NoBJSortir = h.NoBJSortir
          )
        )
      )
  `;

  const countRes = await pool
    .request()
    .input("search", sql.VarChar(100), searchTerm)
    .query(`SELECT COUNT(1) AS total FROM dbo.BJSortirReject_h h ${whereClause}`);

  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) return { data: [], total: 0 };

  const dataRes = await pool
    .request()
    .input("search", sql.VarChar(100), searchTerm)
    .input("offset", sql.Int, offset)
    .input("pageSize", sql.Int, ps).query(`
      SELECT
        h.NoBJSortir,
        h.TglBJSortir,
        h.IdWarehouse,
        w.NamaWarehouse,
        h.IdUsername,
        u.Username,
        CASE
          WHEN ISNULL(ro.outputLabelCount, 0) > 0
           AND ISNULL(o.outputLabelCount, 0) > 0 THEN 'barangJadiReject'
          WHEN ISNULL(ro.outputLabelCount, 0) > 0 THEN 'reject'
          WHEN ISNULL(o.outputLabelCount, 0) > 0 THEN 'barangJadi'
          ELSE 'furnitureWip'
        END AS category,
        CASE
          WHEN ISNULL(o.outputLabelCount, 0) > 0 THEN ISNULL(i.inputLabelCount, 0)
          ELSE ISNULL(fwi.inputLabelCount, 0)
        END AS inputLabelCount,
        ISNULL(o.outputLabelCount, 0) + ISNULL(ro.outputLabelCount, 0)
          AS outputLabelCount,
        ISNULL(ro.outputLabelCount, 0) AS rejectOutputLabelCount,
        CASE
          WHEN ISNULL(o.outputLabelCount, 0) = 0 THEN CAST(1 AS bit)
          WHEN ABS(ISNULL(i.totalPcsInput, 0) - ISNULL(o.totalPcsOutput, 0)) < 0.001
          THEN CAST(1 AS bit)
          ELSE CAST(0 AS bit)
        END AS balance
      FROM dbo.BJSortirReject_h h
      LEFT JOIN dbo.MstUsername u
        ON u.IdUsername = h.IdUsername
      LEFT JOIN dbo.MstWarehouse w
        ON w.IdWarehouse = h.IdWarehouse
      OUTER APPLY (
        SELECT
          COUNT(DISTINCT ibj.NoBJ) AS inputLabelCount,
          SUM(ISNULL(bj.Pcs, 0)) AS totalPcsInput
        FROM dbo.BJSortirRejectInputLabelBarangJadi ibj
        INNER JOIN dbo.BarangJadi bj
          ON bj.NoBJ = ibj.NoBJ
        WHERE ibj.NoBJSortir = h.NoBJSortir
      ) i
      OUTER APPLY (
        SELECT
          COUNT(DISTINCT obj.NoBJ) AS outputLabelCount,
          SUM(ISNULL(bj.Pcs, 0)) AS totalPcsOutput
        FROM dbo.BJSortirRejectOutputLabelBarangJadi obj
        INNER JOIN dbo.BarangJadi bj
          ON bj.NoBJ = obj.NoBJ
        WHERE obj.NoBJSortir = h.NoBJSortir
      ) o
      OUTER APPLY (
        SELECT
          COUNT(DISTINCT rmap.NoReject) AS outputLabelCount,
          SUM(
            CASE
              WHEN ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0) < 0
              THEN 0
              ELSE ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0)
            END
          ) AS totalBeratOutput
        FROM dbo.BJSortirRejectOutputLabelReject rmap
        INNER JOIN dbo.RejectV2 r
          ON r.NoReject = rmap.NoReject
        LEFT JOIN (
          SELECT NoReject, SUM(ISNULL(Berat, 0)) AS TotalPartialBerat
          FROM dbo.RejectV2Partial
          GROUP BY NoReject
        ) rp
          ON rp.NoReject = r.NoReject
        WHERE rmap.NoBJSortir = h.NoBJSortir
      ) ro
      OUTER APPLY (
        SELECT
          COUNT(DISTINCT ifw.NoFurnitureWIP) AS inputLabelCount,
          SUM(ISNULL(fw.Pcs, 0)) AS totalPcsInput
        FROM dbo.BJSortirRejectInputLabelFurnitureWIP ifw
        INNER JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = ifw.NoFurnitureWIP
        WHERE ifw.NoBJSortir = h.NoBJSortir
      ) fwi
      ${whereClause}
      ORDER BY h.TglBJSortir DESC, h.NoBJSortir DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
    `);

  return { data: dataRes.recordset || [], total };
};

exports.getDetail = async (noBJSortir) => {
  const pool = await poolPromise;
  const no = String(noBJSortir || "").trim();
  if (!no) throw badReq("noBJSortir wajib diisi");

  const headerRes = await pool.request().input("NoBJSortir", sql.VarChar(50), no)
    .query(`
      SELECT
        h.NoBJSortir,
        h.TglBJSortir,
        h.IdWarehouse,
        w.NamaWarehouse,
        h.IdUsername,
        u.Username
      FROM dbo.BJSortirReject_h h
      LEFT JOIN dbo.MstUsername u
        ON u.IdUsername = h.IdUsername
      LEFT JOIN dbo.MstWarehouse w
        ON w.IdWarehouse = h.IdWarehouse
      WHERE h.NoBJSortir = @NoBJSortir
        AND (
          EXISTS (
            SELECT 1
            FROM dbo.BJSortirRejectOutputLabelReject rejectOut
            WHERE rejectOut.NoBJSortir = h.NoBJSortir
          )
          OR
          EXISTS (
            SELECT 1
            FROM dbo.BJSortirRejectOutputLabelBarangJadi bjv2
            WHERE bjv2.NoBJSortir = h.NoBJSortir
          )
          OR (
            EXISTS (
              SELECT 1
              FROM dbo.BJSortirRejectInputLabelFurnitureWIP fwv2
              WHERE fwv2.NoBJSortir = h.NoBJSortir
            )
            AND NOT EXISTS (
              SELECT 1
              FROM dbo.BJSortirRejectOutputLabelReject rejectOut
              WHERE rejectOut.NoBJSortir = h.NoBJSortir
            )
          )
        )
    `);

  const header = headerRes.recordset?.[0];
  if (!header) {
    const e = new Error(`NoBJSortir ${no} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  const rejectOutputsRes = await pool
    .request()
    .input("NoBJSortir", sql.VarChar(50), no).query(`
      WITH RejectPartialAgg AS (
        SELECT
          NoReject,
          SUM(ISNULL(Berat, 0)) AS TotalPartialBerat
        FROM dbo.RejectV2Partial
        GROUP BY NoReject
      )
      SELECT
        map.NoReject,
        r.DateCreate,
        r.IdReject AS idJenis,
        mr.NamaReject AS namaJenis,
        CASE
          WHEN ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0) < 0
          THEN 0
          ELSE ISNULL(r.Berat, 0) - ISNULL(rp.TotalPartialBerat, 0)
        END AS berat,
        r.IsPartial,
        ISNULL(CAST(r.HasBeenPrinted AS int), 0) AS hasBeenPrinted
      FROM dbo.BJSortirRejectOutputLabelReject map
      INNER JOIN dbo.RejectV2 r
        ON r.NoReject = map.NoReject
      LEFT JOIN dbo.MstReject mr
        ON mr.IdReject = r.IdReject
      LEFT JOIN RejectPartialAgg rp
        ON rp.NoReject = r.NoReject
      WHERE map.NoBJSortir = @NoBJSortir
      ORDER BY map.NoReject ASC
    `);

  const outputsRes = await pool.request().input("NoBJSortir", sql.VarChar(50), no)
    .query(`
      SELECT
        map.NoBJ,
        bj.DateCreate,
        bj.IdBJ AS idJenis,
        mbj.NamaBJ AS namaJenis,
        ISNULL(bj.Pcs, 0) AS pcs,
        ISNULL(CAST(bj.HasBeenPrinted AS int), 0) AS hasBeenPrinted
      FROM dbo.BJSortirRejectOutputLabelBarangJadi map
      INNER JOIN dbo.BarangJadi bj
        ON bj.NoBJ = map.NoBJ
      LEFT JOIN dbo.MstBarangJadi mbj
        ON mbj.IdBJ = bj.IdBJ
      WHERE map.NoBJSortir = @NoBJSortir
      ORDER BY map.NoBJ ASC
    `);

  const hasRejectOutput = rejectOutputsRes.recordset.length > 0;
  const hasBarangJadiOutput = outputsRes.recordset.length > 0;

  if (hasRejectOutput) {
    const inputsBarangJadiRes = await pool
      .request()
      .input("NoBJSortir", sql.VarChar(50), no).query(`
        SELECT
          'barangJadi' AS category,
          map.NoBJ,
          bj.DateCreate,
          bj.IdBJ AS idJenis,
          mbj.NamaBJ AS namaJenis,
          ISNULL(bj.Pcs, 0) AS pcs,
          ISNULL(CAST(bj.HasBeenPrinted AS int), 0) AS hasBeenPrinted
        FROM dbo.BJSortirRejectInputLabelBarangJadi map
        INNER JOIN dbo.BarangJadi bj
          ON bj.NoBJ = map.NoBJ
        LEFT JOIN dbo.MstBarangJadi mbj
          ON mbj.IdBJ = bj.IdBJ
        WHERE map.NoBJSortir = @NoBJSortir
        ORDER BY map.NoBJ ASC
      `);

    const inputsFurnitureWipRes = await pool
      .request()
      .input("NoBJSortir", sql.VarChar(50), no).query(`
        SELECT
          'furnitureWip' AS category,
          map.NoFurnitureWIP,
          fw.DateCreate,
          fw.IdFurnitureWIP AS idJenis,
          mw.Nama AS namaJenis,
          ISNULL(fw.Pcs, 0) AS pcs,
          ISNULL(CAST(fw.HasBeenPrinted AS int), 0) AS hasBeenPrinted
        FROM dbo.BJSortirRejectInputLabelFurnitureWIP map
        INNER JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = map.NoFurnitureWIP
        LEFT JOIN dbo.MstCabinetWIP mw
          ON mw.IdCabinetWIP = fw.IdFurnitureWIP
        WHERE map.NoBJSortir = @NoBJSortir
        ORDER BY map.NoFurnitureWIP ASC
      `);

    const inputs = [
      ...(inputsBarangJadiRes.recordset || []),
      ...(inputsFurnitureWipRes.recordset || []),
    ];
    const totalPcsInput = inputs.reduce(
      (sum, row) => sum + Number(row.pcs || 0),
      0,
    );
    const totalPcsOutput = outputsRes.recordset.reduce(
      (sum, row) => sum + Number(row.pcs || 0),
      0,
    );

    return {
      ...header,
      TglBJSortir: formatYMD(header.TglBJSortir),
      category: hasBarangJadiOutput ? "barangJadiReject" : "reject",
      balance: hasBarangJadiOutput
        ? Math.abs(totalPcsInput - totalPcsOutput) < 0.001
        : null,
      inputs,
      outputs: {
        barangJadi: outputsRes.recordset || [],
        reject: rejectOutputsRes.recordset || [],
      },
    };
  }

  const isBarangJadi = hasBarangJadiOutput;

  const inputsRes = isBarangJadi
    ? await pool.request().input("NoBJSortir", sql.VarChar(50), no).query(`
        SELECT
          map.NoBJ,
          bj.DateCreate,
          bj.IdBJ AS idJenis,
          mbj.NamaBJ AS namaJenis,
          ISNULL(bj.Pcs, 0) AS pcs,
          ISNULL(CAST(bj.HasBeenPrinted AS int), 0) AS hasBeenPrinted
        FROM dbo.BJSortirRejectInputLabelBarangJadi map
        INNER JOIN dbo.BarangJadi bj
          ON bj.NoBJ = map.NoBJ
        LEFT JOIN dbo.MstBarangJadi mbj
          ON mbj.IdBJ = bj.IdBJ
        WHERE map.NoBJSortir = @NoBJSortir
        ORDER BY map.NoBJ ASC
      `)
    : await pool.request().input("NoBJSortir", sql.VarChar(50), no).query(`
        SELECT
          map.NoFurnitureWIP,
          fw.DateCreate,
          fw.IdFurnitureWIP AS idJenis,
          mw.Nama AS namaJenis,
          ISNULL(fw.Pcs, 0) AS pcs,
          ISNULL(CAST(fw.HasBeenPrinted AS int), 0) AS hasBeenPrinted
        FROM dbo.BJSortirRejectInputLabelFurnitureWIP map
        INNER JOIN dbo.FurnitureWIP fw
          ON fw.NoFurnitureWIP = map.NoFurnitureWIP
        LEFT JOIN dbo.MstCabinetWIP mw
          ON mw.IdCabinetWIP = fw.IdFurnitureWIP
        WHERE map.NoBJSortir = @NoBJSortir
        ORDER BY map.NoFurnitureWIP ASC
      `);

  const outputRows = isBarangJadi ? outputsRes.recordset || [] : [];

  const totalPcsInput = inputsRes.recordset.reduce(
    (sum, row) => sum + Number(row.pcs || 0),
    0,
  );
  const totalPcsOutput = outputRows.reduce(
    (sum, row) => sum + Number(row.pcs || 0),
    0,
  );

  return {
    ...header,
    TglBJSortir: formatYMD(header.TglBJSortir),
    category: isBarangJadi ? "barangJadi" : "furnitureWip",
    balance:
      !isBarangJadi || Math.abs(totalPcsInput - totalPcsOutput) < 0.001,
    inputs: inputsRes.recordset || [],
    outputs: outputRows,
  };
};

exports.create = async (payload, ctx) => {
  const firstInput = Array.isArray(payload?.inputs) && payload.inputs[0];
  if (!firstInput) throw badReq("inputs wajib diisi");

  const category = detectCategory(firstInput);
  const outputs = Array.isArray(payload?.outputs) ? payload.outputs : [];

  if (outputs.length > 0) {
    const hasPcs = outputs.some((out) => out?.pcs != null);
    const hasBerat = outputs.some((out) => out?.berat != null);

    if (hasPcs && hasBerat) {
      throw badReq("outputs tidak boleh mencampur pcs dan berat");
    }
    if (hasBerat) {
      return createSortirRejectReject(null, payload, ctx);
    }
    if (hasPcs) {
      return createSortirRejectBarangJadi(payload, ctx);
    }

    throw badReq("outputs wajib berisi pcs atau berat");
  }

  if (category === "furnitureWip") {
    return createSortirRejectFurnitureWip(payload, ctx);
  }
  if (category === "barangJadi") {
    throw badReq("outputs wajib diisi untuk input barangJadi");
  }

  throw badReq(`Label ${firstInput} tidak dikenali kategorinya`);
};

exports.createReject = async (noBJSortir, payload, ctx) =>
  createSortirRejectReject(noBJSortir, payload, ctx);

exports.deleteSortirReject = async (noBJSortir, ctx) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  const no = String(noBJSortir || "").trim();
  if (!no) throw badReq("noBJSortir wajib diisi");

  const { actorId, requestId } = ctx;

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const headerRes = await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), no)
      .query(
        `SELECT 1 FROM dbo.BJSortirReject_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBJSortir=@NoBJSortir`,
      );
    if (headerRes.recordset.length === 0) {
      const e = new Error(`NoBJSortir ${no} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    const rejectOutputRes = await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), no)
      .query(
        `SELECT TOP 1 NoReject FROM dbo.BJSortirRejectOutputLabelReject WHERE NoBJSortir=@NoBJSortir`,
      );
    if (rejectOutputRes.recordset.length > 0) {
      throw conflict(
        "Tidak bisa hapus dari sortir reject v2: transaksi memiliki output reject",
      );
    }

    const outputsRes = await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), no)
      .query(
        `SELECT NoBJ FROM dbo.BJSortirRejectOutputLabelBarangJadi WHERE NoBJSortir=@NoBJSortir`,
      );

    if (outputsRes.recordset.length > 0) {
      const outputJson = JSON.stringify(
        outputsRes.recordset.map((r) => ({ code: r.NoBJ })),
      );

      const usedOutput = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outputJson,
      ).query(`
        SELECT TOP 1 NoBJ
        FROM dbo.BarangJadi
        WHERE NoBJ IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NOT NULL
      `);
      if (usedOutput.recordset.length > 0) {
        throw conflict(
          "Tidak bisa hapus: label output barangJadi sudah digunakan di proses lain",
        );
      }

      await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), no)
        .query(
          `DELETE FROM dbo.BJSortirRejectOutputLabelBarangJadi WHERE NoBJSortir=@NoBJSortir`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outputJson,
      ).query(`
        DELETE FROM dbo.BarangJadi
        WHERE NoBJ IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
      `);
    }

    const inputsBarangJadiRes = await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), no)
      .query(
        `SELECT NoBJ FROM dbo.BJSortirRejectInputLabelBarangJadi WHERE NoBJSortir=@NoBJSortir`,
      );

    if (inputsBarangJadiRes.recordset.length > 0) {
      const inputJson = JSON.stringify(
        inputsBarangJadiRes.recordset.map((r) => ({ code: r.NoBJ })),
      );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inputJson,
      ).query(`
        UPDATE dbo.BarangJadi
        SET DateUsage = NULL
        WHERE NoBJ IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
      `);

      await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), no)
        .query(
          `DELETE FROM dbo.BJSortirRejectInputLabelBarangJadi WHERE NoBJSortir=@NoBJSortir`,
        );
    }

    const inputsFurnitureWipRes = await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), no)
      .query(
        `SELECT NoFurnitureWIP FROM dbo.BJSortirRejectInputLabelFurnitureWIP WHERE NoBJSortir=@NoBJSortir`,
      );

    if (inputsFurnitureWipRes.recordset.length > 0) {
      const inputJson = JSON.stringify(
        inputsFurnitureWipRes.recordset.map((r) => ({
          code: r.NoFurnitureWIP,
        })),
      );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inputJson,
      ).query(`
        UPDATE dbo.FurnitureWIP
        SET DateUsage = NULL
        WHERE NoFurnitureWIP IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
      `);

      await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), no)
        .query(
          `DELETE FROM dbo.BJSortirRejectInputLabelFurnitureWIP WHERE NoBJSortir=@NoBJSortir`,
        );
    }

    if (
      outputsRes.recordset.length === 0 &&
      inputsBarangJadiRes.recordset.length === 0 &&
      inputsFurnitureWipRes.recordset.length === 0
    ) {
      const e = new Error(`NoBJSortir ${no} bukan transaksi sortir reject v2`);
      e.statusCode = 404;
      throw e;
    }

    await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), no)
      .query(`DELETE FROM dbo.BJSortirReject_h WHERE NoBJSortir=@NoBJSortir`);

    await tx.commit();

    return { success: true, noBJSortir: no, audit: { actorId, requestId } };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

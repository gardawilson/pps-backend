const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../sortir-reject-v2-category-registry");

exports.createSortirRejectFurnitureWip = async (payload, ctx) => {
  const { idWarehouse, inputs } = payload;
  const { actorId, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }

  const warehouseId = Number(idWarehouse);
  if (!Number.isFinite(warehouseId) || warehouseId <= 0) {
    throw badReq("idWarehouse wajib diisi");
  }

  for (const code of inputs) {
    if (detectCategory(code) !== "furnitureWip") {
      throw badReq(`Label input ${code} bukan kategori furnitureWip`);
    }
  }

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

    const inputCodesJson = JSON.stringify(inputs.map((c) => ({ code: c })));

    const inputDataRes = await new sql.Request(tx).input(
      "CodesJson",
      sql.NVarChar(sql.MAX),
      inputCodesJson,
    ).query(`
        SELECT
          f.NoFurnitureWIP,
          f.IdFurnitureWIP,
          f.IdWarehouse,
          f.Blok,
          f.IdLokasi,
          f.IsPartial,
          ISNULL(f.Pcs, 0) AS AvailablePcs
        FROM dbo.FurnitureWIP f WITH (UPDLOCK, HOLDLOCK)
        WHERE f.NoFurnitureWIP IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND f.DateUsage IS NULL
      `);

    if (inputDataRes.recordset.length !== inputs.length) {
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );
    }

    for (const row of inputDataRes.recordset) {
      if (row.IsPartial === true || row.IsPartial === 1) {
        throw badReq(`Label input ${row.NoFurnitureWIP} sudah di partial`);
      }
    }

    const totalPcsInput = inputDataRes.recordset.reduce(
      (sum, row) => sum + Number(row.AvailablePcs || 0),
      0,
    );

    const genSortir = () =>
      generateNextCode(tx, {
        tableName: "BJSortirReject_h",
        columnName: "NoBJSortir",
        prefix: "J.",
        width: 10,
      });

    let noBJSortir = await genSortir();
    const sortirExist = await new sql.Request(tx)
      .input("No", sql.VarChar(50), noBJSortir)
      .query(
        `SELECT 1 FROM dbo.BJSortirReject_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBJSortir=@No`,
      );
    if (sortirExist.recordset.length > 0) {
      noBJSortir = await genSortir();
      const sortirExist2 = await new sql.Request(tx)
        .input("No", sql.VarChar(50), noBJSortir)
        .query(
          `SELECT 1 FROM dbo.BJSortirReject_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBJSortir=@No`,
        );
      if (sortirExist2.recordset.length > 0) {
        throw conflict("Gagal generate NoBJSortir unik, coba lagi");
      }
    }

    const nowDate = new Date();

    await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), noBJSortir)
      .input("TglBJSortir", sql.Date, nowDate)
      .input("IdWarehouse", sql.Int, warehouseId)
      .input("IdUsername", sql.Int, actorId).query(`
        INSERT INTO dbo.BJSortirReject_h (
          NoBJSortir, TglBJSortir, IdWarehouse, IdUsername
        )
        VALUES (@NoBJSortir, @TglBJSortir, @IdWarehouse, @IdUsername)
      `);

    await new sql.Request(tx)
      .input("NoBJSortir", sql.VarChar(50), noBJSortir)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        INSERT INTO dbo.BJSortirRejectInputLabelFurnitureWIP (
          NoBJSortir, NoFurnitureWIP
        )
        SELECT @NoBJSortir, j.code
        FROM OPENJSON(@CodesJson)
        WITH (code varchar(50) '$.code') AS j
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.FurnitureWIP
        SET DateUsage = @Tanggal
        WHERE NoFurnitureWIP IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    await tx.commit();

    return {
      noBJSortir,
      tglBJSortir: formatYMD(nowDate),
      idWarehouse: warehouseId,
      category: "furnitureWip",
      totalPcsInput,
      totalPcsOutput: 0,
      inputs,
      outputs: [],
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

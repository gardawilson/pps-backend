const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../sortir-reject-v2-category-registry");

exports.createSortirRejectBarangJadi = async (payload, ctx) => {
  const { idWarehouse, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }

  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output label");
  }

  const warehouseId = Number(idWarehouse);
  if (!Number.isFinite(warehouseId) || warehouseId <= 0) {
    throw badReq("idWarehouse wajib diisi");
  }

  for (const code of inputs) {
    if (detectCategory(code) !== "barangJadi") {
      throw badReq(`Label input ${code} bukan kategori barangJadi`);
    }
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    const idJenis = out.idJenis ?? out.idBJ;
    if (!idJenis || !Number.isFinite(Number(idJenis)) || Number(idJenis) <= 0) {
      throw badReq(`outputs[${i}].idJenis wajib diisi`);
    }
    if (
      out.pcs == null ||
      !Number.isFinite(Number(out.pcs)) ||
      Number(out.pcs) <= 0
    ) {
      throw badReq(`outputs[${i}].pcs wajib diisi dan lebih dari 0`);
    }
    if (!Number.isInteger(Number(out.pcs))) {
      throw badReq(`outputs[${i}].pcs wajib bilangan bulat`);
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
          b.NoBJ,
          b.IdBJ,
          b.IdWarehouse,
          b.Blok,
          b.IdLokasi,
          b.IsPartial,
          ISNULL(b.Pcs, 0) AS AvailablePcs
        FROM dbo.BarangJadi b WITH (UPDLOCK, HOLDLOCK)
        WHERE b.NoBJ IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND b.DateUsage IS NULL
    `);

    if (inputDataRes.recordset.length !== inputs.length) {
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );
    }

    for (const row of inputDataRes.recordset) {
      if (row.IsPartial === true || row.IsPartial === 1) {
        throw badReq(`Label input ${row.NoBJ} sudah di partial`);
      }
    }

    const totalPcsInput = inputDataRes.recordset.reduce(
      (sum, row) => sum + Number(row.AvailablePcs || 0),
      0,
    );
    const totalPcsOutput = outputs.reduce(
      (sum, out) => sum + Number(out.pcs),
      0,
    );

    if (Math.abs(totalPcsInput - totalPcsOutput) > 0.001) {
      throw badReq(
        `Total pcs tidak balance: input=${totalPcsInput}, output=${totalPcsOutput}`,
      );
    }

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
        INSERT INTO dbo.BJSortirRejectInputLabelBarangJadi (NoBJSortir, NoBJ)
        SELECT @NoBJSortir, j.code
        FROM OPENJSON(@CodesJson)
        WITH (code varchar(50) '$.code') AS j
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.BarangJadi
        SET DateUsage = @Tanggal
        WHERE NoBJ IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    const createdOutputs = [];
    const refRow = inputDataRes.recordset[0];

    for (const out of outputs) {
      const genBj = () =>
        generateNextCode(tx, {
          tableName: "BarangJadi",
          columnName: "NoBJ",
          prefix: "BA.",
          width: 10,
        });

      let newNoBJ = await genBj();
      const exist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoBJ)
        .query(
          `SELECT 1 FROM dbo.BarangJadi WITH (UPDLOCK,HOLDLOCK) WHERE NoBJ=@No`,
        );
      if (exist.recordset.length > 0) {
        newNoBJ = await genBj();
        const exist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoBJ)
          .query(
            `SELECT 1 FROM dbo.BarangJadi WITH (UPDLOCK,HOLDLOCK) WHERE NoBJ=@No`,
          );
        if (exist2.recordset.length > 0) {
          throw conflict("Gagal generate NoBJ unik, coba lagi");
        }
      }

      await new sql.Request(tx)
        .input("NoBJ", sql.VarChar(50), newNoBJ)
        .input("DateCreate", sql.Date, nowDate)
        .input("Jam", sql.VarChar(20), null)
        .input("Pcs", sql.Int, Math.trunc(Number(out.pcs)))
        .input("IdBJ", sql.Int, Number(out.idJenis ?? out.idBJ))
        .input("Berat", sql.Decimal(18, 3), 0)
        .input("IsPartial", sql.Bit, 0)
        .input("DateUsage", sql.Date, null)
        .input("IdWarehouse", sql.Int, warehouseId)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, new Date())
        .input("Blok", sql.VarChar(50), refRow.Blok ?? null)
        .input("IdLokasi", sql.Int, refRow.IdLokasi ?? null).query(`
          INSERT INTO dbo.BarangJadi (
            NoBJ, DateCreate, Jam, Pcs, IdBJ, Berat, IsPartial, DateUsage,
            IdWarehouse, CreateBy, DateTimeCreate, Blok, IdLokasi
          )
          VALUES (
            @NoBJ, @DateCreate, @Jam, @Pcs, @IdBJ, @Berat, @IsPartial, @DateUsage,
            @IdWarehouse, @CreateBy, @DateTimeCreate, @Blok, @IdLokasi
          )
        `);

      await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), noBJSortir)
        .input("NoBJ", sql.VarChar(50), newNoBJ).query(`
          INSERT INTO dbo.BJSortirRejectOutputLabelBarangJadi (NoBJSortir, NoBJ)
          VALUES (@NoBJSortir, @NoBJ)
        `);

      createdOutputs.push({
        noBJ: newNoBJ,
        idJenis: Number(out.idJenis ?? out.idBJ),
        pcs: Number(out.pcs),
      });
    }

    await tx.commit();

    return {
      noBJSortir,
      tglBJSortir: formatYMD(nowDate),
      idWarehouse: warehouseId,
      category: "barangJadi",
      totalPcsInput,
      totalPcsOutput,
      inputs,
      outputs: createdOutputs,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

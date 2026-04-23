const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");

exports.createBongkarSusunCrusher = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }
  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output label");
  }

  for (const code of inputs) {
    if (detectCategory(code) !== "crusher") {
      throw badReq(`Label input ${code} bukan kategori crusher`);
    }
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    if (
      !out.idCrusher ||
      !Number.isFinite(Number(out.idCrusher)) ||
      Number(out.idCrusher) <= 0
    ) {
      throw badReq(`outputs[${i}].idCrusher wajib diisi`);
    }
    if (
      out.berat == null ||
      !Number.isFinite(Number(out.berat)) ||
      Number(out.berat) <= 0
    ) {
      throw badReq(`outputs[${i}].berat wajib diisi dan lebih dari 0`);
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
          c.NoCrusher,
          c.IdCrusher,
          c.IdWarehouse,
          c.IdStatus,
          c.Berat,
          c.Blok,
          c.IdLokasi,
          c.DateUsage,
          c.CreateBy,
          c.DateTimeCreate
        FROM dbo.Crusher c WITH (UPDLOCK, HOLDLOCK)
        WHERE c.NoCrusher IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND c.DateUsage IS NULL
      `);

    if (inputDataRes.recordset.length !== inputs.length) {
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );
    }

    const totalBeratInput = inputDataRes.recordset.reduce(
      (sum, row) => sum + Number(row.Berat || 0),
      0,
    );

    const inputByJenis = {};
    for (const row of inputDataRes.recordset) {
      const k = Number(row.IdCrusher);
      inputByJenis[k] = (inputByJenis[k] || 0) + Number(row.Berat || 0);
    }

    const outputByJenis = {};
    for (const out of outputs) {
      const k = Number(out.idCrusher);
      const beratOut = Number(out.berat);
      outputByJenis[k] = (outputByJenis[k] || 0) + beratOut;
    }

    const totalBeratOutput = outputs.reduce(
      (sum, out) => sum + Number(out.berat),
      0,
    );

    for (const idCrusher of Object.keys(outputByJenis)) {
      if (!(idCrusher in inputByJenis)) {
        throw badReq(
          `idCrusher=${idCrusher} pada output tidak ada di input manapun`,
        );
      }
    }

    for (const [idCrusher, beratInput] of Object.entries(inputByJenis)) {
      const beratOutput = outputByJenis[idCrusher] || 0;
      if (Math.abs(beratInput - beratOutput) > 0.001) {
        throw badReq(
          `Berat tidak balance untuk idCrusher=${idCrusher}: input=${beratInput}kg, output=${beratOutput}kg`,
        );
      }
    }

    if (Math.abs(totalBeratInput - totalBeratOutput) > 0.001) {
      throw badReq(
        `Total berat tidak balance: input=${totalBeratInput}kg, output=${totalBeratOutput}kg`,
      );
    }

    const genBg = () =>
      generateNextCode(tx, {
        tableName: "BongkarSusun_h",
        columnName: "NoBongkarSusun",
        prefix: "BG.",
        width: 10,
      });

    let noBongkarSusun = await genBg();
    const bgExist = await new sql.Request(tx)
      .input("No", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT 1 FROM dbo.BongkarSusun_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBongkarSusun=@No`,
      );
    if (bgExist.recordset.length > 0) {
      noBongkarSusun = await genBg();
      const bgExist2 = await new sql.Request(tx)
        .input("No", sql.VarChar(50), noBongkarSusun)
        .query(
          `SELECT 1 FROM dbo.BongkarSusun_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBongkarSusun=@No`,
        );
      if (bgExist2.recordset.length > 0) {
        throw conflict("Gagal generate NoBongkarSusun unik, coba lagi");
      }
    }

    const nowDate = new Date();
    await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("Tanggal", sql.DateTime, nowDate)
      .input("IdUsername", sql.Int, actorId)
      .input("Note", sql.NVarChar(500), note || null).query(`
        INSERT INTO dbo.BongkarSusun_h (NoBongkarSusun, Tanggal, IdUsername, Note)
        VALUES (@NoBongkarSusun, @Tanggal, @IdUsername, @Note)
      `);

    await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        INSERT INTO dbo.BongkarSusunInputCrusher (NoBongkarSusun, NoCrusher)
        SELECT @NoBongkarSusun, j.code
        FROM OPENJSON(@CodesJson)
        WITH (code varchar(50) '$.code') AS j
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.Crusher
        SET DateUsage = @Tanggal
        WHERE NoCrusher IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    const refRow = inputDataRes.recordset[0];
    const createdOutputs = [];

    for (const out of outputs) {
      const genCrusher = () =>
        generateNextCode(tx, {
          tableName: "Crusher",
          columnName: "NoCrusher",
          prefix: "F.",
          width: 10,
        });

      let newNoCrusher = await genCrusher();
      const exist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoCrusher)
        .query(
          `SELECT 1 FROM dbo.Crusher WITH (UPDLOCK,HOLDLOCK) WHERE NoCrusher=@No`,
        );
      if (exist.recordset.length > 0) {
        newNoCrusher = await genCrusher();
        const exist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoCrusher)
          .query(
            `SELECT 1 FROM dbo.Crusher WITH (UPDLOCK,HOLDLOCK) WHERE NoCrusher=@No`,
          );
        if (exist2.recordset.length > 0) {
          throw conflict("Gagal generate NoCrusher unik, coba lagi");
        }
      }

      await new sql.Request(tx)
        .input("NoCrusher", sql.VarChar(50), newNoCrusher)
        .input("DateCreate", sql.Date, nowDate)
        .input("IdCrusher", sql.Int, Math.trunc(Number(out.idCrusher)))
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse)
        .input("DateUsage", sql.DateTime, null)
        .input("Berat", sql.Decimal(18, 3), Number(out.berat))
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("Blok", sql.VarChar(50), refRow.Blok ?? null)
        .input("IdLokasi", sql.Int, refRow.IdLokasi ?? null)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate).query(`
          INSERT INTO dbo.Crusher (
            NoCrusher, DateCreate, IdCrusher, IdWarehouse, DateUsage,
            Berat, IdStatus, Blok, IdLokasi, CreateBy, DateTimeCreate
          )
          VALUES (
            @NoCrusher, @DateCreate, @IdCrusher, @IdWarehouse, @DateUsage,
            @Berat, @IdStatus, @Blok, @IdLokasi, @CreateBy, @DateTimeCreate
          )
        `);

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoCrusher", sql.VarChar(50), newNoCrusher).query(`
          INSERT INTO dbo.BongkarSusunOutputCrusher (NoBongkarSusun, NoCrusher)
          VALUES (@NoBongkarSusun, @NoCrusher)
        `);

      createdOutputs.push({
        noCrusher: newNoCrusher,
        idCrusher: Number(out.idCrusher),
        berat: Number(out.berat),
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "crusher",
      totalBeratInput,
      totalBeratOutput,
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

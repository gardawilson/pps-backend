const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");

exports.createBongkarSusunBarangJadi = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }
  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output label");
  }

  for (const code of inputs) {
    if (detectCategory(code) !== "barangJadi") {
      throw badReq(`Label input ${code} bukan kategori barangJadi`);
    }
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    if (!out.idBJ || !Number.isFinite(Number(out.idBJ)) || Number(out.idBJ) <= 0) {
      throw badReq(`outputs[${i}].idBJ wajib diisi`);
    }
    if (out.pcs == null || !Number.isFinite(Number(out.pcs)) || Number(out.pcs) <= 0) {
      throw badReq(`outputs[${i}].pcs wajib diisi dan lebih dari 0`);
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

    const totalPcsInput = inputDataRes.recordset.reduce(
      (sum, row) => sum + Number(row.AvailablePcs || 0),
      0,
    );

    const refRow = inputDataRes.recordset[0];
    const refIdBJ = Number(refRow.IdBJ);

    for (const row of inputDataRes.recordset) {
      if (Number(row.IdBJ) !== refIdBJ) {
        throw badReq(
          "Semua input barangJadi harus memiliki IdBJ yang sama",
        );
      }
    }

    const inputByJenis = {};
    for (const row of inputDataRes.recordset) {
      const k = refIdBJ;
      if (!inputByJenis[k]) inputByJenis[k] = { pcs: 0 };
      inputByJenis[k].pcs += Number(row.AvailablePcs || 0);
    }

    const outputByJenis = {};
    for (const out of outputs) {
      const k = Number(out.idBJ);
      if (k !== refIdBJ) {
        throw badReq(
          `outputs.idBJ harus sama dengan IdBJ input (${refIdBJ})`,
        );
      }
      if (!outputByJenis[k]) outputByJenis[k] = { pcs: 0 };
      outputByJenis[k].pcs += Number(out.pcs);
    }

    const totalPcsOutput = outputs.reduce(
      (sum, out) => sum + Number(out.pcs),
      0,
    );

    for (const idJenis of Object.keys(outputByJenis)) {
      if (!(idJenis in inputByJenis)) {
        throw badReq(`idBJ=${idJenis} pada output tidak ada di input manapun`);
      }
    }

    for (const [idJenis, valInput] of Object.entries(inputByJenis)) {
      const valOutput = outputByJenis[idJenis] || { pcs: 0 };
      if (Math.abs(valInput.pcs - valOutput.pcs) > 0.001) {
        throw badReq(
          `Pcs tidak balance untuk idBJ=${idJenis}: input=${valInput.pcs}, output=${valOutput.pcs}`,
        );
      }
    }

    if (Math.abs(totalPcsInput - totalPcsOutput) > 0.001) {
      throw badReq(
        `Total pcs tidak balance: input=${totalPcsInput}, output=${totalPcsOutput}`,
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
        INSERT INTO dbo.BongkarSusunInputBarangJadi (NoBongkarSusun, NoBJ)
        SELECT @NoBongkarSusun, j.code
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
        .query(`SELECT 1 FROM dbo.BarangJadi WITH (UPDLOCK,HOLDLOCK) WHERE NoBJ=@No`);
      if (exist.recordset.length > 0) {
        newNoBJ = await genBj();
        const exist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoBJ)
          .query(`SELECT 1 FROM dbo.BarangJadi WITH (UPDLOCK,HOLDLOCK) WHERE NoBJ=@No`);
        if (exist2.recordset.length > 0) {
          throw conflict("Gagal generate NoBJ unik, coba lagi");
        }
      }

      await new sql.Request(tx)
        .input("NoBJ", sql.VarChar(50), newNoBJ)
        .input("DateCreate", sql.Date, nowDate)
        .input("Jam", sql.VarChar(20), null)
        .input("Pcs", sql.Int, Math.trunc(Number(out.pcs)))
        .input("IdBJ", sql.Int, refIdBJ)
        .input("Berat", sql.Decimal(18, 3), 0)
        .input("IsPartial", sql.Bit, 0)
        .input("DateUsage", sql.Date, null)
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse ?? null)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate)
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
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoBJ", sql.VarChar(50), newNoBJ).query(`
          INSERT INTO dbo.BongkarSusunOutputBarangjadi (NoBongkarSusun, NoBJ)
          VALUES (@NoBongkarSusun, @NoBJ)
        `);

      createdOutputs.push({
        noBJ: newNoBJ,
        idBJ: Number(out.idBJ),
        pcs: Number(out.pcs),
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
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

const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");

exports.createBongkarSusunGilingan = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }
  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output label");
  }

  for (const code of inputs) {
    if (detectCategory(code) !== "gilingan") {
      throw badReq(`Label input ${code} bukan kategori gilingan`);
    }
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    if (
      !out.idGilingan ||
      !Number.isFinite(Number(out.idGilingan)) ||
      Number(out.idGilingan) <= 0
    ) {
      throw badReq(`outputs[${i}].idGilingan wajib diisi`);
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

    const inputDataRes = await new sql.Request(tx)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson)
      .query(`
        SELECT
          g.NoGilingan,
          g.IdGilingan,
          g.IdWarehouse,
          g.IdStatus,
          g.Berat,
          g.IsPartial,
          g.Blok,
          g.IdLokasi,
          CASE
            WHEN g.IsPartial = 1 THEN
              CASE
                WHEN ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0) < 0
                  THEN 0
                ELSE ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0)
              END
            ELSE ISNULL(g.Berat, 0)
          END AS AvailableBerat
        FROM dbo.Gilingan g WITH (UPDLOCK, HOLDLOCK)
        LEFT JOIN (
          SELECT NoGilingan, SUM(ISNULL(Berat, 0)) AS TotalPartial
          FROM dbo.GilinganPartial
          GROUP BY NoGilingan
        ) gp
          ON gp.NoGilingan = g.NoGilingan
        WHERE g.NoGilingan IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND g.DateUsage IS NULL
      `);

    if (inputDataRes.recordset.length !== inputs.length) {
      throw badReq("Satu atau lebih label input tidak ditemukan atau sudah terpakai");
    }

    const totalBeratInput = inputDataRes.recordset.reduce(
      (sum, row) => sum + Number(row.AvailableBerat || 0),
      0,
    );

    const inputByJenis = {};
    for (const row of inputDataRes.recordset) {
      const k = Number(row.IdGilingan);
      inputByJenis[k] = (inputByJenis[k] || 0) + Number(row.AvailableBerat || 0);
    }

    const outputByJenis = {};
    for (const out of outputs) {
      const k = Number(out.idGilingan);
      outputByJenis[k] = (outputByJenis[k] || 0) + Number(out.berat);
    }

    const totalBeratOutput = outputs.reduce(
      (sum, out) => sum + Number(out.berat),
      0,
    );

    for (const idJenis of Object.keys(outputByJenis)) {
      if (!(idJenis in inputByJenis)) {
        throw badReq(
          `idGilingan=${idJenis} pada output tidak ada di input manapun`,
        );
      }
    }

    for (const [idJenis, beratInput] of Object.entries(inputByJenis)) {
      const beratOutput = outputByJenis[idJenis] || 0;
      if (Math.abs(beratInput - beratOutput) > 0.001) {
        throw badReq(
          `Berat tidak balance untuk idGilingan=${idJenis}: input=${beratInput}kg, output=${beratOutput}kg`,
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
        INSERT INTO dbo.BongkarSusunInputGilingan (NoBongkarSusun, NoGilingan)
        SELECT @NoBongkarSusun, j.code
        FROM OPENJSON(@CodesJson)
        WITH (code varchar(50) '$.code') AS j
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.Gilingan
        SET DateUsage = @Tanggal
        WHERE NoGilingan IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    const refRow = inputDataRes.recordset[0];
    const createdOutputs = [];

    for (const out of outputs) {
      const genGilingan = () =>
        generateNextCode(tx, {
          tableName: "Gilingan",
          columnName: "NoGilingan",
          prefix: "V.",
          width: 10,
        });

      let newNoGilingan = await genGilingan();
      const exist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoGilingan)
        .query(
          `SELECT 1 FROM dbo.Gilingan WITH (UPDLOCK,HOLDLOCK) WHERE NoGilingan=@No`,
        );
      if (exist.recordset.length > 0) {
        newNoGilingan = await genGilingan();
        const exist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoGilingan)
          .query(
            `SELECT 1 FROM dbo.Gilingan WITH (UPDLOCK,HOLDLOCK) WHERE NoGilingan=@No`,
          );
        if (exist2.recordset.length > 0) {
          throw conflict("Gagal generate NoGilingan unik, coba lagi");
        }
      }

      await new sql.Request(tx)
        .input("NoGilingan", sql.VarChar(50), newNoGilingan)
        .input("DateCreate", sql.Date, nowDate)
        .input("IdGilingan", sql.Int, Math.trunc(Number(out.idGilingan)))
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse)
        .input("DateUsage", sql.DateTime, null)
        .input("Berat", sql.Decimal(18, 3), Number(out.berat))
        .input("IsPartial", sql.Bit, out.isPartial ?? refRow.IsPartial ?? 0)
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("Blok", sql.VarChar(50), refRow.Blok ?? null)
        .input("IdLokasi", sql.Int, refRow.IdLokasi ?? null)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate).query(`
          INSERT INTO dbo.Gilingan (
            NoGilingan, DateCreate, IdGilingan, DateUsage,
            Berat, IsPartial, IdStatus, Blok, IdLokasi,
            CreateBy, DateTimeCreate
          )
          VALUES (
            @NoGilingan, @DateCreate, @IdGilingan, @DateUsage,
            @Berat, @IsPartial, @IdStatus, @Blok, @IdLokasi,
            @CreateBy, @DateTimeCreate
          )
        `);

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoGilingan", sql.VarChar(50), newNoGilingan).query(`
          INSERT INTO dbo.BongkarSusunOutputGilingan (NoBongkarSusun, NoGilingan)
          VALUES (@NoBongkarSusun, @NoGilingan)
        `);

      createdOutputs.push({
        noGilingan: newNoGilingan,
        idGilingan: Number(out.idGilingan),
        berat: Number(out.berat),
        isPartial: out.isPartial ?? refRow.IsPartial ?? 0,
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "gilingan",
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

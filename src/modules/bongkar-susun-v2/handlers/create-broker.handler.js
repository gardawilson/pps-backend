const { sql, poolPromise } = require("../../../core/config/db");
const { generateNextCode } = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const {
  detectCategory,
} = require("../bongkar-susun-v2-category-registry");
exports.createBongkarSusunBroker = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0)
    throw badReq("inputs wajib berisi minimal 1 label");
  if (!Array.isArray(outputs) || outputs.length === 0)
    throw badReq("outputs wajib berisi minimal 1 output label");

  for (const code of inputs) {
    if (detectCategory(code) !== "broker")
      throw badReq(`Label input ${code} bukan kategori broker`);
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    if (
      !out.idJenis ||
      !Number.isFinite(Number(out.idJenis)) ||
      Number(out.idJenis) <= 0
    )
      throw badReq(`outputs[${i}].idJenis wajib diisi`);
    if (!Array.isArray(out.saks) || out.saks.length === 0)
      throw badReq(`outputs[${i}].saks wajib berisi minimal 1 sak`);
    for (const sak of out.saks) {
      if (
        sak.noSak == null ||
        !Number.isFinite(Number(sak.noSak)) ||
        Number(sak.noSak) <= 0
      )
        throw badReq(`noSak tidak valid di outputs[${i}]`);
      if (
        sak.berat == null ||
        !Number.isFinite(Number(sak.berat)) ||
        Number(sak.berat) <= 0
      )
        throw badReq(`berat tidak valid di outputs[${i}]`);
    }
    const sakSet = new Set();
    for (const sak of out.saks) {
      const k = String(Math.trunc(Number(sak.noSak)));
      if (sakSet.has(k))
        throw badReq(`noSak duplikat di outputs[${i}]: ${sak.noSak}`);
      sakSet.add(k);
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
          h.NoBroker,
          h.IdJenisPlastik,
          h.IdWarehouse,
          h.IdStatus,
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
          h.Blok,
          h.IdLokasi,
          h.HasBeenPrinted,
          SUM(d.Berat) AS TotalBerat
        FROM dbo.Broker_h h WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN dbo.Broker_d d WITH (UPDLOCK, HOLDLOCK)
          ON d.NoBroker = h.NoBroker AND d.DateUsage IS NULL
        WHERE h.NoBroker IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        GROUP BY
          h.NoBroker, h.IdJenisPlastik, h.IdWarehouse, h.IdStatus,
          h.Density, h.Moisture, h.MaxMeltTemp, h.MinMeltTemp, h.MFI, h.VisualNote,
          h.Density2, h.Density3, h.Moisture2, h.Moisture3, h.Blok, h.IdLokasi,
          h.HasBeenPrinted
      `);

    if (inputDataRes.recordset.length !== inputs.length)
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );

    const totalBeratInput = inputDataRes.recordset.reduce(
      (sum, r) => sum + (r.TotalBerat || 0),
      0,
    );

    const totalBeratOutput = outputs.reduce(
      (sum, out) => sum + out.saks.reduce((s, sak) => s + Number(sak.berat), 0),
      0,
    );

    const inputByJenis = {};
    for (const row of inputDataRes.recordset) {
      const k = row.IdJenisPlastik;
      inputByJenis[k] = (inputByJenis[k] || 0) + (row.TotalBerat || 0);
    }
    const outputByJenis = {};
    for (const out of outputs) {
      const k = Number(out.idJenis);
      const beratOut = out.saks.reduce((s, sak) => s + Number(sak.berat), 0);
      outputByJenis[k] = (outputByJenis[k] || 0) + beratOut;
    }

    for (const idJenis of Object.keys(outputByJenis)) {
      if (!(idJenis in inputByJenis))
        throw badReq(
          `idJenis=${idJenis} pada output tidak ada di input manapun`,
        );
    }

    for (const [idJenis, beratInput] of Object.entries(inputByJenis)) {
      const beratOutput = outputByJenis[idJenis] || 0;
      if (Math.abs(beratInput - beratOutput) > 0.001) {
        throw badReq(
          `Berat tidak balance untuk idJenis=${idJenis}: input=${beratInput}kg, output=${beratOutput}kg`,
        );
      }
    }

    if (Math.abs(totalBeratInput - totalBeratOutput) > 0.001)
      throw badReq(
        `Total berat tidak balance: input=${totalBeratInput}kg, output=${totalBeratOutput}kg`,
      );

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
        `SELECT 1 FROM BongkarSusun_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBongkarSusun=@No`,
      );
    if (bgExist.recordset.length > 0) {
      noBongkarSusun = await genBg();
      const bgExist2 = await new sql.Request(tx)
        .input("No", sql.VarChar(50), noBongkarSusun)
        .query(
          `SELECT 1 FROM BongkarSusun_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBongkarSusun=@No`,
        );
      if (bgExist2.recordset.length > 0)
        throw conflict("Gagal generate NoBongkarSusun unik, coba lagi");
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
        INSERT INTO dbo.BongkarSusunInputBroker (NoBongkarSusun, NoBroker, NoSak)
        SELECT @NoBongkarSusun, d.NoBroker, d.NoSak
        FROM dbo.Broker_d d
        WHERE d.NoBroker IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND d.DateUsage IS NULL
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.Broker_d
        SET DateUsage = @Tanggal
        WHERE NoBroker IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    const refRow = inputDataRes.recordset[0];
    const createdOutputs = [];

    for (const out of outputs) {
      const genD = () =>
        generateNextCode(tx, {
          tableName: "Broker_h",
          columnName: "NoBroker",
          prefix: "D.",
          width: 10,
        });

      let newNoBroker = await genD();
      const dExist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoBroker)
        .query(
          `SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBroker=@No`,
        );
      if (dExist.recordset.length > 0) {
        newNoBroker = await genD();
        const dExist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoBroker)
          .query(
            `SELECT 1 FROM dbo.Broker_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBroker=@No`,
          );
        if (dExist2.recordset.length > 0)
          throw conflict("Gagal generate NoBroker unik, coba lagi");
      }

      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), newNoBroker)
        .input("IdJenisPlastik", sql.Int, Math.trunc(Number(out.idJenis)))
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse)
        .input("DateCreate", sql.Date, nowDate)
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate)
        .input("Density", sql.Decimal(10, 3), refRow.Density ?? null)
        .input("Moisture", sql.Decimal(10, 3), refRow.Moisture ?? null)
        .input("MaxMeltTemp", sql.Decimal(10, 3), refRow.MaxMeltTemp ?? null)
        .input("MinMeltTemp", sql.Decimal(10, 3), refRow.MinMeltTemp ?? null)
        .input("MFI", sql.Decimal(10, 3), refRow.MFI ?? null)
        .input("VisualNote", sql.VarChar(sql.MAX), refRow.VisualNote ?? null)
        .input("Density2", sql.Decimal(10, 3), refRow.Density2 ?? null)
        .input("Density3", sql.Decimal(10, 3), refRow.Density3 ?? null)
        .input("Moisture2", sql.Decimal(10, 3), refRow.Moisture2 ?? null)
        .input("Moisture3", sql.Decimal(10, 3), refRow.Moisture3 ?? null)
        .input("Blok", sql.VarChar(50), "BSS")
        .input("IdLokasi", sql.Int, refRow.IdLokasi ?? 1).query(`
          INSERT INTO dbo.Broker_h (
            NoBroker, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy, DateTimeCreate,
            Density, Moisture, MaxMeltTemp, MinMeltTemp, MFI, VisualNote,
            Density2, Density3, Moisture2, Moisture3, Blok, IdLokasi
          )
          VALUES (
            @NoBroker, @IdJenisPlastik, @IdWarehouse, @DateCreate, @IdStatus, @CreateBy, @DateTimeCreate,
            @Density, @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @VisualNote,
            @Density2, @Density3, @Moisture2, @Moisture3, @Blok, @IdLokasi
          )
        `);

      const normalizedSaks = out.saks.map((s) => ({
        NoSak: Math.trunc(Number(s.noSak)),
        Berat: Number(s.berat),
      }));
      const saksJson = JSON.stringify(normalizedSaks);

      await new sql.Request(tx)
        .input("NoBroker", sql.VarChar(50), newNoBroker)
        .input("IdLokasi", sql.Int, refRow.IdLokasi ?? 1)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.Broker_d (NoBroker, NoSak, Berat, DateUsage, IsPartial, IdLokasi)
          SELECT @NoBroker, j.NoSak, j.Berat, NULL, 0, @IdLokasi
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak', Berat decimal(18,3) '$.Berat') AS j
        `);

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoBroker", sql.VarChar(50), newNoBroker)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.BongkarSusunOutputBroker (NoBongkarSusun, NoBroker, NoSak)
          SELECT @NoBongkarSusun, @NoBroker, j.NoSak
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak') AS j
        `);

      createdOutputs.push({
        noBroker: newNoBroker,
        jumlahSak: normalizedSaks.length,
        totalBerat: normalizedSaks.reduce((s, x) => s + x.Berat, 0),
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "broker",
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



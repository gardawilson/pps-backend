const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");

exports.createBongkarSusunMixer = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }
  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output label");
  }

  for (const code of inputs) {
    if (detectCategory(code) !== "mixer") {
      throw badReq(`Label input ${code} bukan kategori mixer`);
    }
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    const idJenis = out.idJenis ?? out.idMixer;
    if (!idJenis || !Number.isFinite(Number(idJenis)) || Number(idJenis) <= 0) {
      throw badReq(`outputs[${i}].idJenis wajib diisi`);
    }
    if (!Array.isArray(out.saks) || out.saks.length === 0) {
      throw badReq(`outputs[${i}].saks wajib berisi minimal 1 sak`);
    }
    const sakSet = new Set();
    for (const sak of out.saks) {
      if (
        sak.noSak == null ||
        !Number.isFinite(Number(sak.noSak)) ||
        Number(sak.noSak) <= 0
      ) {
        throw badReq(`noSak tidak valid di outputs[${i}]`);
      }
      if (
        sak.berat == null ||
        !Number.isFinite(Number(sak.berat)) ||
        Number(sak.berat) <= 0
      ) {
        throw badReq(`berat tidak valid di outputs[${i}]`);
      }
      const k = String(Math.trunc(Number(sak.noSak)));
      if (sakSet.has(k)) {
        throw badReq(`noSak duplikat di outputs[${i}]: ${sak.noSak}`);
      }
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

    const inputDataRes = await new sql.Request(tx)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson)
      .query(`
        SELECT
          h.NoMixer,
          h.IdMixer,
          h.IdStatus,
          h.Moisture,
          h.MaxMeltTemp,
          h.MinMeltTemp,
          h.MFI,
          h.Moisture2,
          h.Moisture3,
          h.Blok,
          h.IdLokasi,
          SUM(ISNULL(d.Berat, 0) - ISNULL(mp.TotalPartial, 0)) AS AvailableBerat
        FROM dbo.Mixer_h h WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN dbo.Mixer_d d WITH (UPDLOCK, HOLDLOCK)
          ON d.NoMixer = h.NoMixer
         AND d.DateUsage IS NULL
        LEFT JOIN (
          SELECT
            NoMixer,
            NoSak,
            SUM(ISNULL(Berat, 0)) AS TotalPartial
          FROM dbo.MixerPartial
          GROUP BY NoMixer, NoSak
        ) mp
          ON mp.NoMixer = d.NoMixer
         AND mp.NoSak = d.NoSak
        WHERE h.NoMixer IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        GROUP BY
          h.NoMixer,
          h.IdMixer,
          h.IdStatus,
          h.Moisture,
          h.MaxMeltTemp,
          h.MinMeltTemp,
          h.MFI,
          h.Moisture2,
          h.Moisture3,
          h.Blok,
          h.IdLokasi
      `);

    if (inputDataRes.recordset.length !== inputs.length) {
      throw badReq("Satu atau lebih label input tidak ditemukan atau sudah terpakai");
    }

    const inputJenisSet = new Set(
      inputDataRes.recordset.map((row) => Number(row.IdMixer)),
    );
    if (inputJenisSet.size !== 1) {
      throw badReq("Semua input mixer harus memiliki idJenis yang sama");
    }
    const inputIdJenis = Array.from(inputJenisSet)[0];

    const totalBeratInput = inputDataRes.recordset.reduce(
      (sum, row) => sum + Number(row.AvailableBerat || 0),
      0,
    );

    const inputByJenis = { [inputIdJenis]: totalBeratInput };

    const outputByJenis = {};
    for (const out of outputs) {
      const k = Number(out.idJenis ?? out.idMixer);
      const beratOut = out.saks.reduce((s, sak) => s + Number(sak.berat), 0);
      outputByJenis[k] = (outputByJenis[k] || 0) + beratOut;
    }

    const totalBeratOutput = outputs.reduce(
      (sum, out) => sum + out.saks.reduce((s, sak) => s + Number(sak.berat), 0),
      0,
    );

    for (const idJenis of Object.keys(outputByJenis)) {
      if (!(idJenis in inputByJenis)) {
        throw badReq(
          `idJenis=${idJenis} pada output tidak ada di input manapun`,
        );
      }
    }

    for (const [idJenis, beratInput] of Object.entries(inputByJenis)) {
      const beratOutput = outputByJenis[idJenis] || 0;
      if (Math.abs(beratInput - beratOutput) > 0.001) {
        throw badReq(
          `Berat tidak balance untuk idJenis=${idJenis}: input=${beratInput}kg, output=${beratOutput}kg`,
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
    const refRow = inputDataRes.recordset[0];

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
        INSERT INTO dbo.BongkarSusunInputMixer (NoBongkarSusun, NoMixer, NoSak)
        SELECT @NoBongkarSusun, d.NoMixer, d.NoSak
        FROM dbo.Mixer_d d
        WHERE d.NoMixer IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND d.DateUsage IS NULL
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.Mixer_d
        SET DateUsage = @Tanggal
        WHERE NoMixer IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    const createdOutputs = [];

    for (const out of outputs) {
      const genMixer = () =>
        generateNextCode(tx, {
          tableName: "Mixer_h",
          columnName: "NoMixer",
          prefix: "H.",
          width: 10,
        });

      let newNoMixer = await genMixer();
      const exist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoMixer)
        .query(
          `SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK,HOLDLOCK) WHERE NoMixer=@No`,
        );
      if (exist.recordset.length > 0) {
        newNoMixer = await genMixer();
        const exist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoMixer)
          .query(
            `SELECT 1 FROM dbo.Mixer_h WITH (UPDLOCK,HOLDLOCK) WHERE NoMixer=@No`,
          );
        if (exist2.recordset.length > 0) {
          throw conflict("Gagal generate NoMixer unik, coba lagi");
        }
      }

      const normalizedSaks = out.saks.map((s) => ({
        NoSak: Math.trunc(Number(s.noSak)),
        Berat: Number(s.berat),
      }));
      const saksJson = JSON.stringify(normalizedSaks);
      const outputIdJenis = Math.trunc(Number(out.idJenis ?? out.idMixer));

      await new sql.Request(tx)
        .input("NoMixer", sql.VarChar(50), newNoMixer)
        .input("DateCreate", sql.Date, nowDate)
        .input("IdMixer", sql.Int, outputIdJenis)
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate)
        .input("Moisture", sql.Decimal(10, 3), refRow.Moisture ?? null)
        .input("MaxMeltTemp", sql.Decimal(10, 3), refRow.MaxMeltTemp ?? null)
        .input("MinMeltTemp", sql.Decimal(10, 3), refRow.MinMeltTemp ?? null)
        .input("MFI", sql.Decimal(10, 3), refRow.MFI ?? null)
        .input("Moisture2", sql.Decimal(10, 3), refRow.Moisture2 ?? null)
        .input("Moisture3", sql.Decimal(10, 3), refRow.Moisture3 ?? null)
        .input("Blok", sql.VarChar(50), refRow.Blok ?? null)
        .input("IdLokasi", sql.Int, refRow.IdLokasi ?? null).query(`
          INSERT INTO dbo.Mixer_h (
            NoMixer, IdMixer, DateCreate, IdStatus, CreateBy, DateTimeCreate,
            Moisture, MaxMeltTemp, MinMeltTemp, MFI, Moisture2, Moisture3, Blok, IdLokasi
          )
          VALUES (
            @NoMixer, @IdMixer, @DateCreate, @IdStatus, @CreateBy, @DateTimeCreate,
            @Moisture, @MaxMeltTemp, @MinMeltTemp, @MFI, @Moisture2, @Moisture3, @Blok, @IdLokasi
          )
        `);

      await new sql.Request(tx)
        .input("NoMixer", sql.VarChar(50), newNoMixer)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.Mixer_d (NoMixer, NoSak, Berat, DateUsage, IsPartial)
          SELECT @NoMixer, j.NoSak, j.Berat, NULL, 0
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak', Berat decimal(18,3) '$.Berat') AS j
        `);

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoMixer", sql.VarChar(50), newNoMixer)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.BongkarSusunOutputMixer (NoBongkarSusun, NoMixer, NoSak)
          SELECT @NoBongkarSusun, @NoMixer, j.NoSak
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak') AS j
        `);

      createdOutputs.push({
        noMixer: newNoMixer,
        idJenis: outputIdJenis,
        jumlahSak: normalizedSaks.length,
        totalBerat: normalizedSaks.reduce((s, x) => s + x.Berat, 0),
        saks: normalizedSaks,
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "mixer",
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

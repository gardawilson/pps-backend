const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");
exports.createBongkarSusunWashing = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0)
    throw badReq("inputs wajib berisi minimal 1 label");
  if (!Array.isArray(outputs) || outputs.length === 0)
    throw badReq("outputs wajib berisi minimal 1 output label");

  for (const code of inputs) {
    if (detectCategory(code) !== "washing")
      throw badReq(`Label input ${code} bukan kategori washing`);
  }

  // Validasi struktur outputs
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

    // [AUDIT CTX]
    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    // â”€â”€ 1. Ambil data semua input label (LOCK) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const inputCodesJson = JSON.stringify(inputs.map((c) => ({ code: c })));

    const inputDataRes = await new sql.Request(tx).input(
      "CodesJson",
      sql.NVarChar(sql.MAX),
      inputCodesJson,
    ).query(`
        SELECT
          h.NoWashing,
          h.IdJenisPlastik,
          h.IdWarehouse,
          h.IdStatus,
          h.Density, h.Moisture,
          h.Density2, h.Moisture2,
          h.Density3, h.Moisture3,
          SUM(d.Berat) AS TotalBerat
        FROM Washing_h h WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN Washing_d d WITH (UPDLOCK, HOLDLOCK)
          ON d.NoWashing = h.NoWashing AND d.DateUsage IS NULL
        WHERE h.NoWashing IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        GROUP BY
          h.NoWashing, h.IdJenisPlastik, h.IdWarehouse, h.IdStatus,
          h.Density, h.Moisture, h.Density2, h.Moisture2, h.Density3, h.Moisture3
      `);

    if (inputDataRes.recordset.length !== inputs.length)
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );

    const totalBeratInput = inputDataRes.recordset.reduce(
      (sum, r) => sum + (r.TotalBerat || 0),
      0,
    );

    // â”€â”€ 3. Validasi total berat balance â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const totalBeratOutput = outputs.reduce(
      (sum, out) => sum + out.saks.reduce((s, sak) => s + Number(sak.berat), 0),
      0,
    );

    // Validasi balance PER idJenis
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

    // Cek idJenis output tidak ada di input
    for (const idJenis of Object.keys(outputByJenis)) {
      if (!(idJenis in inputByJenis))
        throw badReq(
          `idJenis=${idJenis} pada output tidak ada di input manapun`,
        );
    }

    // Cek berat per idJenis harus balance
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

    // â”€â”€ 4. Generate NoBongkarSusun â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    // â”€â”€ 5. Insert BongkarSusun_h â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const nowDate = new Date();
    await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("Tanggal", sql.DateTime, nowDate)
      .input("IdUsername", sql.Int, actorId)
      .input("Note", sql.NVarChar(500), note || null).query(`
        INSERT INTO dbo.BongkarSusun_h (NoBongkarSusun, Tanggal, IdUsername, Note)
        VALUES (@NoBongkarSusun, @Tanggal, @IdUsername, @Note)
      `);

    // â”€â”€ 6. Catat input labels ke BongkarSusunInputWashing (per NoSak) â”€â”€â”€â”€â”€â”€â”€
    await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        INSERT INTO dbo.BongkarSusunInputWashing (NoBongkarSusun, NoWashing, NoSak)
        SELECT @NoBongkarSusun, d.NoWashing, d.NoSak
        FROM dbo.Washing_d d
        WHERE d.NoWashing IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND d.DateUsage IS NULL
      `);

    // â”€â”€ 7. Mark input saks sebagai terpakai â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.Washing_d
        SET DateUsage = @Tanggal
        WHERE NoWashing IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    // â”€â”€ 8. Buat output labels â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const refRow = inputDataRes.recordset[0];
    const createdOutputs = [];

    for (const out of outputs) {
      const genW = () =>
        generateNextCode(tx, {
          tableName: "Washing_h",
          columnName: "NoWashing",
          prefix: "B.",
          width: 10,
        });

      let newNoWashing = await genW();
      const wExist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoWashing)
        .query(
          `SELECT 1 FROM Washing_h WITH (UPDLOCK,HOLDLOCK) WHERE NoWashing=@No`,
        );
      if (wExist.recordset.length > 0) {
        newNoWashing = await genW();
        const wExist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoWashing)
          .query(
            `SELECT 1 FROM Washing_h WITH (UPDLOCK,HOLDLOCK) WHERE NoWashing=@No`,
          );
        if (wExist2.recordset.length > 0)
          throw conflict("Gagal generate NoWashing unik, coba lagi");
      }

      // Insert Washing_h â€” metadata diwarisi dari input, override: IdJenisPlastik, DateCreate, CreateBy, Blok, IdLokasi
      await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), newNoWashing)
        .input("IdJenisPlastik", sql.Int, Math.trunc(Number(out.idJenis)))
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse)
        .input("DateCreate", sql.Date, nowDate)
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate)
        .input("Density", sql.Decimal(10, 3), refRow.Density ?? null)
        .input("Moisture", sql.Decimal(10, 3), refRow.Moisture ?? null)
        .input("Density2", sql.Decimal(10, 3), refRow.Density2 ?? null)
        .input("Density3", sql.Decimal(10, 3), refRow.Density3 ?? null)
        .input("Moisture2", sql.Decimal(10, 3), refRow.Moisture2 ?? null)
        .input("Moisture3", sql.Decimal(10, 3), refRow.Moisture3 ?? null)
        .input("Blok", sql.VarChar(50), "BSS")
        .input("IdLokasi", sql.Int, 1).query(`
          INSERT INTO dbo.Washing_h (
            NoWashing, IdJenisPlastik, IdWarehouse, DateCreate, IdStatus,
            CreateBy, DateTimeCreate, Density, Moisture, Density2, Density3,
            Moisture2, Moisture3, Blok, IdLokasi
          ) VALUES (
            @NoWashing, @IdJenisPlastik, @IdWarehouse, @DateCreate, @IdStatus,
            @CreateBy, @DateTimeCreate, @Density, @Moisture, @Density2, @Density3,
            @Moisture2, @Moisture3, @Blok, @IdLokasi
          )
        `);

      // Insert Washing_d (bulk)
      const normalizedSaks = out.saks.map((s) => ({
        NoSak: Math.trunc(Number(s.noSak)),
        Berat: Number(s.berat),
      }));
      const saksJson = JSON.stringify(normalizedSaks);

      await new sql.Request(tx)
        .input("NoWashing", sql.VarChar(50), newNoWashing)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.Washing_d (NoWashing, NoSak, Berat, DateUsage)
          SELECT @NoWashing, j.NoSak, j.Berat, NULL
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak', Berat decimal(18,3) '$.Berat') AS j
        `);

      // Insert BongkarSusunOutputWashing (per NoSak)
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoWashing", sql.VarChar(50), newNoWashing)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.BongkarSusunOutputWashing (NoBongkarSusun, NoWashing, NoSak)
          SELECT @NoBongkarSusun, @NoWashing, j.NoSak
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak') AS j
        `);

      createdOutputs.push({
        noWashing: newNoWashing,
        jumlahSak: normalizedSaks.length,
        totalBerat: normalizedSaks.reduce((s, x) => s + x.Berat, 0),
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "washing",
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

// â”€â”€â”€ POST â€” buat transaksi bongkar susun bonggolan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

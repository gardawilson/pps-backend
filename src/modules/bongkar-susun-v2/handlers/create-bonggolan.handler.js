const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");

exports.createBongkarSusunBonggolan = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0)
    throw badReq("inputs wajib berisi minimal 1 label");
  if (!Array.isArray(outputs) || outputs.length === 0)
    throw badReq("outputs wajib berisi minimal 1 output label");

  for (const code of inputs) {
    if (detectCategory(code) !== "bonggolan")
      throw badReq(`Label input ${code} bukan kategori bonggolan`);
  }

  // Validasi struktur outputs â€” setiap output = 1 label bonggolan baru
  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    if (
      !out.idJenis ||
      !Number.isFinite(Number(out.idJenis)) ||
      Number(out.idJenis) <= 0
    )
      throw badReq(`outputs[${i}].idJenis wajib diisi`);
    if (
      out.berat == null ||
      !Number.isFinite(Number(out.berat)) ||
      Number(out.berat) <= 0
    )
      throw badReq(`outputs[${i}].berat wajib diisi dan lebih dari 0`);
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

    // â”€â”€ 1. Ambil data semua input label bonggolan (LOCK) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const inputCodesJson = JSON.stringify(inputs.map((c) => ({ code: c })));

    const inputDataRes = await new sql.Request(tx).input(
      "CodesJson",
      sql.NVarChar(sql.MAX),
      inputCodesJson,
    ).query(`
        SELECT
          NoBonggolan,
          IdBonggolan,
          IdWarehouse,
          IdStatus,
          Berat
        FROM dbo.Bonggolan WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBonggolan IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    if (inputDataRes.recordset.length !== inputs.length)
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );

    // â”€â”€ 2. Hitung total berat per idJenis dari input â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const inputByJenis = {};
    for (const row of inputDataRes.recordset) {
      const k = row.IdBonggolan;
      inputByJenis[k] = (inputByJenis[k] || 0) + (row.Berat || 0);
    }
    const totalBeratInput = inputDataRes.recordset.reduce(
      (sum, r) => sum + (r.Berat || 0),
      0,
    );

    // â”€â”€ 3. Validasi balance per idJenis â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const outputByJenis = {};
    for (const out of outputs) {
      const k = Number(out.idJenis);
      outputByJenis[k] = (outputByJenis[k] || 0) + Number(out.berat);
    }
    const totalBeratOutput = outputs.reduce(
      (sum, out) => sum + Number(out.berat),
      0,
    );

    for (const idJenis of Object.keys(outputByJenis)) {
      if (!(idJenis in inputByJenis))
        throw badReq(
          `idJenis=${idJenis} pada output tidak ada di input manapun`,
        );
    }
    for (const [idJenis, beratInput] of Object.entries(inputByJenis)) {
      const beratOutput = outputByJenis[idJenis] || 0;
      if (Math.abs(beratInput - beratOutput) > 0.001)
        throw badReq(
          `Berat tidak balance untuk idJenis=${idJenis}: input=${beratInput}kg, output=${beratOutput}kg`,
        );
    }

    // â”€â”€ 4. Generate NoBongkarSusun â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    // â”€â”€ 6. Catat input ke BongkarSusunInputBonggolan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        INSERT INTO dbo.BongkarSusunInputBonggolan (NoBongkarSusun, NoBonggolan)
        SELECT @NoBongkarSusun, j.code
        FROM OPENJSON(@CodesJson)
        WITH (code varchar(50) '$.code') AS j
      `);

    // â”€â”€ 7. Mark input bonggolan sebagai terpakai â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        UPDATE dbo.Bonggolan
        SET DateUsage = @Tanggal
        WHERE NoBonggolan IN (
          SELECT j.code FROM OPENJSON(@CodesJson)
          WITH (code varchar(50) '$.code') AS j
        )
        AND DateUsage IS NULL
      `);

    // â”€â”€ 8. Buat output bonggolan baru â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const refRow = inputDataRes.recordset[0];
    const createdOutputs = [];

    for (const out of outputs) {
      const genM = () =>
        generateNextCode(tx, {
          tableName: "Bonggolan",
          columnName: "NoBonggolan",
          prefix: "M.",
          width: 10,
        });

      let newNoBonggolan = await genM();
      const mExist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoBonggolan)
        .query(
          `SELECT 1 FROM dbo.Bonggolan WITH (UPDLOCK,HOLDLOCK) WHERE NoBonggolan=@No`,
        );
      if (mExist.recordset.length > 0) {
        newNoBonggolan = await genM();
        const mExist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoBonggolan)
          .query(
            `SELECT 1 FROM dbo.Bonggolan WITH (UPDLOCK,HOLDLOCK) WHERE NoBonggolan=@No`,
          );
        if (mExist2.recordset.length > 0)
          throw conflict("Gagal generate NoBonggolan unik, coba lagi");
      }

      await new sql.Request(tx)
        .input("NoBonggolan", sql.VarChar(50), newNoBonggolan)
        .input("DateCreate", sql.Date, nowDate)
        .input("IdBonggolan", sql.Int, Math.trunc(Number(out.idJenis)))
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse)
        .input("Berat", sql.Decimal(18, 3), Number(out.berat))
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate)
        .input("Blok", sql.VarChar(50), "BSS")
        .input("IdLokasi", sql.Int, 1).query(`
          INSERT INTO dbo.Bonggolan (
            NoBonggolan, DateCreate, IdBonggolan, IdWarehouse,
            Berat, IdStatus, CreateBy, DateTimeCreate, Blok, IdLokasi
          ) VALUES (
            @NoBonggolan, @DateCreate, @IdBonggolan, @IdWarehouse,
            @Berat, @IdStatus, @CreateBy, @DateTimeCreate, @Blok, @IdLokasi
          )
        `);

      // Insert BongkarSusunOutputBonggolan
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoBonggolan", sql.VarChar(50), newNoBonggolan).query(`
          INSERT INTO dbo.BongkarSusunOutputBonggolan (NoBongkarSusun, NoBonggolan)
          VALUES (@NoBongkarSusun, @NoBonggolan)
        `);

      createdOutputs.push({
        noBonggolan: newNoBonggolan,
        idJenis: Number(out.idJenis),
        berat: Number(out.berat),
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "bonggolan",
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

// â”€â”€â”€ DELETE transaksi â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

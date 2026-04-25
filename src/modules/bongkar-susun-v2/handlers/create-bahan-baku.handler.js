const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");
const { detectCategory } = require("../bongkar-susun-v2-category-registry");

function parseInputLabel(labelCode) {
  const code = String(labelCode || "").trim();
  if (!code.startsWith("A.")) return null;
  const raw = code.slice(2);
  const parts = raw.split("-");
  if (parts.length !== 2) return null;
  const noBahanBaku = `A.${parts[0].trim()}`;
  const noPallet = Number.parseInt(parts[1], 10);
  if (!noBahanBaku || !Number.isFinite(noPallet) || noPallet <= 0) return null;
  return { noBahanBaku, noPallet };
}

function normalizeSaks(saks, outputIndex) {
  if (!Array.isArray(saks) || saks.length === 0) {
    throw badReq(`outputs[${outputIndex}].saks wajib berisi minimal 1 sak`);
  }

  const seen = new Set();
  return saks.map((sak, sakIndex) => {
    const noSak = Number.parseInt(sak?.noSak, 10);
    const berat = Number(sak?.berat);

    if (!Number.isFinite(noSak) || noSak <= 0) {
      throw badReq(
        `outputs[${outputIndex}].saks[${sakIndex}].noSak wajib valid`,
      );
    }
    if (!Number.isFinite(berat) || berat <= 0) {
      throw badReq(
        `outputs[${outputIndex}].saks[${sakIndex}].berat wajib valid`,
      );
    }

    const key = String(noSak);
    if (seen.has(key)) {
      throw badReq(`noSak duplikat di outputs[${outputIndex}]: ${sak.noSak}`);
    }
    seen.add(key);

    return { NoSak: noSak, Berat: berat };
  });
}

exports.createBongkarSusunBahanBaku = async (payload, ctx) => {
  const { note, inputs, outputs } = payload;
  const { actorId, actorUsername, requestId } = ctx;

  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw badReq("inputs wajib berisi minimal 1 label");
  }
  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output label");
  }

  const seenInputs = new Set();
  const parsedInputs = inputs.map((code, index) => {
    if (detectCategory(code) !== "bahanBaku") {
      throw badReq(`Label input ${code} bukan kategori bahanBaku`);
    }
    const parsed = parseInputLabel(code);
    if (!parsed) {
      throw badReq(
        `Label input ${code} harus memakai format A.<NoBahanBaku>-<NoPallet>`,
      );
    }
    const key = `${parsed.noBahanBaku}-${parsed.noPallet}`;
    if (seenInputs.has(key)) {
      throw badReq(`Label input duplikat di inputs[${index}]: ${code}`);
    }
    seenInputs.add(key);
    return { code: String(code).trim(), ...parsed };
  });

  const uniqueNoBahanBaku = new Set(
    parsedInputs.map((item) => item.noBahanBaku),
  );
  if (uniqueNoBahanBaku.size > 1) {
    throw badReq(
      "Semua input bahan baku harus memiliki NoBahanBaku yang sama, pallet boleh berbeda",
    );
  }

  const normalizedOutputs = outputs.map((out, i) => {
    if (
      !out.idJenis ||
      !Number.isFinite(Number(out.idJenis)) ||
      Number(out.idJenis) <= 0
    ) {
      throw badReq(`outputs[${i}].idJenis wajib diisi`);
    }
    return {
      ...out,
      idJenis: Number(out.idJenis),
      saks: normalizeSaks(out.saks, i),
    };
  });

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

    const inputCodesJson = JSON.stringify(
      parsedInputs.map((c) => ({
        noBahanBaku: c.noBahanBaku,
        noPallet: c.noPallet,
      })),
    );

    const inputDataRes = await new sql.Request(tx).input(
      "CodesJson",
      sql.NVarChar(sql.MAX),
      inputCodesJson,
    ).query(`
        ;WITH InputCodes AS (
          SELECT
            j.noBahanBaku,
            j.noPallet
          FROM OPENJSON(@CodesJson)
          WITH (
            noBahanBaku varchar(50) '$.noBahanBaku',
            noPallet int '$.noPallet'
          ) AS j
        ),
        PartialAgg AS (
          SELECT
            p.NoBahanBaku,
            p.NoPallet,
            p.NoSak,
            SUM(ISNULL(p.Berat, 0)) AS PartialBerat
          FROM dbo.BahanBakuPartial p
          GROUP BY p.NoBahanBaku, p.NoPallet, p.NoSak
        )
        SELECT
          d.NoBahanBaku,
          d.NoPallet,
          d.NoSak,
          ph.IdJenisPlastik,
          ph.IdWarehouse,
          ph.IdStatus,
          ph.Keterangan,
          ph.Moisture,
          ph.MeltingIndex,
          ph.Elasticity,
          ph.Tenggelam,
          ph.Density,
          ph.Density2,
          ph.Density3,
          ph.HasBeenPrinted,
          ph.Blok,
          ph.IdLokasi,
          CASE
            WHEN d.IsPartial = 1 THEN
              CASE
                WHEN ISNULL(d.Berat, 0) - ISNULL(pa.PartialBerat, 0) < 0 THEN 0
                ELSE ISNULL(d.Berat, 0) - ISNULL(pa.PartialBerat, 0)
              END
            ELSE ISNULL(d.Berat, 0)
          END AS Berat
        FROM dbo.BahanBaku_d d WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN dbo.BahanBakuPallet_h ph WITH (UPDLOCK, HOLDLOCK)
          ON ph.NoBahanBaku = d.NoBahanBaku
         AND ph.NoPallet = d.NoPallet
        INNER JOIN InputCodes ic
          ON ic.noBahanBaku = d.NoBahanBaku
         AND ic.noPallet = d.NoPallet
        LEFT JOIN PartialAgg pa
          ON pa.NoBahanBaku = d.NoBahanBaku
         AND pa.NoPallet = d.NoPallet
         AND pa.NoSak = d.NoSak
        WHERE d.DateUsage IS NULL
        ORDER BY d.NoBahanBaku, d.NoPallet, d.NoSak
      `);

    const inputRows = inputDataRes.recordset || [];
    const matchedInputs = new Set(
      inputRows.map((r) => `${r.NoBahanBaku}-${r.NoPallet}`),
    );
    if (matchedInputs.size !== parsedInputs.length) {
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );
    }

    const refRow = inputRows[0];
    if (!refRow) {
      throw badReq(
        "Satu atau lebih label input tidak ditemukan atau sudah terpakai",
      );
    }

    for (const row of inputRows) {
      if (String(row.NoBahanBaku) !== String(refRow.NoBahanBaku)) {
        throw badReq(
          "Semua input bahan baku harus memiliki NoBahanBaku yang sama, pallet boleh berbeda",
        );
      }
      if (Number(row.IdJenisPlastik) !== Number(refRow.IdJenisPlastik)) {
        throw badReq(
          "Semua input bahan baku harus memiliki IdJenisPlastik yang sama",
        );
      }
    }

    const totalBeratInput = inputRows.reduce(
      (sum, row) => sum + Number(row.Berat || 0),
      0,
    );

    const outputByJenis = {};
    for (let i = 0; i < normalizedOutputs.length; i++) {
      const out = normalizedOutputs[i];
      const idJenis = Number(out.idJenis);
      if (idJenis !== Number(refRow.IdJenisPlastik)) {
        throw badReq(
          `outputs[${i}].idJenis harus sama dengan IdJenisPlastik input (${refRow.IdJenisPlastik})`,
        );
      }
      const totalBerat = out.saks.reduce(
        (sum, sak) => sum + Number(sak.Berat),
        0,
      );
      outputByJenis[idJenis] = (outputByJenis[idJenis] || 0) + totalBerat;
    }

    const totalBeratOutput = normalizedOutputs.reduce(
      (sum, out) => sum + out.saks.reduce((s, sak) => s + Number(sak.Berat), 0),
      0,
    );

    for (const [idJenis, beratInput] of Object.entries({
      [refRow.IdJenisPlastik]: totalBeratInput,
    })) {
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
        INSERT INTO dbo.BongkarSusunInputBahanBaku (NoBongkarSusun, NoBahanBaku, NoPallet, NoSak)
        SELECT @NoBongkarSusun, d.NoBahanBaku, d.NoPallet, d.NoSak
        FROM dbo.BahanBaku_d d
        INNER JOIN OPENJSON(@CodesJson)
        WITH (
          noBahanBaku varchar(50) '$.noBahanBaku',
          noPallet int '$.noPallet'
        ) AS ic
          ON ic.noBahanBaku = d.NoBahanBaku
         AND ic.noPallet = d.NoPallet
        WHERE d.DateUsage IS NULL
      `);

    await new sql.Request(tx)
      .input("Tanggal", sql.Date, nowDate)
      .input("CodesJson", sql.NVarChar(sql.MAX), inputCodesJson).query(`
        ;WITH InputCodes AS (
          SELECT
            ic.noBahanBaku,
            ic.noPallet
          FROM OPENJSON(@CodesJson)
          WITH (
            noBahanBaku varchar(50) '$.noBahanBaku',
            noPallet int '$.noPallet'
          ) AS ic
        )
        UPDATE d
        SET DateUsage = @Tanggal
        FROM dbo.BahanBaku_d d
        INNER JOIN InputCodes ic
          ON ic.noBahanBaku = d.NoBahanBaku
         AND ic.noPallet = d.NoPallet
        WHERE d.DateUsage IS NULL
      `);

    const maxPalletRes = await new sql.Request(tx).input(
      "NoBahanBaku",
      sql.VarChar(50),
      refRow.NoBahanBaku,
    ).query(`
        SELECT ISNULL(MAX(TRY_CONVERT(int, NoPallet)), 0) AS MaxNoPallet
        FROM dbo.BahanBakuPallet_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBahanBaku = @NoBahanBaku
      `);
    let nextNoPallet =
      Number(maxPalletRes.recordset?.[0]?.MaxNoPallet || 0) + 1;

    const createdOutputs = [];
    for (const out of normalizedOutputs) {
      const noPallet = nextNoPallet++;
      const saks = out.saks.map((sak) => ({
        NoSak: Math.trunc(Number(sak.NoSak)),
        Berat: Number(sak.Berat),
      }));
      const saksJson = JSON.stringify(saks);

      await new sql.Request(tx)
        .input("NoBahanBaku", sql.VarChar(50), refRow.NoBahanBaku)
        .input("NoPallet", sql.Int, noPallet)
        .input("IdJenisPlastik", sql.Int, Math.trunc(Number(out.idJenis)))
        .input("IdWarehouse", sql.Int, refRow.IdWarehouse ?? null)
        .input("Keterangan", sql.NVarChar(200), refRow.Keterangan ?? null)
        .input("IdStatus", sql.Int, refRow.IdStatus ?? 1)
        .input("Moisture", sql.Decimal(10, 3), refRow.Moisture ?? null)
        .input("MeltingIndex", sql.Decimal(10, 3), refRow.MeltingIndex ?? null)
        .input("Elasticity", sql.Decimal(10, 3), refRow.Elasticity ?? null)
        .input("Tenggelam", sql.Decimal(10, 3), refRow.Tenggelam ?? null)
        .input("Density", sql.Decimal(10, 3), refRow.Density ?? null)
        .input("Density2", sql.Decimal(10, 3), refRow.Density2 ?? null)
        .input("Density3", sql.Decimal(10, 3), refRow.Density3 ?? null)
        .input("HasBeenPrinted", sql.Int, 0)
        .input("Blok", sql.VarChar(50), "BSS")
        .input("IdLokasi", sql.Int, 1).query(`
          INSERT INTO dbo.BahanBakuPallet_h (
            NoBahanBaku, NoPallet, IdJenisPlastik, IdWarehouse, Keterangan,
            IdStatus, Moisture, MeltingIndex, Elasticity, Tenggelam,
            Density, Density2, Density3, HasBeenPrinted, Blok, IdLokasi
          )
          VALUES (
            @NoBahanBaku, @NoPallet, @IdJenisPlastik, @IdWarehouse, @Keterangan,
            @IdStatus, @Moisture, @MeltingIndex, @Elasticity, @Tenggelam,
            @Density, @Density2, @Density3, @HasBeenPrinted, @Blok, @IdLokasi
          )
        `);

      await new sql.Request(tx)
        .input("NoBahanBaku", sql.VarChar(50), refRow.NoBahanBaku)
        .input("NoPallet", sql.Int, noPallet)
        .input("TimeCreate", sql.DateTime, nowDate)
        .input("IdLokasi", sql.Int, 1)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.BahanBaku_d (
            NoBahanBaku, NoPallet, NoSak, Berat, DateUsage,
            IsPartial, BeratAct, TimeCreate, IdLokasi, IsLembab
          )
          SELECT
            @NoBahanBaku,
            @NoPallet,
            j.NoSak,
            j.Berat,
            NULL,
            0,
            NULL,
            @TimeCreate,
            @IdLokasi,
            0
          FROM OPENJSON(@SaksJson)
          WITH (
            NoSak int '$.NoSak',
            Berat decimal(18,3) '$.Berat'
          ) AS j
        `);

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .input("NoBahanBaku", sql.VarChar(50), refRow.NoBahanBaku)
        .input("NoPallet", sql.Int, noPallet)
        .input("SaksJson", sql.NVarChar(sql.MAX), saksJson).query(`
          INSERT INTO dbo.BongkarSusunOutputBahanBaku (
            NoBongkarSusun, NoBahanBaku, NoPallet, NoSak
          )
          SELECT @NoBongkarSusun, @NoBahanBaku, @NoPallet, j.NoSak
          FROM OPENJSON(@SaksJson)
          WITH (NoSak int '$.NoSak') AS j
        `);

      createdOutputs.push({
        noBahanBaku: refRow.NoBahanBaku,
        noPallet,
        idJenis: Number(out.idJenis),
        jumlahSak: saks.length,
        totalBerat: saks.reduce((sum, sak) => sum + sak.Berat, 0),
      });
    }

    await tx.commit();

    return {
      noBongkarSusun,
      tanggal: formatYMD(nowDate),
      category: "bahanBaku",
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

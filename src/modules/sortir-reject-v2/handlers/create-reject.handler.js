const { sql, poolPromise } = require("../../../core/config/db");
const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const { badReq, conflict } = require("../../../core/utils/http-error");
const { formatYMD } = require("../../../core/shared/tutup-transaksi-guard");

function normalizeOutputs(payload) {
  if (Array.isArray(payload?.outputs)) return payload.outputs;
  return [payload || {}];
}

function detectInputCategory(code) {
  const label = String(code || "").trim();
  if (label.startsWith("BA.")) return "barangJadi";
  if (label.startsWith("BB.")) return "furnitureWip";
  return null;
}

exports.createSortirRejectReject = async (noBJSortir, payload, ctx) => {
  const no = String(noBJSortir || "").trim();
  const outputs = normalizeOutputs(payload);
  const { actorId, actorUsername, requestId } = ctx;
  const isNewTransaction = no === "";
  const inputs = Array.isArray(payload?.inputs) ? payload.inputs : [];
  const warehouseId = Number(payload?.idWarehouse);

  if (!isNewTransaction && !no) throw badReq("noBJSortir wajib diisi");
  if (isNewTransaction) {
    if (!Number.isFinite(warehouseId) || warehouseId <= 0) {
      throw badReq("idWarehouse wajib diisi");
    }
    if (!Array.isArray(inputs) || inputs.length === 0) {
      throw badReq("inputs wajib berisi minimal 1 label");
    }
  }
  if (!Array.isArray(outputs) || outputs.length === 0) {
    throw badReq("outputs wajib berisi minimal 1 output reject");
  }

  for (let i = 0; i < outputs.length; i++) {
    const out = outputs[i];
    const idJenis = out.idJenis ?? out.idReject;
    if (!idJenis || !Number.isFinite(Number(idJenis)) || Number(idJenis) <= 0) {
      throw badReq(`outputs[${i}].idJenis wajib diisi`);
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

    const nowDate = new Date();
    let noSortir = no;
    let header = null;
    let lokasi = {};

    if (isNewTransaction) {
      const firstCategory = detectInputCategory(inputs[0]);
      if (!firstCategory) {
        throw badReq(`Label input ${inputs[0]} tidak dikenali kategorinya`);
      }
      for (const code of inputs) {
        if (detectInputCategory(code) !== firstCategory) {
          throw badReq("Semua input harus memiliki kategori yang sama");
        }
      }

      const inputCodesJson = JSON.stringify(inputs.map((c) => ({ code: c })));

      const inputDataRes =
        firstCategory === "barangJadi"
          ? await new sql.Request(tx).input(
              "CodesJson",
              sql.NVarChar(sql.MAX),
              inputCodesJson,
            ).query(`
              SELECT
                b.NoBJ,
                b.Blok,
                b.IdLokasi,
                b.IsPartial
              FROM dbo.BarangJadi b WITH (UPDLOCK, HOLDLOCK)
              WHERE b.NoBJ IN (
                SELECT j.code FROM OPENJSON(@CodesJson)
                WITH (code varchar(50) '$.code') AS j
              )
              AND b.DateUsage IS NULL
            `)
          : await new sql.Request(tx).input(
              "CodesJson",
              sql.NVarChar(sql.MAX),
              inputCodesJson,
            ).query(`
              SELECT
                f.NoFurnitureWIP,
                f.Blok,
                f.IdLokasi,
                f.IsPartial
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
          throw badReq("Label input sudah di partial");
        }
      }

      const genSortir = () =>
        generateNextCode(tx, {
          tableName: "BJSortirReject_h",
          columnName: "NoBJSortir",
          prefix: "J.",
          width: 10,
        });

      noSortir = await genSortir();
      const sortirExist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), noSortir)
        .query(
          `SELECT 1 FROM dbo.BJSortirReject_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBJSortir=@No`,
        );
      if (sortirExist.recordset.length > 0) {
        noSortir = await genSortir();
        const sortirExist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), noSortir)
          .query(
            `SELECT 1 FROM dbo.BJSortirReject_h WITH (UPDLOCK,HOLDLOCK) WHERE NoBJSortir=@No`,
          );
        if (sortirExist2.recordset.length > 0) {
          throw conflict("Gagal generate NoBJSortir unik, coba lagi");
        }
      }

      await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), noSortir)
        .input("TglBJSortir", sql.Date, nowDate)
        .input("IdWarehouse", sql.Int, warehouseId)
        .input("IdUsername", sql.Int, actorId).query(`
          INSERT INTO dbo.BJSortirReject_h (
            NoBJSortir, TglBJSortir, IdWarehouse, IdUsername
          )
          VALUES (@NoBJSortir, @TglBJSortir, @IdWarehouse, @IdUsername)
        `);

      if (firstCategory === "barangJadi") {
        await new sql.Request(tx)
          .input("NoBJSortir", sql.VarChar(50), noSortir)
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
      } else {
        await new sql.Request(tx)
          .input("NoBJSortir", sql.VarChar(50), noSortir)
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
      }

      header = { NoBJSortir: noSortir, IdWarehouse: warehouseId };
      lokasi = inputDataRes.recordset?.[0] || {};
    } else {
      const headerRes = await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), noSortir).query(`
          SELECT TOP 1 NoBJSortir, IdWarehouse
          FROM dbo.BJSortirReject_h WITH (UPDLOCK, HOLDLOCK)
          WHERE NoBJSortir = @NoBJSortir
        `);

      header = headerRes.recordset?.[0];
      if (!header) {
        const e = new Error(`NoBJSortir ${noSortir} tidak ditemukan`);
        e.statusCode = 404;
        throw e;
      }

      const lokasiRes = await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), noSortir).query(`
          SELECT TOP 1 src.Blok, src.IdLokasi
          FROM (
            SELECT bj.Blok, bj.IdLokasi, 1 AS priority
            FROM dbo.BJSortirRejectInputLabelBarangJadi map
            INNER JOIN dbo.BarangJadi bj
              ON bj.NoBJ = map.NoBJ
            WHERE map.NoBJSortir = @NoBJSortir

            UNION ALL

            SELECT fw.Blok, fw.IdLokasi, 2 AS priority
            FROM dbo.BJSortirRejectInputLabelFurnitureWIP map
            INNER JOIN dbo.FurnitureWIP fw
              ON fw.NoFurnitureWIP = map.NoFurnitureWIP
            WHERE map.NoBJSortir = @NoBJSortir
          ) src
          ORDER BY src.priority
        `);

      lokasi = lokasiRes.recordset?.[0] || {};
    }

    const createdOutputs = [];

    for (const out of outputs) {
      const genReject = () =>
        generateNextCode(tx, {
          tableName: "RejectV2",
          columnName: "NoReject",
          prefix: "BF.",
          width: 10,
        });

      let newNoReject = await genReject();
      const exist = await new sql.Request(tx)
        .input("No", sql.VarChar(50), newNoReject)
        .query(
          `SELECT 1 FROM dbo.RejectV2 WITH (UPDLOCK,HOLDLOCK) WHERE NoReject=@No`,
        );
      if (exist.recordset.length > 0) {
        newNoReject = await genReject();
        const exist2 = await new sql.Request(tx)
          .input("No", sql.VarChar(50), newNoReject)
          .query(
            `SELECT 1 FROM dbo.RejectV2 WITH (UPDLOCK,HOLDLOCK) WHERE NoReject=@No`,
          );
        if (exist2.recordset.length > 0) {
          throw conflict("Gagal generate NoReject unik, coba lagi");
        }
      }

      await new sql.Request(tx)
        .input("NoReject", sql.VarChar(50), newNoReject)
        .input("IdReject", sql.Int, Number(out.idJenis ?? out.idReject))
        .input("DateCreate", sql.Date, nowDate)
        .input("DateUsage", sql.Date, null)
        .input("IdWarehouse", sql.Int, header.IdWarehouse)
        .input("Berat", sql.Decimal(18, 3), Number(out.berat))
        .input("Jam", sql.VarChar(20), null)
        .input("CreateBy", sql.VarChar(50), actorUsername)
        .input("DateTimeCreate", sql.DateTime, nowDate)
        .input("IsPartial", sql.Bit, 0)
        .input("Blok", sql.VarChar(50), lokasi.Blok ?? null)
        .input("IdLokasi", sql.Int, lokasi.IdLokasi ?? null).query(`
          INSERT INTO dbo.RejectV2 (
            NoReject, IdReject, DateCreate, DateUsage, IdWarehouse,
            Berat, Jam, CreateBy, DateTimeCreate, IsPartial, Blok, IdLokasi
          )
          VALUES (
            @NoReject, @IdReject, @DateCreate, @DateUsage, @IdWarehouse,
            @Berat, @Jam, @CreateBy, @DateTimeCreate, @IsPartial, @Blok, @IdLokasi
          )
        `);

      await new sql.Request(tx)
        .input("NoBJSortir", sql.VarChar(50), noSortir)
        .input("NoReject", sql.VarChar(50), newNoReject).query(`
          INSERT INTO dbo.BJSortirRejectOutputLabelReject (NoBJSortir, NoReject)
          VALUES (@NoBJSortir, @NoReject)
        `);

      createdOutputs.push({
        noReject: newNoReject,
        idJenis: Number(out.idJenis ?? out.idReject),
        berat: Number(out.berat),
      });
    }

    await tx.commit();

    return {
      noBJSortir: noSortir,
      tanggal: formatYMD(nowDate),
      category: "reject",
      inputs: isNewTransaction ? inputs : undefined,
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

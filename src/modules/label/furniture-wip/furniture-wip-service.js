// services/labels/furniture-wip-service.js
const { sql, poolPromise } = require("../../../core/config/db");
const {
  getBlokLokasiFromKodeProduksi,
} = require("../../../core/shared/mesin-location-helper");

const {
  resolveEffectiveDateForCreate,
  toDateOnly,
  assertNotLocked,
  formatYMD,
} = require("../../../core/shared/tutup-transaksi-guard");

const {
  generateNextCode,
} = require("../../../core/utils/sequence-code-helper");
const {
  badReq,
  conflict,
  notFound,
} = require("../../../core/utils/http-error");
const readRepo = require("./repositories/furniture-wip-read.repository");
const writeRepo = require("./repositories/furniture-wip-write.repository");

const hasOwn = (obj, key) =>
  Object.prototype.hasOwnProperty.call(obj || {}, key);

exports.getAll = async ({ page, limit, search, includeUsed = false }) =>
  readRepo.getAll({ page, limit, search, includeUsed });

async function insertSingleFurnitureWip({
  tx,
  header,
  idFurnitureWip,
  outputCode,
  outputType,
  mappingTable,
  effectiveDateCreate,
  nowDateTime,
}) {
  const gen = async () =>
    generateNextCode(tx, {
      tableName: "dbo.FurnitureWIP",
      columnName: "NoFurnitureWIP",
      prefix: "BB.",
      width: 10,
    });

  const generatedNo = await gen();
  let noFurnitureWip = generatedNo;

  const exist = await readRepo.isNoFurnitureWipExists(tx, generatedNo);
  if (exist) {
    const retryNo = await gen();
    const exist2 = await readRepo.isNoFurnitureWipExists(tx, retryNo);
    if (exist2) {
      throw conflict("Gagal generate NoFurnitureWIP unik, coba lagi.");
    }
    noFurnitureWip = retryNo;
  }

  await writeRepo.insertFurnitureWipHeader(tx, {
    noFurnitureWip,
    header,
    idFurnitureWip,
    effectiveDateCreate,
    nowDateTime,
  });

  await writeRepo.insertOutputMapping(tx, {
    mappingTable,
    outputCode,
    noFurnitureWip,
  });

  return {
    NoFurnitureWIP: noFurnitureWip,
    DateCreate: formatYMD(effectiveDateCreate),
    Jam: header.Jam ?? null,
    Pcs: header.Pcs ?? null,
    IDFurnitureWIP: idFurnitureWip,
    Berat: header.Berat ?? null,
    IsPartial: header.IsPartial ?? 0,
    DateUsage: null,
    IdWarehouse: header.IdWarehouse,
    IdWarna: header.IdWarna ?? null,
    CreateBy: header.CreateBy,
    DateTimeCreate: nowDateTime,
    Blok: header.Blok ?? null,
    IdLokasi: header.IdLokasi ?? null,
    OutputCode: outputCode,
    OutputType: outputType,
  };
}

async function createFromInjectMapping({
  tx,
  header,
  outputCode,
  mappingTable,
  outputType,
  effectiveDateCreate,
  nowDateTime,
}) {
  const inj = await readRepo.getInjectHeaderByNoProduksi(tx, outputCode);
  if (!inj) {
    throw badReq(
      `InjectProduksi_h ${outputCode} tidak ditemukan atau IdCetakan NULL`,
    );
  }

  const mappings = await readRepo.getInjectFurnitureWipMappings(tx, {
    idCetakan: inj.IdCetakan,
    idWarna: inj.IdWarna,
    idFurnitureMaterial: inj.IdFurnitureMaterial ?? 0,
  });

  if (!mappings.length) {
    throw badReq(
      `Mapping FurnitureWIP tidak ditemukan untuk Inject ${outputCode} (IdCetakan=${inj.IdCetakan}, IdWarna=${inj.IdWarna})`,
    );
  }

  const created = [];
  for (const row of mappings) {
    created.push(
      await insertSingleFurnitureWip({
        tx,
        header,
        idFurnitureWip: row.IdFurnitureWIP,
        outputCode,
        outputType,
        mappingTable,
        effectiveDateCreate,
        nowDateTime,
      }),
    );
  }

  return created;
}

exports.createFurnitureWip = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const outputCode = String(payload?.outputCode || "").trim();

  if (!outputCode)
    throw badReq("outputCode wajib diisi (BH., BI., BG., L., BJ., S.)");
  if (!header.CreateBy)
    throw badReq(
      "CreateBy wajib diisi (controller harus overwrite dari token)",
    );

  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  let outputType = null;
  let mappingTable = null;

  if (outputCode.startsWith("BH.")) {
    outputType = "HOTSTAMPING";
    mappingTable = "HotStampingOutputLabelFWIP";
  } else if (outputCode.startsWith("BI.")) {
    outputType = "PASANG_KUNCI";
    mappingTable = "PasangKunciOutputLabelFWIP";
  } else if (outputCode.startsWith("BG.")) {
    outputType = "BONGKAR_SUSUN";
    mappingTable = "BongkarSusunOutputFurnitureWIP";
  } else if (outputCode.startsWith("L.")) {
    outputType = "RETUR";
    mappingTable = "BJReturFurnitureWIP_d";
  } else if (outputCode.startsWith("BJ.")) {
    outputType = "SPANNER";
    mappingTable = "SpannerOutputLabelFWIP";
  } else if (outputCode.startsWith("S.")) {
    outputType = "INJECT";
    mappingTable = "InjectProduksiOutputFurnitureWIP";
  } else {
    throw badReq(
      "outputCode prefix tidak dikenali (BH., BI., BG., L., BJ., S.)",
    );
  }

  const isInject = outputType === "INJECT";
  const idFwipSingle = header.IdFurnitureWIP ?? header.IDFurnitureWIP ?? null;
  if (!isInject && !idFwipSingle)
    throw badReq("IdFurnitureWIP wajib diisi untuk mode non-INJECT");

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const effectiveDateCreate = resolveEffectiveDateForCreate(
      header.DateCreate,
    );
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: "create furniture wip",
      useLock: true,
    });

    const needBlok = header.Blok == null || String(header.Blok).trim() === "";
    const needLokasi = header.IdLokasi == null;

    if (needBlok || needLokasi) {
      const lokasi = await getBlokLokasiFromKodeProduksi({
        kode: outputCode,
        runner: tx,
      });
      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    const nowDateTime = new Date();

    const headers =
      isInject && !idFwipSingle
        ? await createFromInjectMapping({
            tx,
            header,
            outputCode,
            mappingTable,
            outputType,
            effectiveDateCreate,
            nowDateTime,
          })
        : [
            await insertSingleFurnitureWip({
              tx,
              header,
              idFurnitureWip: idFwipSingle,
              outputCode,
              outputType,
              mappingTable,
              effectiveDateCreate,
              nowDateTime,
            }),
          ];

    await tx.commit();

    return {
      headers,
      output: {
        code: outputCode,
        type: outputType,
        mappingTable,
        isMulti: headers.length > 1,
        count: headers.length,
      },
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.updateFurnitureWip = async (noFurnitureWip, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const hasOutputCodeField = hasOwn(payload, "outputCode");
  const outputCode = String(payload?.outputCode || "").trim();

  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const current = await readRepo.getExistingForUpdate(tx, noFurnitureWip);
    if (!current) {
      throw notFound("Furniture WIP not found");
    }

    const isBso = await readRepo.isFromBongkarSusun(tx, noFurnitureWip);
    if (isBso) {
      throw conflict(
        "Data tidak dapat diubah: label ini berasal dari Bongkar Susun.",
      );
    }

    const existingDateCreate = current.DateCreate
      ? toDateOnly(current.DateCreate)
      : null;

    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: "update furniture wip",
      useLock: true,
    });

    const merged = {
      IDFurnitureWIP:
        header.IdFurnitureWIP ??
        header.IDFurnitureWIP ??
        current.IDFurnitureWIP,
      Jam: hasOwn(header, "Jam") ? header.Jam : current.Jam,
      Pcs: hasOwn(header, "Pcs") ? header.Pcs : current.Pcs,
      Berat: hasOwn(header, "Berat") ? header.Berat : current.Berat,
      IsPartial: hasOwn(header, "IsPartial")
        ? header.IsPartial
        : current.IsPartial,
      IdWarehouse: hasOwn(header, "IdWarehouse")
        ? header.IdWarehouse
        : current.IdWarehouse,
      IdWarna: hasOwn(header, "IdWarna") ? header.IdWarna : current.IdWarna,
      Blok: hasOwn(header, "Blok") ? header.Blok : current.Blok,
      IdLokasi: hasOwn(header, "IdLokasi") ? header.IdLokasi : current.IdLokasi,
      DateCreate: hasOwn(header, "DateCreate")
        ? header.DateCreate
        : current.DateCreate,
      CreateBy: hasOwn(header, "CreateBy") ? header.CreateBy : current.CreateBy,
    };

    if (!merged.IDFurnitureWIP) throw badReq("IdFurnitureWIP cannot be empty");

    let dateCreateParam = null;
    if (hasOwn(header, "DateCreate")) {
      if (header.DateCreate === null || header.DateCreate === "") {
        dateCreateParam = toDateOnly(new Date());
      } else {
        dateCreateParam = toDateOnly(header.DateCreate);
        if (!dateCreateParam) throw badReq("Invalid DateCreate");
      }

      await assertNotLocked({
        date: dateCreateParam,
        runner: tx,
        action: "update furniture wip (DateCreate)",
        useLock: true,
      });
    }

    await writeRepo.updateFurnitureWipHeader(
      tx,
      noFurnitureWip,
      merged,
      hasOwn(header, "DateCreate"),
      dateCreateParam,
    );

    let outputType = null;
    let mappingTable = null;

    if (hasOutputCodeField) {
      if (!outputCode) {
        await writeRepo.deleteAllMappings(tx, noFurnitureWip);
      } else {
        if (outputCode.startsWith("BH.")) {
          outputType = "HOTSTAMPING";
          mappingTable = "HotStampingOutputLabelFWIP";
        } else if (outputCode.startsWith("BI.")) {
          outputType = "PASANG_KUNCI";
          mappingTable = "PasangKunciOutputLabelFWIP";
        } else if (outputCode.startsWith("L.")) {
          outputType = "RETUR";
          mappingTable = "BJReturFurnitureWIP_d";
        } else if (outputCode.startsWith("BJ.")) {
          outputType = "SPANNER";
          mappingTable = "SpannerOutputLabelFWIP";
        } else if (outputCode.startsWith("S.")) {
          outputType = "INJECT";
          mappingTable = "InjectProduksiOutputFurnitureWIP";
        } else {
          throw badReq(
            "outputCode prefix not recognized (supported: BH., BI., L., BJ., S.)",
          );
        }

        await writeRepo.deleteAllMappings(tx, noFurnitureWip);
        await writeRepo.insertOutputMapping(tx, {
          mappingTable,
          outputCode,
          noFurnitureWip,
        });
      }
    }

    await tx.commit();

    return {
      header: {
        NoFurnitureWIP: noFurnitureWip,
        DateCreate: hasOwn(header, "DateCreate")
          ? dateCreateParam
            ? formatYMD(dateCreateParam)
            : null
          : formatYMD(current.DateCreate),
        Jam: merged.Jam ?? null,
        Pcs: merged.Pcs ?? null,
        IDFurnitureWIP: merged.IDFurnitureWIP,
        Berat: merged.Berat ?? null,
        IsPartial: merged.IsPartial ?? 0,
        IdWarehouse: merged.IdWarehouse,
        IdWarna: merged.IdWarna ?? null,
        CreateBy: merged.CreateBy ?? null,
        Blok: merged.Blok ?? null,
        IdLokasi: merged.IdLokasi ?? null,
      },
      output: hasOutputCodeField
        ? { code: outputCode || null, type: outputType, mappingTable }
        : undefined,
      audit: { actorId, requestId },
    };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw err;
  }
};

exports.deleteFurnitureWip = async (noFurnitureWip, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoFurnitureWIP = String(noFurnitureWip || "").trim();
  if (!NoFurnitureWIP) throw badReq("NoFurnitureWIP is required");

  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

  if (!actorId)
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await new sql.Request(tx)
      .input("actorId", sql.Int, actorId)
      .input("rid", sql.NVarChar(64), requestId).query(`
        EXEC sys.sp_set_session_context @key=N'actor_id', @value=@actorId;
        EXEC sys.sp_set_session_context @key=N'request_id', @value=@rid;
      `);

    const head = await readRepo.getHeaderForDelete(tx, NoFurnitureWIP);
    if (!head) throw notFound("Furniture WIP not found");

    const trxDate = head.DateCreate ? toDateOnly(head.DateCreate) : null;
    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: "delete furniture wip",
      useLock: true,
    });

    if (head.DateUsage) {
      const err = conflict(
        "Cannot delete: FurnitureWIP already used (DateUsage IS NOT NULL).",
      );
      err.code = "FWIP_ALREADY_USED";
      throw err;
    }

    await writeRepo.deleteAllMappings(tx, NoFurnitureWIP);
    await writeRepo.deleteFurnitureWipPartials(tx, NoFurnitureWIP);
    const rowsDeleted = await writeRepo.deleteFurnitureWipHeader(
      tx,
      NoFurnitureWIP,
    );
    if (rowsDeleted === 0) throw notFound("Furniture WIP not found");

    await tx.commit();

    return {
      noFurnitureWip: NoFurnitureWIP,
      deleted: true,
      audit: { actorId, requestId },
    };
  } catch (err) {
    try {
      await tx.rollback();
    } catch (_) {}

    if (err?.number === 547) {
      const e = conflict(
        err.message || "Delete failed due to foreign key constraint.",
      );
      e.original = err;
      throw e;
    }

    throw err;
  }
};

exports.getPartialInfoByFurnitureWip = async (noFurnitureWip) => {
  const rowsRaw = await readRepo.getPartialInfoRows(noFurnitureWip);

  const seen = new Set();
  let totalPartialPcs = 0;
  for (const row of rowsRaw) {
    const key = row.NoFurnitureWIPPartial;
    if (!seen.has(key)) {
      seen.add(key);
      const pcs = typeof row.Pcs === "number" ? row.Pcs : Number(row.Pcs) || 0;
      totalPartialPcs += pcs;
    }
  }

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? `0${n}` : `${n}`);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = rowsRaw.map((r) => ({
    NoFurnitureWIPPartial: r.NoFurnitureWIPPartial,
    NoFurnitureWIP: r.NoFurnitureWIP,
    Pcs: r.Pcs,
    SourceType: r.SourceType || null,
    NoProduksi: r.NoProduksi || null,
    TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
    IdMesin: r.IdMesin || null,
    NamaMesin: r.NamaMesin || null,
    IdOperator: r.IdOperator || null,
    Jam: r.Jam || null,
    Shift: r.Shift || null,
  }));

  return { totalPartialPcs, rows };
};

exports.incrementHasBeenPrinted = async (payload) => {
  const NoFurnitureWIP = String(payload?.NoFurnitureWIP || "").trim();
  if (!NoFurnitureWIP) throw badReq("NoFurnitureWIP wajib diisi");

  const actorIdNum = Number(payload?.actorId);
  const actorId =
    Number.isFinite(actorIdNum) && actorIdNum > 0 ? actorIdNum : null;
  if (!actorId) {
    throw badReq(
      "actorId kosong. Controller harus inject payload.actorId dari token.",
    );
  }

  const requestId = String(
    payload?.requestId ||
      `${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );

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

    const row = await writeRepo.incrementHasBeenPrinted(tx, NoFurnitureWIP);
    if (!row) {
      const e = new Error(`NoFurnitureWIP ${NoFurnitureWIP} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoFurnitureWIP: row.NoFurnitureWIP,
      HasBeenPrinted: row.HasBeenPrinted,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.getByNoFurnitureWip = async (NoFurnitureWIP) => {
  const first = await readRepo.getByNoFurnitureWip(NoFurnitureWIP);
  if (!first) {
    const e = new Error(`NoFurnitureWIP ${NoFurnitureWIP} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  return {
    NoFurnitureWIP: first.NoFurnitureWIP,
    DateCreate: first.DateCreate,
    IdFurnitureWIP: first.IdFurnitureWIP,
    NamaFurnitureWIP: first.NamaFurnitureWIP,
    IsPartial: first.IsPartial,
    Pcs: first.Pcs,
    Berat: first.Berat,
    HasBeenPrinted: first.HasBeenPrinted,
    CreateBy: first.CreateBy,
    Mesin: first.Mesin,
    Shift: first.Shift,
  };
};

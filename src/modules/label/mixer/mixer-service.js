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
const { badReq, conflict } = require("../../../core/utils/http-error");
const readRepo = require("./repositories/mixer-read.repository");
const writeRepo = require("./repositories/mixer-write.repository");

exports.getAll = async (params) => readRepo.getAllMixerHeaders(params);

exports.getMixerDetailByNoMixer = async (noMixer) =>
  readRepo.getMixerDetailsByNoMixer(noMixer);

exports.getByNoMixer = async (noMixer) => {
  const rs = await readRepo.getMixerHeaderByNoMixer(noMixer);
  const first = rs.recordset?.[0];
  if (!first) {
    const e = new Error(`NoMixer ${noMixer} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  return {
    NoMixer: first.NoMixer,
    DateCreate: first.DateCreate,
    IdMixer: first.IdMixer,
    Jenis: first.Jenis,
    NamaMixer: first.NamaMixer,
    IsPartial: first.IsPartial,
    JumlahSak: first.JumlahSak,
    SisaBerat: first.SisaBerat,
    CreateBy: first.CreateBy,
    HasBeenPrinted: first.HasBeenPrinted,
    Mesin: first.Mesin,
    Shift: first.Shift,
  };
};

exports.getLabelInfoByNoMixer = async (noMixer) => {
  const code = String(noMixer || "").trim();
  if (!code) throw badReq("NoMixer wajib diisi");

  const [header, details] = await Promise.all([
    exports.getByNoMixer(code),
    exports.getMixerDetailByNoMixer(code),
  ]);

  const activeSaks = (details || [])
    .filter((d) => !d.DateUsage)
    .map((d) => ({
      noSak: d.NoSak,
      berat: d.Berat,
      isPartial: d.IsPartial === true || d.IsPartial === 1 ? 1 : 0,
    }));

  const totalBerat = activeSaks.reduce(
    (sum, sak) => sum + Number(sak.berat || 0),
    0,
  );

  return {
    labelCode: header.NoMixer,
    category: "mixer",
    dateCreate: header.DateCreate,
    idJenis: header.IdMixer,
    namaJenis: header.NamaMixer ?? header.Jenis,
    isPartial: activeSaks.some((sak) => sak.isPartial === 1) ? 1 : 0,
    jumlahSak: activeSaks.length,
    berat: totalBerat,
    saks: activeSaks,
    createBy: header.CreateBy,
    mesin: header.Mesin,
    shift: header.Shift,
    hasBeenPrinted: header.HasBeenPrinted ?? 0,
  };
};

exports.createMixerOutputFromBongkarSusunTx = async ({
  tx,
  noBongkarSusun,
  output,
  reference,
  actorUsername,
  nowDate = new Date(),
}) => {
  if (!tx) throw badReq("tx wajib diisi");
  if (!noBongkarSusun) throw badReq("noBongkarSusun wajib diisi");
  if (!output || !Array.isArray(output.saks) || output.saks.length === 0) {
    throw badReq("output.saks wajib berisi minimal 1 sak");
  }

  const outputIdJenis = Math.trunc(Number(output.idJenis ?? output.idMixer));
  if (!Number.isFinite(outputIdJenis) || outputIdJenis <= 0) {
    throw badReq("output.idJenis wajib diisi");
  }

  const genMixer = () =>
    generateNextCode(tx, {
      tableName: "Mixer_h",
      columnName: "NoMixer",
      prefix: "H.",
      width: 10,
    });

  let newNoMixer = await genMixer();
  const exist = await readRepo.isNoMixerExists(tx, newNoMixer);
  if (exist) {
    newNoMixer = await genMixer();
    const exist2 = await readRepo.isNoMixerExists(tx, newNoMixer);
    if (exist2) {
      throw conflict("Gagal generate NoMixer unik, coba lagi");
    }
  }

  const normalizedSaks = output.saks.map((s) => ({
    NoSak: Math.trunc(Number(s.noSak)),
    Berat: Number(s.berat),
  }));
  const saksJson = JSON.stringify(normalizedSaks);

  await writeRepo.insertMixerHeaderFromBongkarSusunTx({
    tx,
    noMixer: newNoMixer,
    nowDate,
    outputIdJenis,
    actorUsername,
    reference,
  });

  await writeRepo.insertMixerDetailsFromBongkarSusunTx({
    tx,
    noMixer: newNoMixer,
    saksJson,
  });

  await writeRepo.insertBongkarSusunOutputMixerTx({
    tx,
    noBongkarSusun,
    noMixer: newNoMixer,
    saksJson,
  });

  return {
    noMixer: newNoMixer,
    idJenis: outputIdJenis,
    jumlahSak: normalizedSaks.length,
    totalBerat: normalizedSaks.reduce((s, x) => s + x.Berat, 0),
    saks: normalizedSaks,
  };
};

exports.createMixerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : [];

  if (!header.IdMixer) throw badReq("IdMixer wajib diisi");
  if (!header.CreateBy) throw badReq("CreateBy wajib diisi");
  if (!Array.isArray(details) || details.length === 0)
    throw badReq("Details wajib berisi minimal 1 item");

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

  const rawOutputCode = payload?.outputCode?.toString().trim() || "";

  let NoProduksi = null;
  let NoBongkarSusun = null;
  let NoInjectProduksi = null;
  let outputKind = null;

  if (rawOutputCode) {
    const upper = rawOutputCode.toUpperCase();
    if (upper.startsWith("BG.")) {
      NoBongkarSusun = rawOutputCode;
      outputKind = "BONGKAR";
    } else if (upper.startsWith("I.")) {
      NoProduksi = rawOutputCode;
      outputKind = "PRODUKSI";
    } else if (upper.startsWith("S.")) {
      NoInjectProduksi = rawOutputCode;
      outputKind = "INJECT";
    } else {
      throw badReq('outputCode harus diawali "BG.", "I." atau "S."');
    }
  }

  const normalizedDetails = details.map((d) => {
    const noSak = Number(d?.NoSak);
    if (!Number.isFinite(noSak) || noSak <= 0) {
      throw badReq(`NoSak tidak valid: ${d?.NoSak}`);
    }

    const berat = d?.Berat == null ? 0 : Number(d.Berat);
    if (!Number.isFinite(berat) || berat < 0) {
      throw badReq(
        `Berat tidak valid pada NoSak ${Math.trunc(noSak)}: ${d?.Berat}`,
      );
    }

    const isPartialRaw = d?.IsPartial;
    const isPartial =
      isPartialRaw === true ||
      isPartialRaw === 1 ||
      String(isPartialRaw).trim() === "1"
        ? 1
        : 0;

    return { NoSak: Math.trunc(noSak), Berat: berat, IsPartial: isPartial };
  });

  const set = new Set();
  for (const x of normalizedDetails) {
    const k = String(x.NoSak);
    if (set.has(k)) throw badReq(`NoSak duplikat di payload: ${x.NoSak}`);
    set.add(k);
  }

  const detailsJson = JSON.stringify(normalizedDetails);

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    await writeRepo.setAuditContext(tx, { actorId, requestId });

    const effectiveDateCreate = resolveEffectiveDateForCreate(
      header.DateCreate,
    );
    await assertNotLocked({
      date: effectiveDateCreate,
      runner: tx,
      action: "create mixer",
      useLock: true,
    });

    const needBlok = header.Blok == null || String(header.Blok).trim() === "";
    const needLokasi = header.IdLokasi == null;

    if ((needBlok || needLokasi) && rawOutputCode) {
      const lokasi = await getBlokLokasiFromKodeProduksi({
        kode: rawOutputCode,
        runner: tx,
      });
      if (lokasi) {
        if (needBlok) header.Blok = lokasi.Blok;
        if (needLokasi) header.IdLokasi = lokasi.IdLokasi;
      }
    }

    const gen = async () =>
      generateNextCode(tx, {
        tableName: "dbo.Mixer_h",
        columnName: "NoMixer",
        prefix: "H.",
        width: 10,
      });

    const generatedNo = await gen();
    const exist = await readRepo.isNoMixerExists(tx, generatedNo);

    if (exist) {
      const retryNo = await gen();
      const exist2 = await readRepo.isNoMixerExists(tx, retryNo);
      if (exist2) throw conflict("Gagal generate NoMixer unik, coba lagi.");
      header.NoMixer = retryNo;
    } else {
      header.NoMixer = generatedNo;
    }

    let idLokasiVal = null;
    if (header.IdLokasi !== undefined && header.IdLokasi !== null) {
      const s = String(header.IdLokasi).trim();
      if (s !== "" && s !== "-") {
        const n = Number(s);
        if (!Number.isFinite(n)) throw badReq("IdLokasi harus angka");
        idLokasiVal = Math.trunc(n);
      }
    }

    const nowDateTime = new Date();
    await writeRepo.insertMixerHeader({
      tx,
      header,
      effectiveDateCreate,
      nowDateTime,
      idLokasiVal,
    });
    await writeRepo.insertMixerDetailsBulk({
      tx,
      noMixer: header.NoMixer,
      detailsJson,
    });

    const detailCount = normalizedDetails.length;
    let outputTarget = null;
    let outputCount = 0;

    if (NoProduksi) {
      await writeRepo.insertMixerProduksiOutputBulk({
        tx,
        noProduksi: NoProduksi,
        noMixer: header.NoMixer,
        detailsJson,
      });
      outputCount = detailCount;
      outputTarget = "MixerProduksiOutput";
    } else if (NoBongkarSusun) {
      await writeRepo.insertBongkarSusunOutputBulk({
        tx,
        noBongkarSusun: NoBongkarSusun,
        noMixer: header.NoMixer,
        detailsJson,
      });
      outputCount = detailCount;
      outputTarget = "BongkarSusunOutputMixer";
    } else if (NoInjectProduksi) {
      await writeRepo.insertInjectProduksiOutputBulk({
        tx,
        noProduksi: NoInjectProduksi,
        noMixer: header.NoMixer,
        detailsJson,
      });
      outputCount = detailCount;
      outputTarget = "InjectProduksiOutputMixer";
    }

    await tx.commit();

    return {
      header: {
        NoMixer: header.NoMixer,
        IdMixer: header.IdMixer,
        IdStatus: header.IdStatus ?? 1,
        CreateBy: header.CreateBy,
        DateCreate: formatYMD(effectiveDateCreate),
        DateTimeCreate: nowDateTime,
        Moisture: header.Moisture ?? null,
        MaxMeltTemp: header.MaxMeltTemp ?? null,
        MinMeltTemp: header.MinMeltTemp ?? null,
        MFI: header.MFI ?? null,
        Moisture2: header.Moisture2 ?? null,
        Moisture3: header.Moisture3 ?? null,
        Blok: header.Blok ?? null,
        IdLokasi: idLokasiVal,
      },
      counts: { detailsInserted: detailCount, outputInserted: outputCount },
      outputKind,
      outputTarget,
      outputCode: rawOutputCode || null,
      audit: { actorId, requestId },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.updateMixerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoMixer = payload?.NoMixer?.toString().trim();
  if (!NoMixer) {
    const e = new Error("NoMixer (path) is required");
    e.statusCode = 400;
    throw e;
  }

  const header = payload?.header || {};
  const details = Array.isArray(payload?.details) ? payload.details : null;
  const hasOutputCodeField = Object.prototype.hasOwnProperty.call(
    payload,
    "outputCode",
  );
  const hasDetailsField = Object.prototype.hasOwnProperty.call(
    payload,
    "details",
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

  let rawOutputCode = null;
  let NoProduksiMixer = null;
  let NoProduksiInject = null;
  let outputKind = null;

  if (hasOutputCodeField) {
    rawOutputCode = payload.outputCode?.toString().trim() || "";
    if (rawOutputCode) {
      const upper = rawOutputCode.toUpperCase();
      if (upper.startsWith("I.")) {
        NoProduksiMixer = rawOutputCode;
        outputKind = "MIXER_PRODUKSI";
      } else if (upper.startsWith("S.")) {
        NoProduksiInject = rawOutputCode;
        outputKind = "INJECT";
      } else {
        const e = new Error(
          'outputCode must start with "I." or "S." if provided',
        );
        e.statusCode = 400;
        throw e;
      }
    }
  }

  let normalizedDetails = null;
  let detailsJson = null;

  if (hasDetailsField) {
    if (!Array.isArray(details) || details.length === 0)
      throw badReq("Details wajib berisi minimal 1 item");

    normalizedDetails = details.map((d) => {
      const noSak = Number(d?.NoSak);
      if (!Number.isFinite(noSak) || noSak <= 0)
        throw badReq(`NoSak tidak valid: ${d?.NoSak}`);

      const berat = d?.Berat == null ? 0 : Number(d.Berat);
      if (!Number.isFinite(berat) || berat < 0)
        throw badReq(
          `Berat tidak valid pada NoSak ${Math.trunc(noSak)}: ${d?.Berat}`,
        );

      const isPartialRaw = d?.IsPartial;
      const isPartial =
        isPartialRaw === true ||
        isPartialRaw === 1 ||
        String(isPartialRaw).trim() === "1"
          ? 1
          : 0;

      return { NoSak: Math.trunc(noSak), Berat: berat, IsPartial: isPartial };
    });

    const set = new Set();
    for (const x of normalizedDetails) {
      const k = String(x.NoSak);
      if (set.has(k)) throw badReq(`NoSak duplikat di payload: ${x.NoSak}`);
      set.add(k);
    }

    detailsJson = JSON.stringify(normalizedDetails);
  }

  try {
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    await writeRepo.setAuditContext(tx, { actorId, requestId });

    const head = await readRepo.getMixerHeaderForUpdate(tx, NoMixer);
    if (!head) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    const isBso = await readRepo.isMixerFromBongkarSusun(tx, NoMixer);
    if (isBso)
      throw conflict(
        "Data tidak dapat diubah: label ini berasal dari Bongkar Susun.",
      );

    const existingDateCreate = head.DateCreate
      ? toDateOnly(head.DateCreate)
      : null;
    await assertNotLocked({
      date: existingDateCreate,
      runner: tx,
      action: "update mixer",
      useLock: true,
    });

    const setParts = [];
    const bind = [];
    const setIf = (col, param, type, val) => {
      if (val !== undefined) {
        setParts.push(`${col} = @${param}`);
        bind.push({ name: param, type, value: val });
      }
    };

    setIf("IdMixer", "IdMixer", sql.Int, header.IdMixer);

    if (header.DateCreate !== undefined) {
      if (header.DateCreate === null || header.DateCreate === "") {
        const utcToday = toDateOnly(new Date());
        await assertNotLocked({
          date: utcToday,
          runner: tx,
          action: "update mixer (DateCreate reset)",
          useLock: true,
        });
        setParts.push("DateCreate = @DateCreate");
        bind.push({ name: "DateCreate", type: sql.Date, value: utcToday });
      } else {
        const d = toDateOnly(header.DateCreate);
        if (!d) {
          const e = new Error("Invalid DateCreate");
          e.statusCode = 400;
          e.meta = { field: "DateCreate", value: header.DateCreate };
          throw e;
        }
        await assertNotLocked({
          date: d,
          runner: tx,
          action: "update mixer (DateCreate)",
          useLock: true,
        });
        setParts.push("DateCreate = @DateCreate");
        bind.push({ name: "DateCreate", type: sql.Date, value: d });
      }
    }

    setIf("IdStatus", "IdStatus", sql.Int, header.IdStatus);
    setIf("Moisture", "Moisture", sql.Decimal(10, 3), header.Moisture ?? null);
    setIf(
      "MaxMeltTemp",
      "MaxMeltTemp",
      sql.Decimal(10, 3),
      header.MaxMeltTemp ?? null,
    );
    setIf(
      "MinMeltTemp",
      "MinMeltTemp",
      sql.Decimal(10, 3),
      header.MinMeltTemp ?? null,
    );
    setIf("MFI", "MFI", sql.Decimal(10, 3), header.MFI ?? null);
    setIf(
      "Moisture2",
      "Moisture2",
      sql.Decimal(10, 3),
      header.Moisture2 ?? null,
    );
    setIf(
      "Moisture3",
      "Moisture3",
      sql.Decimal(10, 3),
      header.Moisture3 ?? null,
    );
    setIf("Blok", "Blok", sql.VarChar(50), header.Blok ?? null);

    if (header.IdLokasi !== undefined) {
      setParts.push("IdLokasi = @IdLokasi");
      bind.push({
        name: "IdLokasi",
        type: sql.Int,
        value:
          header.IdLokasi != null && String(header.IdLokasi).trim() !== ""
            ? Math.trunc(Number(header.IdLokasi))
            : null,
      });
    }

    await writeRepo.updateMixerHeaderDynamic({
      tx,
      noMixer: NoMixer,
      setParts,
      bind,
    });

    let detailsAffected = 0;
    if (hasDetailsField) {
      await writeRepo.deleteActiveDetails(tx, NoMixer);
      await writeRepo.insertMixerDetailsBulk({
        tx,
        noMixer: NoMixer,
        detailsJson,
      });
      detailsAffected = normalizedDetails.length;
    }

    let outputTarget = null;
    let outputCount = 0;

    if (hasOutputCodeField) {
      await writeRepo.clearOutputsByNoMixer(tx, NoMixer);

      if (rawOutputCode) {
        const sourceMode = hasDetailsField ? "JSON" : "DB";

        if (NoProduksiMixer) {
          if (sourceMode === "JSON") {
            await writeRepo.insertMixerProduksiOutputBulk({
              tx,
              noProduksi: NoProduksiMixer,
              noMixer: NoMixer,
              detailsJson,
            });
            outputCount = normalizedDetails.length;
          } else {
            outputCount =
              await writeRepo.insertMixerProduksiOutputFromExistingDetails({
                tx,
                noProduksi: NoProduksiMixer,
                noMixer: NoMixer,
              });
          }
          outputTarget = "MixerProduksiOutput";
        } else if (NoProduksiInject) {
          if (sourceMode === "JSON") {
            await writeRepo.insertInjectProduksiOutputBulk({
              tx,
              noProduksi: NoProduksiInject,
              noMixer: NoMixer,
              detailsJson,
            });
            outputCount = normalizedDetails.length;
          } else {
            outputCount = await writeRepo.insertInjectOutputFromExistingDetails(
              { tx, noProduksi: NoProduksiInject, noMixer: NoMixer },
            );
          }
          outputTarget = "InjectProduksiOutputMixer";
        }
      }
    }

    await tx.commit();

    return {
      header: { NoMixer, ...header },
      counts: { detailsAffected, outputInserted: outputCount },
      outputTarget,
      outputKind,
      outputCode: hasOutputCodeField ? rawOutputCode : undefined,
      audit: { actorId, requestId },
      note: hasDetailsField
        ? "Details aktif (DateUsage IS NULL) diganti sesuai payload."
        : "Details tidak diubah.",
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

exports.deleteMixerCascade = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const NoMixer = (payload?.NoMixer || payload || "").toString().trim();
  if (!NoMixer) {
    const e = new Error("NoMixer is required");
    e.statusCode = 400;
    throw e;
  }

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
    await writeRepo.setAuditContext(tx, { actorId, requestId });

    const head = await readRepo.getMixerHeaderForUpdate(tx, NoMixer);
    if (!head) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    const trxDate = head.DateCreate ? toDateOnly(head.DateCreate) : null;
    await assertNotLocked({
      date: trxDate,
      runner: tx,
      action: "delete mixer",
      useLock: true,
    });

    const used = await readRepo.hasUsedDetails(tx, NoMixer);
    if (used) {
      const e = new Error(
        "Cannot delete: some details are already used (DateUsage IS NOT NULL).",
      );
      e.statusCode = 409;
      throw e;
    }

    const outputs = await writeRepo.deleteOutputsForDeleteCascade(tx, NoMixer);
    const partialInputs = await writeRepo.deletePartialInputReferences(
      tx,
      NoMixer,
    );
    const partialCount = await writeRepo.deleteMixerPartials(tx, NoMixer);
    await writeRepo.deleteActiveDetails(tx, NoMixer);
    const headerDeleted = await writeRepo.deleteMixerHeader(tx, NoMixer);

    if (headerDeleted === 0) {
      const e = new Error(`NoMixer ${NoMixer} not found`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoMixer,
      audit: { actorId, requestId },
      deleted: {
        header: headerDeleted,
        outputs,
        partials: { mixerPartial: partialCount, ...partialInputs },
      },
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}

    if (e.number === 547) {
      e.statusCode = 409;
      e.message = e.message || "Delete failed due to foreign key constraint.";
    }

    throw e;
  }
};

exports.getPartialInfoByMixerAndSak = async (nomixer, nosak) => {
  const rowsRaw = await readRepo.getPartialInfoRowsByMixerAndSak(
    nomixer,
    nosak,
  );

  const seen = new Set();
  let totalPartialWeight = 0;
  for (const row of rowsRaw) {
    const key = row.NoMixerPartial;
    if (!seen.has(key)) {
      seen.add(key);
      const w =
        typeof row.Berat === "number" ? row.Berat : Number(row.Berat) || 0;
      totalPartialWeight += w;
    }
  }

  const formatDate = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? `0${n}` : `${n}`);
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const rows = rowsRaw.map((r) => ({
    NoMixerPartial: r.NoMixerPartial,
    NoMixer: r.NoMixer,
    NoSak: r.NoSak,
    Berat: r.Berat,
    SourceType: r.SourceType || null,
    NoProduksi: r.NoProduksi || null,
    TglProduksi: r.TglProduksi ? formatDate(r.TglProduksi) : null,
    IdMesin: r.IdMesin || null,
    NamaMesin: r.NamaMesin || null,
    IdOperator: r.IdOperator || null,
    Jam: r.Jam || null,
    Shift: r.Shift || null,
  }));

  return { totalPartialWeight, rows };
};

exports.incrementHasBeenPrinted = async (payload) => {
  const NoMixer = String(payload?.NoMixer || "").trim();
  if (!NoMixer) throw badReq("NoMixer wajib diisi");

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
    await writeRepo.setAuditContext(tx, { actorId, requestId });

    const row = await writeRepo.incrementHasBeenPrinted(tx, NoMixer);
    if (!row) {
      const e = new Error(`NoMixer ${NoMixer} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    await tx.commit();

    return {
      NoMixer: row.NoMixer,
      HasBeenPrinted: row.HasBeenPrinted,
    };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
};

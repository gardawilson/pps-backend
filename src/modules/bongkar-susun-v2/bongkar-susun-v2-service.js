// modules/bongkar-susun-v2/bongkar-susun-v2-service.js
const { sql, poolPromise } = require("../../core/config/db");
const { conflict } = require("../../core/utils/http-error");
const { formatYMD } = require("../../core/shared/tutup-transaksi-guard");
const {
  detectCategory,
  CREATE_METHOD_BY_CATEGORY,
  LABEL_INFO_METHOD_BY_CATEGORY,
} = require("./bongkar-susun-v2-category-registry");
const getLabelInfoWashingHandler = require("./handlers/get-label-info-washing.handler");
const getLabelInfoBrokerHandler = require("./handlers/get-label-info-broker.handler");
const getLabelInfoBonggolanHandler = require("./handlers/get-label-info-bonggolan.handler");

// GET label info dispatcher
exports.getLabelInfo = async (labelCode) => {
  const code = String(labelCode || "").trim();
  const category = detectCategory(code);

  if (!category) {
    const e = new Error(`Label code tidak dikenali: ${code}`);
    e.statusCode = 400;
    throw e;
  }

  const method = LABEL_INFO_METHOD_BY_CATEGORY[category];
  if (!method) {
    const e = new Error(`Kategori ${category} belum didukung`);
    e.statusCode = 400;
    throw e;
  }

  const handlers = {
    getLabelInfoWashing: getLabelInfoWashingHandler.getLabelInfoWashing,
    getLabelInfoBroker: getLabelInfoBrokerHandler.getLabelInfoBroker,
    getLabelInfoBonggolan: getLabelInfoBonggolanHandler.getLabelInfoBonggolan,
  };

  const fn = handlers[method];
  if (typeof fn !== "function") {
    const e = new Error(`Handler ${method} tidak tersedia`);
    e.statusCode = 500;
    throw e;
  }

  return fn(code);
};

// â”€â”€â”€ GET list BongkarSusun v2 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
exports.getAll = async (page = 1, pageSize = 20, search = "") => {
  const pool = await poolPromise;
  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(200, Number(pageSize) || 20));
  const offset = (p - 1) * ps;
  const searchTerm = (search || "").trim();

  const whereClause = `WHERE (@search = '' OR h.NoBongkarSusun LIKE '%' + @search + '%')`;

  const countRes = await pool
    .request()
    .input("search", sql.VarChar(100), searchTerm)
    .query(`SELECT COUNT(1) AS total FROM dbo.BongkarSusun_h h ${whereClause}`);

  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) return { data: [], total: 0 };

  const dataRes = await pool
    .request()
    .input("search", sql.VarChar(100), searchTerm)
    .input("offset", sql.Int, offset)
    .input("pageSize", sql.Int, ps).query(`
      SELECT
        h.NoBongkarSusun,
        h.Tanggal,
        h.IdUsername,
        h.Note
      FROM dbo.BongkarSusun_h h
      ${whereClause}
      ORDER BY h.NoBongkarSusun DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY
    `);

  return { data: dataRes.recordset, total };
};

// â”€â”€â”€ GET detail satu transaksi

exports.getDetail = async (noBongkarSusun) => {
  const pool = await poolPromise;

  const headerRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT NoBongkarSusun, Tanggal, IdUsername, Note
      FROM dbo.BongkarSusun_h
      WHERE NoBongkarSusun = @NoBongkarSusun
    `);

  if (!headerRes.recordset.length) {
    const e = new Error(`NoBongkarSusun ${noBongkarSusun} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  // inputs â€” washing
  const inputsWashingRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bi.NoWashing          AS labelCode,
        'washing'             AS category,
        h.IdJenisPlastik      AS idJenis,
        mw.Nama               AS namaJenis,
        COUNT(bi.NoSak)       AS jumlahSak,
        SUM(d.Berat)          AS totalBerat
      FROM BongkarSusunInputWashing bi
      INNER JOIN Washing_h  h  ON h.NoWashing   = bi.NoWashing
      INNER JOIN MstWashing mw ON mw.IdWashing  = h.IdJenisPlastik
      INNER JOIN Washing_d  d  ON d.NoWashing   = bi.NoWashing
                               AND d.NoSak      = bi.NoSak
      WHERE bi.NoBongkarSusun = @NoBongkarSusun
      GROUP BY bi.NoWashing, h.IdJenisPlastik, mw.Nama
    `);

  // inputs â€” bonggolan
  const inputsBonggolanRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bi.NoBonggolan        AS labelCode,
        'bonggolan'           AS category,
        b.IdBonggolan         AS idJenis,
        b.Berat               AS totalBerat
      FROM BongkarSusunInputBonggolan bi
      INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = bi.NoBonggolan
      WHERE bi.NoBongkarSusun = @NoBongkarSusun
    `);

  // outputs â€” washing
  const inputsBrokerRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bi.NoBroker           AS labelCode,
        'broker'              AS category,
        h.IdJenisPlastik      AS idJenis,
        COUNT(bi.NoSak)       AS jumlahSak,
        SUM(d.Berat)          AS totalBerat
      FROM BongkarSusunInputBroker bi
      INNER JOIN dbo.Broker_h h ON h.NoBroker = bi.NoBroker
      INNER JOIN dbo.Broker_d d
        ON d.NoBroker = bi.NoBroker
       AND d.NoSak = bi.NoSak
      WHERE bi.NoBongkarSusun = @NoBongkarSusun
      GROUP BY bi.NoBroker, h.IdJenisPlastik
    `);

  const outputsWashingRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bo.NoWashing          AS labelCode,
        'washing'             AS category,
        h.IdJenisPlastik      AS idJenis,
        mw.Nama               AS namaJenis,
        COUNT(bo.NoSak)       AS jumlahSak,
        SUM(d.Berat)          AS totalBerat
      FROM BongkarSusunOutputWashing bo
      INNER JOIN Washing_h  h  ON h.NoWashing   = bo.NoWashing
      INNER JOIN MstWashing mw ON mw.IdWashing  = h.IdJenisPlastik
      INNER JOIN Washing_d  d  ON d.NoWashing   = bo.NoWashing
                               AND d.NoSak      = bo.NoSak
      WHERE bo.NoBongkarSusun = @NoBongkarSusun
      GROUP BY bo.NoWashing, h.IdJenisPlastik, mw.Nama
    `);

  // outputs â€” bonggolan
  const outputsBonggolanRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bo.NoBonggolan        AS labelCode,
        'bonggolan'           AS category,
        b.IdBonggolan         AS idJenis,
        b.Berat               AS totalBerat
      FROM BongkarSusunOutputBonggolan bo
      INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = bo.NoBonggolan
      WHERE bo.NoBongkarSusun = @NoBongkarSusun
    `);

  const outputsBrokerRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bo.NoBroker           AS labelCode,
        'broker'              AS category,
        h.IdJenisPlastik      AS idJenis,
        COUNT(bo.NoSak)       AS jumlahSak,
        SUM(d.Berat)          AS totalBerat
      FROM BongkarSusunOutputBroker bo
      INNER JOIN dbo.Broker_h h ON h.NoBroker = bo.NoBroker
      INNER JOIN dbo.Broker_d d
        ON d.NoBroker = bo.NoBroker
       AND d.NoSak = bo.NoSak
      WHERE bo.NoBongkarSusun = @NoBongkarSusun
      GROUP BY bo.NoBroker, h.IdJenisPlastik
    `);

  return {
    header: headerRes.recordset[0],
    inputs: [
      ...inputsWashingRes.recordset,
      ...inputsBonggolanRes.recordset,
      ...inputsBrokerRes.recordset,
    ],
    outputs: [
      ...outputsWashingRes.recordset,
      ...outputsBonggolanRes.recordset,
      ...outputsBrokerRes.recordset,
    ],
  };
};

// create handlers
const createWashingHandler = require("./handlers/create-washing.handler");
const createBrokerHandler = require("./handlers/create-broker.handler");
const createBonggolanHandler = require("./handlers/create-bonggolan.handler");
exports.deleteBongkarSusun = async (noBongkarSusun, ctx) => {
  const { actorId, requestId } = ctx;
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

    // Cek header exist + lock
    const headerRes = await new sql.Request(tx).input(
      "NoBongkarSusun",
      sql.VarChar(50),
      noBongkarSusun,
    ).query(`
        SELECT NoBongkarSusun FROM dbo.BongkarSusun_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBongkarSusun = @NoBongkarSusun
      `);

    if (!headerRes.recordset.length) {
      const e = new Error(`NoBongkarSusun ${noBongkarSusun} tidak ditemukan`);
      e.statusCode = 404;
      throw e;
    }

    // Ambil output washings dari transaksi ini (distinct NoWashing)
    const outputsRes = await new sql.Request(tx).input(
      "NoBongkarSusun",
      sql.VarChar(50),
      noBongkarSusun,
    ).query(`
        SELECT DISTINCT NoWashing
        FROM dbo.BongkarSusunOutputWashing
        WHERE NoBongkarSusun = @NoBongkarSusun
      `);

    const outputWashings = outputsRes.recordset.map((r) => r.NoWashing);

    if (outputWashings.length > 0) {
      const outputCodesJson = JSON.stringify(
        outputWashings.map((c) => ({ code: c })),
      );

      // Cek apakah output labels sudah terpakai lagi
      const usedRes = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outputCodesJson,
      ).query(`
          SELECT TOP 1 NoWashing
          FROM dbo.Washing_d
          WHERE NoWashing IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);

      if (usedRes.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output sudah digunakan di proses lain",
        );

      // Hapus BongkarSusunOutputWashing, Washing_d, Washing_h output
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputWashing WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outputCodesJson,
      ).query(`
          DELETE FROM dbo.Washing_d
          WHERE NoWashing IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outputCodesJson,
      ).query(`
          DELETE FROM dbo.Washing_h
          WHERE NoWashing IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
    }

    // Kembalikan DateUsage input washing ke NULL
    const inputsWashingRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT DISTINCT NoWashing FROM dbo.BongkarSusunInputWashing WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsWashingRes.recordset.length > 0) {
      const inputCodesJson = JSON.stringify(
        inputsWashingRes.recordset.map((r) => ({ code: r.NoWashing })),
      );
      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inputCodesJson,
      ).query(`
          UPDATE dbo.Washing_d SET DateUsage = NULL
          WHERE NoWashing IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputWashing WHERE NoBongkarSusun = @NoBongkarSusun`,
        );
    }

    // Handle output & input bonggolan
    const outputsBonggolanRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoBonggolan FROM dbo.BongkarSusunOutputBonggolan WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (outputsBonggolanRes.recordset.length > 0) {
      const outputBonggolanCodes = outputsBonggolanRes.recordset.map(
        (r) => r.NoBonggolan,
      );
      const outBonggolanJson = JSON.stringify(
        outputBonggolanCodes.map((c) => ({ code: c })),
      );

      const usedBonggolan = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBonggolanJson,
      ).query(`
          SELECT TOP 1 NoBonggolan FROM dbo.Bonggolan
          WHERE NoBonggolan IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);
      if (usedBonggolan.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output bonggolan sudah digunakan di proses lain",
        );

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputBonggolan WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBonggolanJson,
      ).query(`
          DELETE FROM dbo.Bonggolan
          WHERE NoBonggolan IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
    }

    // Kembalikan DateUsage input bonggolan ke NULL
    const inputsBonggolanRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoBonggolan FROM dbo.BongkarSusunInputBonggolan WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsBonggolanRes.recordset.length > 0) {
      const inBonggolanJson = JSON.stringify(
        inputsBonggolanRes.recordset.map((r) => ({ code: r.NoBonggolan })),
      );
      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inBonggolanJson,
      ).query(`
          UPDATE dbo.Bonggolan SET DateUsage = NULL
          WHERE NoBonggolan IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputBonggolan WHERE NoBongkarSusun = @NoBongkarSusun`,
        );
    }

    // Handle output & input broker
    const outputsBrokerRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoBroker FROM dbo.BongkarSusunOutputBroker WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (outputsBrokerRes.recordset.length > 0) {
      const outputBrokerCodes = outputsBrokerRes.recordset.map(
        (r) => r.NoBroker,
      );
      const outBrokerJson = JSON.stringify(
        outputBrokerCodes.map((c) => ({ code: c })),
      );

      const usedBroker = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBrokerJson,
      ).query(`
          SELECT TOP 1 NoBroker FROM dbo.Broker_d
          WHERE NoBroker IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);
      if (usedBroker.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output broker sudah digunakan di proses lain",
        );

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputBroker WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBrokerJson,
      ).query(`
          DELETE FROM dbo.Broker_d
          WHERE NoBroker IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBrokerJson,
      ).query(`
          DELETE FROM dbo.Broker_h
          WHERE NoBroker IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
    }

    const inputsBrokerRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoBroker FROM dbo.BongkarSusunInputBroker WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsBrokerRes.recordset.length > 0) {
      const inBrokerJson = JSON.stringify(
        inputsBrokerRes.recordset.map((r) => ({ code: r.NoBroker })),
      );
      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inBrokerJson,
      ).query(`
          UPDATE dbo.Broker_d SET DateUsage = NULL
          WHERE NoBroker IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputBroker WHERE NoBongkarSusun = @NoBongkarSusun`,
        );
    }

    // Hapus header
    await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `DELETE FROM dbo.BongkarSusun_h WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    await tx.commit();

    return { success: true, noBongkarSusun, audit: { actorId, requestId } };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    if (e.number === 547) {
      e.statusCode = 409;
      e.message = "Gagal hapus karena constraint referensi (FK).";
    }
    throw e;
  }
};

exports.createBongkarSusunByCategory = async (category, payload, ctx) => {
  const method = CREATE_METHOD_BY_CATEGORY[category];
  if (!method || typeof exports[method] !== "function") {
    const e = new Error(`Kategori ${category} belum didukung`);
    e.statusCode = 400;
    throw e;
  }
  return exports[method](payload, ctx);
};

exports.createBongkarSusunWashing =
  createWashingHandler.createBongkarSusunWashing;
exports.createBongkarSusunBroker = createBrokerHandler.createBongkarSusunBroker;
exports.createBongkarSusunBonggolan =
  createBonggolanHandler.createBongkarSusunBonggolan;

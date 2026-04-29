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
const getLabelInfoBahanBakuHandler = require("./handlers/get-label-info-bahan-baku.handler");
const getLabelInfoBrokerHandler = require("./handlers/get-label-info-broker.handler");
const getLabelInfoCrusherHandler = require("./handlers/get-label-info-crusher.handler");
const getLabelInfoGilinganHandler = require("./handlers/get-label-info-gilingan.handler");
const getLabelInfoFurnitureWipHandler = require("./handlers/get-label-info-furniture-wip.handler");
const getLabelInfoBonggolanHandler = require("./handlers/get-label-info-bonggolan.handler");
const getLabelInfoBarangJadiHandler = require("./handlers/get-label-info-barang-jadi.handler");
const getLabelInfoMixerHandler = require("./handlers/get-label-info-mixer.handler");

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
    getLabelInfoBahanBaku: getLabelInfoBahanBakuHandler.getLabelInfoBahanBaku,
    getLabelInfoWashing: getLabelInfoWashingHandler.getLabelInfoWashing,
    getLabelInfoBroker: getLabelInfoBrokerHandler.getLabelInfoBroker,
    getLabelInfoCrusher: getLabelInfoCrusherHandler.getLabelInfoCrusher,
    getLabelInfoGilingan: getLabelInfoGilinganHandler.getLabelInfoGilingan,
    getLabelInfoFurnitureWip:
      getLabelInfoFurnitureWipHandler.getLabelInfoFurnitureWip,
    getLabelInfoBarangJadi:
      getLabelInfoBarangJadiHandler.getLabelInfoBarangJadi,
    getLabelInfoBonggolan: getLabelInfoBonggolanHandler.getLabelInfoBonggolan,
    getLabelInfoMixer: getLabelInfoMixerHandler.getLabelInfoMixer,
  };

  const fn = handlers[method];
  if (typeof fn !== "function") {
    const e = new Error(`Handler ${method} tidak tersedia`);
    e.statusCode = 500;
    throw e;
  }

  return fn(code);
};

// â”€â”€â”€ GET list BongkarSusun v2
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
        u.Username,
        h.Note,
        ISNULL(cat.category, '') AS category,
        ISNULL(cat.inputLabelCount, 0) AS inputLabelCount,
        ISNULL(cat.outputLabelCount, 0) AS outputLabelCount,
        ISNULL(bal.balance, CAST(0 AS bit)) AS balance
      FROM dbo.BongkarSusun_h h
      LEFT JOIN dbo.MstUsername u
        ON u.IdUsername = h.IdUsername
      OUTER APPLY (
        SELECT TOP (1)
          x.category,
          x.inputLabelCount,
          x.outputLabelCount
        FROM (
          SELECT
            'bahanBaku' AS category,
            (
              SELECT COUNT(DISTINCT ib.NoBahanBaku + '-' + CAST(ib.NoPallet AS varchar(20)))
              FROM dbo.BongkarSusunInputBahanBaku ib
              WHERE ib.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT ob.NoBahanBaku + '-' + CAST(ob.NoPallet AS varchar(20)))
              FROM dbo.BongkarSusunOutputBahanBaku ob
              WHERE ob.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            1 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputBahanBaku ib
            WHERE ib.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputBahanBaku ob
            WHERE ob.NoBongkarSusun = h.NoBongkarSusun
          )

          UNION ALL

          SELECT
            'washing' AS category,
            (
              SELECT COUNT(DISTINCT iw.NoWashing)
              FROM dbo.BongkarSusunInputWashing iw
              WHERE iw.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT ow.NoWashing)
              FROM dbo.BongkarSusunOutputWashing ow
              WHERE ow.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            2 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputWashing iw
            WHERE iw.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputWashing ow
            WHERE ow.NoBongkarSusun = h.NoBongkarSusun
          )

          UNION ALL

          SELECT
            'broker' AS category,
            (
              SELECT COUNT(DISTINCT ib.NoBroker)
              FROM dbo.BongkarSusunInputBroker ib
              WHERE ib.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT ob.NoBroker)
              FROM dbo.BongkarSusunOutputBroker ob
              WHERE ob.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            3 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputBroker ib
            WHERE ib.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputBroker ob
            WHERE ob.NoBongkarSusun = h.NoBongkarSusun
          )

          UNION ALL

          SELECT
            'crusher' AS category,
            (
              SELECT COUNT(DISTINCT ic.NoCrusher)
              FROM dbo.BongkarSusunInputCrusher ic
              WHERE ic.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT oc.NoCrusher)
              FROM dbo.BongkarSusunOutputCrusher oc
              WHERE oc.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            4 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputCrusher ic
            WHERE ic.NoBongkarSusun = h.NoBongkarSusun
            )
            OR EXISTS (
              SELECT 1
              FROM dbo.BongkarSusunOutputCrusher oc
              WHERE oc.NoBongkarSusun = h.NoBongkarSusun
            )

          UNION ALL

          SELECT
            'gilingan' AS category,
            (
              SELECT COUNT(DISTINCT ig.NoGilingan)
              FROM dbo.BongkarSusunInputGilingan ig
              WHERE ig.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT og.NoGilingan)
              FROM dbo.BongkarSusunOutputGilingan og
              WHERE og.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            5 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputGilingan ig
            WHERE ig.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputGilingan og
            WHERE og.NoBongkarSusun = h.NoBongkarSusun
            )

          UNION ALL

          SELECT
            'mixer' AS category,
            (
              SELECT COUNT(DISTINCT im.NoMixer)
              FROM dbo.BongkarSusunInputMixer im
              WHERE im.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT om.NoMixer)
              FROM dbo.BongkarSusunOutputMixer om
              WHERE om.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            6 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputMixer im
            WHERE im.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputMixer om
            WHERE om.NoBongkarSusun = h.NoBongkarSusun
          )

          UNION ALL

          SELECT
            'furnitureWip' AS category,
            (
              SELECT COUNT(DISTINCT ifw.NoFurnitureWIP)
              FROM dbo.BongkarSusunInputFurnitureWIP ifw
              WHERE ifw.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT ofw.NoFurnitureWIP)
              FROM dbo.BongkarSusunOutputFurnitureWIP ofw
              WHERE ofw.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            7 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputFurnitureWIP ifw
            WHERE ifw.NoBongkarSusun = h.NoBongkarSusun
            )
            OR EXISTS (
              SELECT 1
              FROM dbo.BongkarSusunOutputFurnitureWIP ofw
              WHERE ofw.NoBongkarSusun = h.NoBongkarSusun
            )

          UNION ALL

          SELECT
            'barangJadi' AS category,
            (
              SELECT COUNT(DISTINCT ibj.NoBJ)
              FROM dbo.BongkarSusunInputBarangJadi ibj
              WHERE ibj.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT obj.NoBJ)
              FROM dbo.BongkarSusunOutputBarangjadi obj
              WHERE obj.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            8 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputBarangJadi ibj
            WHERE ibj.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputBarangjadi obj
            WHERE obj.NoBongkarSusun = h.NoBongkarSusun
          )

          UNION ALL

          SELECT
            'bonggolan' AS category,
            (
              SELECT COUNT(DISTINCT ibg.NoBonggolan)
              FROM dbo.BongkarSusunInputBonggolan ibg
              WHERE ibg.NoBongkarSusun = h.NoBongkarSusun
            ) AS inputLabelCount,
            (
              SELECT COUNT(DISTINCT obg.NoBonggolan)
              FROM dbo.BongkarSusunOutputBonggolan obg
              WHERE obg.NoBongkarSusun = h.NoBongkarSusun
            ) AS outputLabelCount,
            9 AS priority
          WHERE EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunInputBonggolan ibg
            WHERE ibg.NoBongkarSusun = h.NoBongkarSusun
          )
          OR EXISTS (
            SELECT 1
            FROM dbo.BongkarSusunOutputBonggolan obg
            WHERE obg.NoBongkarSusun = h.NoBongkarSusun
          )
        ) x
        ORDER BY x.priority
      ) cat
      OUTER APPLY (
        SELECT CASE
          WHEN (
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputBahanBaku  WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputBahanBaku   WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputWashing    WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputWashing      WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputBroker     WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputBroker       WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputCrusher    WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputCrusher      WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputGilingan   WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputGilingan     WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputMixer      WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputMixer        WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputFurnitureWIP WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputFurnitureWIP WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputBarangJadi WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputBarangjadi  WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END) +
            (CASE WHEN EXISTS(SELECT 1 FROM dbo.BongkarSusunInputBonggolan  WHERE NoBongkarSusun = h.NoBongkarSusun) OR EXISTS(SELECT 1 FROM dbo.BongkarSusunOutputBonggolan   WHERE NoBongkarSusun = h.NoBongkarSusun) THEN 1 ELSE 0 END)
          ) > 1 THEN CAST(0 AS bit)
          WHEN cat.category = 'bahanBaku' THEN
            CASE
              WHEN EXISTS (
                SELECT ph.IdJenisPlastik FROM dbo.BongkarSusunInputBahanBaku ib
                INNER JOIN dbo.BahanBakuPallet_h ph ON ph.NoBahanBaku = ib.NoBahanBaku AND ph.NoPallet = ib.NoPallet
                WHERE ib.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT ph.IdJenisPlastik FROM dbo.BongkarSusunOutputBahanBaku ob
                INNER JOIN dbo.BahanBakuPallet_h ph ON ph.NoBahanBaku = ob.NoBahanBaku AND ph.NoPallet = ob.NoPallet
                WHERE ob.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT ph.IdJenisPlastik FROM dbo.BongkarSusunOutputBahanBaku ob
                INNER JOIN dbo.BahanBakuPallet_h ph ON ph.NoBahanBaku = ob.NoBahanBaku AND ph.NoPallet = ob.NoPallet
                WHERE ob.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT ph.IdJenisPlastik FROM dbo.BongkarSusunInputBahanBaku ib
                INNER JOIN dbo.BahanBakuPallet_h ph ON ph.NoBahanBaku = ib.NoBahanBaku AND ph.NoPallet = ib.NoPallet
                WHERE ib.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunInputBahanBaku ib
                  INNER JOIN dbo.BahanBaku_d d
                    ON d.NoBahanBaku = ib.NoBahanBaku
                   AND d.NoPallet = ib.NoPallet
                   AND d.NoSak = ib.NoSak
                  WHERE ib.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunOutputBahanBaku ob
                  LEFT JOIN dbo.BahanBaku_d d
                    ON d.NoBahanBaku = ob.NoBahanBaku
                   AND d.NoPallet = ob.NoPallet
                   AND d.NoSak = ob.NoSak
                  WHERE ob.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'washing' THEN
            CASE
              WHEN EXISTS (
                SELECT wh.IdJenisPlastik FROM dbo.BongkarSusunInputWashing iw
                INNER JOIN dbo.Washing_h wh ON wh.NoWashing = iw.NoWashing
                WHERE iw.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT wh.IdJenisPlastik FROM dbo.BongkarSusunOutputWashing ow
                INNER JOIN dbo.Washing_h wh ON wh.NoWashing = ow.NoWashing
                WHERE ow.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT wh.IdJenisPlastik FROM dbo.BongkarSusunOutputWashing ow
                INNER JOIN dbo.Washing_h wh ON wh.NoWashing = ow.NoWashing
                WHERE ow.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT wh.IdJenisPlastik FROM dbo.BongkarSusunInputWashing iw
                INNER JOIN dbo.Washing_h wh ON wh.NoWashing = iw.NoWashing
                WHERE iw.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunInputWashing iw
                  INNER JOIN dbo.Washing_d d
                    ON d.NoWashing = iw.NoWashing
                   AND d.NoSak = iw.NoSak
                  WHERE iw.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunOutputWashing ow
                  INNER JOIN dbo.Washing_d d
                    ON d.NoWashing = ow.NoWashing
                   AND d.NoSak = ow.NoSak
                  WHERE ow.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'broker' THEN
            CASE
              WHEN EXISTS (
                SELECT bh.IdJenisPlastik FROM dbo.BongkarSusunInputBroker ib
                INNER JOIN dbo.Broker_h bh ON bh.NoBroker = ib.NoBroker
                WHERE ib.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT bh.IdJenisPlastik FROM dbo.BongkarSusunOutputBroker ob
                INNER JOIN dbo.Broker_h bh ON bh.NoBroker = ob.NoBroker
                WHERE ob.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT bh.IdJenisPlastik FROM dbo.BongkarSusunOutputBroker ob
                INNER JOIN dbo.Broker_h bh ON bh.NoBroker = ob.NoBroker
                WHERE ob.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT bh.IdJenisPlastik FROM dbo.BongkarSusunInputBroker ib
                INNER JOIN dbo.Broker_h bh ON bh.NoBroker = ib.NoBroker
                WHERE ib.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunInputBroker ib
                  INNER JOIN dbo.Broker_d d
                    ON d.NoBroker = ib.NoBroker
                   AND d.NoSak = ib.NoSak
                  WHERE ib.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunOutputBroker ob
                  INNER JOIN dbo.Broker_d d
                    ON d.NoBroker = ob.NoBroker
                   AND d.NoSak = ob.NoSak
                  WHERE ob.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'crusher' THEN
            CASE
              WHEN EXISTS (
                SELECT c.IdCrusher FROM dbo.BongkarSusunInputCrusher ic
                INNER JOIN dbo.Crusher c ON c.NoCrusher = ic.NoCrusher
                WHERE ic.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT c.IdCrusher FROM dbo.BongkarSusunOutputCrusher oc
                INNER JOIN dbo.Crusher c ON c.NoCrusher = oc.NoCrusher
                WHERE oc.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT c.IdCrusher FROM dbo.BongkarSusunOutputCrusher oc
                INNER JOIN dbo.Crusher c ON c.NoCrusher = oc.NoCrusher
                WHERE oc.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT c.IdCrusher FROM dbo.BongkarSusunInputCrusher ic
                INNER JOIN dbo.Crusher c ON c.NoCrusher = ic.NoCrusher
                WHERE ic.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(ISNULL(c.Berat, 0))
                  FROM dbo.BongkarSusunInputCrusher ic
                  INNER JOIN dbo.Crusher c ON c.NoCrusher = ic.NoCrusher
                  WHERE ic.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(c.Berat, 0))
                  FROM dbo.BongkarSusunOutputCrusher oc
                  INNER JOIN dbo.Crusher c ON c.NoCrusher = oc.NoCrusher
                  WHERE oc.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'gilingan' THEN
            CASE
              WHEN EXISTS (
                SELECT g.IdGilingan FROM dbo.BongkarSusunInputGilingan ig
                INNER JOIN dbo.Gilingan g ON g.NoGilingan = ig.NoGilingan
                WHERE ig.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT g.IdGilingan FROM dbo.BongkarSusunOutputGilingan og
                INNER JOIN dbo.Gilingan g ON g.NoGilingan = og.NoGilingan
                WHERE og.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT g.IdGilingan FROM dbo.BongkarSusunOutputGilingan og
                INNER JOIN dbo.Gilingan g ON g.NoGilingan = og.NoGilingan
                WHERE og.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT g.IdGilingan FROM dbo.BongkarSusunInputGilingan ig
                INNER JOIN dbo.Gilingan g ON g.NoGilingan = ig.NoGilingan
                WHERE ig.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(
                    CASE
                      WHEN g.IsPartial = 1 THEN
                        CASE
                          WHEN ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0) < 0 THEN 0
                          ELSE ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0)
                        END
                      ELSE ISNULL(g.Berat, 0)
                    END
                  )
                  FROM dbo.BongkarSusunInputGilingan ig
                  INNER JOIN dbo.Gilingan g ON g.NoGilingan = ig.NoGilingan
                  LEFT JOIN (
                    SELECT NoGilingan, SUM(ISNULL(Berat, 0)) AS TotalPartial
                    FROM dbo.GilinganPartial
                    GROUP BY NoGilingan
                  ) gp ON gp.NoGilingan = g.NoGilingan
                  WHERE ig.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(g.Berat, 0))
                  FROM dbo.BongkarSusunOutputGilingan og
                  INNER JOIN dbo.Gilingan g ON g.NoGilingan = og.NoGilingan
                  WHERE og.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'mixer' THEN
            CASE
              WHEN EXISTS (
                SELECT mh.IdMixer FROM dbo.BongkarSusunInputMixer im
                INNER JOIN dbo.Mixer_h mh ON mh.NoMixer = im.NoMixer
                WHERE im.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT mh.IdMixer FROM dbo.BongkarSusunOutputMixer om
                INNER JOIN dbo.Mixer_h mh ON mh.NoMixer = om.NoMixer
                WHERE om.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT mh.IdMixer FROM dbo.BongkarSusunOutputMixer om
                INNER JOIN dbo.Mixer_h mh ON mh.NoMixer = om.NoMixer
                WHERE om.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT mh.IdMixer FROM dbo.BongkarSusunInputMixer im
                INNER JOIN dbo.Mixer_h mh ON mh.NoMixer = im.NoMixer
                WHERE im.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(
                    ISNULL(d.Berat, 0) - ISNULL(mp.TotalPartial, 0)
                  )
                  FROM dbo.BongkarSusunInputMixer im
                  INNER JOIN dbo.Mixer_d d
                    ON d.NoMixer = im.NoMixer
                   AND d.NoSak = im.NoSak
                  LEFT JOIN (
                    SELECT NoMixer, NoSak, SUM(ISNULL(Berat, 0)) AS TotalPartial
                    FROM dbo.MixerPartial
                    GROUP BY NoMixer, NoSak
                  ) mp
                    ON mp.NoMixer = d.NoMixer
                   AND mp.NoSak = d.NoSak
                  WHERE im.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(d.Berat, 0))
                  FROM dbo.BongkarSusunOutputMixer om
                  INNER JOIN dbo.Mixer_d d
                    ON d.NoMixer = om.NoMixer
                   AND d.NoSak = om.NoSak
                  WHERE om.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'furnitureWip' THEN
            CASE
              WHEN EXISTS (
                SELECT f.IdFurnitureWIP FROM dbo.BongkarSusunInputFurnitureWIP ifw
                INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ifw.NoFurnitureWIP
                WHERE ifw.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT f.IdFurnitureWIP FROM dbo.BongkarSusunOutputFurnitureWIP ofw
                INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ofw.NoFurnitureWIP
                WHERE ofw.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT f.IdFurnitureWIP FROM dbo.BongkarSusunOutputFurnitureWIP ofw
                INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ofw.NoFurnitureWIP
                WHERE ofw.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT f.IdFurnitureWIP FROM dbo.BongkarSusunInputFurnitureWIP ifw
                INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ifw.NoFurnitureWIP
                WHERE ifw.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(
                    CASE
                      WHEN f.IsPartial = 1 THEN
                        CASE
                          WHEN ISNULL(f.Pcs, 0) - ISNULL(fp.TotalPartialPcs, 0) < 0 THEN 0
                          ELSE ISNULL(f.Pcs, 0) - ISNULL(fp.TotalPartialPcs, 0)
                        END
                      ELSE ISNULL(f.Pcs, 0)
                    END
                  )
                  FROM dbo.BongkarSusunInputFurnitureWIP ifw
                  INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ifw.NoFurnitureWIP
                  LEFT JOIN (
                    SELECT NoFurnitureWIP, SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
                    FROM dbo.FurnitureWIPPartial
                    GROUP BY NoFurnitureWIP
                  ) fp ON fp.NoFurnitureWIP = f.NoFurnitureWIP
                  WHERE ifw.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(f.Pcs, 0))
                  FROM dbo.BongkarSusunOutputFurnitureWIP ofw
                  INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ofw.NoFurnitureWIP
                  WHERE ofw.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'barangJadi' THEN
            CASE
              WHEN EXISTS (
                SELECT b.IdBJ FROM dbo.BongkarSusunInputBarangJadi ibj
                INNER JOIN dbo.BarangJadi b ON b.NoBJ = ibj.NoBJ
                WHERE ibj.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT b.IdBJ FROM dbo.BongkarSusunOutputBarangjadi obj
                INNER JOIN dbo.BarangJadi b ON b.NoBJ = obj.NoBJ
                WHERE obj.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT b.IdBJ FROM dbo.BongkarSusunOutputBarangjadi obj
                INNER JOIN dbo.BarangJadi b ON b.NoBJ = obj.NoBJ
                WHERE obj.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT b.IdBJ FROM dbo.BongkarSusunInputBarangJadi ibj
                INNER JOIN dbo.BarangJadi b ON b.NoBJ = ibj.NoBJ
                WHERE ibj.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(
                    CASE
                      WHEN b.IsPartial = 1 THEN
                        CASE
                          WHEN ISNULL(b.Pcs, 0) - ISNULL(bp.TotalPartialPcs, 0) < 0 THEN 0
                          ELSE ISNULL(b.Pcs, 0) - ISNULL(bp.TotalPartialPcs, 0)
                        END
                      ELSE ISNULL(b.Pcs, 0)
                    END
                  )
                  FROM dbo.BongkarSusunInputBarangJadi ibj
                  INNER JOIN dbo.BarangJadi b ON b.NoBJ = ibj.NoBJ
                  LEFT JOIN (
                    SELECT NoBJ, SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
                    FROM dbo.BarangJadiPartial
                    GROUP BY NoBJ
                  ) bp ON bp.NoBJ = b.NoBJ
                  WHERE ibj.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(b.Pcs, 0))
                  FROM dbo.BongkarSusunOutputBarangjadi obj
                  INNER JOIN dbo.BarangJadi b ON b.NoBJ = obj.NoBJ
                  WHERE obj.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          WHEN cat.category = 'bonggolan' THEN
            CASE
              WHEN EXISTS (
                SELECT b.IdBonggolan FROM dbo.BongkarSusunInputBonggolan ibg
                INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = ibg.NoBonggolan
                WHERE ibg.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT b.IdBonggolan FROM dbo.BongkarSusunOutputBonggolan obg
                INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = obg.NoBonggolan
                WHERE obg.NoBongkarSusun = h.NoBongkarSusun
              ) OR EXISTS (
                SELECT b.IdBonggolan FROM dbo.BongkarSusunOutputBonggolan obg
                INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = obg.NoBonggolan
                WHERE obg.NoBongkarSusun = h.NoBongkarSusun
                EXCEPT
                SELECT b.IdBonggolan FROM dbo.BongkarSusunInputBonggolan ibg
                INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = ibg.NoBonggolan
                WHERE ibg.NoBongkarSusun = h.NoBongkarSusun
              ) THEN CAST(0 AS bit)
              WHEN ABS(
                ISNULL((
                  SELECT SUM(ISNULL(b.Berat, 0))
                  FROM dbo.BongkarSusunInputBonggolan ibg
                  INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = ibg.NoBonggolan
                  WHERE ibg.NoBongkarSusun = h.NoBongkarSusun
                ), 0) -
                ISNULL((
                  SELECT SUM(ISNULL(b.Berat, 0))
                  FROM dbo.BongkarSusunOutputBonggolan obg
                  INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = obg.NoBonggolan
                  WHERE obg.NoBongkarSusun = h.NoBongkarSusun
                ), 0)
              ) < 0.001 THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END
          ELSE CAST(0 AS bit)
        END AS balance
      ) bal
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
      SELECT
        h.NoBongkarSusun,
        h.Tanggal,
        h.IdUsername,
        u.Username,
        h.Note
      FROM dbo.BongkarSusun_h
      h
      LEFT JOIN dbo.MstUsername u
        ON u.IdUsername = h.IdUsername
      WHERE NoBongkarSusun = @NoBongkarSusun
    `);

  if (!headerRes.recordset.length) {
    const e = new Error(`NoBongkarSusun ${noBongkarSusun} tidak ditemukan`);
    e.statusCode = 404;
    throw e;
  }

  // inputs â€” bahanBaku
  const inputsBahanBakuDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      WITH PartialAgg AS (
        SELECT
          NoBahanBaku,
          NoPallet,
          NoSak,
          SUM(ISNULL(Berat, 0)) AS PartialBerat
        FROM dbo.BahanBakuPartial
        GROUP BY NoBahanBaku, NoPallet, NoSak
      )
      SELECT
        ib.NoBahanBaku       AS noBahanBaku,
        ib.NoPallet          AS noPallet,
        'bahanBaku'          AS category,
        ph.IdJenisPlastik    AS idJenis,
        jp.Jenis             AS namaJenis,
        ph.IdWarehouse       AS idWarehouse,
        w.NamaWarehouse      AS namaWarehouse,
        ph.Keterangan        AS keterangan,
        ph.IdStatus          AS idStatus,
        CASE
          WHEN ph.IdStatus = 1 THEN 'PASS'
          WHEN ph.IdStatus = 0 THEN 'HOLD'
          ELSE ''
        END                  AS statusText,
        ph.Moisture          AS moisture,
        ph.MeltingIndex      AS meltingIndex,
        ph.Elasticity        AS elasticity,
        ph.Tenggelam         AS tenggelam,
        ph.Density           AS density,
        ph.Density2          AS density2,
        ph.Density3          AS density3,
        ISNULL(CAST(ph.HasBeenPrinted AS int), 0) AS hasBeenPrinted,
        ph.Blok              AS blok,
        ph.IdLokasi          AS idLokasi,
        d.NoSak              AS noSak,
        CASE
          WHEN d.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(d.Berat, 0) - ISNULL(pa.PartialBerat, 0) < 0 THEN 0
              ELSE ISNULL(d.Berat, 0) - ISNULL(pa.PartialBerat, 0)
            END
          ELSE ISNULL(d.Berat, 0)
        END                  AS beratSak
      FROM dbo.BongkarSusunInputBahanBaku ib
      INNER JOIN dbo.BahanBakuPallet_h ph
        ON ph.NoBahanBaku = ib.NoBahanBaku
       AND ph.NoPallet = ib.NoPallet
      INNER JOIN dbo.MstJenisPlastik jp
        ON jp.IdJenisPlastik = ph.IdJenisPlastik
      INNER JOIN dbo.MstWarehouse w
        ON w.IdWarehouse = ph.IdWarehouse
      INNER JOIN dbo.BahanBaku_d d
        ON d.NoBahanBaku = ib.NoBahanBaku
       AND d.NoPallet = ib.NoPallet
       AND d.NoSak = ib.NoSak
      LEFT JOIN PartialAgg pa
        ON pa.NoBahanBaku = d.NoBahanBaku
       AND pa.NoPallet = d.NoPallet
       AND pa.NoSak = d.NoSak
      WHERE ib.NoBongkarSusun = @NoBongkarSusun
      ORDER BY ib.NoBahanBaku, ib.NoPallet, d.NoSak
    `);

  // inputs â€” washing
  const inputsWashingDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bi.NoWashing          AS labelCode,
        'washing'             AS category,
        h.IdJenisPlastik      AS idJenis,
        mw.Nama               AS namaJenis,
        bi.NoSak              AS noSak,
        d.Berat               AS beratSak
      FROM BongkarSusunInputWashing bi
      INNER JOIN Washing_h  h  ON h.NoWashing   = bi.NoWashing
      INNER JOIN MstWashing mw ON mw.IdWashing  = h.IdJenisPlastik
      INNER JOIN Washing_d  d  ON d.NoWashing   = bi.NoWashing
                               AND d.NoSak      = bi.NoSak
      WHERE bi.NoBongkarSusun = @NoBongkarSusun
      ORDER BY bi.NoWashing, bi.NoSak
    `);

  // inputs â€” bonggolan
  const inputsBonggolanRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bi.NoBonggolan        AS labelCode,
        'bonggolan'           AS category,
        b.IdBonggolan         AS idJenis,
        mb.NamaBonggolan      AS namaJenis,
        b.Berat               AS totalBerat
      FROM BongkarSusunInputBonggolan bi
      INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = bi.NoBonggolan
      INNER JOIN dbo.MstBonggolan mb ON mb.IdBonggolan = b.IdBonggolan
      WHERE bi.NoBongkarSusun = @NoBongkarSusun
    `);

  // outputs â€” washing
  const inputsBrokerDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bi.NoBroker           AS labelCode,
        'broker'              AS category,
        h.IdJenisPlastik      AS idJenis,
        mb.Nama               AS namaJenis,
        bi.NoSak              AS noSak,
        d.Berat               AS beratSak,
        d.IsPartial           AS isPartial
      FROM BongkarSusunInputBroker bi
      INNER JOIN dbo.Broker_h h ON h.NoBroker = bi.NoBroker
      INNER JOIN dbo.MstBroker mb ON mb.IdBroker = h.IdJenisPlastik
      INNER JOIN dbo.Broker_d d
        ON d.NoBroker = bi.NoBroker
       AND d.NoSak = bi.NoSak
      WHERE bi.NoBongkarSusun = @NoBongkarSusun
      ORDER BY bi.NoBroker, bi.NoSak
    `);

  // inputs â€” mixer
  const inputsMixerDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        im.NoMixer            AS labelCode,
        'mixer'               AS category,
        h.IdMixer             AS idJenis,
        mx.Jenis              AS namaJenis,
        im.NoSak              AS noSak,
        CASE
          WHEN d.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(d.Berat, 0) - ISNULL(mp.TotalPartial, 0) < 0
                THEN 0
              ELSE ISNULL(d.Berat, 0) - ISNULL(mp.TotalPartial, 0)
            END
          ELSE ISNULL(d.Berat, 0)
        END AS beratSak
      FROM BongkarSusunInputMixer im
      INNER JOIN dbo.Mixer_h h ON h.NoMixer = im.NoMixer
      INNER JOIN dbo.MstMixer mx ON mx.IdMixer = h.IdMixer
      INNER JOIN dbo.Mixer_d d
        ON d.NoMixer = im.NoMixer
       AND d.NoSak = im.NoSak
      LEFT JOIN (
        SELECT NoMixer, NoSak, SUM(ISNULL(Berat, 0)) AS TotalPartial
        FROM dbo.MixerPartial
        GROUP BY NoMixer, NoSak
      ) mp ON mp.NoMixer = d.NoMixer AND mp.NoSak = d.NoSak
      WHERE im.NoBongkarSusun = @NoBongkarSusun
      ORDER BY im.NoMixer, im.NoSak
    `);

  // inputs â€” crusher
  const inputsCrusherRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        ic.NoCrusher          AS labelCode,
        'crusher'             AS category,
        c.IdCrusher           AS idJenis,
        mc.NamaCrusher        AS namaJenis,
        c.Berat               AS totalBerat
      FROM BongkarSusunInputCrusher ic
      INNER JOIN dbo.Crusher c ON c.NoCrusher = ic.NoCrusher
      INNER JOIN dbo.MstCrusher mc ON mc.IdCrusher = c.IdCrusher
      WHERE ic.NoBongkarSusun = @NoBongkarSusun
    `);

  // inputs â€” gilingan
  const inputsGilinganRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        ig.NoGilingan         AS labelCode,
        'gilingan'            AS category,
        g.IdGilingan          AS idJenis,
        mg.NamaGilingan       AS namaJenis,
        CASE
          WHEN g.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0) < 0
                THEN 0
              ELSE ISNULL(g.Berat, 0) - ISNULL(gp.TotalPartial, 0)
            END
          ELSE ISNULL(g.Berat, 0)
        END AS totalBerat
      FROM BongkarSusunInputGilingan ig
      INNER JOIN dbo.Gilingan g ON g.NoGilingan = ig.NoGilingan
      INNER JOIN dbo.MstGilingan mg ON mg.IdGilingan = g.IdGilingan
      LEFT JOIN (
        SELECT NoGilingan, SUM(ISNULL(Berat, 0)) AS TotalPartial
        FROM dbo.GilinganPartial
        GROUP BY NoGilingan
      ) gp ON gp.NoGilingan = g.NoGilingan
      WHERE ig.NoBongkarSusun = @NoBongkarSusun
    `);

  // inputs â€” furnitureWip
  const inputsFurnitureWipRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        ifw.NoFurnitureWIP    AS labelCode,
        'furnitureWip'        AS category,
        f.IdFurnitureWIP      AS idJenis,
        cw.Nama               AS namaJenis,
        CASE
          WHEN f.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(f.Pcs, 0) - ISNULL(fp.TotalPartialPcs, 0) < 0
                THEN 0
              ELSE ISNULL(f.Pcs, 0) - ISNULL(fp.TotalPartialPcs, 0)
            END
          ELSE ISNULL(f.Pcs, 0)
        END AS pcs
      FROM dbo.BongkarSusunInputFurnitureWIP ifw
      INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ifw.NoFurnitureWIP
      INNER JOIN dbo.MstCabinetWIP cw ON cw.IdCabinetWIP = f.IdFurnitureWIP
      LEFT JOIN (
        SELECT NoFurnitureWIP, SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
        FROM dbo.FurnitureWIPPartial
        GROUP BY NoFurnitureWIP
      ) fp ON fp.NoFurnitureWIP = f.NoFurnitureWIP
      WHERE ifw.NoBongkarSusun = @NoBongkarSusun
    `);

  const inputsBarangJadiRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        ibj.NoBJ              AS labelCode,
        'barangJadi'          AS category,
        b.IdBJ                AS idJenis,
        mbj.NamaBJ            AS namaJenis,
        CASE
          WHEN b.IsPartial = 1 THEN
            CASE
              WHEN ISNULL(b.Pcs, 0) - ISNULL(bp.TotalPartialPcs, 0) < 0
                THEN 0
              ELSE ISNULL(b.Pcs, 0) - ISNULL(bp.TotalPartialPcs, 0)
            END
          ELSE ISNULL(b.Pcs, 0)
        END AS pcs,
        ISNULL(b.Berat, 0)    AS berat
      FROM dbo.BongkarSusunInputBarangJadi ibj
      INNER JOIN dbo.BarangJadi b ON b.NoBJ = ibj.NoBJ
      INNER JOIN dbo.MstBarangJadi mbj ON mbj.IdBJ = b.IdBJ
      LEFT JOIN (
        SELECT NoBJ, SUM(ISNULL(Pcs, 0)) AS TotalPartialPcs
        FROM dbo.BarangJadiPartial
        GROUP BY NoBJ
      ) bp ON bp.NoBJ = b.NoBJ
      WHERE ibj.NoBongkarSusun = @NoBongkarSusun
    `);

  const outputsWashingDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bo.NoWashing          AS labelCode,
        'washing'             AS category,
        h.IdJenisPlastik      AS idJenis,
        mw.Nama               AS namaJenis,
        bo.NoSak              AS noSak,
        d.Berat               AS beratSak
      FROM BongkarSusunOutputWashing bo
      INNER JOIN Washing_h  h  ON h.NoWashing   = bo.NoWashing
      INNER JOIN MstWashing mw ON mw.IdWashing  = h.IdJenisPlastik
      INNER JOIN Washing_d  d  ON d.NoWashing   = bo.NoWashing
                               AND d.NoSak      = bo.NoSak
      WHERE bo.NoBongkarSusun = @NoBongkarSusun
      ORDER BY bo.NoWashing, bo.NoSak
    `);

  const outputsBahanBakuDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        ob.NoBahanBaku       AS noBahanBaku,
        ob.NoPallet          AS noPallet,
        'bahanBaku'          AS category,
        ph.IdJenisPlastik    AS idJenis,
        jp.Jenis             AS namaJenis,
        ph.IdWarehouse       AS idWarehouse,
        w.NamaWarehouse      AS namaWarehouse,
        ph.Keterangan        AS keterangan,
        ph.IdStatus          AS idStatus,
        CASE
          WHEN ph.IdStatus = 1 THEN 'PASS'
          WHEN ph.IdStatus = 0 THEN 'HOLD'
          ELSE ''
        END                  AS statusText,
        ph.Moisture          AS moisture,
        ph.MeltingIndex      AS meltingIndex,
        ph.Elasticity        AS elasticity,
        ph.Tenggelam         AS tenggelam,
        ph.Density           AS density,
        ph.Density2          AS density2,
        ph.Density3          AS density3,
        ISNULL(CAST(ph.HasBeenPrinted AS int), 0) AS hasBeenPrinted,
        ph.Blok              AS blok,
        ph.IdLokasi          AS idLokasi,
        ISNULL(d.NoSak, ob.NoSak) AS noSak,
        ISNULL(d.Berat, 0)   AS beratSak
      FROM dbo.BongkarSusunOutputBahanBaku ob
      LEFT JOIN dbo.BahanBakuPallet_h ph
        ON ph.NoBahanBaku = ob.NoBahanBaku
       AND ph.NoPallet = ob.NoPallet
      LEFT JOIN dbo.MstJenisPlastik jp
        ON jp.IdJenisPlastik = ph.IdJenisPlastik
      LEFT JOIN dbo.MstWarehouse w
        ON w.IdWarehouse = ph.IdWarehouse
      LEFT JOIN dbo.BahanBaku_d d
        ON d.NoBahanBaku = ob.NoBahanBaku
       AND d.NoPallet = ob.NoPallet
       AND d.NoSak = ob.NoSak
      WHERE ob.NoBongkarSusun = @NoBongkarSusun
      ORDER BY ob.NoBahanBaku, ob.NoPallet, ob.NoSak
    `);

  // outputs â€” bonggolan
  const outputsBonggolanRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bo.NoBonggolan        AS labelCode,
        'bonggolan'           AS category,
        b.IdBonggolan         AS idJenis,
        mb.NamaBonggolan      AS namaJenis,
        b.Berat               AS totalBerat
      FROM BongkarSusunOutputBonggolan bo
      INNER JOIN dbo.Bonggolan b ON b.NoBonggolan = bo.NoBonggolan
      INNER JOIN dbo.MstBonggolan mb ON mb.IdBonggolan = b.IdBonggolan
      WHERE bo.NoBongkarSusun = @NoBongkarSusun
    `);

  // outputs â€” crusher
  const outputsCrusherRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        oc.NoCrusher          AS labelCode,
        'crusher'             AS category,
        c.IdCrusher           AS idJenis,
        mc.NamaCrusher        AS namaJenis,
        c.Berat               AS totalBerat
      FROM BongkarSusunOutputCrusher oc
      INNER JOIN dbo.Crusher c ON c.NoCrusher = oc.NoCrusher
      INNER JOIN dbo.MstCrusher mc ON mc.IdCrusher = c.IdCrusher
      WHERE oc.NoBongkarSusun = @NoBongkarSusun
    `);

  // outputs â€” gilingan
  const outputsGilinganRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        og.NoGilingan         AS labelCode,
        'gilingan'            AS category,
        g.IdGilingan          AS idJenis,
        mg.NamaGilingan       AS namaJenis,
        ISNULL(g.Berat, 0) AS totalBerat
      FROM BongkarSusunOutputGilingan og
      INNER JOIN dbo.Gilingan g ON g.NoGilingan = og.NoGilingan
      INNER JOIN dbo.MstGilingan mg ON mg.IdGilingan = g.IdGilingan
      WHERE og.NoBongkarSusun = @NoBongkarSusun
    `);

  // outputs â€” furnitureWip
  const outputsFurnitureWipRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        ofw.NoFurnitureWIP    AS labelCode,
        'furnitureWip'        AS category,
        f.IdFurnitureWIP      AS idJenis,
        cw.Nama               AS namaJenis,
        ISNULL(f.Pcs, 0) AS pcs
      FROM dbo.BongkarSusunOutputFurnitureWIP ofw
      INNER JOIN dbo.FurnitureWIP f ON f.NoFurnitureWIP = ofw.NoFurnitureWIP
      INNER JOIN dbo.MstCabinetWIP cw ON cw.IdCabinetWIP = f.IdFurnitureWIP
      WHERE ofw.NoBongkarSusun = @NoBongkarSusun
    `);

  const outputsBarangJadiRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        obj.NoBJ              AS labelCode,
        'barangJadi'          AS category,
        b.IdBJ                AS idJenis,
        mbj.NamaBJ            AS namaJenis,
        ISNULL(b.Pcs, 0)      AS pcs,
        ISNULL(b.Berat, 0)    AS berat
      FROM dbo.BongkarSusunOutputBarangjadi obj
      INNER JOIN dbo.BarangJadi b ON b.NoBJ = obj.NoBJ
      INNER JOIN dbo.MstBarangJadi mbj ON mbj.IdBJ = b.IdBJ
      WHERE obj.NoBongkarSusun = @NoBongkarSusun
    `);

  const outputsBrokerDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        bo.NoBroker           AS labelCode,
        'broker'              AS category,
        h.IdJenisPlastik      AS idJenis,
        mb.Nama               AS namaJenis,
        bo.NoSak              AS noSak,
        d.Berat               AS beratSak,
        d.IsPartial           AS isPartial
      FROM BongkarSusunOutputBroker bo
      INNER JOIN dbo.Broker_h h ON h.NoBroker = bo.NoBroker
      INNER JOIN dbo.MstBroker mb ON mb.IdBroker = h.IdJenisPlastik
      INNER JOIN dbo.Broker_d d
        ON d.NoBroker = bo.NoBroker
       AND d.NoSak = bo.NoSak
      WHERE bo.NoBongkarSusun = @NoBongkarSusun
      ORDER BY bo.NoBroker, bo.NoSak
    `);

  const brokerHasPartial = [
    ...(inputsBrokerDetailRes.recordset || []),
    ...(outputsBrokerDetailRes.recordset || []),
  ].some((row) => row.isPartial === true || row.isPartial === 1);

  if (brokerHasPartial) {
    throw conflict("Tidak dapat bongkar susun label yang sudah di partial");
  }

  // outputs â€” mixer
  const outputsMixerDetailRes = await pool
    .request()
    .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun).query(`
      SELECT
        om.NoMixer            AS labelCode,
        'mixer'               AS category,
        h.IdMixer             AS idJenis,
        mx.Jenis              AS namaJenis,
        om.NoSak              AS noSak,
        ISNULL(d.Berat, 0) AS beratSak
      FROM BongkarSusunOutputMixer om
      INNER JOIN dbo.Mixer_h h ON h.NoMixer = om.NoMixer
      INNER JOIN dbo.MstMixer mx ON mx.IdMixer = h.IdMixer
      INNER JOIN dbo.Mixer_d d
        ON d.NoMixer = om.NoMixer
       AND d.NoSak = om.NoSak
      WHERE om.NoBongkarSusun = @NoBongkarSusun
      ORDER BY om.NoMixer, om.NoSak
    `);

  const groupBahanBakuRows = (rows) => {
    const grouped = new Map();

    for (const row of rows) {
      const key = `${row.noBahanBaku}-${row.noPallet}`;
      if (!grouped.has(key)) {
        grouped.set(key, {
          labelCode: key,
          category: row.category,
          noBahanBaku: row.noBahanBaku,
          noPallet: row.noPallet,
          idJenis: row.idJenis,
          namaJenis: row.namaJenis,
          idWarehouse: row.idWarehouse,
          namaWarehouse: row.namaWarehouse,
          keterangan: row.keterangan,
          idStatus: row.idStatus,
          statusText: row.statusText,
          moisture: row.moisture,
          meltingIndex: row.meltingIndex,
          elasticity: row.elasticity,
          tenggelam: row.tenggelam,
          density: row.density,
          density2: row.density2,
          density3: row.density3,
          hasBeenPrinted: row.hasBeenPrinted,
          blok: row.blok,
          idLokasi: row.idLokasi,
          jumlahSak: 0,
          totalBerat: 0,
          saks: [],
        });
      }

      const item = grouped.get(key);
      item.saks.push({
        noSak: row.noSak,
        berat: row.beratSak,
      });
      item.jumlahSak += 1;
      item.totalBerat += Number(row.beratSak || 0);
    }

    return Array.from(grouped.values());
  };

  const groupBrokerRows = (rows) => {
    const grouped = new Map();

    for (const row of rows) {
      const key = row.labelCode;
      if (!grouped.has(key)) {
        grouped.set(key, {
          labelCode: row.labelCode,
          category: row.category,
          idJenis: row.idJenis,
          namaJenis: row.namaJenis,
          jumlahSak: 0,
          totalBerat: 0,
          saks: [],
        });
      }

      const item = grouped.get(key);
      item.saks.push({
        noSak: row.noSak,
        berat: row.beratSak,
      });
      item.jumlahSak += 1;
      item.totalBerat += Number(row.beratSak || 0);
    }

    return Array.from(grouped.values());
  };

  const inputsBahanBakuRes = groupBahanBakuRows(
    inputsBahanBakuDetailRes.recordset || [],
  );
  const inputsBrokerRes = groupBrokerRows(
    inputsBrokerDetailRes.recordset || [],
  );
  const inputsWashingRes = groupBrokerRows(
    inputsWashingDetailRes.recordset || [],
  );
  const inputsMixerRes = groupBrokerRows(inputsMixerDetailRes.recordset || []);
  const outputsWashingRes = groupBrokerRows(
    outputsWashingDetailRes.recordset || [],
  );
  const outputsBahanBakuRes = groupBahanBakuRows(
    outputsBahanBakuDetailRes.recordset || [],
  );
  const outputsBrokerRes = groupBrokerRows(
    outputsBrokerDetailRes.recordset || [],
  );
  const outputsMixerRes = groupBrokerRows(
    outputsMixerDetailRes.recordset || [],
  );

  return {
    header: headerRes.recordset[0],
    inputs: [
      ...inputsBahanBakuRes,
      ...inputsWashingRes,
      ...inputsBonggolanRes.recordset,
      ...inputsBrokerRes,
      ...inputsMixerRes,
      ...inputsCrusherRes.recordset,
      ...inputsGilinganRes.recordset,
      ...inputsFurnitureWipRes.recordset,
      ...inputsBarangJadiRes.recordset,
    ],
    outputs: [
      ...outputsBahanBakuRes,
      ...outputsWashingRes,
      ...outputsBonggolanRes.recordset,
      ...outputsBrokerRes,
      ...outputsMixerRes,
      ...outputsCrusherRes.recordset,
      ...outputsGilinganRes.recordset,
      ...outputsFurnitureWipRes.recordset,
      ...outputsBarangJadiRes.recordset,
    ],
  };
};

// create handlers
const createWashingHandler = require("./handlers/create-washing.handler");
const createBahanBakuHandler = require("./handlers/create-bahan-baku.handler");
const createBrokerHandler = require("./handlers/create-broker.handler");
const createCrusherHandler = require("./handlers/create-crusher.handler");
const createGilinganHandler = require("./handlers/create-gilingan.handler");
const createFurnitureWipHandler = require("./handlers/create-furniture-wip.handler");
const createBarangJadiHandler = require("./handlers/create-barang-jadi.handler");
const createBonggolanHandler = require("./handlers/create-bonggolan.handler");
const createMixerHandler = require("./handlers/create-mixer.handler");
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

    const outputGuard = await new sql.Request(tx).input(
      "NoBongkarSusun",
      sql.VarChar(50),
      noBongkarSusun,
    ).query(`
        SELECT CASE WHEN
          EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputWashing      WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBroker    WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputCrusher   WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputGilingan   WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBonggolan  WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputMixer      WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputFurnitureWIP WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBarangjadi WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
          OR EXISTS (SELECT 1 FROM dbo.BongkarSusunOutputBahanBaku  WITH (NOLOCK) WHERE NoBongkarSusun = @NoBongkarSusun)
        THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS HasOutput;
      `);

    const hasOutput = outputGuard.recordset?.[0]?.HasOutput === true;
    if (hasOutput) {
      throw conflict(
        "Tidak bisa hapus: transaksi ini sudah menerbitkan label/output",
      );
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

    // Handle output & input gilingan
    const outputsGilinganRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoGilingan FROM dbo.BongkarSusunOutputGilingan WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (outputsGilinganRes.recordset.length > 0) {
      const outputGilinganCodes = outputsGilinganRes.recordset.map(
        (r) => r.NoGilingan,
      );
      const outGilinganJson = JSON.stringify(
        outputGilinganCodes.map((c) => ({ code: c })),
      );

      const usedGilingan = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outGilinganJson,
      ).query(`
          SELECT TOP 1 NoGilingan FROM dbo.Gilingan
          WHERE NoGilingan IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);
      if (usedGilingan.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output gilingan sudah digunakan di proses lain",
        );

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputGilingan WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outGilinganJson,
      ).query(`
          DELETE FROM dbo.Gilingan
          WHERE NoGilingan IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
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

    const inputsGilinganRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoGilingan FROM dbo.BongkarSusunInputGilingan WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsGilinganRes.recordset.length > 0) {
      const inGilinganJson = JSON.stringify(
        inputsGilinganRes.recordset.map((r) => ({ code: r.NoGilingan })),
      );
      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inGilinganJson,
      ).query(`
          UPDATE dbo.Gilingan SET DateUsage = NULL
          WHERE NoGilingan IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputGilingan WHERE NoBongkarSusun = @NoBongkarSusun`,
        );
    }

    const outputsMixerRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT DISTINCT NoMixer FROM dbo.BongkarSusunOutputMixer WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (outputsMixerRes.recordset.length > 0) {
      const outputMixerCodes = outputsMixerRes.recordset.map((r) => r.NoMixer);
      const outMixerJson = JSON.stringify(
        outputMixerCodes.map((c) => ({ code: c })),
      );

      const usedMixer = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outMixerJson,
      ).query(`
          SELECT TOP 1 NoMixer FROM dbo.Mixer_d
          WHERE NoMixer IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);
      if (usedMixer.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output mixer sudah digunakan di proses lain",
        );

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputMixer WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outMixerJson,
      ).query(`
          DELETE FROM dbo.Mixer_d
          WHERE NoMixer IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outMixerJson,
      ).query(`
          DELETE FROM dbo.Mixer_h
          WHERE NoMixer IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
    }

    const inputsMixerRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoMixer, NoSak FROM dbo.BongkarSusunInputMixer WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsMixerRes.recordset.length > 0) {
      const inMixerJson = JSON.stringify(
        inputsMixerRes.recordset.map((r) => ({
          noMixer: r.NoMixer,
          noSak: r.NoSak,
        })),
      );
      await new sql.Request(tx).input(
        "PairsJson",
        sql.NVarChar(sql.MAX),
        inMixerJson,
      ).query(`
          UPDATE d
          SET DateUsage = NULL
          FROM dbo.Mixer_d d
          INNER JOIN OPENJSON(@PairsJson)
          WITH (
            noMixer varchar(50) '$.noMixer',
            noSak int '$.noSak'
          ) j
            ON j.noMixer = d.NoMixer
           AND j.noSak = d.NoSak
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputMixer WHERE NoBongkarSusun = @NoBongkarSusun`,
        );
    }

    const outputsFurnitureWipRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoFurnitureWIP FROM dbo.BongkarSusunOutputFurnitureWIP WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (outputsFurnitureWipRes.recordset.length > 0) {
      const outputFurnitureWipCodes = outputsFurnitureWipRes.recordset.map(
        (r) => r.NoFurnitureWIP,
      );
      const outFurnitureWipJson = JSON.stringify(
        outputFurnitureWipCodes.map((c) => ({ code: c })),
      );

      const usedFurnitureWip = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outFurnitureWipJson,
      ).query(`
          SELECT TOP 1 NoFurnitureWIP FROM dbo.FurnitureWIP
          WHERE NoFurnitureWIP IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);
      if (usedFurnitureWip.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output furnitureWip sudah digunakan di proses lain",
        );

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputFurnitureWIP WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outFurnitureWipJson,
      ).query(`
          DELETE FROM dbo.FurnitureWIP
          WHERE NoFurnitureWIP IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
    }

    const inputsFurnitureWipRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoFurnitureWIP FROM dbo.BongkarSusunInputFurnitureWIP WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsFurnitureWipRes.recordset.length > 0) {
      const inFurnitureWipJson = JSON.stringify(
        inputsFurnitureWipRes.recordset.map((r) => ({
          code: r.NoFurnitureWIP,
        })),
      );
      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inFurnitureWipJson,
      ).query(`
          UPDATE dbo.FurnitureWIP SET DateUsage = NULL
          WHERE NoFurnitureWIP IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputFurnitureWIP WHERE NoBongkarSusun = @NoBongkarSusun`,
        );
    }

    const outputsBarangJadiRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoBJ FROM dbo.BongkarSusunOutputBarangjadi WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (outputsBarangJadiRes.recordset.length > 0) {
      const outputBarangJadiCodes = outputsBarangJadiRes.recordset.map(
        (r) => r.NoBJ,
      );
      const outBarangJadiJson = JSON.stringify(
        outputBarangJadiCodes.map((c) => ({ code: c })),
      );

      const usedBarangJadi = await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBarangJadiJson,
      ).query(`
          SELECT TOP 1 NoBJ FROM dbo.BarangJadi
          WHERE NoBJ IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
          AND DateUsage IS NOT NULL
        `);
      if (usedBarangJadi.recordset.length > 0)
        throw conflict(
          "Tidak bisa hapus: label output barangJadi sudah digunakan di proses lain",
        );

      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunOutputBarangjadi WHERE NoBongkarSusun = @NoBongkarSusun`,
        );

      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        outBarangJadiJson,
      ).query(`
          DELETE FROM dbo.BarangJadi
          WHERE NoBJ IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
    }

    const inputsBarangJadiRes = await new sql.Request(tx)
      .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
      .query(
        `SELECT NoBJ FROM dbo.BongkarSusunInputBarangJadi WHERE NoBongkarSusun = @NoBongkarSusun`,
      );

    if (inputsBarangJadiRes.recordset.length > 0) {
      const inBarangJadiJson = JSON.stringify(
        inputsBarangJadiRes.recordset.map((r) => ({ code: r.NoBJ })),
      );
      await new sql.Request(tx).input(
        "CodesJson",
        sql.NVarChar(sql.MAX),
        inBarangJadiJson,
      ).query(`
          UPDATE dbo.BarangJadi SET DateUsage = NULL
          WHERE NoBJ IN (
            SELECT j.code FROM OPENJSON(@CodesJson) WITH (code varchar(50) '$.code') AS j
          )
        `);
      await new sql.Request(tx)
        .input("NoBongkarSusun", sql.VarChar(50), noBongkarSusun)
        .query(
          `DELETE FROM dbo.BongkarSusunInputBarangJadi WHERE NoBongkarSusun = @NoBongkarSusun`,
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
exports.createBongkarSusunBahanBaku =
  createBahanBakuHandler.createBongkarSusunBahanBaku;
exports.createBongkarSusunBroker = createBrokerHandler.createBongkarSusunBroker;
exports.createBongkarSusunCrusher =
  createCrusherHandler.createBongkarSusunCrusher;
exports.createBongkarSusunGilingan =
  createGilinganHandler.createBongkarSusunGilingan;
exports.createBongkarSusunFurnitureWip =
  createFurnitureWipHandler.createBongkarSusunFurnitureWip;
exports.createBongkarSusunBarangJadi =
  createBarangJadiHandler.createBongkarSusunBarangJadi;
exports.createBongkarSusunBonggolan =
  createBonggolanHandler.createBongkarSusunBonggolan;
exports.createBongkarSusunMixer = createMixerHandler.createBongkarSusunMixer;

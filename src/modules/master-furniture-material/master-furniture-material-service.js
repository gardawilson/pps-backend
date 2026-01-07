const sql = require('mssql');
const { poolPromise } = require('../../core/config/db');

/**
 * Get all master cabinet materials with stock info
 * @param {number} idWarehouse - Warehouse ID for stock calculation
 * @returns {Promise<{found: boolean, count: number, data: Array}>}
 */
async function getMasterCabinetMaterials({ idWarehouse }) {
  const pool = await poolPromise;
  const req = pool.request();

  req.input('IdWarehouse', sql.Int, idWarehouse);

  const query = `
    DECLARE @TglAkhir date = CAST(GETDATE() AS date);

    ;WITH A AS (
      -- Master Cabinet Material
      SELECT 
        m.IdCabinetMaterial,
        m.Nama,
        m.ItemCode,
        m.TglSaldoAwal,
        m.IdUOM,
        m.Enable,
        u.NamaUOM
      FROM dbo.MstCabinetMaterial m WITH (NOLOCK)
      INNER JOIN dbo.MstUOM u WITH (NOLOCK) ON u.IdUOM = m.IdUOM
      WHERE m.Enable = 1
    ),
    W AS (
      -- Warehouse info
      SELECT w.IdWarehouse, w.NamaWarehouse
      FROM dbo.MstWarehouse w WITH (NOLOCK)
      WHERE w.IdWarehouse = @IdWarehouse
    ),
    K AS (
      -- Saldo Awal per material
      SELECT
        a.IdCabinetMaterial,
        w.IdWarehouse,
        w.NamaWarehouse,
        a.TglSaldoAwal,
        SUM(ISNULL(sa.SaldoAwal, 0)) AS SaldoAwal
      FROM A a
      CROSS JOIN W w
      LEFT JOIN dbo.MstCabinetMaterialSaldoAwal sa WITH (NOLOCK)
        ON sa.IdCabinetMaterial = a.IdCabinetMaterial
       AND sa.IdWarehouse = w.IdWarehouse
      GROUP BY a.IdCabinetMaterial, w.IdWarehouse, w.NamaWarehouse, a.TglSaldoAwal
    ),
    B AS (
      -- Penerimaan Material
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS PenrmnMaterl
      FROM dbo.CabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.CabinetMaterial_h h WITH (NOLOCK)
        ON h.NoCabinetMaterial = d.NoCabinetMaterial
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),
    C AS (
      -- Barang Jual Material
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS BJualMaterl
      FROM dbo.BJJualCabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.BJJual_h h WITH (NOLOCK)
        ON h.NoBJJual = d.NoBJJual
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),
    D AS (
      -- Retur Material
      SELECT d.IdCabinetMaterial, h.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS ReturMaterl
      FROM dbo.BJReturCabinetMaterial_d d WITH (NOLOCK)
      INNER JOIN dbo.BJRetur_h h WITH (NOLOCK)
        ON h.NoRetur = d.NoRetur
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWarehouse
    ),
    E AS (
      -- Cabinet Assembly Material (HotStamp, Packing, PasangKunci, Spanner)
      SELECT Z.IdCabinetMaterial, K.IdWarehouse, SUM(Z.CabAssblMaterl) AS CabAssblMaterl
      FROM K
      INNER JOIN (
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0)) AS CabAssblMaterl
        FROM dbo.HotStampingInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.HotStamping_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0))
        FROM dbo.PackingProduksiInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.PackingProduksi_h b WITH (NOLOCK) ON b.NoPacking = a.NoPacking
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0))
        FROM dbo.PasangKunciInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.PasangKunci_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal

        UNION ALL
        SELECT a.IdCabinetMaterial, b.Tanggal, SUM(ISNULL(a.Jumlah, 0))
        FROM dbo.SpannerInputMaterial a WITH (NOLOCK)
        INNER JOIN dbo.Spanner_h b WITH (NOLOCK) ON b.NoProduksi = a.NoProduksi
        GROUP BY a.IdCabinetMaterial, b.Tanggal
      ) Z ON Z.IdCabinetMaterial = K.IdCabinetMaterial
      WHERE Z.Tanggal >= K.TglSaldoAwal AND Z.Tanggal <= @TglAkhir
      GROUP BY Z.IdCabinetMaterial, K.IdWarehouse
    ),
    F AS (
      -- Goods Transfer In
      SELECT d.IdCabinetMaterial, h.IdWhTujuan AS IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS GoodTrfIn
      FROM dbo.GoodsTransfer_d_CabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.GoodsTransfer_h h WITH (NOLOCK)
        ON h.NoGT = d.NoGT
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWhTujuan
      WHERE h.DateCreate >= K.TglSaldoAwal AND h.DateCreate <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWhTujuan
    ),
    G AS (
      -- Goods Transfer Out
      SELECT d.IdCabinetMaterial, h.IdWhAsal AS IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS GoodTrfOut
      FROM dbo.GoodsTransfer_d_CabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.GoodsTransfer_h h WITH (NOLOCK)
        ON h.NoGT = d.NoGT
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = h.IdWhAsal
      WHERE h.DateCreate >= K.TglSaldoAwal AND h.DateCreate <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, h.IdWhAsal
    ),
    H AS (
      -- Inject Produksi Material
      SELECT d.IdCabinetMaterial, K.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS InjectProdMaterl
      FROM dbo.InjectProduksiInputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.InjectProduksi_h h WITH (NOLOCK)
        ON h.NoProduksi = d.NoProduksi
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
      WHERE h.TglProduksi >= K.TglSaldoAwal AND h.TglProduksi <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, K.IdWarehouse
    ),
    I AS (
      -- Adjustment Input
      SELECT d.IdCabinetMaterial, d.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS AdjInput
      FROM dbo.AdjustmentInputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.Adjustment_h h WITH (NOLOCK)
        ON h.NoAdjustment = d.NoAdjustment
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = d.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, d.IdWarehouse
    ),
    J AS (
      -- Adjustment Output
      SELECT d.IdCabinetMaterial, d.IdWarehouse, SUM(ISNULL(d.Pcs, 0)) AS AdjOutput
      FROM dbo.AdjustmentOutputCabinetMaterial d WITH (NOLOCK)
      INNER JOIN dbo.Adjustment_h h WITH (NOLOCK)
        ON h.NoAdjustment = d.NoAdjustment
      INNER JOIN K ON K.IdCabinetMaterial = d.IdCabinetMaterial
       AND K.IdWarehouse = d.IdWarehouse
      WHERE h.Tanggal >= K.TglSaldoAwal AND h.Tanggal <= @TglAkhir
      GROUP BY d.IdCabinetMaterial, d.IdWarehouse
    )

    SELECT
      a.IdCabinetMaterial,
      a.Nama,
      a.ItemCode,
      a.NamaUOM,
      K.IdWarehouse,
      K.NamaWarehouse,
      K.TglSaldoAwal,
      ISNULL(K.SaldoAwal, 0) AS SaldoAwal,
      ISNULL(B.PenrmnMaterl, 0) AS PenrmnMaterl,
      ISNULL(C.BJualMaterl, 0) AS BJualMaterl,
      ISNULL(D.ReturMaterl, 0) AS ReturMaterl,
      ISNULL(E.CabAssblMaterl, 0) AS CabAssblMaterl,
      ISNULL(F.GoodTrfIn, 0) AS GoodTrfIn,
      ISNULL(G.GoodTrfOut, 0) AS GoodTrfOut,
      ISNULL(H.InjectProdMaterl, 0) AS InjectProdMaterl,
      ISNULL(I.AdjInput, 0) AS AdjInput,
      ISNULL(J.AdjOutput, 0) AS AdjOutput,
      (
          ISNULL(K.SaldoAwal, 0)
        + ISNULL(B.PenrmnMaterl, 0)
        - ISNULL(C.BJualMaterl, 0)
        + ISNULL(D.ReturMaterl, 0)
        - ISNULL(E.CabAssblMaterl, 0)
        + ISNULL(F.GoodTrfIn, 0)
        - ISNULL(G.GoodTrfOut, 0)
        - ISNULL(H.InjectProdMaterl, 0)
        - ISNULL(I.AdjInput, 0)
        + ISNULL(J.AdjOutput, 0)
      ) AS SaldoAkhir
    FROM A a
    INNER JOIN K ON K.IdCabinetMaterial = a.IdCabinetMaterial
    LEFT JOIN B ON B.IdCabinetMaterial = a.IdCabinetMaterial AND B.IdWarehouse = K.IdWarehouse
    LEFT JOIN C ON C.IdCabinetMaterial = a.IdCabinetMaterial AND C.IdWarehouse = K.IdWarehouse
    LEFT JOIN D ON D.IdCabinetMaterial = a.IdCabinetMaterial AND D.IdWarehouse = K.IdWarehouse
    LEFT JOIN E ON E.IdCabinetMaterial = a.IdCabinetMaterial AND E.IdWarehouse = K.IdWarehouse
    LEFT JOIN F ON F.IdCabinetMaterial = a.IdCabinetMaterial AND F.IdWarehouse = K.IdWarehouse
    LEFT JOIN G ON G.IdCabinetMaterial = a.IdCabinetMaterial AND G.IdWarehouse = K.IdWarehouse
    LEFT JOIN H ON H.IdCabinetMaterial = a.IdCabinetMaterial AND H.IdWarehouse = K.IdWarehouse
    LEFT JOIN I ON I.IdCabinetMaterial = a.IdCabinetMaterial AND I.IdWarehouse = K.IdWarehouse
    LEFT JOIN J ON J.IdCabinetMaterial = a.IdCabinetMaterial AND J.IdWarehouse = K.IdWarehouse
    ORDER BY a.Nama;
  `;

  const result = await req.query(query);
  const rows = result.recordset || [];

  return {
    found: rows.length > 0,
    count: rows.length,
    data: rows,
  };
}



async function getByCetakanWarna({ idCetakan, idWarna }) {
  const pool = await poolPromise;
  const request = pool.request();

  request.input('IdCetakan', sql.Int, idCetakan);
  request.input('IdWarna', sql.Int, idWarna);

  const query = `
    SELECT TOP (1)
      cw.IdFurnitureMaterial,
      cm.Nama,
      cm.ItemCode,
      ISNULL(cm.Enable, 1) AS Enable
    FROM dbo.CetakanWarna_h cw
    LEFT JOIN dbo.MstCabinetMaterial cm
      ON cm.IdCabinetMaterial = cw.IdFurnitureMaterial
    WHERE cw.IdCetakan = @IdCetakan
      AND cw.IdWarna = @IdWarna;
  `;

  const result = await request.query(query);
  return result.recordset?.[0] || null;
}

module.exports = { getMasterCabinetMaterials, getByCetakanWarna };

const { sql } = require("../config/db");
const { badReq } = require("../utils/http-error");

async function getBrokerProductionWeightSummary(runner, noProduksi) {
  const code = String(noProduksi || "").trim();
  if (!code) throw badReq("noProduksi wajib diisi");
  if (!runner) throw badReq("runner transaksi tidak tersedia");

  const req = new sql.Request(runner);
  req.input("NoProduksi", sql.VarChar(50), code);

  const result = await req.query(`
    ;WITH InputRows AS (
      SELECT ISNULL(br.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputBroker ib WITH (NOLOCK)
      LEFT JOIN dbo.Broker_d br WITH (NOLOCK)
        ON br.NoBroker = ib.NoBroker AND br.NoSak = ib.NoSak
      WHERE ib.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(bb.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputBB ibb WITH (NOLOCK)
      LEFT JOIN dbo.BahanBaku_d bb WITH (NOLOCK)
        ON bb.NoBahanBaku = ibb.NoBahanBaku
       AND bb.NoPallet = ibb.NoPallet
       AND bb.NoSak = ibb.NoSak
      WHERE ibb.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(wd.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputWashing iw WITH (NOLOCK)
      LEFT JOIN dbo.Washing_d wd WITH (NOLOCK)
        ON wd.NoWashing = iw.NoWashing AND wd.NoSak = iw.NoSak
      WHERE iw.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(c.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputCrusher ic WITH (NOLOCK)
      LEFT JOIN dbo.Crusher c WITH (NOLOCK)
        ON c.NoCrusher = ic.NoCrusher
      WHERE ic.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(g.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputGilingan ig WITH (NOLOCK)
      LEFT JOIN dbo.Gilingan g WITH (NOLOCK)
        ON g.NoGilingan = ig.NoGilingan
      WHERE ig.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(md.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputMixer im WITH (NOLOCK)
      LEFT JOIN dbo.Mixer_d md WITH (NOLOCK)
        ON md.NoMixer = im.NoMixer AND md.NoSak = im.NoSak
      WHERE im.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(rj.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputReject ir WITH (NOLOCK)
      LEFT JOIN dbo.RejectV2 rj WITH (NOLOCK)
        ON rj.NoReject = ir.NoReject
      WHERE ir.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(bp.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputBrokerPartial ibp WITH (NOLOCK)
      LEFT JOIN dbo.BrokerPartial bp WITH (NOLOCK)
        ON bp.NoBrokerPartial = ibp.NoBrokerPartial
      WHERE ibp.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(bbp.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputBBPartial ibbp WITH (NOLOCK)
      LEFT JOIN dbo.BahanBakuPartial bbp WITH (NOLOCK)
        ON bbp.NoBBPartial = ibbp.NoBBPartial
      WHERE ibbp.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(gp.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputGilinganPartial igp WITH (NOLOCK)
      LEFT JOIN dbo.GilinganPartial gp WITH (NOLOCK)
        ON gp.NoGilinganPartial = igp.NoGilinganPartial
      WHERE igp.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(mp.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputMixerPartial imp WITH (NOLOCK)
      LEFT JOIN dbo.MixerPartial mp WITH (NOLOCK)
        ON mp.NoMixerPartial = imp.NoMixerPartial
      WHERE imp.NoProduksi = @NoProduksi

      UNION ALL
      SELECT ISNULL(rp.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiInputRejectPartial irp WITH (NOLOCK)
      LEFT JOIN dbo.RejectV2Partial rp WITH (NOLOCK)
        ON rp.NoRejectPartial = irp.NoRejectPartial
      WHERE irp.NoProduksi = @NoProduksi
    ),
    OutputBrokerRows AS (
      SELECT ISNULL(d.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiOutput o WITH (UPDLOCK, HOLDLOCK)
      LEFT JOIN dbo.Broker_d d WITH (NOLOCK)
        ON d.NoBroker = o.NoBroker AND d.NoSak = o.NoSak
      WHERE o.NoProduksi = @NoProduksi
    ),
    OutputBonggolanRows AS (
      SELECT ISNULL(b.Berat, 0) AS Berat
      FROM dbo.BrokerProduksiOutputBonggolan ob WITH (UPDLOCK, HOLDLOCK)
      LEFT JOIN dbo.Bonggolan b WITH (NOLOCK)
        ON b.NoBonggolan = ob.NoBonggolan
      WHERE ob.NoProduksi = @NoProduksi
    )
    SELECT
      CAST(ISNULL((SELECT SUM(Berat) FROM InputRows), 0) AS decimal(18,3)) AS TotalBeratInputKg,
      CAST(
        ISNULL((SELECT SUM(Berat) FROM OutputBrokerRows), 0) +
        ISNULL((SELECT SUM(Berat) FROM OutputBonggolanRows), 0)
        AS decimal(18,3)
      ) AS TotalBeratOutputExistingKg;
  `);

  const row = result.recordset?.[0] || {};
  return {
    totalBeratInputKg: Number(row.TotalBeratInputKg || 0),
    totalBeratOutputExistingKg: Number(row.TotalBeratOutputExistingKg || 0),
  };
}

async function assertBrokerProductionOutputWeightWithinInput({
  runner,
  noProduksi,
  tambahanBeratKg,
  contextLabel = "output",
}) {
  const add = Number(tambahanBeratKg || 0);
  if (!Number.isFinite(add) || add < 0) {
    throw badReq("tambahanBeratKg tidak valid");
  }

  const { totalBeratInputKg, totalBeratOutputExistingKg } =
    await getBrokerProductionWeightSummary(runner, noProduksi);

  const totalBeratOutputSetelahSimpanKg = totalBeratOutputExistingKg + add;
  if (totalBeratOutputSetelahSimpanKg > totalBeratInputKg) {
    throw badReq(
      `Total berat ${contextLabel} melebihi input produksi. Input=${totalBeratInputKg} kg, OutputExisting=${totalBeratOutputExistingKg} kg, OutputBaru=${add} kg, OutputSetelahSimpan=${totalBeratOutputSetelahSimpanKg} kg.`,
    );
  }

  return {
    totalBeratInputKg,
    totalBeratOutputExistingKg,
    totalBeratOutputSetelahSimpanKg,
  };
}

module.exports = {
  getBrokerProductionWeightSummary,
  assertBrokerProductionOutputWeightWithinInput,
};

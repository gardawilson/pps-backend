const { sql, poolPromise } = require("../../../../core/config/db");

async function getMixerDetailsByNoMixer(noMixer) {
  const pool = await poolPromise;
  const result = await pool.request().input("NoMixer", sql.VarChar, noMixer)
    .query(`
      SELECT
        d.NoMixer,
        d.NoSak,
        CASE
          WHEN d.IsPartial = 1 THEN
            d.Berat - ISNULL((
              SELECT SUM(p.Berat)
              FROM dbo.MixerPartial p
              WHERE p.NoMixer = d.NoMixer
                AND p.NoSak = d.NoSak
            ), 0)
          ELSE d.Berat
        END AS Berat,
        d.DateUsage,
        d.IsPartial
      FROM dbo.Mixer_d d
      WHERE d.NoMixer = @NoMixer
      ORDER BY d.NoSak;
    `);

  const formatDateTime = (date) => {
    if (!date) return null;
    const d = new Date(date);
    const pad = (n) => (n < 10 ? `0${n}` : String(n));
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  };

  return result.recordset.map((item) => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDateTime(item.DateUsage) }),
  }));
}

module.exports = {
  getMixerDetailsByNoMixer,
};

// services/label-detail-service.js
const { sql, connectDb } = require('../../core/config/db');
const { formatDate } = require('../../core/utils/date-helper');


function formatBerat(value) {
  if (value == null) return null;
  return parseFloat(parseFloat(value).toFixed(2));
}


async function getDetailByNomorLabel(nomorLabel) {
  let pool;
  try {
    pool = await connectDb();
    const request = new sql.Request(pool);

    // === Label Bahan Baku: format A.XXXX-YY ===
    if (nomorLabel.startsWith('A.') && nomorLabel.includes('-')) {
      const parts = nomorLabel.split('-');

      if (parts.length === 2 && parts[0] && parts[1]) {
        const noBahanBaku = parts[0]; // sudah termasuk 'A.'
        const noPallet = parts[1];

        // Validasi noPallet (angka atau alfanumerik)
        if (!/^[\w\d]+$/.test(noPallet)) {
          return null;
        }

        request.input('noBahanBaku', sql.VarChar, noBahanBaku);
        request.input('noPallet', sql.Int, parseInt(noPallet)); // karena di DB bertipe INT

        // Ambil data utama dari header
        const headerResult = await request.query(`
          SELECT
            'bahanbaku' AS LabelType,
            CAST(h.NoBahanBaku AS VARCHAR) + '-' + CAST(h.NoPallet AS VARCHAR) AS NomorLabel,
            jp.Jenis AS NamaJenisPlastik,
            mw.NamaWarehouse,
            h.Keterangan,
            h.Moisture,
            h.MeltingIndex,
            h.Elasticity,
            h.Tenggelam
          FROM BahanBakuPallet_h h
          LEFT JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
          LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = h.IdWarehouse
          WHERE h.NoBahanBaku = @noBahanBaku AND h.NoPallet = @noPallet
        `);

        // Ambil total berat dan jumlah sak
        const detailResult = await request.query(`
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS TotalBerat
          FROM BahanBaku_d
          WHERE NoBahanBaku = @noBahanBaku AND NoPallet = @noPallet
        `);

        // Ambil DateCreate dari header BahanBaku_h
        const dateResult = await request.query(`
          SELECT TOP 1 DateCreate
          FROM BahanBaku_h
          WHERE NoBahanBaku = @noBahanBaku
        `);

        // Ambil IdLokasi dari detail BahanBaku_d (ambil salah satu/top 1)
        const lokasiResult = await request.query(`
          SELECT TOP 1 IdLokasi
          FROM BahanBaku_d
          WHERE NoBahanBaku = @noBahanBaku AND NoPallet = @noPallet
        `);

        // Gabungkan hasil
        if (headerResult.recordset.length > 0) {
          const header = headerResult.recordset[0];
          const detail = detailResult.recordset[0] || {};
          const date = dateResult.recordset[0]?.DateCreate;
          const lokasi = lokasiResult.recordset[0]?.IdLokasi;

          return {
            ...header,
            ...detail,
            TotalBerat: formatBerat(detail.TotalBerat),
            ...(date && { DateCreate: formatDate(date) }),
            ...(lokasi && { IdLokasi: lokasi }),
          };
        }
      }

      return null;
    }


    // === Label Washing ===
    if (nomorLabel.startsWith('B.')) {
      request.input('noWashing', sql.VarChar, nomorLabel);

      // Ambil data utama dari header
      const headerResult = await request.query(`
        SELECT
          'washing' AS LabelType,
          h.NoWashing AS NomorLabel,
          jp.Jenis AS NamaJenisPlastik,
          mw.NamaWarehouse,
          h.DateCreate
        FROM Washing_h h
        LEFT JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
        LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = h.IdWarehouse
        WHERE h.NoWashing = @noWashing
      `);

      // Ambil total berat dan jumlah sak
      const detailResult = await request.query(`
        SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS TotalBerat
        FROM Washing_d
        WHERE NoWashing = @noWashing
      `);

      // Ambil IdLokasi dari Washing_d
      const lokasiResult = await request.query(`
        SELECT TOP 1 IdLokasi
        FROM Washing_d
        WHERE NoWashing = @noWashing
      `);

      if (headerResult.recordset.length > 0) {
        const header = headerResult.recordset[0];
        const detail = detailResult.recordset[0] || {};
        const lokasi = lokasiResult.recordset[0]?.IdLokasi;

        return {
          ...header,
          ...detail,
          TotalBerat: formatBerat(detail.TotalBerat),
          ...(header.DateCreate && { DateCreate: formatDate(header.DateCreate) }),
          ...(lokasi && { IdLokasi: lokasi }),
        };
      }
    }


    // === Label Broker ===
    if (nomorLabel.startsWith('D.')) {
      request.input('noBroker', sql.VarChar, nomorLabel);

      // Ambil data dari header
      const headerResult = await request.query(`
        SELECT
          'broker' AS LabelType,
          h.NoBroker AS NomorLabel,
          jp.Jenis AS NamaJenisPlastik,
          mw.NamaWarehouse,
          h.DateCreate
        FROM Broker_h h
        LEFT JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
        LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = h.IdWarehouse
        WHERE h.NoBroker = @noBroker
      `);

      // Ambil total berat dan jumlah sak
      const detailResult = await request.query(`
        SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS TotalBerat
        FROM Broker_d
        WHERE NoBroker = @noBroker
      `);

      // Ambil IdLokasi dari salah satu detail
      const lokasiResult = await request.query(`
        SELECT TOP 1 IdLokasi
        FROM Broker_d
        WHERE NoBroker = @noBroker
      `);

      if (headerResult.recordset.length > 0) {
        const header = headerResult.recordset[0];
        const detail = detailResult.recordset[0] || {};
        const lokasi = lokasiResult.recordset[0]?.IdLokasi;

        return {
          ...header,
          ...detail,
          TotalBerat: formatBerat(detail.TotalBerat),
          ...(header.DateCreate && { DateCreate: formatDate(header.DateCreate) }),
          ...(lokasi && { IdLokasi: lokasi }),
        };
      }
    }


    // === Label Crusher ===
    if (nomorLabel.startsWith('F.')) {
      request.input('noCrusher', sql.VarChar, nomorLabel);

      const result = await request.query(`
      SELECT
        'crusher' AS LabelType,
        c.NoCrusher AS NomorLabel,
        c.DateCreate,
        c.Berat,
        mw.NamaWarehouse,
        mc.NamaCrusher,
        c.IdLokasi
      FROM Crusher c
      LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = c.IdWarehouse
      LEFT JOIN MstCrusher mc ON mc.IdCrusher = c.IdCrusher
      WHERE c.NoCrusher = @noCrusher
    `);

      if (result.recordset.length > 0) {
        const data = result.recordset[0];
        return {
          ...data,
          Berat: formatBerat(data.Berat),
          ...(data.DateCreate && { DateCreate: formatDate(data.DateCreate) }),
        };
      }
    }


    // === Label Bonggolan ===
    if (nomorLabel.startsWith('M.')) {
      request.input('noBonggolan', sql.VarChar, nomorLabel);

      const result = await request.query(`
      SELECT
        'bonggolan' AS LabelType,
        b.NoBonggolan AS NomorLabel,
        b.DateCreate,
        b.Berat,
        mw.NamaWarehouse,
        mb.NamaBonggolan,
        b.IdLokasi
      FROM Bonggolan b
      LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = b.IdWarehouse
      LEFT JOIN MstBonggolan mb ON mb.IdBonggolan = b.IdBonggolan
      WHERE b.NoBonggolan = @noBonggolan
    `);

      if (result.recordset.length > 0) {
        const data = result.recordset[0];
        return {
          ...data,
          Berat: formatBerat(data.Berat),
          ...(data.DateCreate && { DateCreate: formatDate(data.DateCreate) }),
        };
      }
    }


    // === Label Gilingan ===
    if (nomorLabel.startsWith('V.')) {
      request.input('noGilingan', sql.VarChar, nomorLabel);

      const result = await request.query(`
      SELECT
        'gilingan' AS LabelType,
        g.NoGilingan AS NomorLabel,
        g.DateCreate,
        g.Berat,
        g.IsPartial,
        mw.NamaWarehouse,
        mg.NamaGilingan,
        g.IdLokasi
      FROM Gilingan g
      LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = g.IdWarehouse
      LEFT JOIN MstGilingan mg ON mg.IdGilingan = g.IdGilingan
      WHERE g.NoGilingan = @noGilingan
    `);

      if (result.recordset.length > 0) {
        const data = result.recordset[0];
        return {
          ...data,
          Berat: formatBerat(data.Berat),
          ...(data.DateCreate && { DateCreate: formatDate(data.DateCreate) }),
        };
      }
    }

    // === Label Mixer ===
    if (nomorLabel.startsWith('H.')) {
      request.input('noMixer', sql.VarChar, nomorLabel);

      // Ambil data dari header
      const headerResult = await request.query(`
          SELECT
            'mixer' AS LabelType,
            h.NoMixer AS NomorLabel,
            jp.Jenis AS NamaMixer,
            mw.NamaWarehouse,
            h.DateCreate
          FROM Mixer_h h
          LEFT JOIN MstMixer jp ON jp.IdMixer = h.IdMixer
          LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = h.IdWarehouse
          WHERE h.NoMixer = @noMixer
        `);

      // Ambil total berat dan jumlah sak
      const detailResult = await request.query(`
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS TotalBerat
          FROM Mixer_d
          WHERE NoMixer = @noMixer
        `);

      // Ambil IdLokasi dari salah satu detail
      const lokasiResult = await request.query(`
          SELECT TOP 1 IdLokasi
          FROM Mixer_d
          WHERE NoMixer = @noMixer
        `);

      if (headerResult.recordset.length > 0) {
        const header = headerResult.recordset[0];
        const detail = detailResult.recordset[0] || {};
        const lokasi = lokasiResult.recordset[0]?.IdLokasi;

        return {
          ...header,
          ...detail,
          TotalBerat: formatBerat(detail.TotalBerat),
          ...(header.DateCreate && { DateCreate: formatDate(header.DateCreate) }),
          ...(lokasi && { IdLokasi: lokasi }),
        };
      }
    }


    // === Label Furniture WIP ===
    if (nomorLabel.startsWith('BB.')) {
      request.input('noFurnitureWIP', sql.VarChar, nomorLabel);

      const result = await request.query(`
      SELECT
        'furniturewip' AS LabelType,
        g.NoFurnitureWIP AS NomorLabel,
        g.DateCreate,
        g.Pcs,
        g.Berat,
        g.IsPartial,
        mw.NamaWarehouse,
        mg.Nama,
        g.IdLokasi
      FROM FurnitureWIP g
      LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = g.IdWarehouse
      LEFT JOIN MstCabinetWIP mg ON mg.IdCabinetWIP = g.IdFurnitureWIP
      WHERE g.NoFurnitureWIP = @noFurnitureWIP
    `);

      if (result.recordset.length > 0) {
        const data = result.recordset[0];
        return {
          ...data,
          Berat: formatBerat(data.Berat),
          ...(data.DateCreate && { DateCreate: formatDate(data.DateCreate) }),
        };
      }
    }

    // === Label Barang Jadi ===
    if (nomorLabel.startsWith('BA.')) {
      request.input('noBJ', sql.VarChar, nomorLabel);

      const result = await request.query(`
                SELECT
                  'barangjadi' AS LabelType,
                  g.NoBJ AS NomorLabel,
                  g.DateCreate,
                  g.Pcs,
                  g.Berat,
                  mw.NamaWarehouse,
                  mg.NamaBJ,
                  g.IdLokasi
                FROM BarangJadi g
                LEFT JOIN MstWarehouse mw ON mw.IdWarehouse = g.IdWarehouse
                LEFT JOIN MstBarangJadi mg ON mg.IdBJ = g.IdBJ
                WHERE g.NoBJ = @noBJ
              `);

      if (result.recordset.length > 0) {
        const data = result.recordset[0];
        return {
          ...data,
          Berat: formatBerat(data.Berat),
          ...(data.DateCreate && { DateCreate: formatDate(data.DateCreate) }),
        };
      }
    }

    return null;
  } finally {
    if (pool) await pool.close();
  }
}

module.exports = { getDetailByNomorLabel };

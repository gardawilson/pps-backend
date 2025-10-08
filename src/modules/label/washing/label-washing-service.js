// services/label-washing-service.js
const { sql, poolPromise } = require('../../../core/config/db');
const { formatDate } = require('../../../core/utils/date-helper');

// GET all header with pagination & search
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT 
      h.NoWashing,
      h.DateCreate,
      h.IdJenisPlastik,
      jp.Jenis AS NamaJenisPlastik,
      w.NamaWarehouse,
      MAX(d.IdLokasi) AS IdLokasi,
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,
      h.Density,
      h.Moisture,
      -- ambil NoProduksi & NamaMesin
      MAX(wpo.NoProduksi) AS NoProduksi,
      MAX(m.NamaMesin) AS NamaMesin,
      -- ambil NoBongkarSusun
      MAX(bso.NoBongkarSusun) AS NoBongkarSusun
      -- kalau mau semua: STRING_AGG(bso.NoBongkarSusun, ', ') AS NoBongkarSusun
    FROM Washing_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    LEFT JOIN Washing_d d ON h.NoWashing = d.NoWashing
    LEFT JOIN WashingProduksiOutput wpo ON wpo.NoWashing = h.NoWashing
    LEFT JOIN WashingProduksi_h wph ON wph.NoProduksi = wpo.NoProduksi
    LEFT JOIN MstMesin m ON m.IdMesin = wph.IdMesin
    LEFT JOIN BongkarSusunOutputWashing bso ON bso.NoWashing = h.NoWashing
    WHERE 1=1
      ${search ? `AND (h.NoWashing LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ''}
      AND d.DateUsage IS NULL
    GROUP BY 
      h.NoWashing, h.DateCreate, h.IdJenisPlastik, jp.Jenis, w.NamaWarehouse, h.IdStatus, h.Density, h.Moisture
    ORDER BY h.NoWashing DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoWashing) as total
    FROM Washing_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    LEFT JOIN Washing_d d ON h.NoWashing = d.NoWashing
    LEFT JOIN WashingProduksiOutput wpo ON wpo.NoWashing = h.NoWashing
    LEFT JOIN WashingProduksi_h wph ON wph.NoProduksi = wpo.NoProduksi
    LEFT JOIN MstMesin m ON m.IdMesin = wph.IdMesin
    LEFT JOIN BongkarSusunOutputWashing bso ON bso.NoWashing = h.NoWashing
    WHERE 1=1
      ${search ? `AND (h.NoWashing LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ''}
      AND d.DateUsage IS NULL
  `;

  request.input('offset', sql.Int, offset).input('limit', sql.Int, limit);

  if (search) {
    request.input('search', sql.VarChar, `%${search}%`);
  }

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery)
  ]);

  const data = dataResult.recordset.map(item => ({
    ...item,
    ...(item.DateCreate && { DateCreate: formatDate(item.DateCreate) })
  }));

  const total = countResult.recordset[0].total;

  return { data, total };
};


// GET details by NoWashing
exports.getWashingDetailByNoWashing = async (nowashing) => {
  const pool = await poolPromise;
  const result = await pool.request()
    .input('NoWashing', sql.VarChar, nowashing)
    .query(`
      SELECT *
      FROM Washing_d
      WHERE NoWashing = @NoWashing AND DateUsage IS NULL
      ORDER BY NoSak
    `);

  return result.recordset.map(item => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) })
  }));
};

// INSERT header washing
exports.insertWashingData = async (data) => {
  const { IdJenisPlastik, IdWarehouse, DateCreate, IdStatus, CreateBy } = data;
  const pool = await poolPromise;

  // Ambil NoWashing terakhir
  const last = await pool.request().query(`
    SELECT TOP 1 NoWashing 
    FROM Washing_h 
    WHERE NoWashing LIKE 'B.%'
    ORDER BY NoWashing DESC
  `);

  let newNoWashing = 'B.0000000001';

  if (last.recordset.length > 0) {
    const lastCode = last.recordset[0].NoWashing;
    const numberPart = parseInt(lastCode.split('.')[1], 10);
    const nextNumber = numberPart + 1;
    const padded = nextNumber.toString().padStart(10, '0');
    newNoWashing = `B.${padded}`;
  }

  await pool.request()
    .input('NoWashing', sql.VarChar, newNoWashing)
    .input('IdJenisPlastik', sql.Int, IdJenisPlastik)
    .input('IdWarehouse', sql.Int, IdWarehouse)
    .input('DateCreate', sql.Date, DateCreate)
    .input('IdStatus', sql.Int, IdStatus)
    .input('CreateBy', sql.VarChar, CreateBy)
    .query(`
      INSERT INTO Washing_h (
        NoWashing,
        IdJenisPlastik,
        IdWarehouse,
        DateCreate,
        IdStatus,
        CreateBy,
        DateTimeCreate
      ) VALUES (
        @NoWashing,
        @IdJenisPlastik,
        @IdWarehouse,
        @DateCreate,
        @IdStatus,
        @CreateBy,
        GETDATE()
      )
    `);

  return { NoWashing: newNoWashing };
};

// INSERT details
exports.insertWashingDetailData = async (dataList) => {
  const pool = await poolPromise;

  for (const { NoWashing, NoSak, Berat, DateUsage, IdLokasi } of dataList) {
    await pool.request()
      .input('NoWashing', sql.VarChar, NoWashing)
      .input('NoSak', sql.Int, NoSak)
      .input('Berat', sql.Decimal(18, 2), Berat)
      .input('DateUsage', sql.DateTime, DateUsage)
      .input('IdLokasi', sql.Int, IdLokasi)
      .query(`
        INSERT INTO Washing_d (NoWashing, NoSak, Berat, DateUsage, IdLokasi)
        VALUES (@NoWashing, @NoSak, @Berat, @DateUsage, @IdLokasi)
      `);
  }

  return dataList;
};

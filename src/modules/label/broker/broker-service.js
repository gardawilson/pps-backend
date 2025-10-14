// services/broker-service.js
const { sql, poolPromise } = require('../../../core/config/db');

// GET all header Broker with pagination & search (mirror of Washing.getAll)
exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();

  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      h.NoBroker,
      h.DateCreate,
      h.IdJenisPlastik,
      jp.Jenis AS NamaJenisPlastik,
      h.IdWarehouse,
      w.NamaWarehouse,
      h.Blok,
      h.IdLokasi,
      CASE 
        WHEN h.IdStatus = 1 THEN 'PASS'
        WHEN h.IdStatus = 0 THEN 'HOLD'
        ELSE '' 
      END AS StatusText,
      -- kolom kualitas/notes (ikutkan agar konsisten dengan tabel)
      h.Density,
      h.Moisture,
      h.MaxMeltTemp,
      h.MinMeltTemp,
      h.MFI,
      h.VisualNote,
      h.Density2,
      h.Density3,
      h.Moisture2,
      h.Moisture3
    FROM Broker_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    LEFT JOIN Broker_d d ON d.NoBroker = h.NoBroker
    WHERE 1=1
      ${search ? `AND (h.NoBroker LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ''}
      AND NOT EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NOT NULL
      )
    GROUP BY
      h.NoBroker, h.DateCreate, h.IdJenisPlastik, jp.Jenis,
      h.IdWarehouse, w.NamaWarehouse, h.IdStatus,
      h.Density, h.Moisture, h.MaxMeltTemp, h.MinMeltTemp, h.MFI, h.VisualNote,
      h.Density2, h.Density3, h.Moisture2, h.Moisture3,
      h.Blok, h.IdLokasi
    ORDER BY h.NoBroker DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT h.NoBroker) AS total
    FROM Broker_h h
    INNER JOIN MstJenisPlastik jp ON jp.IdJenisPlastik = h.IdJenisPlastik
    INNER JOIN MstWarehouse w ON w.IdWarehouse = h.IdWarehouse
    WHERE 1=1
      ${search ? `AND (h.NoBroker LIKE @search OR jp.Jenis LIKE @search OR w.NamaWarehouse LIKE @search)` : ''}
      AND NOT EXISTS (
        SELECT 1 
        FROM Broker_d d2 
        WHERE d2.NoBroker = h.NoBroker 
          AND d2.DateUsage IS NOT NULL
      )
  `;

  request.input('offset', sql.Int, offset).input('limit', sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset.map(item => ({ ...item }));
  const total = countResult.recordset[0]?.total ?? 0;

  return { data, total };
};

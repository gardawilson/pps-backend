const { sql, poolPromise } = require('../../../core/config/db');
const { formatDate } = require('../../../core/utils/date-helper');

async function getAllLabels(page = 1, limit = 50, kategori = null, idlokasi = null, blok = null) {
  const pool = await poolPromise;
  const offset = (page - 1) * limit;

  // CTE semua kategori + Qty & Berat (partial-aware sesuai spesifikasi)
  const cte = `
;WITH
A AS ( -- Bahan Baku (partial-aware)
  SELECT
    LabelCode = CAST(p.NoBahanBaku AS NVARCHAR(50)) + '-' + CAST(p.NoPallet AS NVARCHAR(10)),
    DateCreate = h.DateCreate,
    NamaJenis  = jp.Jenis,
    Kategori   = N'bahanbaku',
    Blok       = p.Blok,
    IdLokasi   = p.IdLokasi,
    Qty        = ISNULL(bbAgg.TotalPcs, 0),
    Berat      = ISNULL(bbAgg.TotalBerat, 0)
  FROM dbo.BahanBakuPallet_h p
  JOIN dbo.BahanBaku_h h ON h.NoBahanBaku = p.NoBahanBaku
  LEFT JOIN dbo.MstJenisPlastik jp ON jp.IdJenisPlastik = p.IdJenisPlastik
  LEFT JOIN (
      SELECT 
          d.NoBahanBaku,
          d.NoPallet,
          COUNT(*) AS TotalPcs,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS TotalBerat
      FROM dbo.BahanBaku_d d
      LEFT JOIN (
          SELECT NoBahanBaku, NoPallet, NoSak, SUM(ISNULL(Berat,0)) AS TotalPartial
          FROM dbo.BahanBakuPartial
          GROUP BY NoBahanBaku, NoPallet, NoSak
      ) p ON d.NoBahanBaku = p.NoBahanBaku AND d.NoPallet = p.NoPallet AND d.NoSak = p.NoSak
      WHERE d.DateUsage IS NULL
      GROUP BY d.NoBahanBaku, d.NoPallet
  ) bbAgg ON bbAgg.NoBahanBaku = p.NoBahanBaku AND bbAgg.NoPallet = p.NoPallet
  WHERE EXISTS (
    SELECT 1 FROM dbo.BahanBaku_d d
    WHERE d.NoBahanBaku = p.NoBahanBaku AND d.NoPallet = p.NoPallet AND d.DateUsage IS NULL
  )
),
B AS ( -- Washing (no partial)
  SELECT
    LabelCode = wh.NoWashing,
    DateCreate = wh.DateCreate,
    NamaJenis  = jp.Jenis,
    Kategori   = N'washing',
    Blok       = wh.Blok,
    IdLokasi   = wh.IdLokasi,
    Qty        = ISNULL(wAgg.TotalPcs,0),
    Berat      = ISNULL(wAgg.TotalBerat,0)
  FROM dbo.Washing_h wh
  LEFT JOIN dbo.MstJenisPlastik jp ON jp.IdJenisPlastik = wh.IdJenisPlastik
  LEFT JOIN (
      SELECT wd.NoWashing, COUNT(*) AS TotalPcs, SUM(ISNULL(wd.Berat,0)) AS TotalBerat
      FROM dbo.Washing_d wd
      WHERE wd.DateUsage IS NULL
      GROUP BY wd.NoWashing
  ) wAgg ON wAgg.NoWashing = wh.NoWashing
  WHERE EXISTS (
    SELECT 1 FROM dbo.Washing_d wd
    WHERE wd.NoWashing = wh.NoWashing AND wd.DateUsage IS NULL
  )
),
D AS ( -- Broker (partial-aware)
  SELECT
    LabelCode = bh.NoBroker,
    DateCreate = bh.DateCreate,
    NamaJenis  = jp.Jenis,
    Kategori   = N'broker',
    Blok       = bh.Blok,
    IdLokasi   = bh.IdLokasi,
    Qty        = ISNULL(bAgg.TotalPcs,0),
    Berat      = ISNULL(bAgg.TotalBerat,0)
  FROM dbo.Broker_h bh
  LEFT JOIN dbo.MstJenisPlastik jp ON jp.IdJenisPlastik = bh.IdJenisPlastik
  LEFT JOIN (
      SELECT 
          d.NoBroker,
          COUNT(*) AS TotalPcs,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS TotalBerat
      FROM dbo.Broker_d d
      LEFT JOIN (
          SELECT NoBroker, NoSak, SUM(Berat) AS TotalPartial
          FROM dbo.BrokerPartial
          GROUP BY NoBroker, NoSak
      ) p ON d.NoBroker = p.NoBroker AND d.NoSak = p.NoSak
      WHERE d.DateUsage IS NULL
      GROUP BY d.NoBroker
  ) bAgg ON bAgg.NoBroker = bh.NoBroker
  WHERE EXISTS (
    SELECT 1 FROM dbo.Broker_d bd
    WHERE bd.NoBroker = bh.NoBroker AND bd.DateUsage IS NULL
  )
),
F AS ( -- Crusher (header only)
  SELECT
    LabelCode = c.NoCrusher,
    DateCreate = c.DateCreate,
    NamaJenis  = mc.NamaCrusher,
    Kategori   = N'crusher',
    Blok       = c.Blok,
    IdLokasi   = c.IdLokasi,
    Qty        = NULL,
    Berat      = ISNULL(c.Berat,0)
  FROM dbo.Crusher c
  LEFT JOIN dbo.MstCrusher mc ON mc.IdCrusher = c.IdCrusher
  WHERE c.DateUsage IS NULL
),
M AS ( -- Bonggolan (header only)
  SELECT
    LabelCode = b.NoBonggolan,
    DateCreate = b.DateCreate,
    NamaJenis  = mb.NamaBonggolan,
    Kategori   = N'bonggolan',
    Blok       = b.Blok,
    IdLokasi   = b.IdLokasi,
    Qty        = NULL,
    Berat      = ISNULL(b.Berat,0)
  FROM dbo.Bonggolan b
  LEFT JOIN dbo.MstBonggolan mb ON mb.IdBonggolan = b.IdBonggolan
  WHERE b.DateUsage IS NULL
),
V AS ( -- Gilingan (partial-aware, agregat dari tabel Gilingan + GilinganPartial)
  SELECT
    LabelCode  = g.NoGilingan,
    DateCreate = g.DateCreate,
    NamaJenis  = mg.NamaGilingan,
    Kategori   = N'gilingan',
    Blok       = g.Blok,
    IdLokasi   = g.IdLokasi,
    Qty        = ISNULL(vAgg.TotalPcs,0),
    Berat      = ISNULL(vAgg.TotalBerat,0)
  FROM dbo.Gilingan g
  LEFT JOIN dbo.MstGilingan mg ON mg.IdGilingan = g.IdGilingan
  LEFT JOIN (
      SELECT 
          d.NoGilingan,
          COUNT(*) AS TotalPcs,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS TotalBerat
      FROM dbo.Gilingan d
      LEFT JOIN (
          SELECT NoGilingan, SUM(Berat) AS TotalPartial
          FROM dbo.GilinganPartial
          GROUP BY NoGilingan
      ) p ON d.NoGilingan = p.NoGilingan
      WHERE d.DateUsage IS NULL
      GROUP BY d.NoGilingan
  ) vAgg ON vAgg.NoGilingan = g.NoGilingan
  WHERE g.DateUsage IS NULL
),
H AS ( -- Mixer (partial-aware)
  SELECT
    LabelCode = mh.NoMixer,
    DateCreate = mh.DateCreate,
    NamaJenis  = mm.Jenis,
    Kategori   = N'mixer',
    Blok       = mh.Blok,
    IdLokasi   = mh.IdLokasi, 
    Qty        = ISNULL(hAgg.TotalPcs,0),
    Berat      = ISNULL(hAgg.TotalBerat,0)
  FROM dbo.Mixer_h mh
  LEFT JOIN dbo.MstMixer mm ON mm.IdMixer = mh.IdMixer
  LEFT JOIN (
      SELECT 
          d.NoMixer,
          COUNT(*) AS TotalPcs,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS TotalBerat
      FROM dbo.Mixer_d d
      LEFT JOIN (
          SELECT NoMixer, NoSak, SUM(Berat) AS TotalPartial
          FROM dbo.MixerPartial
          GROUP BY NoMixer, NoSak
      ) p ON d.NoMixer = p.NoMixer AND d.NoSak = p.NoSak
      WHERE d.DateUsage IS NULL
      GROUP BY d.NoMixer
  ) hAgg ON hAgg.NoMixer = mh.NoMixer
  WHERE EXISTS (
    SELECT 1 FROM dbo.Mixer_d md
    WHERE md.NoMixer = mh.NoMixer AND md.DateUsage IS NULL
  )
),
BB AS ( -- FurnitureWIP (partial-aware Pcs)
  SELECT
    LabelCode = fw.NoFurnitureWIP,
    DateCreate = fw.DateCreate,
    NamaJenis  = mcw.Nama,
    Kategori   = N'furniturewip',
    Blok       = fw.Blok,
    IdLokasi   = fw.IdLokasi,
    Qty        = ISNULL(bbAgg.TotalPcs,0),
    Berat      = ISNULL(bbAgg.TotalBerat,0)
  FROM dbo.FurnitureWIP fw
  LEFT JOIN dbo.MstCabinetWIP mcw ON mcw.IdCabinetWIP = fw.IdFurnitureWIP
  LEFT JOIN (
      SELECT 
          d.NoFurnitureWIP,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Pcs,0) - ISNULL(p.TotalPartialPcs,0)
                  ELSE ISNULL(d.Pcs,0)
              END
          ) AS TotalPcs,
          SUM(ISNULL(d.Berat,0)) AS TotalBerat
      FROM dbo.FurnitureWIP d
      LEFT JOIN (
          SELECT NoFurnitureWIP, SUM(Pcs) AS TotalPartialPcs
          FROM dbo.FurnitureWIPPartial
          GROUP BY NoFurnitureWIP
      ) p ON d.NoFurnitureWIP = p.NoFurnitureWIP
      WHERE d.DateUsage IS NULL
      GROUP BY d.NoFurnitureWIP
  ) bbAgg ON bbAgg.NoFurnitureWIP = fw.NoFurnitureWIP
  WHERE fw.DateUsage IS NULL
),
BA AS ( -- BarangJadi (partial-aware Pcs)
  SELECT
    LabelCode = bj.NoBJ,
    DateCreate = bj.DateCreate,
    NamaJenis  = mbj.NamaBJ,
    Kategori   = N'barangjadi',
    Blok       = bj.Blok,
    IdLokasi   = bj.IdLokasi,
    Qty        = ISNULL(baAgg.TotalPcs,0),
    Berat      = ISNULL(baAgg.TotalBerat,0)
  FROM dbo.BarangJadi bj
  LEFT JOIN dbo.MstBarangJadi mbj ON mbj.IdBJ = bj.IdBJ
  LEFT JOIN (
      SELECT 
          d.NoBJ,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Pcs,0) - ISNULL(p.TotalPartialPcs,0)
                  ELSE ISNULL(d.Pcs,0)
              END
          ) AS TotalPcs,
          SUM(ISNULL(d.Berat,0)) AS TotalBerat
      FROM dbo.BarangJadi d
      LEFT JOIN (
          SELECT NoBJ, SUM(Pcs) AS TotalPartialPcs
          FROM dbo.BarangJadiPartial
          GROUP BY NoBJ
      ) p ON d.NoBJ = p.NoBJ
      WHERE d.DateUsage IS NULL
      GROUP BY d.NoBJ
  ) baAgg ON baAgg.NoBJ = bj.NoBJ
  WHERE bj.DateUsage IS NULL
),
BF AS ( -- Reject (header only)
  SELECT
    LabelCode  = r.NoReject,
    DateCreate = r.DateCreate,
    NamaJenis  = mr.NamaReject,
    Kategori   = N'reject',
    Blok       = r.Blok,
    IdLokasi   = r.IdLokasi,
    Qty        = NULL,
    Berat      = ISNULL(r.Berat,0)
  FROM dbo.RejectV2 r
  LEFT JOIN dbo.MstReject mr ON mr.IdReject = r.IdReject
  WHERE r.DateUsage IS NULL
)
`;

  // union semua kategori
  const allUnion = `
  SELECT * FROM A
  UNION ALL SELECT * FROM B
  UNION ALL SELECT * FROM D
  UNION ALL SELECT * FROM F
  UNION ALL SELECT * FROM M
  UNION ALL SELECT * FROM V
  UNION ALL SELECT * FROM H
  UNION ALL SELECT * FROM BB
  UNION ALL SELECT * FROM BA
  UNION ALL SELECT * FROM BF
`;

  let filterUnion = allUnion;
  if (kategori) {
    switch ((kategori || '').toLowerCase()) {
      case 'bahanbaku':    filterUnion = 'SELECT * FROM A'; break;
      case 'washing':      filterUnion = 'SELECT * FROM B'; break;
      case 'broker':       filterUnion = 'SELECT * FROM D'; break;
      case 'crusher':      filterUnion = 'SELECT * FROM F'; break;
      case 'bonggolan':    filterUnion = 'SELECT * FROM M'; break;
      case 'gilingan':     filterUnion = 'SELECT * FROM V'; break;
      case 'mixer':        filterUnion = 'SELECT * FROM H'; break;
      case 'furniturewip': filterUnion = 'SELECT * FROM BB'; break;
      case 'barangjadi':   filterUnion = 'SELECT * FROM BA'; break;
      case 'reject':       filterUnion = 'SELECT * FROM BF'; break;
    }
  }

  // filter lokasi (pakai parameter agar aman)
  let lokasiWhere = '';
  if (idlokasi && blok) {
    lokasiWhere = 'WHERE IdLokasi = @IdLokasi AND Blok = @Blok';
  } else if (idlokasi) {
    lokasiWhere = 'WHERE IdLokasi = @IdLokasi';
  } else if (blok) {
    lokasiWhere = 'WHERE Blok = @Blok';
  }
  
  // DATA (paged)
  const dataQuery = `
${cte}
SELECT LabelCode, DateCreate, NamaJenis, Kategori, Blok, IdLokasi,
       ISNULL(Qty,0)   AS Qty,
       ISNULL(Berat,0) AS Berat
FROM (${filterUnion}) AS X
${lokasiWhere}
ORDER BY DateCreate DESC, LabelCode DESC
OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY;
`;

  // COUNT total baris (untuk pagination)
  const countQuery = `
${cte}
SELECT COUNT(*) AS TotalCount
FROM (${filterUnion}) AS AllData
${lokasiWhere};
`;

  // SUM totalQty & totalBerat (keseluruhan hasil filter, bukan per halaman)
  const sumQuery = `
${cte}
SELECT 
  SUM(ISNULL(Qty,0))   AS TotalQty,
  SUM(ISNULL(Berat,0)) AS TotalBerat
FROM (${filterUnion}) AS AllData
${lokasiWhere};
`;

  const dataReq  = pool.request();
  const countReq = pool.request();
  const sumReq   = pool.request();
  if (idlokasi) {
    dataReq.input('IdLokasi', sql.NVarChar, idlokasi);
    countReq.input('IdLokasi', sql.NVarChar, idlokasi);
    sumReq.input('IdLokasi', sql.NVarChar, idlokasi);
  }

  if (blok) {
    dataReq.input('Blok', sql.NVarChar(3), blok);
    countReq.input('Blok', sql.NVarChar(3), blok);
    sumReq.input('Blok', sql.NVarChar(3), blok);
  }

  const [dataResult, countResult, sumResult] = await Promise.all([
    dataReq.query(dataQuery),
    countReq.query(countQuery),
    sumReq.query(sumQuery)
  ]);

  const data = dataResult.recordset.map(r => ({
    ...r,
    ...(r.DateCreate && { DateCreate: formatDate(r.DateCreate) })
  }));

  const total       = countResult.recordset[0]?.TotalCount || 0;
  const totalQty    = sumResult.recordset[0]?.TotalQty || 0;
  const totalBerat  = sumResult.recordset[0]?.TotalBerat || 0;

  return {
    // ✅ metadata baru (lebih ramah UI)
    success: true,
    message: `Data label${kategori ? ` (${kategori})` : ''} berhasil diambil`,
    kategori: kategori || 'semua',
    blok: blok || 'semua',
    idlokasi: idlokasi || 'semua',
    totalData: total,                 // alias dari total
    currentPage: page,                // alias dari page
    totalPages: Math.ceil(total / limit),
    perPage: limit,                   // alias dari limit
  
    // ✅ agregat baru
    totalQty,
    totalBerat,
  
    // ✅ payload data
    data,
  
    // ✅ field legacy (biar backward-compatible)
    total,                            // = totalData
    page,                             // = currentPage
    limit                             // = perPage
  };
  
}

async function updateLabelLocation(labelCode, idLokasi, blok) {
  const pool = await poolPromise;
  const request = pool.request();

  const prefix = labelCode.split('.')[0].toUpperCase();

  let query = '';
  let tableName = '';

  // mapping prefix ke tabel
  switch (prefix) {
    case 'A': tableName = 'dbo.BahanBakuPallet_h'; break;
    case 'B': tableName = 'dbo.Washing_h'; break;
    case 'D': tableName = 'dbo.Broker_h'; break;
    case 'F': tableName = 'dbo.Crusher'; break;
    case 'M': tableName = 'dbo.Bonggolan'; break;
    case 'V': tableName = 'dbo.Gilingan'; break;
    case 'H': tableName = 'dbo.Mixer_h'; break;
    case 'BB': tableName = 'dbo.FurnitureWIP'; break;
    case 'BA': tableName = 'dbo.BarangJadi'; break;
    case 'BF': tableName = 'dbo.RejectV2'; break;
    default:
      return { success: false, message: `Prefix ${prefix} tidak dikenali untuk nomor label ${labelCode}` };
  }

  // Cek tipe kolom IdLokasi di tabel target
  const checkTypeQuery = `
    SELECT DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = PARSENAME('${tableName}', 1) 
      AND COLUMN_NAME = 'IdLokasi'
  `;

  const typeResult = await pool.request().query(checkTypeQuery);
  const idLokasiType = typeResult.recordset[0]?.DATA_TYPE;

  // bikin query update aman untuk dua kolom
  query = `
    UPDATE ${tableName}
    SET 
      IdLokasi = ${idLokasiType === 'int' ? 'TRY_CONVERT(INT, @IdLokasi)' : '@IdLokasi'},
      Blok = @Blok
    WHERE 
      ${prefix === 'A' 
        ? "(CAST(NoBahanBaku AS NVARCHAR(50)) + '-' + CAST(NoPallet AS NVARCHAR(10)))" 
        : getLabelColumn(prefix)
      } = @LabelCode
  `;

  // binding parameter
  request.input('LabelCode', sql.NVarChar, labelCode);
  request.input('IdLokasi', sql.NVarChar, idLokasi);
  request.input('Blok', sql.NVarChar(3), blok);

  const result = await request.query(query);

  if (result.rowsAffected[0] === 0) {
    return {
      success: false,
      message: `Nomor label ${labelCode} tidak ditemukan di tabel ${tableName}`,
    };
  }

  return {
    success: true,
    message: `Lokasi label ${labelCode} berhasil diupdate ke ${idLokasi} (blok ${blok})`,
    updated: { labelCode, idLokasi, blok },
  };
}

// helper fungsi untuk ambil kolom label
function getLabelColumn(prefix) {
  switch (prefix) {
    case 'B': return 'NoWashing';
    case 'D': return 'NoBroker';
    case 'F': return 'NoCrusher';
    case 'M': return 'NoBonggolan';
    case 'V': return 'NoGilingan';
    case 'H': return 'NoMixer';
    case 'BB': return 'NoFurnitureWIP';
    case 'BA': return 'NoBJ';
    case 'BF': return 'NoReject';
    default: return 'NoLabel';
  }
}


module.exports = { getAllLabels, updateLabelLocation };
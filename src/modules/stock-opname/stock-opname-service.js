const { sql, connectDb } = require('../../core/config/db');
const { formatDate } = require('../../core/utils/date-helper');

async function getNoStockOpname() {
  let pool;
  try {
    pool = await connectDb();
    const result = await sql.query(`
      SELECT
        soh.NoSO,
        soh.Tanggal,
        STRING_AGG(wh.NamaWarehouse, ', ') AS NamaWarehouse,
        soh.IsBahanBaku,
        soh.IsWashing,
        soh.IsBonggolan,
        soh.IsCrusher,
        soh.IsBroker,
        soh.IsGilingan,
        soh.IsMixer,
        soh.IsFurnitureWIP,
        soh.IsBarangJadi
      FROM StockOpname_h soh
      LEFT JOIN StockOpname_h_WarehouseID sohw ON soh.NoSO = sohw.NoSO
      LEFT JOIN MstWarehouse wh ON sohw.IdWarehouse = wh.IdWarehouse
      WHERE soh.Tanggal > (
        SELECT ISNULL(MAX(PeriodHarian), '2000-01-01') 
        FROM MstTutupTransaksiHarian
      )
      GROUP BY
        soh.NoSO,
        soh.Tanggal,
        soh.IsBahanBaku,
        soh.IsWashing,
        soh.IsBonggolan,
        soh.IsCrusher,
        soh.IsBroker,
        soh.IsGilingan,
        soh.IsMixer,
        soh.IsFurnitureWIP,
        soh.IsBarangJadi
      ORDER BY soh.NoSO DESC
    `);

    if (!result.recordset || result.recordset.length === 0) {
      return null;
    }

    return result.recordset.map(({
      NoSO,
      Tanggal,
      NamaWarehouse,
      IsBahanBaku,
      IsWashing,
      IsBonggolan,
      IsCrusher,
      IsBroker,
      IsGilingan,
      IsMixer,
      IsFurnitureWIP,
      IsBarangJadi
    }) => ({
      NoSO,
      Tanggal: formatDate(Tanggal),
      NamaWarehouse: NamaWarehouse || '-',
      IsBahanBaku,
      IsWashing,
      IsBonggolan,
      IsCrusher,
      IsBroker,
      IsGilingan,
      IsMixer,
      IsFurnitureWIP,
      IsBarangJadi
    }));
  } catch (err) {
    throw new Error(`Stock Opname Service Error: ${err.message}`);
  } finally {
    if (pool) await pool.close();
  }
}


async function getStockOpnameAcuan({ noso, page = 1, pageSize = 20, filterBy = 'all', idLokasi, search = '' }) {
  const offset = (page - 1) * pageSize;
  let pool;

  const filterMap = {
    'bahanbaku': {
      table: 'StockOpnameBahanBaku',
      labelExpr: "CONCAT(NoBahanBaku, '-', NoPallet)",
      label: 'Bahan Baku',
      hasilTable: 'StockOpnameHasilBahanBaku',
      hasilWhereClause: "CONCAT(hasil.NoBahanBaku, '-', hasil.NoPallet) = CONCAT(src.NoBahanBaku, '-', src.NoPallet)",
      fields: {
        jmlhSak: 'JmlhSak',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'washing': {
      table: 'StockOpnameWashing',
      labelExpr: 'NoWashing',
      label: 'Washing',
      hasilTable: 'StockOpnameHasilWashing',
      hasilWhereClause: 'hasil.NoWashing = src.NoWashing',
      fields: {
        jmlhSak: 'JmlhSak',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'broker': {
      table: 'StockOpnameBroker',
      labelExpr: 'NoBroker',
      label: 'Broker',
      hasilTable: 'StockOpnameHasilBroker',
      hasilWhereClause: 'hasil.NoBroker = src.NoBroker',
      fields: {
        jmlhSak: 'JmlhSak',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'crusher': {
      table: 'StockOpnameCrusher',
      labelExpr: 'NoCrusher',
      label: 'Crusher',
      hasilTable: 'StockOpnameHasilCrusher',
      hasilWhereClause: 'hasil.NoCrusher = src.NoCrusher',
      fields: {
        jmlhSak: 'NULL',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'bonggolan': {
      table: 'StockOpnameBonggolan',
      labelExpr: 'NoBonggolan',
      label: 'Bonggolan',
      hasilTable: 'StockOpnameHasilBonggolan',
      hasilWhereClause: 'hasil.NoBonggolan = src.NoBonggolan',
      fields: {
        jmlhSak: 'NULL',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'gilingan': {
      table: 'StockOpnameGilingan',
      labelExpr: 'NoGilingan',
      label: 'Gilingan',
      hasilTable: 'StockOpnameHasilGilingan',
      hasilWhereClause: 'hasil.NoGilingan = src.NoGilingan',
      fields: {
        jmlhSak: 'NULL',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'mixer': {
      table: 'StockOpnameMixer',
      labelExpr: 'NoMixer',
      label: 'Mixer',
      hasilTable: 'StockOpnameHasilMixer',
      hasilWhereClause: 'hasil.NoMixer = src.NoMixer',
      fields: {
        jmlhSak: 'JmlhSak',
        berat: 'ROUND(Berat, 2)'
      }
    },
    'furniturewip': {
      table: 'StockOpnameFurnitureWIP',
      labelExpr: 'NoFurnitureWIP',
      label: 'Furniture WIP',
      hasilTable: 'StockOpnameHasilFurnitureWIP',
      hasilWhereClause: 'hasil.NoFurnitureWIP = src.NoFurnitureWIP',
      fields: {
        jmlhSak: 'Pcs',
        berat: 'Berat'
      }
    },
    'barangjadi': {
      table: 'StockOpnameBarangJadi',
      labelExpr: 'NoBJ',
      label: 'Barang Jadi',
      hasilTable: 'StockOpnameHasilBarangJadi',
      hasilWhereClause: 'hasil.NoBJ = src.NoBJ',
      fields: {
        jmlhSak: 'Pcs',
        berat: 'Berat'
      }
    },
  };

  try {
    pool = await connectDb();
    const request = new sql.Request(pool);
    request.input('noso', sql.VarChar, noso);
    if (idLokasi && idLokasi !== 'all') request.input('idLokasi', sql.VarChar, idLokasi);
    if (search) request.input('search', sql.VarChar, `%${search}%`);

    const makeQuery = (table, labelExpr, labelType, hasilTable, hasilWhereClause, fields = {}) => `
      SELECT 
        ${labelExpr} AS NomorLabel, 
        '${labelType}' AS LabelType,
        ${fields.jmlhSak || 'NULL'} AS JmlhSak,
        ${fields.berat || 'NULL'} AS Berat,
        IdLokasi
      FROM ${table} AS src
      WHERE NoSO = @noso
        ${idLokasi && idLokasi !== 'all' ? 'AND IdLokasi = @idLokasi' : ''}
        ${search ? `AND ${labelExpr} LIKE @search` : ''}
        AND NOT EXISTS (
          SELECT 1 FROM ${hasilTable} AS hasil
          WHERE hasil.NoSO = src.NoSO AND ${hasilWhereClause}
        )
    `;

    const makeCount = (table, labelExpr, hasilTable, hasilWhereClause) => `
      SELECT COUNT(*) AS total
      FROM ${table} AS src
      WHERE NoSO = @noso
        ${idLokasi && idLokasi !== 'all' ? 'AND IdLokasi = @idLokasi' : ''}
        ${search ? `AND ${labelExpr} LIKE @search` : ''}
        AND NOT EXISTS (
          SELECT 1 FROM ${hasilTable} AS hasil
          WHERE hasil.NoSO = src.NoSO AND ${hasilWhereClause}
        )
    `;

    const overallTotalQuery = (() => {
      // jika ada filterBy spesifik, pakai 1 table
      if (filterBy !== 'all') {
        const f = filterMap[filterBy.toLowerCase()];
        return `
          SELECT
            COUNT(*) AS TotalLabelGlobal,
            ROUND(SUM(CAST(${f.fields.berat || '0'} AS FLOAT)), 2) AS TotalBeratGlobal,
            SUM(CAST(${f.fields.jmlhSak || '0'} AS INT)) AS TotalSakGlobal
          FROM ${f.table} AS src
          WHERE NoSO = @noso
            ${idLokasi && idLokasi !== 'all' ? 'AND IdLokasi = @idLokasi' : ''}
            ${search ? `AND ${f.labelExpr} LIKE @search` : ''}
        `;
      } else {
        // semua tipe digabung
        return `
          SELECT
            COUNT(*) AS TotalLabelGlobal,
            ROUND(SUM(CAST(beratSum.Berat AS FLOAT)), 2) AS TotalBeratGlobal,
            SUM(CAST(beratSum.JmlhSak AS INT)) AS TotalSakGlobal
          FROM (
            ${Object.values(filterMap).map(f => `
              SELECT
                ${f.fields.berat || '0'} AS Berat,
                ${f.fields.jmlhSak || '0'} AS JmlhSak
              FROM ${f.table} AS src
              WHERE NoSO = @noso
                ${idLokasi && idLokasi !== 'all' ? 'AND IdLokasi = @idLokasi' : ''}
                ${search ? `AND ${f.labelExpr} LIKE @search` : ''}
            `).join(' UNION ALL ')}
          ) AS beratSum
        `;
      }
    })();

    let query = '', totalQuery = '', beratSakQuery = '';

    if (filterBy !== 'all') {
      const filter = filterMap[filterBy.toLowerCase()];
      if (!filter) throw new Error('Invalid filterBy');

      query = `
        ${makeQuery(filter.table, filter.labelExpr, filter.label, filter.hasilTable, filter.hasilWhereClause, filter.fields)}
        ORDER BY NomorLabel
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;

      totalQuery = makeCount(filter.table, filter.labelExpr, filter.hasilTable, filter.hasilWhereClause);

      beratSakQuery = `
        SELECT
          ROUND(SUM(CAST(${filter.fields.berat || '0'} AS FLOAT)), 2) AS TotalBerat,
          SUM(CAST(${filter.fields.jmlhSak || '0'} AS INT)) AS TotalSak
        FROM ${filter.table} AS src
        WHERE NoSO = @noso
          ${idLokasi && idLokasi !== 'all' ? 'AND IdLokasi = @idLokasi' : ''}
          ${search ? `AND ${filter.labelExpr} LIKE @search` : ''}
          AND NOT EXISTS (
            SELECT 1 FROM ${filter.hasilTable} AS hasil
            WHERE hasil.NoSO = src.NoSO AND ${filter.hasilWhereClause}
          )
      `;
    } else {
      const allQueries = Object.values(filterMap).map(f => makeQuery(f.table, f.labelExpr, f.label, f.hasilTable, f.hasilWhereClause, f.fields));
      const allCounts = Object.values(filterMap).map(f => makeCount(f.table, f.labelExpr, f.hasilTable, f.hasilWhereClause));
      query = `
        SELECT * FROM (
          ${allQueries.join(' UNION ALL ')}
        ) AS acuan
        ORDER BY NomorLabel
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;
      totalQuery = `
        SELECT SUM(total) AS total FROM (
          ${allCounts.join(' UNION ALL ')}
        ) AS totalData;
      `;
      beratSakQuery = `
        SELECT
          ROUND(SUM(CAST(beratSum.Berat AS FLOAT)), 2) AS TotalBerat,
          SUM(CAST(beratSum.JmlhSak AS INT)) AS TotalSak
        FROM (
          ${Object.values(filterMap).map(f => `
            SELECT
              ${f.fields.berat || '0'} AS Berat,
              ${f.fields.jmlhSak || '0'} AS JmlhSak
            FROM ${f.table} AS src
            WHERE NoSO = @noso
              ${idLokasi && idLokasi !== 'all' ? 'AND IdLokasi = @idLokasi' : ''}
              ${search ? `AND ${f.labelExpr} LIKE @search` : ''}
              AND NOT EXISTS (
                SELECT 1 FROM ${f.hasilTable} AS hasil
                WHERE hasil.NoSO = src.NoSO AND ${f.hasilWhereClause}
              )
          `).join(' UNION ALL ')}
        ) AS beratSum
      `;
    }


    const [result, total, totalBeratSak, overallTotal] = await Promise.all([
      request.query(query),
      request.query(totalQuery),
      request.query(beratSakQuery),
      request.query(overallTotalQuery)
    ]);


    const formattedData = result.recordset.map(item => ({
      ...item,
      Berat: item.Berat !== null ? parseFloat(Number(item.Berat).toFixed(2)) : null
    }));

    return {
      data: formattedData,
      hasData: formattedData.length > 0,
      currentPage: page,
      pageSize,
      totalData: total.recordset[0].total,
      totalPages: Math.ceil(total.recordset[0].total / pageSize),
      totalBerat: totalBeratSak.recordset[0].TotalBerat || 0,
      totalSak: totalBeratSak.recordset[0].TotalSak || 0,
      totalLabelGlobal: overallTotal.recordset[0].TotalLabelGlobal || 0,
      totalBeratGlobal: overallTotal.recordset[0].TotalBeratGlobal || 0,
      totalSakGlobal: overallTotal.recordset[0].TotalSakGlobal || 0
    };

  } catch (err) {
    throw new Error(`Stock Opname Acuan Service Error: ${err.message}`);
  } finally {
    if (pool) await pool.close();
  }
}




async function getStockOpnameHasil({
  noso,
  page = 1,
  pageSize = 20,
  filterBy = 'all',
  idLokasi,
  search = '',
  filterByUser = false,
  username = ''
}) {
  const offset = (page - 1) * pageSize;
  let pool;

  const filterMap = {
    'bahanbaku': {
      table: 'StockOpnameHasilBahanBaku',
      labelExpr: "CONCAT(so.NoBahanBaku, '-', so.NoPallet)",
      label: 'Bahan Baku',
      joinClause: `LEFT JOIN (
          SELECT NoBahanBaku, NoPallet, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoBahanBaku, NoPallet ORDER BY TimeCreate DESC) as rn
          FROM BahanBaku_d 
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBahanBaku = detail.NoBahanBaku AND so.NoPallet = detail.NoPallet AND detail.rn = 1`,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },
    'washing': {
      table: 'StockOpnameHasilWashing',
      labelExpr: 'so.NoWashing',
      label: 'Washing',
      joinClause: `LEFT JOIN (
          SELECT NoWashing, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoWashing ORDER BY DateUsage DESC) as rn
          FROM Washing_d 
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoWashing = detail.NoWashing AND detail.rn = 1`,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },
    'broker': {
      table: 'StockOpnameHasilBroker',
      labelExpr: 'so.NoBroker',
      label: 'Broker',
      joinClause: `LEFT JOIN (
          SELECT NoBroker, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoBroker ORDER BY DateUsage DESC) as rn
          FROM Broker_d 
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBroker = detail.NoBroker AND detail.rn = 1`,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },
    'crusher': {
      table: 'StockOpnameHasilCrusher',
      labelExpr: 'so.NoCrusher',
      label: 'Crusher',
      joinClause: `LEFT JOIN (
          SELECT NoCrusher, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoCrusher ORDER BY DateUsage DESC) as rn
          FROM Crusher
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoCrusher = detail.NoCrusher AND detail.rn = 1`,
      fields: {
        jmlhSak: 'NULL',
        berat: 'so.Berat'
      }
    },
    'bonggolan': {
      table: 'StockOpnameHasilBonggolan',
      labelExpr: 'so.NoBonggolan',
      label: 'Bonggolan',
      joinClause: `LEFT JOIN (
          SELECT NoBonggolan, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoBonggolan ORDER BY DateUsage DESC) as rn
          FROM Bonggolan
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBonggolan = detail.NoBonggolan AND detail.rn = 1`,
      fields: {
        jmlhSak: 'NULL',
        berat: 'so.Berat'
      }
    },
    'gilingan': {
      table: 'StockOpnameHasilGilingan',
      labelExpr: 'so.NoGilingan',
      label: 'Gilingan',
      joinClause: `LEFT JOIN (
          SELECT NoGilingan, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoGilingan ORDER BY DateUsage DESC) as rn
          FROM Gilingan
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoGilingan = detail.NoGilingan AND detail.rn = 1`,
      fields: {
        jmlhSak: 'NULL',
        berat: 'so.Berat'
      }
    },
    'mixer': {
      table: 'StockOpnameHasilMixer',
      labelExpr: 'so.NoMixer',
      label: 'Mixer',
      joinClause: `LEFT JOIN (
          SELECT NoMixer, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoMixer ORDER BY DateUsage DESC) as rn
          FROM Mixer_d 
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoMixer = detail.NoMixer AND detail.rn = 1`,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },
    'furniturewip': {
      table: 'StockOpnameHasilFurnitureWIP',
      labelExpr: 'so.NoFurnitureWIP',
      label: 'Furniture WIP',
      joinClause: `LEFT JOIN (
          SELECT NoFurnitureWIP, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoFurnitureWIP ORDER BY DateUsage DESC) as rn
          FROM FurnitureWIP 
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoFurnitureWIP = detail.NoFurnitureWIP AND detail.rn = 1`,
      fields: {
        jmlhSak: 'so.Pcs',
        berat: 'so.Berat'
      }
    },
    'barangjadi': {
      table: 'StockOpnameHasilBarangJadi',
      labelExpr: 'so.NoBJ',
      label: 'Barang Jadi',
      joinClause: `LEFT JOIN (
          SELECT NoBJ, IdLokasi,
                 ROW_NUMBER() OVER (PARTITION BY NoBJ ORDER BY DateUsage DESC) as rn
          FROM BarangJadi 
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBJ = detail.NoBJ AND detail.rn = 1`,
      fields: {
        jmlhSak: 'so.Pcs',
        berat: 'so.Berat'
      }
    }
  };

  try {
    pool = await connectDb();
    const request = new sql.Request(pool);
    request.input('noso', sql.VarChar, noso);
    if (filterByUser) request.input('username', sql.VarChar, username);
    if (idLokasi && idLokasi !== 'all') request.input('idLokasi', sql.VarChar, idLokasi);
    if (search) request.input('search', sql.VarChar, `%${search}%`);

    const makeQuery = (table, labelExpr, labelType, joinClause, fields = {}) => `
      SELECT 
        ${labelExpr} AS NomorLabel, 
        '${labelType}' AS LabelType, 
        ${fields.jmlhSak || 'NULL'} AS JmlhSak, 
        ${fields.berat || 'NULL'} AS Berat,
        ISNULL(so.DateTimeScan, '1900-01-01') AS DateTimeScan,
        detail.IdLokasi,
        so.Username
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${idLokasi && idLokasi !== 'all' ? 'AND detail.IdLokasi = @idLokasi' : ''}
      ${search ? `AND ${labelExpr} LIKE @search` : ''}
    `;

    const makeCount = (table, labelExpr, joinClause) => `
      SELECT COUNT(*) AS total
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${idLokasi && idLokasi !== 'all' ? 'AND detail.IdLokasi = @idLokasi' : ''}
      ${search ? `AND ${labelExpr} LIKE @search` : ''}
    `;

    const makeTotalSak = (table, fieldSak, joinClause) => `
      SELECT ROUND(SUM(${fieldSak}), 2) AS total FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${idLokasi && idLokasi !== 'all' ? 'AND detail.IdLokasi = @idLokasi' : ''}
      ${search ? `AND ${fieldSak !== 'NULL' ? fieldSak : '1'} IS NOT NULL AND 1=1` : ''}
    `;

    const makeTotalBerat = (table, fieldBerat, joinClause) => `
      SELECT ROUND(SUM(${fieldBerat}), 2) AS total FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${idLokasi && idLokasi !== 'all' ? 'AND detail.IdLokasi = @idLokasi' : ''}
      ${search ? `AND ${fieldBerat !== 'NULL' ? fieldBerat : '1'} IS NOT NULL AND 1=1` : ''}
    `;

    let query = '', totalQuery = '', totalBeratQuery = '', totalSakQuery = '';

    if (filterBy !== 'all') {
      const filter = filterMap[filterBy.toLowerCase()];
      if (!filter) throw new Error('Invalid filterBy');

      query = `
        ${makeQuery(filter.table, filter.labelExpr, filter.label, filter.joinClause, filter.fields)}
        ORDER BY so.DateTimeScan DESC
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;
      totalQuery = makeCount(filter.table, filter.labelExpr, filter.joinClause);
      totalBeratQuery = makeTotalBerat(filter.table, filter.fields.berat, filter.joinClause);
      totalSakQuery = filter.fields.jmlhSak !== 'NULL'
        ? makeTotalSak(filter.table, filter.fields.jmlhSak, filter.joinClause)
        : 'SELECT NULL AS total;';
    } else {
      const all = Object.values(filterMap);
      const allQueries = all.map(f => makeQuery(f.table, f.labelExpr, f.label, f.joinClause, f.fields));
      const allCounts = all.map(f => makeCount(f.table, f.labelExpr, f.joinClause));
      const allSak = all.map(f => f.fields.jmlhSak !== 'NULL' ? makeTotalSak(f.table, f.fields.jmlhSak, f.joinClause) : 'SELECT 0 AS total');
      const allBerat = all.map(f => makeTotalBerat(f.table, f.fields.berat, f.joinClause));

      query = `
        SELECT * FROM (
          ${allQueries.join(' UNION ALL ')}
        ) AS hasil
        ORDER BY DateTimeScan DESC
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;
      totalQuery = `
        SELECT SUM(total) AS total FROM (
          ${allCounts.join(' UNION ALL ')}
        ) AS totalData;
      `;
      totalSakQuery = `
        SELECT ROUND(SUM(total), 2) AS total FROM (
          ${allSak.join(' UNION ALL ')}
        ) AS sakData;
      `;
      totalBeratQuery = `
        SELECT ROUND(SUM(total), 2) AS total FROM (
          ${allBerat.join(' UNION ALL ')}
        ) AS beratData;
      `;
    }

    const [result, total, berat, sak] = await Promise.all([
      request.query(query),
      request.query(totalQuery),
      request.query(totalBeratQuery),
      request.query(totalSakQuery)
    ]);

    const formattedData = result.recordset.map(item => ({
      ...item,
      DateTimeScan:
        item.DateTimeScan && item.DateTimeScan !== '1900-01-01'
          ? formatDate(item.DateTimeScan)
          : '-',
      Username: item.Username || '-'
    }));

    return {
      data: formattedData,
      hasData: formattedData.length > 0,
      currentPage: page,
      pageSize,
      totalData: total.recordset[0].total,
      totalBerat: berat.recordset[0].total ?? 0,
      totalSak: sak.recordset[0].total ?? 0,
      totalPages: Math.ceil(total.recordset[0].total / pageSize)
    };
  } catch (err) {
    throw new Error(`Stock Opname Hasil Service Error: ${err.message}`);
  } finally {
    if (pool) await pool.close();
  }
}


async function deleteStockOpnameHasil({ noso, nomorLabel }) {
  if (!nomorLabel) {
    throw new Error('NomorLabel wajib diisi');
  }

  let pool;
  try {
    pool = await connectDb();
    const request = new sql.Request(pool);
    request.input('noso', sql.VarChar, noso);

    let deleteQuery = '';
    let labelTypeDetected = '';

    // === BAHAN BAKU ===
    const [noBahanBaku, noPallet] = nomorLabel.split('-');
    if (noBahanBaku && noPallet) {
      request.input('noBahanBaku', sql.VarChar, noBahanBaku);
      request.input('noPallet', sql.VarChar, noPallet);

      const checkBBK = await request.query(`
          SELECT 1 FROM StockOpnameHasilBahanBaku 
          WHERE NoSO = @noso AND NoBahanBaku = @noBahanBaku AND NoPallet = @noPallet
        `);

      if (checkBBK.recordset.length > 0) {
        deleteQuery = `
            DELETE FROM StockOpnameHasilBahanBaku 
            WHERE NoSO = @noso AND NoBahanBaku = @noBahanBaku AND NoPallet = @noPallet
          `;
        labelTypeDetected = 'bahanbaku';
      }
    }

    // === LABEL BIASA (tanpa dash) ===
    const tryDeleteLabel = async (table, column, typeName, inputName) => {
      if (deleteQuery) return; // skip jika sudah ketemu

      request.input(inputName, sql.VarChar, nomorLabel);
      const check = await request.query(`
          SELECT 1 FROM ${table} 
          WHERE NoSO = @noso AND ${column} = @${inputName}
        `);

      if (check.recordset.length > 0) {
        deleteQuery = `
            DELETE FROM ${table}
            WHERE NoSO = @noso AND ${column} = @${inputName}
          `;
        labelTypeDetected = typeName;
      }
    };

    await tryDeleteLabel('StockOpnameHasilWashing', 'NoWashing', 'washing', 'noWashing');
    await tryDeleteLabel('StockOpnameHasilBroker', 'NoBroker', 'broker', 'noBroker');
    await tryDeleteLabel('StockOpnameHasilCrusher', 'NoCrusher', 'crusher', 'noCrusher');
    await tryDeleteLabel('StockOpnameHasilBonggolan', 'NoBonggolan', 'bonggolan', 'noBonggolan');
    await tryDeleteLabel('StockOpnameHasilGilingan', 'NoGilingan', 'gilingan', 'noGilingan');

    if (!deleteQuery) {
      return { success: false, message: 'NomorLabel tidak ditemukan dalam data stock opname' };
    }

    // Eksekusi query DELETE
    await request.query(deleteQuery);

    return { success: true, message: `Label ${nomorLabel} berhasil dihapus dari tipe '${labelTypeDetected}'` };
  } finally {
    if (pool) await pool.close();
  }
}


async function validateStockOpnameLabel({ noso, label, username }) {
  // Helper function untuk membuat response format yang konsisten
  const createResponse = (success, data = {}, message = '') => {
    return {
      success,
      message,
      label: label || '',
      labelType: data.labelType || '',
      parsed: data.parsed || {},
      noso: noso || '',
      username: username || '',
      isValidFormat: data.isValidFormat || false,
      isValidCategory: data.isValidCategory || false,
      isValidWarehouse: data.isValidWarehouse || false,
      isDuplicate: data.isDuplicate || false,
      foundInStockOpname: data.foundInStockOpname || false,
      canInsert: data.canInsert || false,
      idWarehouse: data.idWarehouse || null,
      // Detail fields - flatten seperti di route
      jmlhSak: data.detail?.JmlhSak || null,
      berat: data.detail?.Berat || null,
      idLokasi: data.detail?.IdLokasi || null
    };
  };

  // Validasi input dasar
  if (!label) {
    return createResponse(false, {}, 'Label wajib diisi');
  }

  // 1. VALIDASI FORMAT LABEL
  const isBahanBaku = label.startsWith('A.') && label.includes('-');
  const isWashing = label.startsWith('B.') && !label.includes('-');
  const isBroker = label.startsWith('D.') && !label.includes('-');
  const isCrusher = label.startsWith('F.') && !label.includes('-');
  const isBonggolan = label.startsWith('M.') && !label.includes('-');
  const isGilingan = label.startsWith('V.') && !label.includes('-');
  const isMixer = label.startsWith('H.') && !label.includes('-');
  const isFurnitureWIP = label.startsWith('BB.') && !label.includes('-');
  const isBarangJadi = label.startsWith('BA.') && !label.includes('-');

  if (!isBahanBaku && !isWashing && !isBroker && !isCrusher && !isBonggolan && !isGilingan && !isMixer && !isFurnitureWIP && !isBarangJadi) {
    return createResponse(false, {
      isValidFormat: false
    }, 'Kode label tidak dikenali. Hanya A., B., F., M., V., H., BB., BA., atau D. yang valid.');
  }

  let pool;
  try {
    pool = await connectDb();
    const request = new sql.Request(pool);
    request.input('noso', sql.VarChar, noso);
    request.input('username', sql.VarChar, username);

    let checkQuery = '', detailQuery = '', parsed = {}, labelType = '';
    let idWarehouse = null;
    let fallbackQuery = '';
    let originalDataQuery = '';
    let warehouseQuery = '';

    // 2. SETUP QUERIES BERDASARKAN TIPE LABEL
    if (isBahanBaku) {
      labelType = 'Bahan Baku';
      const [noBahanBaku, noPallet] = label.split('-');
      if (!noBahanBaku || !noPallet) {
        return createResponse(false, {
          isValidFormat: false,
          labelType
        }, 'Format label bahan baku tidak valid. Contoh: A.0001-1');
      }

      parsed = { NoBahanBaku: noBahanBaku, NoPallet: noPallet };
      request.input('NoBahanBaku', sql.VarChar, noBahanBaku);
      request.input('NoPallet', sql.VarChar, noPallet);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilBahanBaku
          WHERE NoSO = @noso AND NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet AND Username = @username
        `;
      detailQuery = `
          SELECT JmlhSak, Berat, IdLokasi
          FROM StockOpnameBahanBaku
          WHERE NoSO = @noso AND NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM BahanBakuPallet_h
          WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
        `;
      fallbackQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM BahanBaku_d bb
          JOIN BahanBakuPallet_h bbh ON bb.NoBahanBaku = bbh.NoBahanBaku AND bb.NoPallet = bbh.NoPallet
          WHERE bb.NoBahanBaku = @NoBahanBaku AND bb.NoPallet = @NoPallet 
          AND (bb.DateUsage IS NULL OR bbh.IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM BahanBaku_d bb
          JOIN BahanBakuPallet_h bbh ON bb.NoBahanBaku = bbh.NoBahanBaku AND bb.NoPallet = bbh.NoPallet
          WHERE bb.NoBahanBaku = @NoBahanBaku AND bb.NoPallet = @NoPallet
        `;

    } else if (isWashing) {
      labelType = 'Washing';
      parsed = { NoWashing: label };
      request.input('NoWashing', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilWashing
          WHERE NoSO = @noso AND NoWashing = @NoWashing AND Username = @username
        `;
      detailQuery = `
          SELECT JmlhSak, Berat, IdLokasi
          FROM StockOpnameWashing
          WHERE NoSO = @noso AND NoWashing = @NoWashing
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM Washing_h
          WHERE NoWashing = @NoWashing
        `;
      fallbackQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Washing_d wd
          JOIN Washing_h wh ON wd.NoWashing = wh.NoWashing
          WHERE wd.NoWashing = @NoWashing 
          AND (wd.DateUsage IS NULL OR wh.IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Washing_d wd
          JOIN Washing_h wh ON wd.NoWashing = wh.NoWashing
          WHERE wd.NoWashing = @NoWashing
        `;

    } else if (isBroker) {
      labelType = 'Broker';
      parsed = { NoBroker: label };
      request.input('NoBroker', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilBroker
          WHERE NoSO = @noso AND NoBroker = @NoBroker AND Username = @username
        `;
      detailQuery = `
          SELECT JmlhSak, Berat, IdLokasi
          FROM StockOpnameBroker
          WHERE NoSO = @noso AND NoBroker = @NoBroker
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM Broker_h
          WHERE NoBroker = @NoBroker
        `;
      fallbackQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Broker_d bd
          JOIN Broker_h bh ON bd.NoBroker = bh.NoBroker
          WHERE bd.NoBroker = @NoBroker 
          AND (bd.DateUsage IS NULL OR bh.IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Broker_d bd
          JOIN Broker_h bh ON bd.NoBroker = bh.NoBroker
          WHERE bd.NoBroker = @NoBroker
        `;

    } else if (isCrusher) {
      labelType = 'Crusher';
      parsed = { NoCrusher: label };
      request.input('NoCrusher', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilCrusher
          WHERE NoSO = @noso AND NoCrusher = @NoCrusher AND Username = @username
        `;
      detailQuery = `
          SELECT Berat, IdLokasi
          FROM StockOpnameCrusher
          WHERE NoSO = @noso AND NoCrusher = @NoCrusher
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM Crusher
          WHERE NoCrusher = @NoCrusher
        `;
      fallbackQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Crusher
          WHERE NoCrusher = @NoCrusher 
          AND (DateUsage IS NULL OR IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Crusher
          WHERE NoCrusher = @NoCrusher
        `;

    } else if (isBonggolan) {
      labelType = 'Bonggolan';
      parsed = { NoBonggolan: label };
      request.input('NoBonggolan', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilBonggolan
          WHERE NoSO = @noso AND NoBonggolan = @NoBonggolan AND Username = @username
        `;
      detailQuery = `
          SELECT Berat, IdLokasi
          FROM StockOpnameBonggolan
          WHERE NoSO = @noso AND NoBonggolan = @NoBonggolan
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM Bonggolan
          WHERE NoBonggolan = @NoBonggolan
        `;
      fallbackQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Bonggolan
          WHERE NoBonggolan = @NoBonggolan 
          AND (DateUsage IS NULL OR IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Bonggolan
          WHERE NoBonggolan = @NoBonggolan
        `;

    } else if (isGilingan) {
      labelType = 'Gilingan';
      parsed = { NoGilingan: label };
      request.input('NoGilingan', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilGilingan
          WHERE NoSO = @noso AND NoGilingan = @NoGilingan AND Username = @username
        `;
      detailQuery = `
          SELECT Berat, IdLokasi
          FROM StockOpnameGilingan
          WHERE NoSO = @noso AND NoGilingan = @NoGilingan
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM Gilingan
          WHERE NoGilingan = @NoGilingan
        `;
      fallbackQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Gilingan
          WHERE NoGilingan = @NoGilingan 
          AND (DateUsage IS NULL OR IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Gilingan
          WHERE NoGilingan = @NoGilingan
        `;

    } else if (isMixer) {
      labelType = 'Mixer';
      parsed = { NoMixer: label };
      request.input('NoMixer', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilMixer
          WHERE NoSO = @noso AND NoMixer = @NoMixer AND Username = @username
        `;
      detailQuery = `
          SELECT JmlhSak, Berat, IdLokasi
          FROM StockOpnameMixer
          WHERE NoSO = @noso AND NoMixer = @NoMixer
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM Mixer_h
          WHERE NoMixer = @NoMixer
        `;
      fallbackQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Mixer_d md
          JOIN Mixer_h mh ON md.NoMixer = mh.NoMixer
          WHERE md.NoMixer = @NoMixer 
          AND (md.DateUsage IS NULL OR mh.IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM Mixer_d md
          JOIN Mixer_h mh ON md.NoMixer = mh.NoMixer
          WHERE md.NoMixer = @NoMixer
        `;

    } else if (isFurnitureWIP) {
      labelType = 'Furniture WIP';
      parsed = { NoFurnitureWIP: label };
      request.input('NoFurnitureWIP', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilFurnitureWIP
          WHERE NoSO = @noso AND NoFurnitureWIP = @NoFurnitureWIP AND Username = @username
        `;
      detailQuery = `
          SELECT Pcs AS JmlhSak, Berat, IdLokasi
          FROM StockOpnameFurnitureWIP
          WHERE NoSO = @noso AND NoFurnitureWIP = @NoFurnitureWIP
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM FurnitureWIP
          WHERE NoFurnitureWIP = @NoFurnitureWIP
        `;
      fallbackQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM FurnitureWIP
          WHERE NoFurnitureWIP = @NoFurnitureWIP 
          AND (DateUsage IS NULL OR IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM FurnitureWIP
          WHERE NoFurnitureWIP = @NoFurnitureWIP
        `;

    } else if (isBarangJadi) {
      labelType = 'Barang Jadi';
      parsed = { NoBJ: label };
      request.input('NoBJ', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilBarangJadi
          WHERE NoSO = @noso AND NoBJ = @NoBJ AND Username = @username
        `;
      detailQuery = `
          SELECT Pcs AS JmlhSak, Berat, IdLokasi
          FROM StockOpnameBarangJadi
          WHERE NoSO = @noso AND NoBJ = @NoBJ
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM BarangJadi
          WHERE NoBJ = @NoBJ
        `;
      fallbackQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM BarangJadi
          WHERE NoBJ = @NoBJ 
          AND (DateUsage IS NULL OR IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM BarangJadi
          WHERE NoBJ = @NoBJ
        `;
    }

    // 3. CEK DUPLIKASI PERTAMA KALI (EARLY DUPLICATE CHECK)
    const checkResult = await request.query(checkQuery);
    const isDuplicate = checkResult.recordset[0].count > 0;

    // Jika duplikat, langsung return tanpa validasi lainnya
    if (isDuplicate) {
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse: true,
        isDuplicate: true,
        foundInStockOpname: true,
        canInsert: false,
        labelType,
        parsed,
        idWarehouse: null // Akan diisi nanti jika diperlukan
      }, 'Label sudah pernah discan sebelumnya.');
    }

    // 4. VALIDASI KUALIFIKASI NOSO
    const nosoQualificationCheck = await request.query(`
        SELECT IsBahanBaku, IsWashing, IsBroker, IsBonggolan, IsCrusher, IsGilingan, IsMixer, IsFurnitureWIP, IsBarangJadi
        FROM StockOpname_h
        WHERE NoSO = @noso
      `);

    if (nosoQualificationCheck.recordset.length === 0) {
      return createResponse(false, {
        isValidFormat: true,
        isDuplicate: false,
        labelType,
        parsed
      }, 'NoSO tidak ditemukan dalam sistem.');
    }

    const qualifications = nosoQualificationCheck.recordset[0];

    // Validasi kategori
    let isValidCategory = true;
    let categoryMessage = '';

    if (isBahanBaku && !qualifications.IsBahanBaku) {
      isValidCategory = false;
      categoryMessage = 'Kategori Bahan Baku tidak sesuai dengan NoSO ini.';
    } else if (isWashing && !qualifications.IsWashing) {
      isValidCategory = false;
      categoryMessage = 'Kategori Washing tidak sesuai dengan NoSO ini.';
    } else if (isBroker && !qualifications.IsBroker) {
      isValidCategory = false;
      categoryMessage = 'Kategori Broker tidak sesuai dengan NoSO ini.';
    } else if (isCrusher && !qualifications.IsCrusher) {
      isValidCategory = false;
      categoryMessage = 'Kategori Crusher tidak sesuai dengan NoSO ini.';
    } else if (isBonggolan && !qualifications.IsBonggolan) {
      isValidCategory = false;
      categoryMessage = 'Kategori Bonggolan tidak sesuai dengan NoSO ini.';
    } else if (isGilingan && !qualifications.IsGilingan) {
      isValidCategory = false;
      categoryMessage = 'Kategori Gilingan tidak sesuai dengan NoSO ini.';
    } else if (isMixer && !qualifications.IsMixer) {
      isValidCategory = false;
      categoryMessage = 'Kategori Mixer tidak sesuai dengan NoSO ini.';
    } else if (isFurnitureWIP && !qualifications.IsFurnitureWIP) {
      isValidCategory = false;
      categoryMessage = 'Kategori Furniture WIP tidak sesuai dengan NoSO ini.';
    } else if (isBarangJadi && !qualifications.IsBarangJadi) {
      isValidCategory = false;
      categoryMessage = 'Kategori Barang Jadi tidak sesuai dengan NoSO ini.';
    }

    // 5. AMBIL ID WAREHOUSE
    const whResult = await request.query(warehouseQuery);
    idWarehouse = whResult.recordset[0]?.IdWarehouse ?? null;

    // Jika kategori tidak valid, cek data asli untuk detail
    if (!isValidCategory) {
      const originalDataResult = await request.query(originalDataQuery);
      const originalData = originalDataResult.recordset[0];
      
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: false,
        isValidWarehouse: false,
        isDuplicate: false,
        foundInStockOpname: false,
        canInsert: false,
        labelType,
        parsed,
        idWarehouse,
        detail: originalData ? {
          JmlhSak: originalData.JumlahSak || null,
          Berat: originalData?.Berat != null ? Number(originalData.Berat.toFixed(2)) : null,
          IdLokasi: originalData.IdLokasi
        } : null
      }, categoryMessage);
    }

    // 6. CEK WAREHOUSE
    if (!idWarehouse) {
      const originalDataResult = await request.query(originalDataQuery);
      const originalData = originalDataResult.recordset[0];
      
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse: false,
        isDuplicate: false,
        foundInStockOpname: false,
        canInsert: false,
        labelType,
        parsed,
        idWarehouse,
        detail: originalData ? {
          JmlhSak: originalData.JumlahSak || null,
          Berat: originalData?.Berat != null ? Number(originalData.Berat.toFixed(2)) : null,
          IdLokasi: originalData.IdLokasi
        } : null
      }, 'Label tidak valid atau warehouse tidak ditemukan di sumber.');
    }

    // 7. VALIDASI WAREHOUSE TERHADAP NoSO
    const soWarehouseCheck = await request.query(`
        SELECT COUNT(*) AS count
        FROM StockOpname_h_WarehouseID
        WHERE NoSO = @noso AND IdWarehouse = ${idWarehouse}
      `);
    const isValidWarehouse = soWarehouseCheck.recordset[0].count > 0;

    // 8. CEK DI STOCKOPNAME
    const detailResult = await request.query(detailQuery);
    const detailData = detailResult.recordset[0];

    // DITEMUKAN DALAM STOCK OPNAME
    if (detailData) {
      if (!isValidWarehouse) {
        return createResponse(false, {
          isValidFormat: true,
          isValidCategory: true,
          isValidWarehouse: false,
          isDuplicate: false,
          foundInStockOpname: true,
          canInsert: false,
          labelType,
          parsed,
          idWarehouse,
          detail: {
            ...detailData,
            Berat: detailData?.Berat != null ? Number(detailData.Berat.toFixed(2)) : null
          }
        }, `Label ini tidak tersedia pada warehouse NoSO ini (IdWarehouse: ${idWarehouse}).`);
      }

      return createResponse(true, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse: true,
        isDuplicate: false,
        foundInStockOpname: true,
        canInsert: true,
        labelType,
        parsed,
        idWarehouse,
        detail: {
          ...detailData,
          Berat: detailData?.Berat != null ? Number(detailData.Berat.toFixed(2)) : null
        }
      }, 'Label valid dan siap disimpan.');
    }

    // 9. TIDAK DITEMUKAN DI STOCKOPNAME → CEK FALLBACK DATA
    const fallbackResult = await request.query(fallbackQuery);
    const fallbackData = fallbackResult.recordset[0];

    if (fallbackData && (fallbackData.JumlahSak > 0 || fallbackData.Berat > 0)) {
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse,
        isDuplicate: false,
        foundInStockOpname: false,
        canInsert: false,
        labelType,
        parsed,
        idWarehouse: fallbackData.IdWarehouse || idWarehouse,
        detail: {
          JmlhSak: fallbackData.JumlahSak || null,
          Berat: fallbackData?.Berat != null ? Number(fallbackData.Berat.toFixed(2)) : null,
          IdLokasi: fallbackData.IdLokasi
        }
      }, 'Item tidak masuk dalam daftar Stock Opname atau belum diproses.');
    }

    // 10. TIDAK DITEMUKAN SAMA SEKALI ATAU SEMUA SUDAH DIPROSES
    const originalDataResult = await request.query(originalDataQuery);
    const originalData = originalDataResult.recordset[0];

    if (originalData && (originalData.JumlahSak > 0 || originalData.Berat > 0)) {
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse,
        isDuplicate: false,
        foundInStockOpname: false,
        canInsert: false,
        labelType,
        parsed,
        idWarehouse: originalData.IdWarehouse || idWarehouse,
        detail: {
          JmlhSak: originalData.JumlahSak || null,
          Berat: originalData?.Berat != null ? Number(originalData.Berat.toFixed(2)) : null,
          IdLokasi: originalData.IdLokasi
        }
      }, 'Item telah diproses sebelumnya.');
    }

    return createResponse(false, {
      isValidFormat: true,
      isValidCategory: true,
      isValidWarehouse,
      isDuplicate: false,
      foundInStockOpname: false,
      canInsert: false,
      labelType,
      parsed,
      idWarehouse
    }, 'Item tidak ditemukan dalam sistem.');

  } finally {
    if (pool) await pool.close();
  }
}


async function insertStockOpnameLabel({ noso, label, jmlhSak = 0, berat = 0, idlokasi, username }) {
  if (!label) {
    throw new Error('Label wajib diisi');
  }

  let pool;
  try {
    pool = await connectDb();
    const request = new sql.Request(pool);

    request.input('noso', sql.VarChar, noso);
    request.input('username', sql.VarChar, username);
    request.input('jmlhSak', sql.Int, jmlhSak);
    request.input('berat', sql.Float, berat);
    request.input('DateTimeScan', sql.DateTime, new Date());
    request.input('idlokasi', sql.VarChar, idlokasi);

    const isBahanBaku = label.startsWith('A.') && label.includes('-');
    const isWashing = label.startsWith('B.') && !label.includes('-');
    const isBroker = label.startsWith('D.') && !label.includes('-');
    const isCrusher = label.startsWith('F.') && !label.includes('-');
    const isBonggolan = label.startsWith('M.') && !label.includes('-');
    const isGilingan = label.startsWith('V.') && !label.includes('-');
    const isMixer = label.startsWith('H.') && !label.includes('-');
    const isFurnitureWIP = label.startsWith('BB.') && !label.includes('-');
    const isBarangJadi = label.startsWith('BA.') && !label.includes('-');



    let insertedData = null;

    if (isBahanBaku) {
      const [noBahanBaku, noPallet] = label.split('-');
      if (!noBahanBaku || !noPallet) {
        throw new Error('Format label bahan baku tidak valid. Contoh: A.0001-1');
      }

      request.input('NoBahanBaku', sql.VarChar, noBahanBaku);
      request.input('NoPallet', sql.VarChar, noPallet);

      await request.query(`
          INSERT INTO StockOpnameHasilBahanBaku
          (NoSO, NoBahanBaku, NoPallet, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoBahanBaku, @NoPallet, @jmlhSak, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE BahanBaku_d
          SET IdLokasi = @idlokasi
          WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Bahan Baku', labelTypeCode: 'bahanbaku',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isWashing) {
      request.input('NoWashing', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilWashing
          (NoSO, NoWashing, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoWashing, @jmlhSak, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE Washing_d
          SET IdLokasi = @idlokasi
          WHERE NoWashing = @NoWashing
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Washing', labelTypeCode: 'washing',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isBroker) {
      request.input('NoBroker', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilBroker
          (NoSO, NoBroker, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoBroker, @jmlhSak, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE Broker_d
          SET IdLokasi = @idlokasi
          WHERE NoBroker = @NoBroker
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Broker', labelTypeCode: 'broker',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isCrusher) {
      request.input('NoCrusher', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilCrusher
          (NoSO, NoCrusher, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoCrusher, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE Crusher
          SET IdLokasi = @idlokasi
          WHERE NoCrusher = @NoCrusher
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Crusher', labelTypeCode: 'crusher',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isBonggolan) {
      request.input('NoBonggolan', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilBonggolan
          (NoSO, NoBonggolan, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoBonggolan, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE Bonggolan
          SET IdLokasi = @idlokasi
          WHERE NoBonggolan = @NoBonggolan
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Bonggolan', labelTypeCode: 'bonggolan',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isGilingan) {
      request.input('NoGilingan', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilGilingan
          (NoSO, NoGilingan, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoGilingan, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE Gilingan
          SET IdLokasi = @idlokasi
          WHERE NoGilingan = @NoGilingan
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Gilingan', labelTypeCode: 'gilingan',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isMixer) {
      request.input('NoMixer', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilMixer
          (NoSO, NoMixer, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoMixer, @jmlhSak, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE Mixer_d
          SET IdLokasi = @idlokasi
          WHERE NoMixer = @NoMixer
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Mixer', labelTypeCode: 'mixer',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isFurnitureWIP) {
      request.input('NoFurnitureWIP', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilFurnitureWIP
          (NoSO, NoFurnitureWIP, Pcs, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoFurnitureWIP, @jmlhSak, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE FurnitureWIP
          SET IdLokasi = @idlokasi
          WHERE NoFurnitureWIP = @NoFurnitureWIP
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Furniture WIP', labelTypeCode: 'furniturewip',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    } else if (isBarangJadi) {
      request.input('NoBJ', sql.VarChar, label);

      await request.query(`
          INSERT INTO StockOpnameHasilBarangJadi
          (NoSO, NoBJ, Pcs, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoBJ, @jmlhSak, @berat, @username, @DateTimeScan)
        `);

      await request.query(`
          UPDATE BarangJadi
          SET IdLokasi = @idlokasi
          WHERE NoBJ = @NoBJ
        `);

      insertedData = {
        noso, nomorLabel: label, labelType: 'Barang Jadi', labelTypeCode: 'barangjadi',
        jmlhSak, berat, idlokasi, username, timestamp: new Date()
      };

    }

    else {
      throw new Error('Kode label tidak dikenali dalam sistem. Hanya label dengan awalan A., B., F., M., V., H., BB., BA., atau D. yang valid.');
    }

    // Emit ke socket jika global.io tersedia
    if (global.io) {
      global.io.emit('label_inserted', insertedData);
    }

    return { success: true, message: 'Label berhasil disimpan dan lokasi diperbarui' };

  } finally {
    if (pool) await pool.close();
  }
}

module.exports = {
  getNoStockOpname,
  getStockOpnameAcuan,
  getStockOpnameHasil,
  deleteStockOpnameHasil,
  validateStockOpnameLabel,
  insertStockOpnameLabel
};
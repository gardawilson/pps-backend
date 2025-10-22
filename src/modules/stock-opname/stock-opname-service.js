const { sql, poolPromise } = require('../../core/config/db');
const { formatDate } = require('../../core/utils/date-helper');


async function getNoStockOpname() {
  try {
    const pool = await poolPromise; // ✅ ambil pool global
    const result = await pool.request().query(`
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
        soh.IsBarangJadi,
        soh.IsReject,
        soh.IsAscend
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
        soh.IsBarangJadi,
        soh.IsReject,
        soh.IsAscend
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
      IsBarangJadi,
      IsReject,
      IsAscend
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
      IsBarangJadi,
      IsReject,
      IsAscend
    }));
  } catch (err) {
    throw new Error(`Stock Opname Service Error: ${err.message}`);
  }
}



async function getStockOpnameAcuan({
  noso,
  page = 1,
  pageSize = 20,
  filterBy = 'all',
  blok,                // ✅ varchar
  idLokasi,            // ✅ int
  search = ''
}) {
  const offset = (page - 1) * pageSize;

  const filterMap = {
    bahanbaku: {
      table: 'StockOpnameBahanBaku',
      labelExpr: "CONCAT(NoBahanBaku, '-', NoPallet)",
      label: 'Bahan Baku',
      hasilTable: 'StockOpnameHasilBahanBaku',
      hasilWhereClause: "CONCAT(hasil.NoBahanBaku, '-', hasil.NoPallet) = CONCAT(src.NoBahanBaku, '-', src.NoPallet)",
      fields: { jmlhSak: 'JmlhSak', berat: 'ROUND(Berat, 2)' }
    },
    washing: {
      table: 'StockOpnameWashing',
      labelExpr: 'NoWashing',
      label: 'Washing',
      hasilTable: 'StockOpnameHasilWashing',
      hasilWhereClause: 'hasil.NoWashing = src.NoWashing',
      fields: { jmlhSak: 'JmlhSak', berat: 'ROUND(Berat, 2)' }
    },
    broker: {
      table: 'StockOpnameBroker',
      labelExpr: 'NoBroker',
      label: 'Broker',
      hasilTable: 'StockOpnameHasilBroker',
      hasilWhereClause: 'hasil.NoBroker = src.NoBroker',
      fields: { jmlhSak: 'JmlhSak', berat: 'ROUND(Berat, 2)' }
    },
    crusher: {
      table: 'StockOpnameCrusher',
      labelExpr: 'NoCrusher',
      label: 'Crusher',
      hasilTable: 'StockOpnameHasilCrusher',
      hasilWhereClause: 'hasil.NoCrusher = src.NoCrusher',
      fields: { jmlhSak: 'NULL', berat: 'ROUND(Berat, 2)' }
    },
    bonggolan: {
      table: 'StockOpnameBonggolan',
      labelExpr: 'NoBonggolan',
      label: 'Bonggolan',
      hasilTable: 'StockOpnameHasilBonggolan',
      hasilWhereClause: 'hasil.NoBonggolan = src.NoBonggolan',
      fields: { jmlhSak: 'NULL', berat: 'ROUND(Berat, 2)' }
    },
    gilingan: {
      table: 'StockOpnameGilingan',
      labelExpr: 'NoGilingan',
      label: 'Gilingan',
      hasilTable: 'StockOpnameHasilGilingan',
      hasilWhereClause: 'hasil.NoGilingan = src.NoGilingan',
      fields: { jmlhSak: 'NULL', berat: 'ROUND(Berat, 2)' }
    },
    mixer: {
      table: 'StockOpnameMixer',
      labelExpr: 'NoMixer',
      label: 'Mixer',
      hasilTable: 'StockOpnameHasilMixer',
      hasilWhereClause: 'hasil.NoMixer = src.NoMixer',
      fields: { jmlhSak: 'JmlhSak', berat: 'ROUND(Berat, 2)' }
    },
    furniturewip: {
      table: 'StockOpnameFurnitureWIP',
      labelExpr: 'NoFurnitureWIP',
      label: 'Furniture WIP',
      hasilTable: 'StockOpnameHasilFurnitureWIP',
      hasilWhereClause: 'hasil.NoFurnitureWIP = src.NoFurnitureWIP',
      fields: { jmlhSak: 'Pcs', berat: 'Berat' }
    },
    barangjadi: {
      table: 'StockOpnameBarangJadi',
      labelExpr: 'NoBJ',
      label: 'Barang Jadi',
      hasilTable: 'StockOpnameHasilBarangJadi',
      hasilWhereClause: 'hasil.NoBJ = src.NoBJ',
      fields: { jmlhSak: 'Pcs', berat: 'Berat' }
    },
    reject: {
      table: 'StockOpnameReject',
      labelExpr: 'NoReject',
      label: 'Reject',
      hasilTable: 'StockOpnameHasilReject',
      hasilWhereClause: 'hasil.NoReject = src.NoReject',
      fields: { jmlhSak: 'NULL', berat: 'Berat' }
    }
  };

  try {
    const pool = await poolPromise;
    const request = pool.request();

    // --- input dasar ---
    request.input('noso', sql.VarChar, noso);
    if (blok && blok !== 'all') request.input('blok', sql.VarChar, blok);
    if (idLokasi && idLokasi !== 'all') request.input('idLokasi', sql.Int, parseInt(idLokasi));
    if (search) request.input('search', sql.VarChar, `%${search}%`);

    // === helper untuk filter blok & lokasi ===
    const makeWhereLokasi = () => {
      if (blok && blok !== 'all' && idLokasi && idLokasi !== 'all') {
        return 'AND Blok = @blok AND IdLokasi = @idLokasi';
      } else if (blok && blok !== 'all') {
        return 'AND Blok = @blok';
      } else if (idLokasi && idLokasi !== 'all') {
        return 'AND IdLokasi = @idLokasi';
      }
      return '';
    };

    // === builder ===
    const makeQuery = (table, labelExpr, labelType, hasilTable, hasilWhereClause, fields = {}) => `
      SELECT 
        ${labelExpr} AS NomorLabel, 
        '${labelType}' AS LabelType,
        ${fields.jmlhSak || 'NULL'} AS JmlhSak,
        ${fields.berat || 'NULL'} AS Berat,
        Blok,
        IdLokasi
      FROM ${table} AS src
      WHERE NoSO = @noso
        ${makeWhereLokasi()}
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
        ${makeWhereLokasi()}
        ${search ? `AND ${labelExpr} LIKE @search` : ''}
        AND NOT EXISTS (
          SELECT 1 FROM ${hasilTable} AS hasil
          WHERE hasil.NoSO = src.NoSO AND ${hasilWhereClause}
        )
    `;

    // === total global builder ===
    const overallTotalQuery = (() => {
      if (filterBy !== 'all') {
        const f = filterMap[filterBy.toLowerCase()];
        return `
          SELECT
            COUNT(*) AS TotalLabelGlobal,
            ROUND(SUM(CAST(${f.fields.berat || '0'} AS FLOAT)), 2) AS TotalBeratGlobal,
            SUM(CAST(${f.fields.jmlhSak || '0'} AS INT)) AS TotalSakGlobal
          FROM ${f.table} AS src
          WHERE NoSO = @noso
            ${makeWhereLokasi()}
            ${search ? `AND ${f.labelExpr} LIKE @search` : ''}
        `;
      } else {
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
                ${makeWhereLokasi()}
                ${search ? `AND ${f.labelExpr} LIKE @search` : ''}
            `).join(' UNION ALL ')}
          ) AS beratSum
        `;
      }
    })();

    let query = '', totalQuery = '', beratSakQuery = '';

    if (filterBy !== 'all') {
      const f = filterMap[filterBy.toLowerCase()];
      if (!f) throw new Error('Invalid filterBy');

      query = `
        ${makeQuery(f.table, f.labelExpr, f.label, f.hasilTable, f.hasilWhereClause, f.fields)}
        ORDER BY NomorLabel
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;

      totalQuery = makeCount(f.table, f.labelExpr, f.hasilTable, f.hasilWhereClause);

      beratSakQuery = `
        SELECT
          ROUND(SUM(CAST(${f.fields.berat || '0'} AS FLOAT)), 2) AS TotalBerat,
          SUM(CAST(${f.fields.jmlhSak || '0'} AS INT)) AS TotalSak
        FROM ${f.table} AS src
        WHERE NoSO = @noso
          ${makeWhereLokasi()}
          ${search ? `AND ${f.labelExpr} LIKE @search` : ''}
          AND NOT EXISTS (
            SELECT 1 FROM ${f.hasilTable} AS hasil
            WHERE hasil.NoSO = src.NoSO AND ${f.hasilWhereClause}
          )
      `;
    } else {
      const all = Object.values(filterMap);
      const allQueries = all.map(f => makeQuery(f.table, f.labelExpr, f.label, f.hasilTable, f.hasilWhereClause, f.fields));
      const allCounts = all.map(f => makeCount(f.table, f.labelExpr, f.hasilTable, f.hasilWhereClause));

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
          ${all.map(f => `
            SELECT
              ${f.fields.berat || '0'} AS Berat,
              ${f.fields.jmlhSak || '0'} AS JmlhSak
            FROM ${f.table} AS src
            WHERE NoSO = @noso
              ${makeWhereLokasi()}
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
  }
}




// ✅ getStockOpnameHasil.js (versi final, blok dan idLokasi dipisah)

async function getStockOpnameHasil({
  noso,
  page = 1,
  pageSize = 20,
  filterBy = 'all',
  blok,                // varchar
  idLokasi,            // int
  search = '',
  filterByUser = false,
  username = ''
}) {
  const offset = (page - 1) * pageSize;

  const filterMap = {
    bahanbaku: {
      table: 'StockOpnameHasilBahanBaku',
      labelExpr: "CONCAT(so.NoBahanBaku, '-', so.NoPallet)",
      label: 'Bahan Baku',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoBahanBaku,
            NoPallet,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoBahanBaku, NoPallet ORDER BY IdLokasi DESC) AS rn
          FROM BahanBakuPallet_h
          WHERE IdLokasi IS NOT NULL
        ) detail 
          ON so.NoBahanBaku = detail.NoBahanBaku 
          AND so.NoPallet = detail.NoPallet
          AND detail.rn = 1
      `,
      fields: { 
        jmlhSak: 'so.JmlhSak', 
        berat: 'so.Berat' 
      }
    },    
    washing: {
      table: 'StockOpnameHasilWashing',
      labelExpr: 'so.NoWashing',
      label: 'Washing',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoWashing, 
            Blok, 
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoWashing ORDER BY DateCreate DESC) AS rn
          FROM Washing_h
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoWashing = detail.NoWashing
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },

    broker: {
      table: 'StockOpnameHasilBroker',
      labelExpr: 'so.NoBroker',
      label: 'Broker',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoBroker,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoBroker ORDER BY DateCreate DESC) AS rn
          FROM Broker_h
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBroker = detail.NoBroker
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },
    crusher: {
      table: 'StockOpnameHasilCrusher',
      labelExpr: 'so.NoCrusher',
      label: 'Crusher',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoCrusher,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoCrusher ORDER BY DateCreate DESC) AS rn
          FROM Crusher
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoCrusher = detail.NoCrusher
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'NULL',    // ⚠️ tidak ada kolom JmlhSak di tabel ini
        berat: 'so.Berat'
      }
    },
    bonggolan: {
      table: 'StockOpnameHasilBonggolan',
      labelExpr: 'so.NoBonggolan',
      label: 'Bonggolan',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoBonggolan,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoBonggolan ORDER BY DateCreate DESC) AS rn
          FROM Bonggolan
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBonggolan = detail.NoBonggolan
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'NULL',   // ❌ tidak ada jumlah sak
        berat: 'so.Berat'
      }
    },
    gilingan: {
      table: 'StockOpnameHasilGilingan',     // ✅ pakai tabel yang benar
      labelExpr: 'so.NoGilingan',
      label: 'Gilingan',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoGilingan,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoGilingan ORDER BY DateCreate DESC) AS rn
          FROM Gilingan
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoGilingan = detail.NoGilingan
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'NULL',   // ❌ Gilingan tidak punya jumlah sak, hanya berat
        berat: 'so.Berat'
      }
    },    
    mixer: {
      table: 'StockOpnameHasilMixer',
      labelExpr: 'so.NoMixer',
      label: 'Mixer',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoMixer,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoMixer ORDER BY DateCreate DESC) AS rn
          FROM Mixer_h
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoMixer = detail.NoMixer
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'so.JmlhSak',
        berat: 'so.Berat'
      }
    },
    furniturewip: {
      table: 'StockOpnameHasilFurnitureWIP',
      labelExpr: 'so.NoFurnitureWIP',
      label: 'Furniture WIP',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoFurnitureWIP,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoFurnitureWIP ORDER BY DateCreate DESC) AS rn
          FROM FurnitureWIP
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoFurnitureWIP = detail.NoFurnitureWIP
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'so.Pcs',     // ⚠️ gunakan Pcs sebagai pengganti jumlah sak
        berat: 'so.Berat'
      }
    },
    barangjadi: {
      table: 'StockOpnameHasilBarangJadi',
      labelExpr: 'so.NoBJ',
      label: 'Barang Jadi',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoBJ,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoBJ ORDER BY DateCreate DESC) AS rn
          FROM BarangJadi
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoBJ = detail.NoBJ
                 AND detail.rn = 1
      `,
      fields: {
        jmlhSak: 'so.Pcs',     // ✅ gunakan PCS sebagai pengganti jumlah sak
        berat: 'so.Berat'
      }
    },
    reject: {
      table: 'StockOpnameHasilReject',
      labelExpr: 'so.NoReject',
      label: 'Reject',
      joinClause: `
        LEFT JOIN (
          SELECT 
            NoReject,
            Blok,
            IdLokasi,
            ROW_NUMBER() OVER (PARTITION BY NoReject ORDER BY DateCreate DESC) AS rn
          FROM RejectV2
          WHERE IdLokasi IS NOT NULL
        ) detail ON so.NoReject = detail.NoReject
                 AND detail.rn = 1
      `,
      fields: { jmlhSak: 'NULL', berat: 'so.Berat' }
    }
  };

  try {
    const pool = await poolPromise;
    const request = pool.request();

    // --- input dasar ---
    request.input('noso', sql.VarChar, noso);
    if (filterByUser) request.input('username', sql.VarChar, username);
    if (search) request.input('search', sql.VarChar, `%${search}%`);

    // --- lokasi & blok dipisah ---
    if (blok && blok !== 'all') request.input('blok', sql.VarChar, blok);
    if (idLokasi && idLokasi !== 'all') request.input('idLokasi', sql.Int, parseInt(idLokasi));

    const makeWhereLokasi = () => {
      if (blok && blok !== 'all' && idLokasi && idLokasi !== 'all') {
        return 'AND detail.Blok = @blok AND detail.IdLokasi = @idLokasi';
      } else if (blok && blok !== 'all') {
        return 'AND detail.Blok = @blok';
      } else if (idLokasi && idLokasi !== 'all') {
        return 'AND detail.IdLokasi = @idLokasi';
      }
      return '';
    };

    // === query builder ===
    const makeQuery = (table, labelExpr, labelType, joinClause, fields = {}) => `
      SELECT 
        ${labelExpr} AS NomorLabel, 
        '${labelType}' AS LabelType, 
        ${fields.jmlhSak || 'NULL'} AS JmlhSak, 
        ${fields.berat || 'NULL'} AS Berat,
        ISNULL(so.DateTimeScan, '1900-01-01') AS DateTimeScan,
        detail.Blok,
        detail.IdLokasi,
        so.Username
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${makeWhereLokasi()}
      ${search ? `AND ${labelExpr} LIKE @search` : ''}
    `;

    const makeCount = (table, labelExpr, joinClause) => `
      SELECT COUNT(*) AS total
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${makeWhereLokasi()}
      ${search ? `AND ${labelExpr} LIKE @search` : ''}
    `;

    const makeTotal = (field, table, joinClause) => `
      SELECT ROUND(SUM(${field}), 2) AS total
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso
      ${filterByUser ? 'AND so.Username = @username' : ''}
      ${makeWhereLokasi()}
    `;

    // === generate final query ===
    let query = '', totalQuery = '', totalSakQuery = '', totalBeratQuery = '';

    if (filterBy !== 'all') {
      const filter = filterMap[filterBy.toLowerCase()];
      if (!filter) throw new Error('Invalid filterBy');

      query = `
        ${makeQuery(filter.table, filter.labelExpr, filter.label, filter.joinClause, filter.fields)}
        ORDER BY so.DateTimeScan DESC
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;
      totalQuery = makeCount(filter.table, filter.labelExpr, filter.joinClause);
      totalSakQuery = filter.fields.jmlhSak !== 'NULL'
        ? makeTotal(filter.fields.jmlhSak, filter.table, filter.joinClause)
        : 'SELECT NULL AS total';
      totalBeratQuery = makeTotal(filter.fields.berat, filter.table, filter.joinClause);
    } else {
      const all = Object.values(filterMap);
      const allQueries = all.map(f => makeQuery(f.table, f.labelExpr, f.label, f.joinClause, f.fields));
      const allCounts = all.map(f => makeCount(f.table, f.labelExpr, f.joinClause));
      const allSak = all.map(f =>
        f.fields.jmlhSak !== 'NULL'
          ? makeTotal(f.fields.jmlhSak, f.table, f.joinClause)
          : 'SELECT 0 AS total'
      );
      const allBerat = all.map(f => makeTotal(f.fields.berat, f.table, f.joinClause));

      query = `
        SELECT * FROM (
          ${allQueries.join(' UNION ALL ')}
        ) AS hasil
        ORDER BY DateTimeScan DESC
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;
      totalQuery = `SELECT SUM(total) AS total FROM (${allCounts.join(' UNION ALL ')}) AS totalData;`;
      totalSakQuery = `SELECT ROUND(SUM(total), 2) AS total FROM (${allSak.join(' UNION ALL ')}) AS sakData;`;
      totalBeratQuery = `SELECT ROUND(SUM(total), 2) AS total FROM (${allBerat.join(' UNION ALL ')}) AS beratData;`;
    }

    // === eksekusi paralel ===
    const [result, total, berat, sak] = await Promise.all([
      request.query(query),
      request.query(totalQuery),
      request.query(totalBeratQuery),
      request.query(totalSakQuery)
    ]);

    // === format output ===
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
  }
}



async function deleteStockOpnameHasil({ noso, nomorLabel }) {
  if (!nomorLabel) {
    throw new Error('NomorLabel wajib diisi');
  }

  const pool = await poolPromise; // ✅ pakai pool global
  const request = pool.request();
  request.input('noso', sql.VarChar, noso);

  let deleteQuery = '';
  let labelTypeDetected = '';

  // === BAHAN BAKU ===
  const [noBahanBaku, noPallet] = nomorLabel.split('-');
  if (noBahanBaku && noPallet) {
    request.input('noBahanBaku', sql.VarChar, noBahanBaku);
    request.input('noPallet', sql.VarChar, noPallet);

    const checkBBK = await request.query(`
      SELECT 1 
      FROM StockOpnameHasilBahanBaku 
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
    if (deleteQuery) return; // skip kalau sudah ketemu

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
  await tryDeleteLabel('StockOpnameHasilMixer', 'NoMixer', 'mixer', 'noMixer');
  await tryDeleteLabel('StockOpnameHasilFurnitureWIP', 'NoFurnitureWIP', 'furniturewip', 'noFurnitureWIP');
  await tryDeleteLabel('StockOpnameHasilBarangJadi', 'NoBJ', 'barangjadi', 'noBJ');
  await tryDeleteLabel('StockOpnameHasilReject', 'NoReject', 'reject', 'noReject');

  if (!deleteQuery) {
    return { success: false, message: 'NomorLabel tidak ditemukan dalam data stock opname' };
  }

  // Eksekusi query DELETE
  await request.query(deleteQuery);

  return { success: true, message: `Label ${nomorLabel} berhasil dihapus dari tipe '${labelTypeDetected}'` };
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
      idLokasi: data.detail?.IdLokasi || null,
      mesinInfo: data.mesinInfo || [] // tambahkan ini untuk info mesin bonggolan

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
  const isReject = label.startsWith('BF.') && !label.includes('-');

  if (!isBahanBaku && !isWashing && !isBroker && !isCrusher && !isBonggolan && !isGilingan && !isMixer && !isFurnitureWIP && !isBarangJadi && !isReject) {
    return createResponse(false, {
      isValidFormat: false
    }, 'Kode label tidak dikenali. Hanya A., B., F., M., V., H., BB., BA., BF., atau D. yang valid.');
  }

    const pool = await poolPromise;
    const request = pool.request();
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
          SELECT 
              MIN(d.IdLokasi) AS IdLokasi,
              COUNT(*) AS JmlhSak,
              SUM(  
                  CASE 
                      WHEN d.IsPartial = 1 
                          THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                      ELSE ISNULL(d.Berat,0)
                  END
              ) AS Berat
          FROM [PPS_TEST2].[dbo].[BahanBaku_d] d
          LEFT JOIN (
              SELECT 
                  NoBahanBaku, 
                  NoPallet, 
                  NoSak, 
                  SUM(Berat) AS TotalPartial
              FROM [PPS_TEST2].[dbo].[BahanBakuPartial]
              WHERE NoBahanBaku = @NoBahanBaku 
                AND NoPallet = @NoPallet
              GROUP BY NoBahanBaku, NoPallet, NoSak
          ) p 
              ON d.NoBahanBaku = p.NoBahanBaku 
            AND d.NoPallet   = p.NoPallet
            AND d.NoSak      = p.NoSak
          WHERE d.DateUsage IS NULL 
            AND d.NoBahanBaku = @NoBahanBaku 
            AND d.NoPallet = @NoPallet;
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
          SELECT 
            MIN(d.IdLokasi) AS IdLokasi,
            COUNT(*) AS JmlhSak,
            SUM(
                CASE 
                    WHEN d.IsPartial = 1 
                        THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                    ELSE ISNULL(d.Berat,0)
                END
            ) AS Berat
        FROM [PPS_TEST2].[dbo].[Broker_d] d
        LEFT JOIN (
            SELECT 
                NoBroker,
                NoSak,
                SUM(Berat) AS TotalPartial
            FROM [PPS_TEST2].[dbo].[BrokerPartial]
            WHERE NoBroker = @NoBroker
            GROUP BY NoBroker, NoSak
        ) p 
            ON d.NoBroker = p.NoBroker
          AND d.NoSak    = p.NoSak
        WHERE d.DateUsage IS NULL
          AND d.NoBroker = @NoBroker;
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

      // --- Query info mesin untuk bonggolan (UNION ALL multi sumber) ---
      const mesinInfoQuery = `
        SELECT 
            iob.NoProduksi AS Nomor,
            iph.IdMesin,
            mm.NamaMesin,
            iph.IdOperator,
            mop.NamaOperator
        FROM InjectProduksiOutputBonggolan iob
        LEFT JOIN InjectProduksi_h iph ON iob.NoProduksi = iph.NoProduksi
        LEFT JOIN MstMesin mm ON iph.IdMesin = mm.IdMesin
        LEFT JOIN MstOperator mop ON iph.IdOperator = mop.IdOperator
        WHERE iob.NoBonggolan = @NoBonggolan

        UNION ALL

        SELECT 
            bpob.NoProduksi AS Nomor,
            bph.IdMesin,
            mm.NamaMesin,
            bph.IdOperator,
            mop.NamaOperator
        FROM BrokerProduksiOutputBonggolan bpob
        LEFT JOIN BrokerProduksi_h bph ON bpob.NoProduksi = bph.NoProduksi
        LEFT JOIN MstMesin mm ON bph.IdMesin = mm.IdMesin
        LEFT JOIN MstOperator mop ON bph.IdOperator = mop.IdOperator
        WHERE bpob.NoBonggolan = @NoBonggolan

        UNION ALL

        SELECT 
            bsob.NoBongkarSusun AS Nomor,
            NULL AS IdMesin,
            'Bongkar Susun' AS NamaMesin,
            NULL AS IdOperator,
            NULL AS NamaOperator
        FROM BongkarSusunOutputBonggolan bsob
        WHERE bsob.NoBonggolan = @NoBonggolan

        UNION ALL

        SELECT 
            aob.NoAdjustment AS Nomor,
            NULL AS IdMesin,
            'Adjustment' AS NamaMesin,
            NULL AS IdOperator,
            NULL AS NamaOperator
        FROM AdjustmentOutputBonggolan aob
        WHERE aob.NoBonggolan = @NoBonggolan
    `;      
    
    // Jalankan query di awal, simpan hasilnya
    const mesinInfoResult = await request.query(mesinInfoQuery);
    var mesinInfo = mesinInfoResult.recordset || [];

    } else if (isGilingan) {
      labelType = 'Gilingan';
      parsed = { NoGilingan: label };
      request.input('NoGilingan', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilGilingan
          WHERE NoSO = @noso AND NoGilingan = @NoGilingan AND Username = @username
        `;
      detailQuery = `
        SELECT 
          MIN(d.IdLokasi)   AS IdLokasi,
          MIN(d.IdWarehouse) AS IdWarehouse,
          COUNT(*) AS JmlhSak,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS Berat
      FROM [PPS_TEST2].[dbo].[Gilingan] d
      LEFT JOIN (
          SELECT 
              NoGilingan,
              SUM(Berat) AS TotalPartial
          FROM [PPS_TEST2].[dbo].[GilinganPartial]
          WHERE NoGilingan = @NoGilingan
          GROUP BY NoGilingan
      ) p 
          ON d.NoGilingan = p.NoGilingan
      WHERE d.DateUsage IS NULL
        AND d.NoGilingan = @NoGilingan;
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
        SELECT 
          MIN(d.IdLokasi) AS IdLokasi,
          COUNT(*) AS JmlhSak,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS Berat
      FROM [PPS_TEST2].[dbo].[Mixer_d] d
      LEFT JOIN (
          SELECT 
              NoMixer,
              NoSak,
              SUM(Berat) AS TotalPartial
          FROM [PPS_TEST2].[dbo].[MixerPartial]
          WHERE NoMixer = @NoMixer
          GROUP BY NoMixer, NoSak
      ) p 
          ON d.NoMixer = p.NoMixer
        AND d.NoSak   = p.NoSak
      WHERE d.DateUsage IS NULL
        AND d.NoMixer = @NoMixer;
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
        SELECT 
          MIN(d.IdLokasi)    AS IdLokasi,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Pcs,0) - ISNULL(p.TotalPartialPcs,0)
                  ELSE ISNULL(d.Pcs,0)
              END
          ) AS JmlhSak,
          SUM(d.Berat) AS Berat
      FROM [PPS_TEST2].[dbo].[FurnitureWIP] d
      LEFT JOIN (
          SELECT 
              NoFurnitureWIP,
              SUM(Pcs) AS TotalPartialPcs
          FROM [PPS_TEST2].[dbo].[FurnitureWIPPartial]
          WHERE NoFurnitureWIP = @NoFurnitureWIP
          GROUP BY NoFurnitureWIP
      ) p 
          ON d.NoFurnitureWIP = p.NoFurnitureWIP
      WHERE d.DateUsage IS NULL
        AND d.NoFurnitureWIP = @NoFurnitureWIP;
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
        SELECT 
          MIN(d.IdLokasi)    AS IdLokasi,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Pcs,0) - ISNULL(p.TotalPartialPcs,0)
                  ELSE ISNULL(d.Pcs,0)
              END
          ) AS JmlhSak,
          SUM(d.Berat) AS Berat
      FROM [PPS_TEST2].[dbo].[BarangJadi] d
      LEFT JOIN (
          SELECT 
              NoBJ,
              SUM(Pcs) AS TotalPartialPcs
          FROM [PPS_TEST2].[dbo].[BarangJadiPartial]
          WHERE NoBJ = @NoBJ
          GROUP BY NoBJ
      ) p 
          ON d.NoBJ = p.NoBJ
      WHERE d.DateUsage IS NULL
        AND d.NoBJ = @NoBJ;
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
    } else if (isReject) {
      labelType = 'Reject';
      parsed = { NoReject: label };
      request.input('NoReject', sql.VarChar, label);

      checkQuery = `
          SELECT COUNT(*) AS count FROM StockOpnameHasilReject
          WHERE NoSO = @noso AND NoReject = @NoReject AND Username = @username
        `;
      detailQuery = `
          SELECT Berat, IdLokasi
          FROM StockOpnameReject
          WHERE NoSO = @noso AND NoReject = @NoReject
        `;
      warehouseQuery = `
          SELECT IdWarehouse FROM RejectV2
          WHERE NoReject = @NoReject
        `;
      fallbackQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM RejectV2
          WHERE NoReject = @NoReject 
          AND (DateUsage IS NULL OR IdWarehouse NOT IN (SELECT IdWarehouse FROM StockOpname_h_WarehouseID WHERE NoSO = @noso))
        `;
      originalDataQuery = `
          SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
          FROM BarangJadi
          WHERE NoReject = @NoReject
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
        idWarehouse: null,
        mesinInfo: isBonggolan ? mesinInfo : []
        
      }, 'Label sudah pernah discan sebelumnya.');
    }

    // 4. VALIDASI KUALIFIKASI NOSO
    const nosoQualificationCheck = await request.query(`
        SELECT IsBahanBaku, IsWashing, IsBroker, IsBonggolan, IsCrusher, IsGilingan, IsMixer, IsFurnitureWIP, IsBarangJadi, IsReject
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
    } else if (isReject && !qualifications.IsReject) {
      isValidCategory = false;
      categoryMessage = 'Kategori Reject tidak sesuai dengan NoSO ini.';
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
        } : null,
        mesinInfo: isBonggolan ? mesinInfo : []
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
        } : null,
        mesinInfo: isBonggolan ? mesinInfo : []

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
          },
          mesinInfo: isBonggolan ? mesinInfo : []
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
        },
        mesinInfo: isBonggolan ? mesinInfo : []

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
        },
        mesinInfo: isBonggolan ? mesinInfo : []

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
        },
        mesinInfo: isBonggolan ? mesinInfo : []
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
}


async function insertStockOpnameLabel({ noso, label, jmlhSak = 0, berat = 0, idlokasi, blok, username }) {
  if (!label) {
    throw new Error('Label wajib diisi');
  }

    const pool = await poolPromise;
    const request = pool.request();

    request.input('noso', sql.VarChar, noso);
    request.input('username', sql.VarChar, username);
    request.input('jmlhSak', sql.Int, jmlhSak);
    request.input('berat', sql.Float, berat);
    request.input('DateTimeScan', sql.DateTime, new Date());
    request.input('idlokasi', sql.Int, idlokasi);         // <<== INT (penting)
    request.input('blok', sql.VarChar(3), blok);          // <<== Blok (char/varchar(3))
    
    const isBahanBaku = label.startsWith('A.') && label.includes('-');
    const isWashing = label.startsWith('B.') && !label.includes('-');
    const isBroker = label.startsWith('D.') && !label.includes('-');
    const isCrusher = label.startsWith('F.') && !label.includes('-');
    const isBonggolan = label.startsWith('M.') && !label.includes('-');
    const isGilingan = label.startsWith('V.') && !label.includes('-');
    const isMixer = label.startsWith('H.') && !label.includes('-');
    const isFurnitureWIP = label.startsWith('BB.') && !label.includes('-');
    const isBarangJadi = label.startsWith('BA.') && !label.includes('-');
    const isReject = label.startsWith('BF.') && !label.includes('-');


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
          UPDATE BahanBakuPallet_h
          SET Blok = @blok, IdLokasi = @idlokasi
          WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
        `);

        insertedData = {
          noso, nomorLabel: label, labelType: 'Bahan Baku', labelTypeCode: 'bahanbaku',
          jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
        };

    } else if (isWashing) {
      request.input('NoWashing', sql.VarChar, label);
      // pastikan sudah bind ini di atas:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan
      await request.query(`
        INSERT INTO StockOpnameHasilWashing
          (NoSO, NoWashing, JmlhSak, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoWashing, @jmlhSak, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi di HEADER saja
      const upd = await request.query(`
        UPDATE Washing_h
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoWashing = @NoWashing
      `);
    
      // jika tidak ada baris yang ter-update, fail fast
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoWashing tidak ditemukan di Washing_h');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Washing',
        labelTypeCode: 'washing',
        jmlhSak,
        berat,
        idlokasi,
        blok,
        username,
        timestamp: new Date()
      };

    } else if (isBroker) {
      request.input('NoBroker', sql.VarChar, label);
      // pastikan sudah bind ini di atas service:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan
      await request.query(`
        INSERT INTO StockOpnameHasilBroker
          (NoSO, NoBroker, JmlhSak, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoBroker, @jmlhSak, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi di HEADER saja (Broker_h)
      const upd = await request.query(`
        UPDATE Broker_h
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoBroker = @NoBroker
      `);
    
      // jika tidak ada baris ter-update, lempar error biar ketahuan datanya belum ada
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoBroker tidak ditemukan di Broker_h');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Broker',
        labelTypeCode: 'broker',
        jmlhSak,
        berat,
        idlokasi,
        blok,
        username,
        timestamp: new Date()
      };
      
    } else if (isCrusher) {
      request.input('NoCrusher', sql.VarChar, label);

      // 1) catat hasil scan (Crusher tidak pakai jmlhSak)
      await request.query(`
        INSERT INTO StockOpnameHasilCrusher
          (NoSO, NoCrusher, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoCrusher, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi & blok di HEADER Crusher
      const upd = await request.query(`
        UPDATE Crusher
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoCrusher = @NoCrusher
      `);
    
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoCrusher tidak ditemukan di tabel Crusher');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Crusher',
        labelTypeCode: 'crusher',
        jmlhSak,           // tetap ikut dikembalikan meski tidak dipakai saat insert
        berat,
        idlokasi,
        blok,              // <-- tambahkan supaya client tahu blok yang di-set
        username,
        timestamp: new Date()
      };

    } else if (isBonggolan) {
      request.input('NoBonggolan', sql.VarChar, label);
      // pastikan di atas sudah ada:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan (Bonggolan tidak pakai jmlhSak)
      await request.query(`
        INSERT INTO StockOpnameHasilBonggolan
          (NoSO, NoBonggolan, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoBonggolan, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi & blok di header Bonggolan
      const upd = await request.query(`
        UPDATE Bonggolan
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoBonggolan = @NoBonggolan
      `);
    
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoBonggolan tidak ditemukan di tabel Bonggolan');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Bonggolan',
        labelTypeCode: 'bonggolan',
        jmlhSak,         // dikembalikan apa adanya (walau tidak di-insert)
        berat,
        idlokasi,
        blok,            // <-- kirim balik blok yang di-set
        username,
        timestamp: new Date()
      };

    } else if (isGilingan) {
      request.input('NoGilingan', sql.VarChar, label);
      // pastikan sebelum ini sudah ada:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan (Gilingan tidak pakai jmlhSak)
      await request.query(`
        INSERT INTO StockOpnameHasilGilingan
          (NoSO, NoGilingan, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoGilingan, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi & blok di header Gilingan
      const upd = await request.query(`
        UPDATE Gilingan
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoGilingan = @NoGilingan
      `);
    
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoGilingan tidak ditemukan di tabel Gilingan');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Gilingan',
        labelTypeCode: 'gilingan',
        jmlhSak,       // dikembalikan apa adanya (walau tidak dipakai saat insert)
        berat,
        idlokasi,
        blok,          // kirim balik blok yang di-set
        username,
        timestamp: new Date()
      };

    } else if (isMixer) {
      request.input('NoMixer', sql.VarChar, label);
      // pastikan di atas sudah ada:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);

      // 1) insert hasil scan
      await request.query(`
        INSERT INTO StockOpnameHasilMixer
          (NoSO, NoMixer, JmlhSak, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoMixer, @jmlhSak, @berat, @username, @DateTimeScan)
      `);

      // 2) update lokasi & blok di HEADER: Mixer_h
      const upd = await request.query(`
        UPDATE Mixer_h
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoMixer = @NoMixer
      `);

      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoMixer tidak ditemukan di Mixer_h');
      }

      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Mixer',
        labelTypeCode: 'mixer',
        jmlhSak,
        berat,
        idlokasi,
        blok,          // kirim balik blok yang di-set
        username,
        timestamp: new Date()
      };

    } else if (isFurnitureWIP) {
      request.input('NoFurnitureWIP', sql.VarChar, label);
      // pastikan di atas sudah ada:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan (Pcs = jmlhSak)
      await request.query(`
        INSERT INTO StockOpnameHasilFurnitureWIP
          (NoSO, NoFurnitureWIP, Pcs, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoFurnitureWIP, @jmlhSak, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi & blok di header FurnitureWIP
      const upd = await request.query(`
        UPDATE FurnitureWIP
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoFurnitureWIP = @NoFurnitureWIP
      `);
    
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoFurnitureWIP tidak ditemukan di tabel FurnitureWIP');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Furniture WIP',
        labelTypeCode: 'furniturewip',
        jmlhSak,      // dipakai sebagai Pcs pada insert hasil
        berat,
        idlokasi,
        blok,         // kirim balik blok yang di-set
        username,
        timestamp: new Date()
      };

    } else if (isBarangJadi) {
      request.input('NoBJ', sql.VarChar, label);
      // pastikan di atas sudah ada binding:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan (Pcs = jmlhSak)
      await request.query(`
        INSERT INTO StockOpnameHasilBarangJadi
          (NoSO, NoBJ, Pcs, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoBJ, @jmlhSak, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi & blok di header BarangJadi
      const upd = await request.query(`
        UPDATE BarangJadi
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoBJ = @NoBJ
      `);
    
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoBJ tidak ditemukan di tabel BarangJadi');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Barang Jadi',
        labelTypeCode: 'barangjadi',
        jmlhSak,     // dipakai sebagai Pcs pada insert hasil
        berat,
        idlokasi,
        blok,        // kirim balik blok yang di-set
        username,
        timestamp: new Date()
      };

    } else if (isReject) {
      request.input('NoReject', sql.VarChar, label);
      // pastikan sebelum ini sudah ada:
      // request.input('idlokasi', sql.Int, idlokasi);
      // request.input('blok', sql.VarChar(3), blok);
    
      // 1) insert hasil scan (Reject tidak pakai jmlhSak)
      await request.query(`
        INSERT INTO StockOpnameHasilReject
          (NoSO, NoReject, Berat, Username, DateTimeScan)
        VALUES
          (@noso, @NoReject, @berat, @username, @DateTimeScan)
      `);
    
      // 2) update lokasi & blok di RejectV2
      const upd = await request.query(`
        UPDATE RejectV2
        SET Blok = @blok,
            IdLokasi = @idlokasi
        WHERE NoReject = @NoReject
      `);
    
      if (!upd.rowsAffected || upd.rowsAffected[0] === 0) {
        throw new Error('NoReject tidak ditemukan di tabel RejectV2');
      }
    
      insertedData = {
        noso,
        nomorLabel: label,
        labelType: 'Reject',
        labelTypeCode: 'reject',
        jmlhSak,      // dikembalikan apa adanya (tidak di-insert)
        berat,
        idlokasi,
        blok,         // kirim balik blok yang di-set
        username,
        timestamp: new Date()
      };
    }

    else {
      throw new Error('Kode label tidak dikenali dalam sistem. Hanya label dengan awalan A., B., F., M., V., H., BB., BA., BF., atau D. yang valid.');
    }

    // Emit ke socket jika global.io tersedia
    if (global.io) {
      global.io.emit('label_inserted', insertedData);
    }

    return { success: true, message: 'Label berhasil disimpan dan lokasi diperbarui' };
}


async function getStockOpnameFamilies(noSO) {
  try {
    const pool = await poolPromise;
    const result = await pool.request()
      .input('noSO', sql.VarChar, noSO)
      .query(`
        SELECT 
          f.NoSO,
          f.CategoryID,
          f.FamilyID,
          ISNULL(sf.FamilyName, '') AS FamilyName,
          COUNT(s.ItemID) AS TotalItem,
          COUNT(DISTINCT sh.ItemID) AS CompleteItem
        FROM [dbo].[StockOpnameAscend_dFamily] f
        LEFT JOIN [AS_GSU_2022].[dbo].[IC_StockFamily] sf 
               ON f.FamilyID = sf.FamilyID
        LEFT JOIN [dbo].[StockOpnameAscend] s 
               ON f.NoSO = s.NoSO 
              AND f.CategoryID = s.CategoryID 
              AND f.FamilyID = s.FamilyID
        LEFT JOIN [dbo].[StockOpnameAscendHasil] sh 
               ON s.NoSO = sh.NoSO 
              AND s.ItemID = sh.ItemID
        WHERE f.NoSO = @noSO
        GROUP BY f.NoSO, f.CategoryID, f.FamilyID, sf.FamilyName
        ORDER BY f.FamilyID ASC
      `);

    if (!result.recordset || result.recordset.length === 0) {
      return null;
    }

    return result.recordset.map(({ 
      NoSO, 
      CategoryID, 
      FamilyID, 
      FamilyName, 
      TotalItem, 
      CompleteItem 
    }) => ({
      NoSO,
      CategoryID,
      FamilyID,
      FamilyName,
      TotalItem,
      CompleteItem
    }));
  } catch (err) {
    throw new Error(`Stock Opname Family Service Error: ${err.message}`);
  }
}


async function getStockOpnameAscendData({ noSO, familyID, keyword }) {

  try {
    const pool = await poolPromise;
    const result = await pool.request()
      .input('noSO', sql.VarChar, noSO)
      .input('familyID', sql.VarChar, familyID)
      .input('keyword', sql.VarChar, `%${keyword || ''}%`)
      .query(`
        SELECT 
          so.NoSO,
          so.ItemID,
          it.ItemCode,
          it.ItemName,
          so.Pcs,
          sh.QtyFisik,
          sh.QtyUsage,
          sh.UsageRemark,
          sh.IsUpdateUsage
        FROM [dbo].[StockOpnameAscend] so
        LEFT JOIN [AS_GSU_2022].[dbo].[IC_Items] it 
               ON so.ItemID = it.ItemID
        LEFT JOIN [dbo].[StockOpnameAscendHasil] sh 
               ON so.NoSO = sh.NoSO 
              AND so.ItemID = sh.ItemID
        WHERE so.NoSO = @noSO 
          AND so.FamilyID = @familyID
          AND (so.ItemID LIKE @keyword OR it.ItemName LIKE @keyword)
        ORDER BY it.ItemName ASC
      `);

    if (!result.recordset || result.recordset.length === 0) {
      return [];
    }

    return result.recordset.map(row => ({
      NoSO: row.NoSO,
      ItemID: row.ItemID,
      ItemCode: row.ItemCode,
      ItemName: row.ItemName,
      Pcs: row.Pcs,
      QtyFisik: row.QtyFisik !== null ? row.QtyFisik : null,
      QtyUsage: row.QtyUsage !== null ? row.QtyUsage : -1.0,
      UsageRemark: row.UsageRemark || '',
      IsUpdateUsage: row.IsUpdateUsage
    }));
  } catch (err) {
    throw new Error(`Stock Opname Ascend Service Error: ${err.message}`);
  } 
}


async function saveStockOpnameAscendHasil(noSO, dataList) {
  let transaction;
  try {
    console.log('🟢 Start saveStockOpnameAscendHasil');
    console.log('➡️ noSO:', noSO);
    console.log('➡️ dataList length:', dataList?.length);

    const pool = await poolPromise;
    transaction = new sql.Transaction(pool);
    await transaction.begin();
    console.log('✅ Transaction started');

    for (const [index, data] of dataList.entries()) {
      console.log(`\n🔹 Processing item ${index + 1}:`, data);

      // Skip kalau qtyFound kosong
      if (data.qtyFound === null || data.qtyFound === undefined) {
        console.log('⏭️ Skipped karena qtyFound null/undefined');
        continue;
      }

      const request = new sql.Request(transaction);
      const result = await request
        .input('NoSO', sql.VarChar, noSO)
        .input('ItemID', sql.Int, data.itemId)
        .input('QtyFisik', sql.Decimal(18, 6), data.qtyFound)
        .input('QtyUsage', sql.Decimal(18, 6), data.qtyUsage)
        .input('UsageRemark', sql.VarChar, data.usageRemark || '')
        .input('IsUpdateUsage', sql.Bit, 1)
        .query(`
          MERGE [dbo].[StockOpnameAscendHasil] AS target
          USING (SELECT 
                    @NoSO AS NoSO, 
                    @ItemID AS ItemID, 
                    @QtyFisik AS QtyFisik, 
                    @QtyUsage AS QtyUsage, 
                    @UsageRemark AS UsageRemark, 
                    @IsUpdateUsage AS IsUpdateUsage) AS source
          ON (target.NoSO = source.NoSO AND target.ItemID = source.ItemID)
          WHEN MATCHED THEN
            UPDATE SET QtyFisik = source.QtyFisik,
                       QtyUsage = source.QtyUsage,
                       UsageRemark = source.UsageRemark,
                       IsUpdateUsage = source.IsUpdateUsage
          WHEN NOT MATCHED THEN
            INSERT (NoSO, ItemID, QtyFisik, QtyUsage, UsageRemark, IsUpdateUsage)
            VALUES (source.NoSO, source.ItemID, source.QtyFisik, source.QtyUsage, source.UsageRemark, source.IsUpdateUsage);
        `);

      console.log(`✅ Query executed for itemId=${data.itemId}, rowsAffected:`, result.rowsAffected);
    }

    await transaction.commit();
    console.log('💾 Transaction committed');
    return { success: true, message: 'Data StockOpnameAscendHasil berhasil disimpan/diupdate' };
  } catch (err) {
    if (transaction) {
      try {
        await transaction.rollback();
        console.error('↩️ Transaction rolled back');
      } catch (rollbackErr) {
        console.error('❌ Rollback gagal:', rollbackErr.message);
      }
    }
    console.error('❌ Error saat saveStockOpnameAscendHasil:', err.message);
    throw new Error(`Stock Opname Ascend Save Service Error: ${err.message}`);
  }
}



async function fetchQtyUsage(itemId, tglSO) {
  try {
    const pool = await poolPromise;
    const request = pool.request();

    const result = await request
      .input('ItemID', sql.Int, itemId)   // ItemID = INT
      .input('Tanggal', sql.Date, tglSO) // Tanggal = DATE
      .query(`
        SELECT
            Z.ItemID,
            (0 - ISNULL(Z.QtyUsg,0) + ISNULL(Z.QtyUbb,0)
              - ISNULL(Z.QtySls,0) - ISNULL(Z.QtyPR,0)) AS Hasil
        FROM (
            SELECT AA.ItemID, AA.ItemCode,
                   ISNULL(BB.QtyPrcIn,0)  AS QtyPrcIn,
                   ISNULL(CC.QtyUsg,0)    AS QtyUsg,
                   ISNULL(DD.QtyUsg,0)    AS QtyUbb,
                   ISNULL(EE.QtySls,0)    AS QtySls,
                   ISNULL(FF.QtyPrcOut,0) AS QtyPR
            FROM (
                SELECT I.ItemID, I.ItemCode
                FROM [AS_GSU_2022].[dbo].[IC_Items] I
                WHERE I.Disabled = 0
                  AND I.ItemType = 0
            ) AA
            LEFT JOIN (
                SELECT D.ItemID,
                       SUM([AS_GSU_2022].[dbo].[UDF_Common_ConvertToSmallestUOMEx](
                           Packing2,Packing3,Packing4,Quantity,UOMLevel)) AS QtyPrcIn
                FROM [AS_GSU_2022].[dbo].[AP_PurchaseDetails] D
                JOIN [AS_GSU_2022].[dbo].[AP_Purchases] P ON P.PurchaseID=D.PurchaseID
                INNER JOIN [AS_GSU_2022].[dbo].[IC_Items] I ON I.ItemID = D.ItemID
                WHERE P.PurchaseDate >= @Tanggal AND P.Void=0 AND IsPurchase=1
                GROUP BY D.ItemID
            ) BB ON BB.ItemID=AA.ItemID
            LEFT JOIN (
                SELECT U.ItemID,
                       SUM([AS_GSU_2022].[dbo].[UDF_Common_ConvertToSmallestUOMEx](
                           Packing2,Packing3,Packing4,Quantity,UOMLevel)) AS QtyUsg
                FROM [AS_GSU_2022].[dbo].[IC_UsageDetails] U
                JOIN [AS_GSU_2022].[dbo].[IC_Usages] UH ON UH.UsageID=U.UsageID
                INNER JOIN [AS_GSU_2022].[dbo].[IC_Items] I ON I.ItemID = U.ItemID
                WHERE UH.UsageDate >= @Tanggal AND UH.Void=0 AND UH.Approved=1
                GROUP BY U.ItemID
            ) CC ON CC.ItemID=AA.ItemID
            LEFT JOIN (
                SELECT U.ItemID,
                       SUM([AS_GSU_2022].[dbo].[UDF_Common_ConvertToSmallestUOMEx](
                           Packing2,Packing3,Packing4,QtyAdjustBy,UOMLevel)) AS QtyUsg
                FROM [AS_GSU_2022].[dbo].[IC_AdjustmentDetails] U
                JOIN [AS_GSU_2022].[dbo].[IC_Adjustments] UH ON UH.AdjustmentID=U.AdjustmentID
                INNER JOIN [AS_GSU_2022].[dbo].[IC_Items] I ON I.ItemID = U.ItemID
                WHERE UH.AdjustmentDate >= @Tanggal AND UH.Void=0 AND UH.Approved=1
                GROUP BY U.ItemID
            ) DD ON DD.ItemID=AA.ItemID
            LEFT JOIN (
                SELECT U.ItemID,
                       SUM([AS_GSU_2022].[dbo].[UDF_Common_ConvertToSmallestUOMEx](
                           Packing2,Packing3,Packing4,Quantity,UOMLevel)) AS QtySls
                FROM [AS_GSU_2022].[dbo].[AR_InvoiceDetails] U
                JOIN [AS_GSU_2022].[dbo].[AR_Invoices] UH ON UH.InvoiceID=U.InvoiceID
                INNER JOIN [AS_GSU_2022].[dbo].[IC_Items] I ON I.ItemID = U.ItemID
                WHERE UH.InvoiceDate >= @Tanggal AND UH.Void=0
                GROUP BY U.ItemID
            ) EE ON EE.ItemID=AA.ItemID
            LEFT JOIN (
                SELECT D.ItemID,
                       SUM([AS_GSU_2022].[dbo].[UDF_Common_ConvertToSmallestUOMEx](
                           Packing2,Packing3,Packing4,Quantity,UOMLevel)) AS QtyPrcOut
                FROM [AS_GSU_2022].[dbo].[AP_PurchaseDetails] D
                JOIN [AS_GSU_2022].[dbo].[AP_Purchases] P ON P.PurchaseID=D.PurchaseID
                INNER JOIN [AS_GSU_2022].[dbo].[IC_Items] I ON I.ItemID = D.ItemID
                WHERE P.PurchaseDate >= @Tanggal AND P.Void=0 AND IsPurchase=0
                GROUP BY D.ItemID
            ) FF ON FF.ItemID=AA.ItemID
        ) Z
        WHERE Z.ItemID = @ItemID
      `);

    return result.recordset[0]?.Hasil || 0.0;
  } catch (err) {
    throw new Error(`Fetch QtyUsage Service Error: ${err.message}`);
  }
}



async function deleteStockOpnameHasilAscend(noso, itemId) {
  try {
    const pool = await poolPromise;
    const request = pool.request();

    request.input('NoSO', sql.VarChar(50), noso);
    request.input('ItemID', sql.Int, itemId);

    const result = await request.query(`
      DELETE FROM [dbo].[StockOpnameAscendHasil]
      WHERE NoSO = @NoSO AND ItemID = @ItemID
    `);

    return { deletedCount: result.rowsAffected?.[0] ?? 0 };
  } catch (err) {
    throw new Error(`deleteStockOpnameHasilAscend Service Error: ${err.message}`);
  }
}


module.exports = {
  getNoStockOpname,
  getStockOpnameAcuan,
  getStockOpnameHasil,
  deleteStockOpnameHasil,
  validateStockOpnameLabel,
  insertStockOpnameLabel,
  getStockOpnameFamilies,
  getStockOpnameAscendData,
  saveStockOpnameAscendHasil,
  fetchQtyUsage,
  deleteStockOpnameHasilAscend   
};

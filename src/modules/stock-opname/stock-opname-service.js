const { sql, poolPromise } = require('../../core/config/db');
const { formatDate } = require('../../core/utils/date-helper');
const { insertLogMappingLokasi } = require('../../core/shared/log'); // sesuaikan path



async function getNoStockOpname() {
  try {
    const pool = await poolPromise;

    const result = await pool.request().query(`
      SELECT
        soh.NoSO,
        soh.Tanggal,
        STRING_AGG(
          CASE 
            WHEN soh.IsAscend = 1 THEN icw.Name            -- nama gudang dari IC_Warehouses
            ELSE wh.NamaWarehouse                          -- nama gudang dari MstWarehouse
          END, 
          ', '
        ) AS NamaWarehouse,
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
      LEFT JOIN StockOpname_h_WarehouseID sohw 
        ON soh.NoSO = sohw.NoSO

      -- Join ke master lokal hanya bila IsAscend = 0
      LEFT JOIN MstWarehouse wh 
        ON sohw.IdWarehouse = wh.IdWarehouse
       AND soh.IsAscend = 0

      -- Join ke master ASCEND hanya bila IsAscend = 1
      LEFT JOIN [AS_GSU_2022].[dbo].[IC_Warehouses] icw
        ON sohw.IdWarehouse = icw.WarehouseID
       AND soh.IsAscend = 1

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

    if (!result.recordset || result.recordset.length === 0) return null;

    return result.recordset.map(({
      NoSO, Tanggal, NamaWarehouse,
      IsBahanBaku, IsWashing, IsBonggolan, IsCrusher, IsBroker,
      IsGilingan, IsMixer, IsFurnitureWIP, IsBarangJadi, IsReject, IsAscend
    }) => ({
      NoSO,
      Tanggal: formatDate(Tanggal),
      NamaWarehouse: NamaWarehouse || '-',
      IsBahanBaku, IsWashing, IsBonggolan, IsCrusher, IsBroker,
      IsGilingan, IsMixer, IsFurnitureWIP, IsBarangJadi, IsReject, IsAscend
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



async function validateStockOpnameLabel({ noso, label, username, blok, idlokasi }) {
  // Helper response
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
      idDiscrepancy: data.idDiscrepancy || null,
      idWarehouse: data.idWarehouse || null,

      // flatten detail
      jmlhSak: data.detail?.JmlhSak ?? null,
      berat: data.detail?.Berat ?? null,
      blok: data.detail?.Blok ?? null,
      idLokasi: data.detail?.IdLokasi ?? null,
      mesinInfo: data.mesinInfo || []
    };
  };

  // Normalizer & mismatch checker for Blok & IdLokasi
  const normBlok = (s) => (s ?? '').toString().trim().toUpperCase();
  const ctrlBlok = normBlok(blok);
  const ctrlIdLokasi = (idlokasi ?? idlokasi === 0) ? Number(idlokasi) : null;

  const isBlokLokasiMismatch = (dataDetail) => {
    if (!dataDetail) return false;
    const dataBlok = normBlok(dataDetail.Blok);
    const dataId = (dataDetail.IdLokasi ?? dataDetail.IdLokasi === 0) ? Number(dataDetail.IdLokasi) : null;

    // Jika controller mengirim blok dan/atau idlokasi, bandingkan yang tersedia.
    const blokMismatch = ctrlBlok ? (ctrlBlok !== dataBlok) : false;
    const idMismatch = (ctrlIdLokasi !== null && dataId !== null) ? (ctrlIdLokasi !== dataId) : false;

    // Anggap "gabungan tidak serupa" jika salah satu beda (blok atau idlokasi)
    return blokMismatch || idMismatch;
  };

  // Validasi input dasar
  if (!label) {
    return createResponse(false, {}, 'Label wajib diisi');
  }

  // 1) Validasi format label
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
    return createResponse(false, { isValidFormat: false }, 'Kode label tidak dikenali. Hanya A., B., F., M., V., H., BB., BA., BF., atau D. yang valid.');
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
  var mesinInfo = []; // function-scoped agar aman diakses di return

  // 2) Setup queries per tipe
  if (isBahanBaku) {
    labelType = 'Bahan Baku';
    const [noBahanBaku, noPallet] = label.split('-');
    if (!noBahanBaku || !noPallet) {
      return createResponse(false, { isValidFormat: false, labelType }, 'Format label bahan baku tidak valid. Contoh: A.0001-1');
    }
    parsed = { NoBahanBaku: noBahanBaku, NoPallet: noPallet };
    request.input('NoBahanBaku', sql.VarChar, noBahanBaku);
    request.input('NoPallet', sql.VarChar, noPallet);

    checkQuery = `
      SELECT COUNT(*) AS count FROM StockOpnameHasilBahanBaku
      WHERE NoSO = @noso AND NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
    `;
    detailQuery = `
      SELECT 
          bbh.Blok,
          bbh.IdLokasi,
          COUNT(*) AS JmlhSak,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                  ELSE ISNULL(d.Berat,0)
              END
          ) AS Berat
      FROM dbo.BahanBaku_d AS d
      LEFT JOIN (
          SELECT 
              NoBahanBaku, 
              NoPallet, 
              NoSak, 
              SUM(ISNULL(Berat,0)) AS TotalPartial
          FROM dbo.BahanBakuPartial
          WHERE NoBahanBaku = @NoBahanBaku
            AND NoPallet    = @NoPallet
          GROUP BY NoBahanBaku, NoPallet, NoSak
      ) AS p 
          ON d.NoBahanBaku = p.NoBahanBaku 
        AND d.NoPallet    = p.NoPallet
        AND d.NoSak       = p.NoSak
      INNER JOIN dbo.BahanBakuPallet_h AS bbh
          ON bbh.NoBahanBaku = d.NoBahanBaku
        AND bbh.NoPallet    = d.NoPallet
      WHERE d.DateUsage    IS NULL
        AND d.NoBahanBaku  = @NoBahanBaku
        AND d.NoPallet     = @NoPallet
        AND EXISTS (
              SELECT 1
              FROM dbo.StockOpnameBahanBaku AS sobb
              WHERE sobb.NoSO        = @NoSO
                AND sobb.NoBahanBaku = d.NoBahanBaku
                AND sobb.NoPallet    = d.NoPallet
        )
      GROUP BY 
          bbh.Blok, 
          bbh.IdLokasi;
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[BahanBakuPallet_h] bbh
      JOIN [dbo].[MstBlok] mb
        ON mb.Blok = bbh.Blok
      WHERE bbh.NoBahanBaku = @NoBahanBaku
        AND bbh.NoPallet    = @NoPallet;
    `;
    fallbackQuery = `
      SELECT 
          bbh.Blok,
          bbh.IdLokasi,
          bbh.IdWarehouse,
          COUNT(*) AS JumlahSak,
          SUM(
              CASE 
                  WHEN d.IsPartial = 1 
                      THEN ISNULL(d.Berat, 0) - ISNULL(p.TotalPartial, 0)
                  ELSE ISNULL(d.Berat, 0)
              END
          ) AS Berat
      FROM dbo.BahanBaku_d AS d
      LEFT JOIN (
          SELECT 
              NoBahanBaku, 
              NoPallet, 
              NoSak, 
              SUM(Berat) AS TotalPartial
          FROM dbo.BahanBakuPartial
          WHERE NoBahanBaku = @NoBahanBaku
            AND NoPallet    = @NoPallet
          GROUP BY NoBahanBaku, NoPallet, NoSak
      ) AS p
          ON d.NoBahanBaku = p.NoBahanBaku 
        AND d.NoPallet    = p.NoPallet
        AND d.NoSak       = p.NoSak
      INNER JOIN dbo.BahanBakuPallet_h AS bbh
          ON bbh.NoBahanBaku = d.NoBahanBaku
        AND bbh.NoPallet    = d.NoPallet
      WHERE d.NoBahanBaku = @NoBahanBaku
        AND d.NoPallet    = @NoPallet
        -- logic lama kamu: boleh yang belum dipakai ATAU warehouse-nya tidak ada di SO itu
        AND (
              d.DateUsage IS NULL
            )
      GROUP BY 
          bbh.Blok,
          bbh.IdLokasi,
          bbh.IdWarehouse;
    `;
    originalDataQuery = `
      SELECT 
          COUNT(*) AS JumlahSak,
          SUM(bb.Berat) AS Berat,          -- berat dari detail
          MAX(bb.IdLokasi) AS IdLokasi,    -- atau MAX(bbh.IdLokasi) pilih yang benar
          MAX(bbh.IdWarehouse) AS IdWarehouse
      FROM BahanBaku_d AS bb
      JOIN BahanBakuPallet_h AS bbh
        ON bb.NoBahanBaku = bbh.NoBahanBaku
      AND bb.NoPallet    = bbh.NoPallet
      WHERE bb.NoBahanBaku = @NoBahanBaku
        AND bb.NoPallet    = @NoPallet;
    `;

  } else if (isWashing) {
    labelType = 'Washing';
    parsed = { NoWashing: label };
    request.input('NoWashing', sql.VarChar, label);

    checkQuery = `
      SELECT COUNT(*) AS count FROM StockOpnameHasilWashing
      WHERE NoSO = @noso AND NoWashing = @NoWashing
    `;
    detailQuery = `
      SELECT
          ISNULL(dstats.JmlhSak, 0) AS JmlhSak,
          ISNULL(dstats.Berat, 0.0) AS Berat,
          h.Blok,
          h.IdLokasi
      FROM dbo.Washing_h AS h
      LEFT JOIN (
          SELECT
              NoWashing,
              COUNT(1) AS JmlhSak,
              SUM(ISNULL(Berat, 0.0)) AS Berat
          FROM dbo.Washing_d
          WHERE NoWashing = @NoWashing
            AND DateUsage IS NULL
          GROUP BY NoWashing
      ) AS dstats
          ON dstats.NoWashing = h.NoWashing
      WHERE h.NoWashing = @NoWashing
        AND EXISTS (
              SELECT 1
              FROM dbo.StockOpnameWashing AS sow
              WHERE sow.NoSO      = @NoSO
                AND sow.NoWashing = h.NoWashing
        );
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[Washing_h] wh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(wh.Blok)))
      WHERE wh.NoWashing = @NoWashing;
    `;
    fallbackQuery = `
      SELECT
        ISNULL(dstats.JmlhSak, 0) AS JmlhSak,
        ISNULL(dstats.Berat, 0.0) AS Berat,
        h.Blok,
        h.IdLokasi
      FROM [dbo].[Washing_h] AS h
      LEFT JOIN (
        SELECT
          NoWashing,
          COUNT(1) AS JmlhSak,
          SUM(ISNULL(Berat, 0.0)) AS Berat
        FROM [dbo].[Washing_d]
        WHERE NoWashing = @NoWashing
          AND DateUsage IS NULL
        GROUP BY NoWashing
      ) AS dstats
        ON dstats.NoWashing = h.NoWashing
      WHERE h.NoWashing = @NoWashing;
    `;
    originalDataQuery = `
      SELECT 
          COUNT(*) AS JumlahSak,
          SUM(wd.Berat) AS Berat,
          MAX(wd.IdLokasi) AS IdLokasi,      -- kalau IdLokasi adanya di detail
          MAX(wh.IdWarehouse) AS IdWarehouse -- ambil dari header
      FROM Washing_d AS wd
      JOIN Washing_h AS wh 
        ON wd.NoWashing = wh.NoWashing
      WHERE wd.NoWashing = @NoWashing;
    `;

  } else if (isBroker) {
    labelType = 'Broker';
    parsed = { NoBroker: label };
    request.input('NoBroker', sql.VarChar, label);

    checkQuery = `
      SELECT COUNT(*) AS count FROM StockOpnameHasilBroker
      WHERE NoSO = @noso AND NoBroker = @NoBroker
    `;
    detailQuery = `
      SELECT
          ISNULL(agg.JmlhSak, 0) AS JmlhSak,
          ISNULL(agg.Berat,   0) AS Berat,
          h.Blok,
          h.IdLokasi
      FROM dbo.Broker_h AS h
      LEFT JOIN (
          SELECT
              d.NoBroker,
              COUNT(*) AS JmlhSak,
              SUM(
                  CASE 
                      WHEN d.IsPartial = 1 THEN 
                          CASE 
                              WHEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0) < 0 
                                  THEN 0 
                              ELSE ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                          END
                      ELSE ISNULL(d.Berat,0)
                  END
              ) AS Berat
          FROM dbo.Broker_d AS d
          LEFT JOIN (
              SELECT 
                  NoBroker,
                  NoSak,
                  SUM(ISNULL(Berat,0)) AS TotalPartial
              FROM dbo.BrokerPartial
              WHERE NoBroker = @NoBroker
              GROUP BY NoBroker, NoSak
          ) AS p
              ON p.NoBroker = d.NoBroker
            AND p.NoSak    = d.NoSak
          WHERE d.NoBroker = @NoBroker
            AND d.DateUsage IS NULL
          GROUP BY d.NoBroker
      ) AS agg
          ON agg.NoBroker = h.NoBroker
      WHERE h.NoBroker = @NoBroker
        AND EXISTS (
              SELECT 1
              FROM dbo.StockOpnameBroker AS sob
              WHERE sob.NoSO     = @noso      -- pastikan sesuai SO yang sedang discan
                AND sob.NoBroker = h.NoBroker -- pastikan broker itu memang dicatat di SO tsb
        );
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[Broker_h] bh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(bh.Blok)))
      WHERE bh.NoBroker = @NoBroker;
    `;
    fallbackQuery = `
      SELECT
        ISNULL(agg.JmlhSak, 0) AS JmlhSak,
        ISNULL(agg.Berat,   0) AS Berat,
        h.Blok,
        h.IdLokasi
      FROM [dbo].[Broker_h] AS h
      LEFT JOIN (
        SELECT
          d.NoBroker,
          COUNT(*) AS JmlhSak,
          SUM(
            CASE 
              WHEN d.IsPartial = 1 THEN 
                CASE 
                  WHEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0) < 0 
                    THEN 0 
                  ELSE ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                END
              ELSE ISNULL(d.Berat,0)
            END
          ) AS Berat
        FROM [dbo].[Broker_d] AS d
        LEFT JOIN (
          SELECT 
            NoBroker,
            NoSak,
            SUM(ISNULL(Berat,0)) AS TotalPartial
          FROM [dbo].[BrokerPartial]
          WHERE NoBroker = @NoBroker
          GROUP BY NoBroker, NoSak
        ) AS p
          ON p.NoBroker = d.NoBroker
         AND p.NoSak    = d.NoSak
        WHERE d.NoBroker = @NoBroker
          AND d.DateUsage IS NULL
        GROUP BY d.NoBroker
      ) AS agg
        ON agg.NoBroker = h.NoBroker
      WHERE h.NoBroker = @NoBroker;
    `;
    originalDataQuery = `
      SELECT 
          COUNT(*) AS JumlahSak,
          SUM(bd.Berat) AS Berat,
          MAX(bd.IdLokasi) AS IdLokasi,      -- ganti ke MAX(bh.IdLokasi) kalau lokasinya di header
          MAX(bh.IdWarehouse) AS IdWarehouse
      FROM Broker_d AS bd
      JOIN Broker_h AS bh 
        ON bd.NoBroker = bh.NoBroker
      WHERE bd.NoBroker = @NoBroker;
    `;

  } else if (isCrusher) {
    labelType = 'Crusher';
    parsed = { NoCrusher: label };
    request.input('NoCrusher', sql.VarChar, label);

    checkQuery = `
      SELECT COUNT(*) AS count FROM StockOpnameHasilCrusher
      WHERE NoSO = @noso AND NoCrusher = @NoCrusher
    `;
    detailQuery = `
      SELECT TOP (1)
          ISNULL(c.Berat, 0) AS Berat,
          c.Blok,
          c.IdLokasi
      FROM dbo.Crusher AS c
      WHERE c.NoCrusher = @NoCrusher
        AND c.DateUsage IS NULL
        AND EXISTS (
              SELECT 1
              FROM dbo.StockOpnameCrusher AS s
              WHERE s.NoSO = @noso
                AND s.NoCrusher = c.NoCrusher
          )
      ORDER BY c.DateCreate DESC;
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[Crusher] ch
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(ch.Blok)))
      WHERE ch.NoCrusher = @NoCrusher;
    `;
    fallbackQuery = `
      SELECT TOP (1)
        ISNULL(c.Berat, 0) AS Berat,
        c.Blok,
        c.IdLokasi
      FROM [dbo].[Crusher] AS c
      WHERE c.NoCrusher = @NoCrusher AND DateUsage IS NULL
      ORDER BY c.DateCreate DESC;
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
      WHERE NoSO = @noso AND NoBonggolan = @NoBonggolan
    `;
    detailQuery = `
      SELECT TOP (1)
          CASE 
              WHEN b.DateUsage IS NOT NULL THEN 0
              ELSE ISNULL(b.Berat, 0)
          END AS Berat,
          b.Blok,
          b.IdLokasi
      FROM dbo.Bonggolan AS b
      WHERE b.NoBonggolan = @NoBonggolan
        AND EXISTS (
              SELECT 1
              FROM dbo.StockOpnameBonggolan AS sob
              WHERE sob.NoSO        = @NoSO
                AND sob.NoBonggolan = b.NoBonggolan
          )
      ORDER BY b.DateCreate DESC;
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[Bonggolan] bh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(bh.Blok)))
      WHERE bh.NoBonggolan = @NoBonggolan;
    `;
    fallbackQuery = `
      SELECT TOP (1)
          CASE 
              WHEN b.DateUsage IS NOT NULL THEN 0
              ELSE ISNULL(b.Berat, 0)
          END AS Berat,
          b.Blok,
          b.IdLokasi
      FROM [dbo].[Bonggolan] AS b
      WHERE b.NoBonggolan = @NoBonggolan
      ORDER BY b.DateCreate DESC;
    `;
    originalDataQuery = `
      SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
      FROM Bonggolan
      WHERE NoBonggolan = @NoBonggolan
    `;

    // Info mesin bonggolan
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
    const mesinInfoResult = await request.query(mesinInfoQuery);
    mesinInfo = mesinInfoResult.recordset || [];

  } else if (isGilingan) {
    labelType = 'Gilingan';
    parsed = { NoGilingan: label };
    request.input('NoGilingan', sql.VarChar, label);

    checkQuery = `
      SELECT COUNT(*) AS count FROM StockOpnameHasilGilingan
      WHERE NoSO = @noso AND NoGilingan = @NoGilingan
    `;
    detailQuery = `
      SELECT
          agg.JmlhSak,
          agg.Berat,
          h.Blok,
          h.IdLokasi
      FROM (
          SELECT
              COUNT(*) AS JmlhSak,
              SUM(
                  CASE 
                      WHEN d.IsPartial = 1 
                          THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                      ELSE ISNULL(d.Berat,0)
                  END
              ) AS Berat
          FROM dbo.Gilingan AS d
          LEFT JOIN (
              SELECT 
                  NoGilingan,
                  SUM(ISNULL(Berat,0)) AS TotalPartial
              FROM dbo.GilinganPartial
              WHERE NoGilingan = @NoGilingan
              GROUP BY NoGilingan
          ) AS p
              ON p.NoGilingan = d.NoGilingan
          WHERE d.NoGilingan = @NoGilingan
            AND d.DateUsage IS NULL
      ) AS agg
      CROSS APPLY (
          SELECT TOP (1)
              g.Blok,
              g.IdLokasi
          FROM dbo.Gilingan AS g
          WHERE g.NoGilingan = @NoGilingan
          ORDER BY g.DateCreate DESC
      ) AS h
      WHERE EXISTS (
          SELECT 1
          FROM dbo.StockOpnameGilingan AS sog
          WHERE sog.NoSO       = @NoSO
            AND sog.NoGilingan = @NoGilingan
      );
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[Gilingan] gh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(gh.Blok)))
      WHERE gh.NoGilingan = @NoGilingan;
    `;
    fallbackQuery = `
      SELECT
        agg.JmlhSak,
        agg.Berat,
        h.Blok,
        h.IdLokasi
      FROM (
        SELECT
          COUNT(*) AS JmlhSak,
          SUM(
            CASE 
              WHEN d.IsPartial = 1 
                THEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
              ELSE ISNULL(d.Berat,0)
            END
          ) AS Berat
        FROM [dbo].[Gilingan] AS d
        LEFT JOIN (
          SELECT 
            NoGilingan,
            SUM(ISNULL(Berat,0)) AS TotalPartial
          FROM [dbo].[GilinganPartial]
          WHERE NoGilingan = @NoGilingan
          GROUP BY NoGilingan
        ) AS p
          ON p.NoGilingan = d.NoGilingan
        WHERE d.NoGilingan = @NoGilingan
          AND d.DateUsage IS NULL
      ) AS agg
      CROSS APPLY (
        SELECT TOP (1)
          g.Blok,
          g.IdLokasi
        FROM [dbo].[Gilingan] AS g
        WHERE g.NoGilingan = @NoGilingan
        ORDER BY g.DateCreate DESC
      ) AS h;
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
      WHERE NoSO = @noso AND NoMixer = @NoMixer
    `;
    detailQuery = `
      SELECT
        agg.JmlhSak,
        agg.Berat,
        h.Blok,
        h.IdLokasi
    FROM dbo.Mixer_h AS h
    LEFT JOIN (
        SELECT
            d.NoMixer,
            COUNT(*) AS JmlhSak,
            SUM(
                CASE
                    WHEN d.IsPartial = 1 THEN
                        CASE
                            WHEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0) < 0
                                THEN 0
                            ELSE ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                        END
                    ELSE ISNULL(d.Berat,0)
                END
            ) AS Berat
        FROM dbo.Mixer_d AS d
        LEFT JOIN (
            SELECT
                mp.NoMixer,
                mp.NoSak,
                SUM(ISNULL(mp.Berat,0)) AS TotalPartial
            FROM dbo.MixerPartial AS mp
            WHERE mp.NoMixer = @NoMixer
            GROUP BY mp.NoMixer, mp.NoSak
        ) AS p
            ON p.NoMixer = d.NoMixer
          AND p.NoSak   = d.NoSak
        WHERE d.NoMixer = @NoMixer
          AND d.DateUsage IS NULL
        GROUP BY d.NoMixer
    ) AS agg
        ON agg.NoMixer = h.NoMixer
    WHERE h.NoMixer = @NoMixer
      AND EXISTS (
            SELECT 1
            FROM dbo.StockOpnameMixer AS som
            WHERE som.NoSO    = @NoSO
              AND som.NoMixer = h.NoMixer
      );
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[Mixer_h] mh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(mh.Blok)))
      WHERE mh.NoMixer = @NoMixer;
    `;
    fallbackQuery = `
      SELECT
        agg.JmlhSak,
        agg.Berat,
        h.Blok,
        h.IdLokasi
      FROM [dbo].[Mixer_h] AS h
      LEFT JOIN (
        SELECT
          d.NoMixer,
          COUNT(*) AS JmlhSak,
          SUM(
            CASE
              WHEN d.IsPartial = 1 THEN
                CASE
                  WHEN ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0) < 0
                    THEN 0
                  ELSE ISNULL(d.Berat,0) - ISNULL(p.TotalPartial,0)
                END
              ELSE ISNULL(d.Berat,0)
            END
          ) AS Berat
        FROM [dbo].[Mixer_d] AS d
        LEFT JOIN (
          SELECT
            NoMixer,
            NoSak,
            SUM(ISNULL(Berat,0)) AS TotalPartial
          FROM [dbo].[MixerPartial]
          WHERE NoMixer = @NoMixer
          GROUP BY NoMixer, NoSak
        ) AS p
            ON p.NoMixer = d.NoMixer
           AND p.NoSak   = d.NoSak
        WHERE d.NoMixer = @NoMixer
          AND d.DateUsage IS NULL
        GROUP BY d.NoMixer
      ) AS agg
        ON agg.NoMixer = h.NoMixer
      WHERE h.NoMixer = @NoMixer;
    `;
    originalDataQuery = `
      SELECT 
          COUNT(*) AS JumlahSak,
          SUM(md.Berat) AS Berat,          -- berat biasanya di detail
          MAX(md.IdLokasi) AS IdLokasi,    -- atau MAX(mh.IdLokasi) kalau lokasinya di header
          MAX(mh.IdWarehouse) AS IdWarehouse
      FROM dbo.Mixer_d AS md
      JOIN dbo.Mixer_h AS mh 
        ON md.NoMixer = mh.NoMixer
      WHERE md.NoMixer = @NoMixer;
    `;

  } else if (isFurnitureWIP) {
    labelType = 'Furniture WIP';
    parsed = { NoFurnitureWIP: label };
    request.input('NoFurnitureWIP', sql.VarChar, label);

    checkQuery = `
      SELECT COUNT(*) AS count FROM StockOpnameHasilFurnitureWIP
      WHERE NoSO = @noso AND NoFurnitureWIP = @NoFurnitureWIP
    `;
    detailQuery = `
      WITH base AS (
        SELECT
            d.NoFurnitureWIP, 
            SUM(CASE WHEN d.IsPartial = 1 THEN 0 ELSE ISNULL(d.Pcs,0) END) AS SumNonPartialPcs,
            SUM(CASE WHEN d.IsPartial = 1 THEN ISNULL(d.Pcs,0) ELSE 0 END) AS SumPartialPcs,
            SUM(ISNULL(d.Berat,0)) AS TotalBerat
        FROM dbo.FurnitureWIP AS d
        WHERE d.NoFurnitureWIP = @NoFurnitureWIP
          AND d.DateUsage IS NULL
        GROUP BY d.NoFurnitureWIP
    ),
    p AS (
        SELECT
            fp.NoFurnitureWIP,
            SUM(ISNULL(fp.Pcs,0)) AS TotalPartialPcs
        FROM dbo.FurnitureWIPPartial AS fp
        WHERE fp.NoFurnitureWIP = @NoFurnitureWIP
        GROUP BY fp.NoFurnitureWIP
    ),
    agg AS (
        SELECT
            b.NoFurnitureWIP,
            b.SumNonPartialPcs +
            CASE
                WHEN ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0) < 0
                    THEN 0
                ELSE ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0)
            END AS JmlhSak,
            b.TotalBerat AS Berat
        FROM base AS b
        LEFT JOIN p
          ON p.NoFurnitureWIP = b.NoFurnitureWIP
    )
    SELECT
        a.JmlhSak,
        a.Berat,
        h.Blok,
        h.IdLokasi
    FROM agg AS a
    CROSS APPLY (
        SELECT TOP (1)
            fh.Blok,
            fh.IdLokasi
        FROM dbo.FurnitureWIP AS fh
        WHERE fh.NoFurnitureWIP = a.NoFurnitureWIP
        ORDER BY fh.DateCreate DESC, fh.DateTimeCreate DESC
    ) AS h
    WHERE EXISTS (
        SELECT 1
        FROM dbo.StockOpnameFurnitureWIP AS sof
        WHERE sof.NoSO          = @NoSO
          AND sof.NoFurnitureWIP = a.NoFurnitureWIP
    );
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[FurnitureWIP] fh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(fh.Blok)))
      WHERE fh.NoFurnitureWIP = @NoFurnitureWIP;
    `;
    fallbackQuery = `
          WITH base AS (
            SELECT
              d.NoFurnitureWIP,
              SUM(CASE WHEN d.IsPartial = 1 THEN 0 ELSE ISNULL(d.Pcs,0) END) AS SumNonPartialPcs,
              SUM(CASE WHEN d.IsPartial = 1 THEN ISNULL(d.Pcs,0) ELSE 0 END) AS SumPartialPcs,
              SUM(ISNULL(d.Berat,0)) AS TotalBerat
            FROM [dbo].[FurnitureWIP] d
            WHERE d.NoFurnitureWIP = @NoFurnitureWIP
              AND d.DateUsage IS NULL
            GROUP BY d.NoFurnitureWIP
          ),
          p AS (
            SELECT
              fp.NoFurnitureWIP,
              SUM(ISNULL(fp.Pcs,0)) AS TotalPartialPcs
            FROM [dbo].[FurnitureWIPPartial] fp
            WHERE fp.NoFurnitureWIP = @NoFurnitureWIP
            GROUP BY fp.NoFurnitureWIP
          ),
          agg AS (
            SELECT
              b.NoFurnitureWIP,
              b.SumNonPartialPcs +
              CASE
                WHEN ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0) < 0
                  THEN 0
                ELSE ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0)
              END AS JmlhSak,
              b.TotalBerat AS Berat
            FROM base b
            LEFT JOIN p ON p.NoFurnitureWIP = b.NoFurnitureWIP
          )
          SELECT
            a.JmlhSak,
            a.Berat,
            h.Blok,
            h.IdLokasi
          FROM agg a
          CROSS APPLY (
            SELECT TOP (1)
              fh.Blok,
              fh.IdLokasi
            FROM [dbo].[FurnitureWIP] AS fh
            WHERE fh.NoFurnitureWIP = a.NoFurnitureWIP
            ORDER BY fh.DateCreate DESC, fh.DateTimeCreate DESC
          ) AS h;
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
      WHERE NoSO = @noso AND NoBJ = @NoBJ
    `;
    detailQuery = `
      ;WITH base AS (
          SELECT
              d.NoBJ,
              SUM(CASE WHEN d.IsPartial = 1 THEN 0 ELSE ISNULL(d.Pcs,0) END) AS SumNonPartialPcs,
              SUM(CASE WHEN d.IsPartial = 1 THEN ISNULL(d.Pcs,0) ELSE 0 END) AS SumPartialPcs,
              SUM(ISNULL(d.Berat,0)) AS TotalBerat
          FROM dbo.BarangJadi AS d
          WHERE d.NoBJ = @NoBJ
            AND d.DateUsage IS NULL
          GROUP BY d.NoBJ
      ),
      p AS (
          SELECT
              bp.NoBJ,
              SUM(ISNULL(bp.Pcs,0)) AS TotalPartialPcs
          FROM dbo.BarangJadiPartial AS bp
          WHERE bp.NoBJ = @NoBJ
          GROUP BY bp.NoBJ
      ),
      agg AS (
          SELECT
              b.NoBJ,
              b.SumNonPartialPcs +
              CASE
                  WHEN ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0) < 0
                      THEN 0
                  ELSE ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0)
              END AS JmlhSak,
              b.TotalBerat AS Berat
          FROM base AS b
          LEFT JOIN p
            ON p.NoBJ = b.NoBJ
      )
      SELECT
          a.JmlhSak,
          a.Berat,
          h.Blok,
          h.IdLokasi
      FROM agg AS a
      CROSS APPLY (
          SELECT TOP (1)
              bh.Blok,
              bh.IdLokasi
          FROM dbo.BarangJadi AS bh
          WHERE bh.NoBJ = a.NoBJ
          ORDER BY bh.DateCreate DESC, bh.DateTimeCreate DESC
      ) AS h
      WHERE EXISTS (
          SELECT 1
          FROM dbo.StockOpnameBarangJadi AS sobj
          WHERE sobj.NoSO = @NoSO
            AND sobj.NoBJ = a.NoBJ
      );
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[BarangJadi] bh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(bh.Blok)))
      WHERE bh.NoBJ = @NoBJ;
    `;
    fallbackQuery = `
       ;WITH base AS (
        SELECT
          d.NoBJ,
          SUM(CASE WHEN d.IsPartial = 1 THEN 0 ELSE ISNULL(d.Pcs,0) END) AS SumNonPartialPcs,
          SUM(CASE WHEN d.IsPartial = 1 THEN ISNULL(d.Pcs,0) ELSE 0 END) AS SumPartialPcs,
          SUM(ISNULL(d.Berat,0)) AS TotalBerat
        FROM [dbo].[BarangJadi] d
        WHERE d.NoBJ = @NoBJ
          AND d.DateUsage IS NULL
        GROUP BY d.NoBJ
      ),
      p AS (
        SELECT
          NoBJ,
          SUM(ISNULL(Pcs,0)) AS TotalPartialPcs
        FROM [dbo].[BarangJadiPartial]
        WHERE NoBJ = @NoBJ
        GROUP BY NoBJ
      ),
      agg AS (
        SELECT
          b.NoBJ,
          b.SumNonPartialPcs +
          CASE
            WHEN ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0) < 0
              THEN 0
            ELSE ISNULL(b.SumPartialPcs,0) - ISNULL(p.TotalPartialPcs,0)
          END AS JmlhSak,
          b.TotalBerat AS Berat
        FROM base b
        LEFT JOIN p ON p.NoBJ = b.NoBJ
      )
      SELECT
        a.JmlhSak,
        a.Berat,
        h.Blok,
        h.IdLokasi
      FROM agg a
      CROSS APPLY (
        SELECT TOP (1)
          bh.Blok,
          bh.IdLokasi
        FROM [dbo].[BarangJadi] AS bh
        WHERE bh.NoBJ = a.NoBJ
        ORDER BY bh.DateCreate DESC, bh.DateTimeCreate DESC
      ) AS h;
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
      WHERE NoSO = @noso AND NoReject = @NoReject
    `;
    detailQuery = `
      SELECT TOP (1)
          r.Berat,
          r.Blok,
          r.IdLokasi
      FROM dbo.RejectV2 AS r
      WHERE r.NoReject = @NoReject
        AND EXISTS (
              SELECT 1
              FROM dbo.StockOpnameReject AS sor
              WHERE sor.NoSO    = @NoSO
                AND sor.NoReject = r.NoReject
        )
      ORDER BY r.DateCreate DESC, r.DateTimeCreate DESC;
    `;
    warehouseQuery = `
      SELECT mb.IdWarehouse
      FROM [dbo].[RejectV2] rh
      JOIN [dbo].[MstBlok]   mb
        ON UPPER(LTRIM(RTRIM(mb.Blok))) = UPPER(LTRIM(RTRIM(rh.Blok)))
      WHERE rh.NoReject = @NoReject;
    `;
    fallbackQuery = `
      SELECT TOP (1)
        r.Berat,
        r.Blok,
        r.IdLokasi
      FROM [dbo].[RejectV2] AS r
      WHERE r.NoReject = @NoReject AND DateUsage IS NULL
      ORDER BY r.DateCreate DESC, r.DateTimeCreate DESC;
    `;
    // FIX: originalData dari RejectV2 
    originalDataQuery = `
      SELECT SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi, MAX(IdWarehouse) AS IdWarehouse
      FROM RejectV2
      WHERE NoReject = @NoReject
    `;
  }

  // 3) Early duplicate
  const checkResult = await request.query(checkQuery);
  const isDuplicate = checkResult.recordset[0].count > 0;
  if (isDuplicate) {
    return createResponse(false, {
      isValidFormat: true,
      isValidCategory: true,
      isValidWarehouse: true,
      isDuplicate: true,
      foundInStockOpname: true,
      idDiscrepancy: null,
      labelType,
      parsed,
      idWarehouse,
      mesinInfo: isBonggolan ? mesinInfo : []
    }, 'Label telah discan!');
  }

  // 4) Kualifikasi NoSO
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

  let isValidCategory = true;
  let categoryMessage = '';
  if (isBahanBaku && !qualifications.IsBahanBaku) { isValidCategory = false; categoryMessage = 'Kategori Bahan Baku tidak sesuai dengan SO ini.'; }
  else if (isWashing && !qualifications.IsWashing) { isValidCategory = false; categoryMessage = 'Kategori Washing tidak sesuai dengan SO ini.'; }
  else if (isBroker && !qualifications.IsBroker) { isValidCategory = false; categoryMessage = 'Kategori Broker tidak sesuai dengan SO ini.'; }
  else if (isCrusher && !qualifications.IsCrusher) { isValidCategory = false; categoryMessage = 'Kategori Crusher tidak sesuai dengan SO ini.'; }
  else if (isBonggolan && !qualifications.IsBonggolan) { isValidCategory = false; categoryMessage = 'Kategori Bonggolan tidak sesuai dengan SO ini.'; }
  else if (isGilingan && !qualifications.IsGilingan) { isValidCategory = false; categoryMessage = 'Kategori Gilingan tidak sesuai dengan SO ini.'; }
  else if (isMixer && !qualifications.IsMixer) { isValidCategory = false; categoryMessage = 'Kategori Mixer tidak sesuai dengan SO ini.'; }
  else if (isFurnitureWIP && !qualifications.IsFurnitureWIP) { isValidCategory = false; categoryMessage = 'Kategori Furniture WIP tidak sesuai dengan SO ini.'; }
  else if (isBarangJadi && !qualifications.IsBarangJadi) { isValidCategory = false; categoryMessage = 'Kategori Barang Jadi tidak sesuai dengan SO ini.'; }
  else if (isReject && !qualifications.IsReject) { isValidCategory = false; categoryMessage = 'Kategori Reject tidak sesuai dengan SO ini.'; }

  // 5) Ambil IdWarehouse
  const whResult = await request.query(warehouseQuery);
  idWarehouse = whResult.recordset[0]?.IdWarehouse ?? null;

  // Jika kategori tidak valid → return dengan detail asli
  if (!isValidCategory) {
    const originalDataResult = await request.query(originalDataQuery);
    const originalData = originalDataResult.recordset[0];

    return createResponse(false, {
      isValidFormat: true,
      isValidCategory: false,
      isValidWarehouse: false,
      isDuplicate: false,
      foundInStockOpname: false,
      idDiscrepancy: null,
      labelType,
      parsed,
      idWarehouse,
      detail: originalData ? {
        JmlhSak: originalData.JumlahSak ?? null,
        Berat: originalData?.Berat != null ? Number(Number(originalData.Berat).toFixed(2)) : null,
        Blok: originalData.Blok,
        IdLokasi: originalData.IdLokasi
      } : null,
      mesinInfo: isBonggolan ? mesinInfo : []
    }, categoryMessage);
  }
  
  // 7) Validasi warehouse terhadap NoSO
  const soWarehouseCheck = await request.query(`
    SELECT COUNT(*) AS count
    FROM StockOpname_h_WarehouseID
    WHERE NoSO = @noso AND IdWarehouse = ${idWarehouse}
  `);
  const isValidWarehouse = soWarehouseCheck.recordset[0].count > 0;

  // 8) Ambil detail utama
  const detailResult = await request.query(detailQuery);
  const detailData = detailResult.recordset[0];

  // Ditemukan pada sumber utama
  if (detailData) {

    if (!isValidWarehouse) {
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse: false,
        isDuplicate: false,
        foundInStockOpname: true,
        idDiscrepancy: 2,
        labelType,
        parsed,
        idWarehouse,
        detail: {
          ...detailData,
          Berat: detailData?.Berat != null ? Number(Number(detailData.Berat).toFixed(2)) : null
        },
        mesinInfo: isBonggolan ? mesinInfo : []
      }, `Warehouse tidak sesuai!`);
    }

    // Cek mismatch Blok/IdLokasi
    if (isBlokLokasiMismatch(detailData)) {
      return createResponse(false, {
        isValidFormat: true,
        isValidCategory: true,
        isValidWarehouse: isValidWarehouse,
        isDuplicate: false,
        foundInStockOpname: true,
        idDiscrepancy: null,
        labelType,
        parsed,
        idWarehouse,
        detail: {
          ...detailData,
          // jaga-jaga format angka
          Berat: detailData?.Berat != null ? Number(Number(detailData.Berat).toFixed(2)) : null
        },
        mesinInfo: isBonggolan ? mesinInfo : []
      }, `Lokasi label saat ini berada di ${detailData.Blok}${detailData.IdLokasi}`);
    }

    return createResponse(true, {
      isValidFormat: true,
      isValidCategory: true,
      isValidWarehouse: true,
      isDuplicate: false,
      foundInStockOpname: true,
      idDiscrepancy: null,
      labelType,
      parsed,
      idWarehouse,
      detail: {
        ...detailData,
        Berat: detailData?.Berat != null ? Number(Number(detailData.Berat).toFixed(2)) : null
      },
      mesinInfo: isBonggolan ? mesinInfo : []
    }, 'Label Valid');
  }



  // 9) Fallback (tidak ditemukan di query utama)
  const fallbackResult = await request.query(fallbackQuery);
  const fallbackData = fallbackResult.recordset[0];

  if (fallbackData && (fallbackData.JmlhSak > 0 || fallbackData.Berat > 0)) {
    return createResponse(false, {
      isValidFormat: true,
      isValidCategory: true,
      isValidWarehouse,
      isDuplicate: false,
      foundInStockOpname: false,
      idDiscrepancy: 3,
      labelType,
      parsed,
      idWarehouse: fallbackData.IdWarehouse || idWarehouse,
      detail: {
        JmlhSak: fallbackData.JumlahSak ?? null,
        Berat: fallbackData?.Berat != null ? Number(Number(fallbackData.Berat).toFixed(2)) : null,
        Blok: fallbackData.Blok,
        IdLokasi: fallbackData.IdLokasi
      },
      mesinInfo: isBonggolan ? mesinInfo : []
    }, 'Label tidak masuk dalam daftar Stock Opname');
  }

  // 10) Original (semua sudah diproses / truly not found)
  const originalDataResult = await request.query(originalDataQuery);
  const originalData = originalDataResult.recordset[0];

  if (originalData && (originalData.JumlahSak > 0 || originalData.Berat > 0)) {
    return createResponse(false, {
      isValidFormat: true,
      isValidCategory: true,
      isValidWarehouse,
      isDuplicate: false,
      foundInStockOpname: false,
      idDiscrepancy: 1,
      labelType,
      parsed,
      idWarehouse: originalData.IdWarehouse || idWarehouse,
      detail: {
        JmlhSak: 0,
        Berat: 0,
        Blok: '-',
        IdLokasi: '-'
      },
      mesinInfo: isBonggolan ? mesinInfo : []
    }, 'Item telah diproses!');
  }

  return createResponse(false, {
    isValidFormat: true,
    isValidCategory: true,
    isValidWarehouse,
    isDuplicate: false,
    foundInStockOpname: false,
    idDiscrepancy: 1,
    labelType,
    parsed,
    idWarehouse
  }, 'Item tidak ditemukan dalam sistem.');
}



/**
 * Insert hasil scan stock-opname + update lokasi + tulis log mapping lokasi.
 * SELALU dalam 1 transaksi agar konsisten.
 *
 * @param {Object} p
 * @param {string} p.noso
 * @param {string} p.label
 * @param {number} [p.jmlhSak=0]
 * @param {number} [p.berat=0]
 * @param {number} p.idlokasi
 * @param {string} p.blok                 // char(3)
 * @param {string} p.username             // untuk catatan hasil scan
 * @param {number} p.idUsername           // INT, untuk log (wajib jika ingin log berisi user id)
 */
async function insertStockOpnameLabel({
  noso, label, jmlhSak = 0, berat = 0, idlokasi, blok, username, idUsername, idDiscrepancy
}) {
  if (!label) throw new Error('Label wajib diisi');

  // deteksi tipe label
  const isBahanBaku    = label.startsWith('A.')  && label.includes('-');
  const isWashing      = label.startsWith('B.')  && !label.includes('-');
  const isBroker       = label.startsWith('D.')  && !label.includes('-');
  const isCrusher      = label.startsWith('F.')  && !label.includes('-');
  const isBonggolan    = label.startsWith('M.')  && !label.includes('-');
  const isGilingan     = label.startsWith('V.')  && !label.includes('-');
  const isMixer        = label.startsWith('H.')  && !label.includes('-');
  const isFurnitureWIP = label.startsWith('BB.') && !label.includes('-');
  const isBarangJadi   = label.startsWith('BA.') && !label.includes('-');
  const isReject       = label.startsWith('BF.') && !label.includes('-');

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin();

  try {
    const request = new sql.Request(tx);

    // common bindings
    request.input('noso',         sql.VarChar, noso);
    request.input('username',     sql.VarChar, username);
    request.input('jmlhSak',      sql.Int,     jmlhSak);
    request.input('berat',        sql.Float,   berat);
    request.input('DateTimeScan', sql.DateTime, new Date());
    request.input('idlokasi',     sql.Int,     idlokasi);  // INT
    request.input('blok',         sql.VarChar(3), blok);   // char(3)
    request.input('IdDiscrepancy',         sql.Int, idDiscrepancy);   // INT

    let insertedData = null;

    // ======= BAHAN BAKU =======
    if (isBahanBaku) {
      const [noBahanBaku, noPallet] = label.split('-');
      if (!noBahanBaku || !noPallet) throw new Error('Format label bahan baku tidak valid. Contoh: A.0001-1');

      request.input('NoBahanBaku', sql.VarChar, noBahanBaku);
      request.input('NoPallet',    sql.VarChar, noPallet);

      // BEFORE
      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM BahanBakuPallet_h
        WHERE NoBahanBaku=@NoBahanBaku AND NoPallet=@NoPallet
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      // INSERT hasil
      await request.query(`
        INSERT INTO StockOpnameHasilBahanBaku
          (NoSO, NoBahanBaku, NoPallet, JmlhSak, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoBahanBaku, @NoPallet, @jmlhSak, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      // UPDATE header
      const upd = await request.query(`
        UPDATE BahanBakuPallet_h
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoBahanBaku=@NoBahanBaku AND NoPallet=@NoPallet
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('Pallet tidak ditemukan di BahanBakuPallet_h');

      // LOG (hanya kalau berubah)
      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Bahan Baku', labelTypeCode: 'bahanbaku',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= WASHING =======
    } else if (isWashing) {
      request.input('NoWashing', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM Washing_h WHERE NoWashing=@NoWashing
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilWashing
          (NoSO, NoWashing, JmlhSak, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoWashing, @jmlhSak, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE Washing_h
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoWashing=@NoWashing
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoWashing tidak ditemukan di Washing_h');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Washing', labelTypeCode: 'washing',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= BROKER =======
    } else if (isBroker) {
      request.input('NoBroker', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM Broker_h WHERE NoBroker=@NoBroker
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilBroker
          (NoSO, NoBroker, JmlhSak, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoBroker, @jmlhSak, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE Broker_h
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoBroker=@NoBroker
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoBroker tidak ditemukan di Broker_h');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Broker', labelTypeCode: 'broker',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= CRUSHER =======
    } else if (isCrusher) {
      request.input('NoCrusher', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM Crusher WHERE NoCrusher=@NoCrusher
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilCrusher
          (NoSO, NoCrusher, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoCrusher, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE Crusher
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoCrusher=@NoCrusher
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoCrusher tidak ditemukan di tabel Crusher');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Crusher', labelTypeCode: 'crusher',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= BONGGOLAN =======
    } else if (isBonggolan) {
      request.input('NoBonggolan', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM Bonggolan WHERE NoBonggolan=@NoBonggolan
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilBonggolan
          (NoSO, NoBonggolan, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoBonggolan, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE Bonggolan
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoBonggolan=@NoBonggolan
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoBonggolan tidak ditemukan di tabel Bonggolan');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Bonggolan', labelTypeCode: 'bonggolan',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= GILINGAN =======
    } else if (isGilingan) {
      request.input('NoGilingan', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM Gilingan WHERE NoGilingan=@NoGilingan
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilGilingan
          (NoSO, NoGilingan, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoGilingan, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE Gilingan
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoGilingan=@NoGilingan
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoGilingan tidak ditemukan di tabel Gilingan');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Gilingan', labelTypeCode: 'gilingan',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= MIXER =======
    } else if (isMixer) {
      request.input('NoMixer', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM Mixer_h WHERE NoMixer=@NoMixer
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilMixer
          (NoSO, NoMixer, JmlhSak, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoMixer, @jmlhSak, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE Mixer_h
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoMixer=@NoMixer
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoMixer tidak ditemukan di Mixer_h');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Mixer', labelTypeCode: 'mixer',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= FURNITURE WIP =======
    } else if (isFurnitureWIP) {
      request.input('NoFurnitureWIP', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM FurnitureWIP WHERE NoFurnitureWIP=@NoFurnitureWIP
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilFurnitureWIP
          (NoSO, NoFurnitureWIP, Pcs, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoFurnitureWIP, @jmlhSak, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE FurnitureWIP
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoFurnitureWIP=@NoFurnitureWIP
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoFurnitureWIP tidak ditemukan di tabel FurnitureWIP');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Furniture WIP', labelTypeCode: 'furniturewip',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= BARANG JADI =======
    } else if (isBarangJadi) {
      request.input('NoBJ', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM BarangJadi WHERE NoBJ=@NoBJ
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilBarangJadi
          (NoSO, NoBJ, Pcs, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoBJ, @jmlhSak, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE BarangJadi
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoBJ=@NoBJ
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoBJ tidak ditemukan di tabel BarangJadi');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Barang Jadi', labelTypeCode: 'barangjadi',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    // ======= REJECT =======
    } else if (isReject) {
      request.input('NoReject', sql.VarChar, label);

      const before = await request.query(`
        SELECT TOP 1 Blok AS BeforeBlok, IdLokasi AS BeforeIdLokasi
        FROM RejectV2 WHERE NoReject=@NoReject
      `);
      const beforeBlok = before.recordset?.[0]?.BeforeBlok ?? null;
      const beforeId   = before.recordset?.[0]?.BeforeIdLokasi ?? null;

      await request.query(`
        INSERT INTO StockOpnameHasilReject
          (NoSO, NoReject, Berat, Username, DateTimeScan, IdDiscrepancy)
        VALUES (@noso, @NoReject, @berat, @username, @DateTimeScan, @idDiscrepancy)
      `);

      const upd = await request.query(`
        UPDATE RejectV2
        SET Blok=@blok, IdLokasi=@idlokasi
        WHERE NoReject=@NoReject
      `);
      if (upd.rowsAffected?.[0] === 0) throw new Error('NoReject tidak ditemukan di tabel RejectV2');

      if (beforeBlok !== blok || Number(beforeId) !== Number(idlokasi)) {
        await insertLogMappingLokasi({
          runner: tx, noLabel: label,
          beforeBlok, beforeIdLokasi: beforeId,
          afterBlok: blok, afterIdLokasi: idlokasi,
          idUsername: idUsername ?? null,
        });
      }

      insertedData = {
        noso, nomorLabel: label, labelType: 'Reject', labelTypeCode: 'reject',
        jmlhSak, berat, idlokasi, blok, username, timestamp: new Date()
      };

    } else {
      throw new Error('Kode label tidak dikenali. Valid: A., B., D., F., M., V., H., BB., BA., BF.');
    }

    // selesai OK → commit
    await tx.commit();

    // broadcast (di luar trx)
    if (global.io) {
      global.io.emit('label_inserted', insertedData);
    }

    return { success: true, message: 'Label berhasil disimpan dan lokasi diperbarui' };

  } catch (err) {
    await tx.rollback();
    throw err;
  }
}



//////////////////////////
////ASCEND SERVICES//////
////////////////////////


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
        -- Agregasi shelf per ItemID agar tidak menduplikasi baris
        WITH ShelfPerItem AS (
          SELECT
            iwd.ItemID,
            STRING_AGG(LTRIM(RTRIM(iwd.ShelfCode)), ', ') WITHIN GROUP (ORDER BY LTRIM(RTRIM(iwd.ShelfCode))) AS ShelfCodes
          FROM [AS_GSU_2022].[dbo].[IC_ItemWarehouseDetails] iwd
          WHERE iwd.ShelfCode IS NOT NULL AND LTRIM(RTRIM(iwd.ShelfCode)) <> ''
          GROUP BY iwd.ItemID
        )
        SELECT 
          so.NoSO,
          so.ItemID,
          it.ItemCode,
          it.ItemName,
          so.Pcs,
          sh.QtyFisik,
          sh.QtyUsage,
          sh.UsageRemark,
          sh.IsUpdateUsage,
          spi.ShelfCodes AS ShelfCode
        FROM [dbo].[StockOpnameAscend] so
        LEFT JOIN [AS_GSU_2022].[dbo].[IC_Items] it 
               ON so.ItemID = it.ItemID
        LEFT JOIN [dbo].[StockOpnameAscendHasil] sh 
               ON so.NoSO = sh.NoSO 
              AND so.ItemID = sh.ItemID
        LEFT JOIN ShelfPerItem spi
               ON spi.ItemID = so.ItemID
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
      ShelfCode: row.ShelfCode,
      ItemName: row.ItemName,
      Pcs: row.Pcs,
      QtyFisik: row.QtyFisik !== null ? row.QtyFisik : null,
      QtyUsage: row.QtyUsage !== null ? row.QtyUsage : -1.0,
      UsageRemark: row.UsageRemark,
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

const express = require('express');
const verifyToken = require('../middleware/verifyToken');  // Mengimpor middleware
const moment = require('moment');
const { sql, connectDb } = require('../db');
const router = express.Router();

const formatDate = (date) => {
    return moment(date).format('DD MMM YYYY');
};

// Route untuk mendapatkan Nomor Stock Opname
router.get('/no-stock-opname', verifyToken, async (req, res) => {
  console.log(`[${new Date().toISOString()}] 🔵 GET /no-stock-opname endpoint hit by user: ${req.user?.username || 'unknown'}`);

  try {
    await connectDb();

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
        soh.IsMixer
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
        soh.IsMixer
      ORDER BY soh.NoSO DESC
    `);

    if (!result.recordset || result.recordset.length === 0) {
      return res.status(404).json({ message: 'Tidak ada data Stock Opname ditemukan.' });
    }

    const formattedData = result.recordset.map(item => ({
      NoSO: item.NoSO,
      Tanggal: formatDate(item.Tanggal),
      NamaWarehouse: item.NamaWarehouse || '-',
      IsBahanBaku: item.IsBahanBaku,
      IsWashing: item.IsWashing,
      IsBonggolan: item.IsBonggolan,
      IsCrusher: item.IsCrusher,
      IsBroker: item.IsBroker,
      IsGilingan: item.IsGilingan,
      IsMixer: item.IsMixer,
    }));

    res.json(formattedData);
  } catch (error) {
    console.error('Error:', error.message);
    res.status(500).json({ message: 'Internal Server Error' });
  }
});

  



// Route untuk mendapatkan data Label berdasarkan No Stock Opname
router.get('/no-stock-opname/:noso/hasil', verifyToken, async (req, res) => {
  const { noso } = req.params;
  const page = parseInt(req.query.page) || 1;
  const pageSize = parseInt(req.query.pageSize) || 20;
  const filterBy = req.query.filterBy || 'all';
  const idLokasi = req.query.idlokasi; // Tambahkan filter idLokasi (huruf kecil)
  const offset = (page - 1) * pageSize;
  const { username } = req;

  console.log(`[${new Date().toISOString()}] StockOpnameHasil - ${username} mengakses kategori: ${filterBy}`);

  let pool;
  try {
    pool = await connectDb();
    const request = new sql.Request(pool);
    request.input('noso', sql.VarChar, noso);
    request.input('username', sql.VarChar, username);
    if (idLokasi && idLokasi !== 'all') {
      request.input('idLokasi', sql.VarChar, idLokasi);
    }

    const makeQuery = (table, labelExpr, labelType, joinClause) => `
      SELECT ${labelExpr} AS NomorLabel, '${labelType}' AS LabelType, so.JmlhSak, so.Berat, 
             ISNULL(so.DateTimeScan, '1900-01-01') AS DateTimeScan,
             detail.IdLokasi
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso AND so.Username = @username
      ${idLokasi && idLokasi !== 'all' ? 'AND detail.IdLokasi = @idLokasi' : ''}
    `;

    const makeCount = (table, joinClause) => `
      SELECT COUNT(*) AS total
      FROM ${table} so
      ${joinClause}
      WHERE so.NoSO = @noso AND so.Username = @username
      ${idLokasi && idLokasi !== 'all' ? 'AND detail.IdLokasi = @idLokasi' : ''}
    `;

    let query = '', totalQuery = '';

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
        ) detail ON so.NoBahanBaku = detail.NoBahanBaku AND so.NoPallet = detail.NoPallet AND detail.rn = 1`
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
        ) detail ON so.NoWashing = detail.NoWashing AND detail.rn = 1`
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
        ) detail ON so.NoBroker = detail.NoBroker AND detail.rn = 1`
      }
    };

    if (filterBy !== 'all') {
      const filter = filterMap[filterBy.toLowerCase()];
      if (!filter) return res.status(400).json({ message: 'Invalid filterBy' });

      query = `
        ${makeQuery(filter.table, filter.labelExpr, filter.label, filter.joinClause)}
        ORDER BY so.DateTimeScan DESC
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;
      totalQuery = makeCount(filter.table, filter.joinClause);
    } else {
      query = `
        SELECT * FROM (
          ${makeQuery('StockOpnameHasilBahanBaku', "CONCAT(so.NoBahanBaku, '-', so.NoPallet)", 'Bahan Baku', filterMap.bahanbaku.joinClause)}
          UNION ALL
          ${makeQuery('StockOpnameHasilWashing', 'so.NoWashing', 'Washing', filterMap.washing.joinClause)}
          UNION ALL
          ${makeQuery('StockOpnameHasilBroker', 'so.NoBroker', 'Broker', filterMap.broker.joinClause)}
        ) AS hasil
        ORDER BY DateTimeScan DESC
        OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY;
      `;

      totalQuery = `
        SELECT SUM(total) AS total FROM (
          ${makeCount('StockOpnameHasilBahanBaku', filterMap.bahanbaku.joinClause)}
          UNION ALL
          ${makeCount('StockOpnameHasilWashing', filterMap.washing.joinClause)}
          UNION ALL
          ${makeCount('StockOpnameHasilBroker', filterMap.broker.joinClause)}
        ) AS totalData;
      `;
    }

    const [result, total] = await Promise.all([
      request.query(query),
      request.query(totalQuery)
    ]);

    const formattedData = result.recordset.map(item => ({
      ...item,
      DateTimeScan:
        item.DateTimeScan && item.DateTimeScan !== '1900-01-01'
          ? moment(item.DateTimeScan).format('DD MMM YYYY')
          : '-'
    }));

    res.json({
      data: formattedData,
      hasData: formattedData.length > 0,
      currentPage: page,
      pageSize,
      totalData: total.recordset[0].total,
      totalPages: Math.ceil(total.recordset[0].total / pageSize)
    });

  } catch (err) {
    console.error('❌ Error:', err.message);
    res.status(500).json({ message: 'Internal Server Error', error: err.message });
  } finally {
    if (pool) await pool.close();
  }
});



//PRECONDITION CHECK LABEL 
router.post('/no-stock-opname/:noso/validate-label', verifyToken, async (req, res) => {
    const { noso } = req.params;
    const { label } = req.body;
    const { username } = req;

    // Standard response format - semua field sejajar
    const createResponse = (success, data = {}, message = '', statusCode = 200) => {
        return res.status(statusCode).json({
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
            // Detail fields - flatten
            jmlhSak: data.detail?.JmlhSak || null,
            berat: data.detail?.Berat || null,
            idLokasi: data.detail?.IdLokasi || null
        });
    };

    // Validasi input dasar
    if (!label) {
        return createResponse(false, {}, 'Label wajib diisi', 400);
    }

    // 1. VALIDASI FORMAT LABEL
    const isBahanBaku = label.startsWith('A.') && label.includes('-');
    const isWashing = label.startsWith('B.') && !label.includes('-');
    const isBroker = label.startsWith('D.') && !label.includes('-');

    if (!isBahanBaku && !isWashing && !isBroker) {
        return createResponse(false, {
            isValidFormat: false
        }, 'Kode label tidak dikenali. Hanya A., B., atau D. yang valid.', 400);
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

        // 2. SETUP QUERIES BERDASARKAN TIPE LABEL
        if (isBahanBaku) {
            labelType = 'Bahan Baku';
            const [noBahanBaku, noPallet] = label.split('-');
            if (!noBahanBaku || !noPallet) {
                return createResponse(false, {
                    isValidFormat: false,
                    labelType
                }, 'Format label bahan baku tidak valid. Contoh: A.0001-1', 400);
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
            fallbackQuery = `
                SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi
                FROM BahanBaku_d
                WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet AND DateUsage IS NULL
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
            fallbackQuery = `
                SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi
                FROM Washing_d
                WHERE NoWashing = @NoWashing AND DateUsage IS NULL
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
            fallbackQuery = `
                SELECT COUNT(*) AS JumlahSak, SUM(Berat) AS Berat, MAX(IdLokasi) AS IdLokasi
                FROM Broker_d
                WHERE NoBroker = @NoBroker AND DateUsage IS NULL
            `;
        }

        // 3. CEK DUPLIKASI PERTAMA KALI (EARLY DUPLICATE CHECK)
        const checkResult = await request.query(checkQuery);
        const isDuplicate = checkResult.recordset[0].count > 0;

        // Jika duplikat, langsung return tanpa validasi lainnya
        if (isDuplicate) {
            return createResponse(false, {
                isValidFormat: true,
                isValidCategory: true, // Asumsi true karena sudah pernah divalidasi sebelumnya
                isValidWarehouse: true, // Asumsi true karena sudah pernah divalidasi sebelumnya
                isDuplicate: true,
                foundInStockOpname: true,
                canInsert: false,
                labelType,
                parsed,
                idWarehouse: null // Akan diisi nanti jika diperlukan
            }, 'Label sudah pernah discan sebelumnya.', 400);
        }

        // 4. VALIDASI KUALIFIKASI NOSO
        const nosoQualificationCheck = await request.query(`
            SELECT IsBahanBaku, IsWashing, IsBroker, IsBonggolan, IsCrusher, IsGilingan, IsMixer
            FROM StockOpname_h
            WHERE NoSO = @noso
        `);
        
        if (nosoQualificationCheck.recordset.length === 0) {
            return createResponse(false, {
                isValidFormat: true,
                isDuplicate: false
            }, 'NoSO tidak ditemukan dalam sistem.', 404);
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
        }

        // 5. AMBIL ID WAREHOUSE
        if (isBahanBaku) {
            const whResult = await request.query(`
                SELECT IdWarehouse FROM BahanBakuPallet_h
                WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
            `);
            idWarehouse = whResult.recordset[0]?.IdWarehouse ?? null;
        } else if (isWashing) {
            const whResult = await request.query(`
                SELECT IdWarehouse FROM Washing_h
                WHERE NoWashing = @NoWashing
            `);
            idWarehouse = whResult.recordset[0]?.IdWarehouse ?? null;
        } else if (isBroker) {
            const whResult = await request.query(`
                SELECT IdWarehouse FROM Broker_h
                WHERE NoBroker = @NoBroker
            `);
            idWarehouse = whResult.recordset[0]?.IdWarehouse ?? null;
        }

        // Jika kategori tidak valid, return dengan format standard
        if (!isValidCategory) {
            return createResponse(false, {
                isValidFormat: true,
                isValidCategory: false,
                isDuplicate: false,
                labelType,
                parsed,
                idWarehouse
            }, categoryMessage, 400);
        }

        // 6. CEK WAREHOUSE
        if (!idWarehouse) {
            return createResponse(false, {
                isValidFormat: true,
                isValidCategory: true,
                isValidWarehouse: false,
                isDuplicate: false,
                labelType,
                parsed,
                idWarehouse
            }, 'Label tidak valid atau warehouse tidak ditemukan di sumber.', 404);
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
                                }, `Label ini tidak tersedia pada warehouse NoSO ini (IdWarehouse: ${idWarehouse}).`, 400);
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
              }            }, 'Label valid dan siap disimpan.', 200);
        }

        // 9. TIDAK DITEMUKAN DI STOCKOPNAME → CEK FALLBACK DATA
        const fallbackResult = await request.query(fallbackQuery);
        const fallbackData = fallbackResult.recordset[0];

        if (fallbackData && fallbackData.JumlahSak > 0) {
            return createResponse(false, {
                isValidFormat: true,
                isValidCategory: true,
                isValidWarehouse,
                isDuplicate: false,
                foundInStockOpname: false,
                canInsert: false,
                labelType,
                parsed,
                idWarehouse,
                detail: {
                  JmlhSak: fallbackData.JumlahSak,
                  Berat: fallbackData?.Berat != null ? Number(fallbackData.Berat.toFixed(2)) : null,
                  IdLokasi: fallbackData.IdLokasi
              }
              
            }, 'Item tidak masuk dalam daftar Stock Opname.', 404);
        }

        // 10. TIDAK DITEMUKAN SAMA SEKALI ATAU SEMUA SUDAH DIPROSES
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
        }, 'Item telah diproses.', 404);

    } catch (err) {
        console.error('❌ Validasi Label Error:', err.message);
        return createResponse(false, {}, 'Gagal memvalidasi label', 500);
    } finally {
        if (pool) await pool.close();
    }
});
  
  
  
  
  router.post('/no-stock-opname/:noso/insert-label', verifyToken, async (req, res) => {
    const { noso } = req.params;
    const { label, jmlhSak = 0, berat = 0, idlokasi } = req.body;
    const { username } = req;
  
    if (!label) {
      return res.status(400).json({ message: 'Label wajib diisi' });
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
  
      if (isBahanBaku) {
        const [noBahanBaku, noPallet] = label.split('-');
        if (!noBahanBaku || !noPallet) {
          return res.status(400).json({ message: 'Format label bahan baku tidak valid. Contoh: A.0001-1' });
        }
  
        request.input('NoBahanBaku', sql.VarChar, noBahanBaku);
        request.input('NoPallet', sql.VarChar, noPallet);
  
        // INSERT ke hasil opname
        await request.query(`
          INSERT INTO StockOpnameHasilBahanBaku
          (NoSO, NoBahanBaku, NoPallet, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoBahanBaku, @NoPallet, @jmlhSak, @berat, @username, @DateTimeScan)
        `);
  
        // UPDATE lokasi di tabel utama
        await request.query(`
          UPDATE BahanBaku_d
          SET IdLokasi = @idlokasi
          WHERE NoBahanBaku = @NoBahanBaku AND NoPallet = @NoPallet
        `);
  
      } else if (isWashing) {
        request.input('NoWashing', sql.VarChar, label);
  
        // INSERT ke hasil opname
        await request.query(`
          INSERT INTO StockOpnameHasilWashing
          (NoSO, NoWashing, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoWashing, @jmlhSak, @berat, @username, @DateTimeScan)
        `);
  
        // UPDATE lokasi di tabel utama
        await request.query(`
          UPDATE Washing_d
          SET IdLokasi = @idlokasi
          WHERE NoWashing = @NoWashing
        `);
  
      } else if (isBroker) {
        request.input('NoBroker', sql.VarChar, label);
  
        // INSERT ke hasil opname
        await request.query(`
          INSERT INTO StockOpnameHasilBroker
          (NoSO, NoBroker, JmlhSak, Berat, Username, DateTimeScan)
          VALUES (@noso, @NoBroker, @jmlhSak, @berat, @username, @DateTimeScan)
        `);
  
        // UPDATE lokasi di tabel utama
        await request.query(`
          UPDATE Broker_d
          SET IdLokasi = @idlokasi
          WHERE NoBroker = @NoBroker
        `);
  
      } else {
        return res.status(400).json({
          message: 'Kode label tidak dikenali dalam sistem. Hanya label dengan awalan A., B., atau D. yang valid.'
        });
      }
  
      res.json({ success: true, message: 'Label berhasil disimpan dan lokasi diperbarui' });
  
    } catch (err) {
      console.error('❌ Insert Label Error:', err.message);
      res.status(500).json({ message: 'Gagal menyimpan label', error: err.message });
    } finally {
      if (pool) await pool.close();
    }
  });
  





module.exports = router;
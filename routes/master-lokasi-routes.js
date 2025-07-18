const express = require('express');
const verifyToken = require('../middleware/verifyToken');  // Mengimpor middleware
const { sql, connectDb } = require('../db');
const router = express.Router();

// ========================================
// ENDPOINT 1: Master Lokasi
// ========================================
router.get('/mst-lokasi', verifyToken, async (req, res) => {
    const { username } = req;
  
    console.log("🔍 Fetching MstLokasi data | Username:", username);
  
    let pool;
    try {
      pool = await connectDb();
      const request = new sql.Request(pool);
  
      // Query untuk mengambil data master lokasi
      const query = `
        SELECT IdLokasi, Blok, Enable
        FROM MstLokasi 
        WHERE Enable = 1
        ORDER BY Blok ASC
      `;
  
      const result = await request.query(query);
  
      if (!result.recordset || result.recordset.length === 0) {
        return res.status(404).json({
          message: 'Data MstLokasi tidak ditemukan',
          data: []
        });
      }
  
      res.json({
        success: true,
        message: 'Data MstLokasi berhasil diambil',
        data: result.recordset,
        totalData: result.recordset.length
      });
  
    } catch (error) {
      console.error('Error fetching MstLokasi:', error);
      res.status(500).json({
        success: false,
        message: 'Internal Server Error',
        error: error.message
      });
    } finally {
      if (pool) await pool.close();
    }
  });

  module.exports = router;
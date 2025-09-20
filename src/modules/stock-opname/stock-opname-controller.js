const {
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
  } = require('./stock-opname-service');
  
async function noStockOpnameHandler(req, res) {
  console.log(`[${new Date().toISOString()}] 🔵 GET /no-stock-opname endpoint hit by user: ${req.user?.username || 'unknown'}`);

  try {
    const data = await getNoStockOpname();
    if (!data) {
      return res.status(404).json({ message: 'Saat ini sedang tidak ada Jadwal Stock Opname' });
    }
    res.json(data);
  } catch (error) {
    console.error('Error:', error.message);
    res.status(500).json({ message: 'Internal Server Error' });
  }
}

async function stockOpnameAcuanHandler(req, res) {
  const { noso } = req.params;
  const page = parseInt(req.query.page) || 1;
  const pageSize = parseInt(req.query.pageSize) || 20;
  const filterBy = req.query.filterBy || 'all';
  const idLokasi = req.query.idlokasi;
  const search = req.query.search || '';
  const { username } = req;

  console.log(`[${new Date().toISOString()}] StockOpnameAcuan - ${username} mengakses kategori: ${filterBy} search: ${search}`);

  try {
    const result = await getStockOpnameAcuan({ noso, page, pageSize, filterBy, idLokasi, search });
    res.json(result);
  } catch (err) {
    console.error('❌ Error:', err.message);
    res.status(500).json({ message: 'Internal Server Error', error: err.message });
  }
}

async function stockOpnameHasilHandler(req, res) {
    const { noso } = req.params;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 20;
    const filterBy = req.query.filterBy || 'all';
    const idLokasi = req.query.idlokasi;
    const search = req.query.search || '';
    const filterByUser = req.query.filterbyuser === 'true';
    const { username } = req;
  
    console.log(`[${new Date().toISOString()}] StockOpnameHasil - ${username} mengakses kategori: ${filterBy} | filterByUser=${filterByUser} | search="${search}"`);
  
    try {
      const result = await getStockOpnameHasil({
        noso,
        page,
        pageSize,
        filterBy,
        idLokasi,
        search,
        filterByUser,
        username
      });
      res.json(result);
    } catch (err) {
      console.error('❌ Error:', err.message);
      res.status(500).json({ message: 'Internal Server Error', error: err.message });
    }
  }


  async function deleteStockOpnameHasilHandler(req, res) {
    const { noso } = req.params;
    const { nomorLabel } = req.body;
  
    try {
      const result = await deleteStockOpnameHasil({ noso, nomorLabel });
      if (!result.success) {
        return res.status(404).json({ message: result.message });
      }
      res.json({ message: result.message });
    } catch (err) {
      res.status(400).json({ message: err.message });
    }
  }
  

  
  async function validateStockOpnameLabelHandler(req, res) {
    const { noso } = req.params;
    const { label } = req.body;
    const { username } = req;
  
    try {
      const result = await validateStockOpnameLabel({ noso, label, username });
      res.status(result.success ? 200 : 400).json(result);
    } catch (err) {
      res.status(500).json({ message: 'Gagal memvalidasi label', error: err.message });
    }
  }

  
  async function insertStockOpnameLabelHandler(req, res) {
    const { noso } = req.params;
    const { label, jmlhSak = 0, berat = 0, idlokasi } = req.body;
    const { username } = req;
  
    try {
      const result = await insertStockOpnameLabel({
        noso, label, jmlhSak, berat, idlokasi, username
      });
      res.json(result);
    } catch (err) {
      res.status(400).json({ message: err.message });
    }
  }

  async function stockOpnameFamiliesHandler(req, res) {
    const { noso } = req.params;
  
    console.log(`[${new Date().toISOString()}] 🔵 GET /no-stock-opname/${noso}/families`);
  
    try {
      const families = await getStockOpnameFamilies(noso);
      if (!families) {
        return res.status(404).json({ message: 'Family Stock Opname tidak ditemukan' });
      }
      res.json(families);
    } catch (err) {
      console.error('❌ Error:', err.message);
      res.status(500).json({ message: 'Internal Server Error', error: err.message });
    }
  }
  

  async function stockOpnameAscendDataHandler(req, res) {
    const { noso, familyid } = req.params;
    const keyword = req.query.keyword || '';
  
    console.log(`[${new Date().toISOString()}] 🔵 GET /no-stock-opname/${noso}/families/${familyid}/ascend?keyword=${keyword}`);
  
    try {
      const data = await getStockOpnameAscendData({ noSO: noso, familyID: familyid, keyword });
      res.json(data);
    } catch (err) {
      console.error('❌ Error:', err.message);
      res.status(500).json({ message: 'Internal Server Error', error: err.message });
    }
  }
  

  async function saveStockOpnameAscendHasilHandler(req, res) {
    const { noso } = req.params;
    const dataList = req.body.dataList; // kirim array data dari frontend
  
    if (!Array.isArray(dataList) || dataList.length === 0) {
      return res.status(400).json({ message: 'dataList harus berupa array dan tidak boleh kosong' });
    }
  
    try {
      const result = await saveStockOpnameAscendHasil(noso, dataList);
      res.json(result);
    } catch (err) {
      console.error('❌ Error:', err.message);
      res.status(500).json({ message: 'Internal Server Error', error: err.message });
    }
  }


  async function fetchQtyUsageHandler(req, res) {
    const { itemId } = req.params;
    const { tglSO } = req.query;  // ambil dari query string
  
    if (!itemId || !tglSO) {
      return res.status(400).json({ message: 'itemId dan tglSO harus disertakan' });
    }
  
    try {
      const qtyUsage = await fetchQtyUsage(itemId, tglSO);
      res.json({ itemId, tglSO, qtyUsage });
    } catch (err) {
      console.error('❌ Error fetchQtyUsage:', err.message);
      res.status(500).json({ message: 'Internal Server Error', error: err.message });
    }
  }
  
  

  async function deleteStockOpnameHasilAscendHandler(req, res) {
    try {
      const { noso, itemId } = req.params;
  
      if (!noso || !itemId) {
        return res.status(400).json({ message: 'noso dan itemId wajib diisi' });
      }
  
      const parsedItemId = parseInt(itemId, 10);
      if (Number.isNaN(parsedItemId)) {
        return res.status(400).json({ message: 'itemId harus berupa angka' });
      }
  
      const { deletedCount } = await deleteStockOpnameHasilAscend(noso, parsedItemId);
  
      if (!deletedCount) {
        return res.status(404).json({ message: 'Data tidak ditemukan / sudah terhapus' });
      }
  
      return res.json({
        message: 'OK',
        noso,
        itemId: parsedItemId,
        deleted: deletedCount,
      });
    } catch (err) {
      console.error('❌ deleteStockOpnameHasilAscendHandler error:', err);
      res.status(500).json({ message: 'Internal Server Error', error: err.message });
    }
  }

  
  module.exports = {
    noStockOpnameHandler,
    stockOpnameAcuanHandler,
    stockOpnameHasilHandler,
    deleteStockOpnameHasilHandler,
    validateStockOpnameLabelHandler,
    insertStockOpnameLabelHandler,
    stockOpnameFamiliesHandler,
    stockOpnameAscendDataHandler,
    saveStockOpnameAscendHasilHandler,
    fetchQtyUsageHandler,
    deleteStockOpnameHasilAscendHandler
  };
  
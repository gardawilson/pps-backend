const {
    getNoStockOpname,
    getStockOpnameAcuan,
    getStockOpnameHasil,
    deleteStockOpnameHasil,
    validateStockOpnameLabel,
    insertStockOpnameLabel
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
  
  
  module.exports = {
    noStockOpnameHandler,
    stockOpnameAcuanHandler,
    stockOpnameHasilHandler,
    deleteStockOpnameHasilHandler,
    validateStockOpnameLabelHandler,
    insertStockOpnameLabelHandler
  };
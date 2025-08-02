const {
    getAllWashingData,
    getWashingDetailByNoWashing,
    insertWashingData,
    insertWashingDetailData 
  } = require('./label-washing-service');
  
  async function getLabelList(req, res) {
    try {
      const result = await getAllWashingData();
      return res.json({ success: true, data: result });
    } catch (err) {
      console.error('Get Washing List Error:', err);
      return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
    }
  }


  const getDetailLabel = async (req, res) => {
    const { nowashing } = req.params;
  
    try {
      const result = await getWashingDetailByNoWashing(nowashing);
  
      if (!result || result.length === 0) {
        return res.status(404).json({
          success: false,
          message: `Data tidak ditemukan untuk NoWashing ${nowashing}`
        });
      }
  
      return res.json({ success: true, data: result });
  
    } catch (err) {
      console.error('Get Washing_d Error:', err);
      return res.status(500).json({ success: false, message: 'Terjadi kesalahan server' });
    }
  };
  
  
  const createWashingData = async (req, res) => {
    const {
      IdJenisPlastik,
      IdWarehouse,
      DateCreate,
      IdStatus,
      CreateBy
    } = req.body;
  
    try {
      if (
        IdJenisPlastik === undefined ||
        IdWarehouse === undefined ||
        !DateCreate ||
        IdStatus === undefined ||
        !CreateBy
      ) {
        return res.status(400).json({
          success: false,
          message: 'Field tidak lengkap',
          received: {
            IdJenisPlastik,
            IdWarehouse,
            DateCreate,
            IdStatus,
            CreateBy
          }
        });
      }
  
      const result = await insertWashingData({
        IdJenisPlastik,
        IdWarehouse,
        DateCreate,
        IdStatus,
        CreateBy
      });
  
      return res.status(201).json({
        success: true,
        message: 'Data berhasil disimpan',
        data: result
      });
  
    } catch (error) {
      console.error('Insert Washing_h Error:', error);
      return res.status(500).json({
        success: false,
        message: 'Terjadi kesalahan saat menyimpan data',
        error: error.message
      });
    }
  };

  const createWashingDetail = async (req, res) => {
    try {
      const dataArray = req.body;
  
      if (!Array.isArray(dataArray) || dataArray.length === 0) {
        return res.status(400).json({ success: false, message: 'Data harus berupa array dan tidak boleh kosong' });
      }
  
      const results = await insertWashingDetailData(dataArray);
  
      res.status(201).json({
        success: true,
        message: 'Semua detail berhasil disimpan',
        data: results
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Gagal simpan detail',
        error: error.message
      });
    }
  };
  
  
  
  
  module.exports = {
    getLabelList,
    getDetailLabel,
    createWashingData,
    createWashingDetail,
  };
  
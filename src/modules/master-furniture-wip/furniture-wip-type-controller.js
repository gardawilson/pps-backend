// src/modules/master/furniture-wip-type-controller.js
const service = require('./furniture-wip-type-service');

async function getAllActive(req, res) {
  const { username } = req;
  console.log(
    '🔍 Fetching MstCabinetWIP (active only / furniture WIP) | Username:',
    username
  );

  try {
    const data = await service.getAllActive();
    return res.status(200).json({
      success: true,
      message: 'Active Furniture WIP master data fetched successfully',
      totalData: data.length,
      data,
    });
  } catch (error) {
    console.error('Error fetching MstCabinetWIP (active):', error);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: error.message,
    });
  }
}

module.exports = { getAllActive };

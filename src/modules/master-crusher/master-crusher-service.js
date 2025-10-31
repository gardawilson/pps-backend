const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdCrusher,
      NamaCrusher,
      Enable
    FROM [PPS_TEST2].[dbo].[MstCrusher]
    WHERE ISNULL(Enable, 1) = 1
    ORDER BY NamaCrusher ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getAllActive };

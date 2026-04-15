const { poolPromise } = require("../../../core/config/db");

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT TOP (1000)
      IdBroker,
      Nama,
      IdUOM,
      IdForm,
      PicPacking,
      PicContent,
      ItemCode,
      IsEnable,
      IsReject,
      IsDisableMinMax
    FROM [dbo].[MstBroker]
    WHERE ISNULL(IsEnable, 1) = 1
    ORDER BY Nama ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getAllActive };

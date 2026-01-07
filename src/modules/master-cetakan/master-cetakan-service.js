const sql = require('mssql');
const { poolPromise } = require('../../core/config/db');

async function getAllActive() {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT
      IdCetakan,
      IdBJ,
      ISNULL(Enable, 1) AS Enable,
      NamaCetakan,
      Lebar,
      Panjang,
      Tebal,
      BeratCetakan,
      BeratCavity,
      JumlahCavity,
      HotRunner,
      HydrolicCore,
      ElectricalSwitch,
      InputAngin,
      InputAir,
      CycleTime,
      PcsPerJam
    FROM [dbo].[MstCetakan]
    WHERE ISNULL(Enable, 1) = 1
    ORDER BY NamaCetakan ASC;
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { getAllActive };

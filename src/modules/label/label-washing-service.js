const { sql, connectDb } = require('../../core/config/db');
const { formatDate } = require('../../core/utils/date-helper');

const getAllWashingData = async () => {
  await connectDb();

  const result = await sql.query`
    SELECT TOP 100
    FROM Washing_h
    ORDER BY DateTimeCreate DESC
  `;

  return result.recordset.map(item => ({
    ...item,
    ...(item.DateCreate && { DateCreate: formatDate(item.DateCreate) }),
    ...(item.DateTimeCreate && { DateTimeCreate: formatDate(item.DateTimeCreate) })
  }));
};

const getWashingDetailByNoWashing = async (nowashing) => {
  await connectDb();

  const result = await sql.query`
    SELECT *
    FROM Washing_d
    WHERE NoWashing = ${nowashing}
    ORDER BY NoSak
  `;

  return result.recordset.map(item => ({
    ...item,
    ...(item.DateUsage && { DateUsage: formatDate(item.DateUsage) })
  }));
};

const insertWashingData = async (data) => {
  const {
    IdJenisPlastik,
    IdWarehouse,
    DateCreate,
    IdStatus,
    CreateBy
  } = data;

  await connectDb();

  // Ambil NoWashing terakhir
  const last = await sql.query`
    SELECT TOP 1 NoWashing 
    FROM Washing_h 
    WHERE NoWashing LIKE 'B.%'
    ORDER BY NoWashing DESC
  `;

  let newNoWashing = 'B.0000000001';

  if (last.recordset.length > 0) {
    const lastCode = last.recordset[0].NoWashing;
    const numberPart = parseInt(lastCode.split('.')[1], 10);
    const nextNumber = numberPart + 1;
    const padded = nextNumber.toString().padStart(10, '0');
    newNoWashing = `B.${padded}`;
  }

  await sql.query`
    INSERT INTO Washing_h (
      NoWashing,
      IdJenisPlastik,
      IdWarehouse,
      DateCreate,
      IdStatus,
      CreateBy,
      DateTimeCreate
    ) VALUES (
      ${newNoWashing},
      ${IdJenisPlastik},
      ${IdWarehouse},
      ${DateCreate},
      ${IdStatus},
      ${CreateBy},
      GETDATE()
    )
  `;

  return { NoWashing: newNoWashing };
};

const insertWashingDetailData = async (dataList) => {
    await connectDb();
  
    for (const { NoWashing, NoSak, Berat, DateUsage, IdLokasi } of dataList) {
      await sql.query`
        INSERT INTO Washing_d (NoWashing, NoSak, Berat, DateUsage, IdLokasi)
        VALUES (
          ${NoWashing},
          ${NoSak},
          ${Berat},
          ${DateUsage},
          ${IdLokasi}
        )
      `;
    }
  
    return dataList;
  };
  

module.exports = {
  getAllWashingData,
  getWashingDetailByNoWashing,
  insertWashingData,
  insertWashingDetailData // ✅ pastikan ini diekspor
};

const { poolPromise, sql } = require('../../../core/config/db');
const { badReq, notFound } = require('../../../core/utils/http-error');

const MAC_ADDRESS_REGEX = /^([0-9A-F]{2}:){5}[0-9A-F]{2}$/;

function normalizeMacAddress(macAddress) {
  const normalized = String(macAddress || '').trim().toUpperCase();

  if (!MAC_ADDRESS_REGEX.test(normalized)) {
    throw badReq('MacAddress tidak valid. Gunakan format AA:BB:CC:DD:EE:FF');
  }

  return normalized;
}

async function listAll() {
  const pool = await poolPromise;
  const result = await pool.request().query(`
    SELECT
      Id,
      MacAddress,
      Alias,
      Description,
      UpdatedBy,
      CreatedAt,
      UpdatedAt
    FROM dbo.MstPrinter
    ORDER BY Alias ASC, MacAddress ASC;
  `);

  return result.recordset || [];
}

async function upsertByMacAddress({ macAddress, alias, description = null, updatedBy = null }) {
  const normalizedMacAddress = normalizeMacAddress(macAddress);
  const normalizedAlias = String(alias || '').trim();
  const normalizedDescription =
    description == null || String(description).trim() === ''
      ? null
      : String(description).trim();
  const normalizedUpdatedBy =
    updatedBy == null || String(updatedBy).trim() === ''
      ? null
      : String(updatedBy).trim();

  if (!normalizedAlias) {
    throw badReq('Alias wajib diisi');
  }

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  try {
    await tx.begin();

    const request = new sql.Request(tx)
      .input('MacAddress', sql.VarChar(17), normalizedMacAddress)
      .input('Alias', sql.NVarChar(100), normalizedAlias)
      .input('Description', sql.NVarChar(255), normalizedDescription)
      .input('UpdatedBy', sql.NVarChar(50), normalizedUpdatedBy);

    await request.query(`
      IF EXISTS (
        SELECT 1
        FROM dbo.MstPrinter WITH (UPDLOCK, HOLDLOCK)
        WHERE MacAddress = @MacAddress
      )
      BEGIN
        UPDATE dbo.MstPrinter
        SET
          Alias = @Alias,
          Description = @Description,
          UpdatedBy = @UpdatedBy,
          UpdatedAt = GETDATE()
        WHERE MacAddress = @MacAddress;
      END
      ELSE
      BEGIN
        INSERT INTO dbo.MstPrinter (
          MacAddress,
          Alias,
          Description,
          UpdatedBy,
          CreatedAt,
          UpdatedAt
        )
        VALUES (
          @MacAddress,
          @Alias,
          @Description,
          @UpdatedBy,
          GETDATE(),
          GETDATE()
        );
      END
    `);

    const dataResult = await new sql.Request(tx)
      .input('MacAddress', sql.VarChar(17), normalizedMacAddress)
      .query(`
        SELECT
          Id,
          MacAddress,
          Alias,
          Description,
          UpdatedBy,
          CreatedAt,
          UpdatedAt
        FROM dbo.MstPrinter
        WHERE MacAddress = @MacAddress;
      `);

    await tx.commit();
    return dataResult.recordset[0] || null;
  } catch (error) {
    try {
      await tx.rollback();
    } catch (_) {}

    throw error;
  }
}

async function deleteByMacAddress(macAddress) {
  const normalizedMacAddress = normalizeMacAddress(macAddress);
  const pool = await poolPromise;
  const result = await pool
    .request()
    .input('MacAddress', sql.VarChar(17), normalizedMacAddress)
    .query(`
      DELETE FROM dbo.MstPrinter
      WHERE MacAddress = @MacAddress;
    `);

  if ((result.rowsAffected && result.rowsAffected[0]) === 0) {
    throw notFound('Data printer tidak ditemukan');
  }

  return true;
}

module.exports = {
  listAll,
  normalizeMacAddress,
  upsertByMacAddress,
  deleteByMacAddress,
};

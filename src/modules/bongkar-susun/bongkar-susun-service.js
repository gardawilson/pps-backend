// services/bongkar-susun-service.js
const { sql, poolPromise } = require('../../core/config/db');

async function getByDate(date /* 'YYYY-MM-DD' */) {
  const pool = await poolPromise;
  const request = pool.request();

  const query = `
    SELECT 
      NoBongkarSusun,
      Tanggal,
      IdUsername,
      Note
    FROM BongkarSusun_h
    WHERE CONVERT(date, Tanggal) = @date
    ORDER BY Tanggal DESC;
  `;

  request.input('date', sql.Date, date);

  const result = await request.query(query);
  return result.recordset;
}

/**
 * Paginated fetch for dbo.BongkarSusun_h
 * Columns:
 *  NoBongkarSusun, Tanggal, IdUsername, Note + Username (from MstUsername)
 */
async function getAllBongkarSusun(page = 1, pageSize = 20, search = '') {
  const pool = await poolPromise;
  const offset = (page - 1) * pageSize;
  const searchTerm = (search || '').trim();

  const whereClause = `
    WHERE (@search = '' OR h.NoBongkarSusun LIKE '%' + @search + '%')
  `;

  // 1) Hitung total
  const countQry = `
    SELECT COUNT(1) AS total
    FROM dbo.BongkarSusun_h h WITH (NOLOCK)
    ${whereClause};
  `;

  const countReq = pool.request();
  countReq.input('search', sql.VarChar(100), searchTerm);
  const countRes = await countReq.query(countQry);

  const total = countRes.recordset?.[0]?.total || 0;
  if (total === 0) {
    return { data: [], total };
  }

  // 2) Ambil page data (JOIN ke MstUsername)
  const dataQry = `
    SELECT
      h.NoBongkarSusun,
      h.Tanggal,
      h.IdUsername,
      u.Username,
      h.Note
    FROM dbo.BongkarSusun_h h WITH (NOLOCK)
    LEFT JOIN dbo.MstUsername u WITH (NOLOCK)
      ON u.IdUsername = h.IdUsername
    ${whereClause}
    ORDER BY h.NoBongkarSusun DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const dataReq = pool.request();
  dataReq.input('search', sql.VarChar(100), searchTerm);
  dataReq.input('offset', sql.Int, offset);
  dataReq.input('limit', sql.Int, pageSize);

  const dataRes = await dataReq.query(dataQry);

  return {
    data: dataRes.recordset || [],
    total,
  };
}

function badReq(msg) {
  const e = new Error(msg);
  e.statusCode = 400;
  return e;
}

function padLeft(num, width) {
  const s = String(num);
  return s.length >= width ? s : '0'.repeat(width - s.length) + s;
}

async function generateNextNoBongkarSusun(
  tx,
  { prefix = 'BG.', width = 10 } = {}
) {
  const rq = new sql.Request(tx);
  const q = `
    SELECT TOP 1 h.NoBongkarSusun
    FROM dbo.BongkarSusun_h AS h WITH (UPDLOCK, HOLDLOCK)
    WHERE h.NoBongkarSusun LIKE @prefix + '%'
    ORDER BY
      TRY_CONVERT(BIGINT, SUBSTRING(h.NoBongkarSusun, LEN(@prefix) + 1, 50)) DESC,
      h.NoBongkarSusun DESC;
  `;
  const r = await rq.input('prefix', sql.VarChar, prefix).query(q);

  let lastNum = 0;
  if (r.recordset.length > 0) {
    const last = r.recordset[0].NoBongkarSusun;
    const numericPart = last.substring(prefix.length);
    lastNum = parseInt(numericPart, 10) || 0;
  }
  const next = lastNum + 1;
  return prefix + padLeft(next, width);
}

// ===========================
//  CREATE BongkarSusun_h
//  idUsername diambil via Username (dari JWT)
// ===========================
async function createBongkarSusun(payload) {
  const must = [];
  if (!payload?.tanggal) must.push('tanggal');
  if (!payload?.username) must.push('username');
  if (must.length) throw badReq(`Field wajib: ${must.join(', ')}`);

  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    // 1) Resolve Username -> IdUsername
    const rqUser = new sql.Request(tx);
    const userRes = await rqUser
      .input('Username', sql.VarChar(100), payload.username)
      .query(`
        SELECT TOP 1 IdUsername
        FROM dbo.MstUsername WITH (NOLOCK)
        WHERE Username = @Username;
      `);

    if (userRes.recordset.length === 0) {
      throw badReq(`Username "${payload.username}" tidak ditemukan di MstUsername`);
    }

    const idUsername = userRes.recordset[0].IdUsername;

    // 2) Generate nomor baru BG.XXXXXXXXXX
    const no1 = await generateNextNoBongkarSusun(tx, {
      prefix: 'BG.',
      width: 10,
    });

    const rqCheck = new sql.Request(tx);
    const exist = await rqCheck
      .input('NoBongkarSusun', sql.VarChar, no1)
      .query(`
        SELECT 1
        FROM dbo.BongkarSusun_h WITH (UPDLOCK, HOLDLOCK)
        WHERE NoBongkarSusun = @NoBongkarSusun
      `);

    const noBongkarSusun = exist.recordset.length
      ? await generateNextNoBongkarSusun(tx, { prefix: 'BG.', width: 10 })
      : no1;

    // 3) Insert header
    const rqIns = new sql.Request(tx);
    rqIns
      .input('NoBongkarSusun', sql.VarChar(50), noBongkarSusun)
      .input('Tanggal', sql.Date, payload.tanggal) // 'YYYY-MM-DD'
      .input('IdUsername', sql.Int, idUsername)
      .input('Note', sql.VarChar(255), payload.note ?? null);

    const insertSql = `
      INSERT INTO dbo.BongkarSusun_h (
        NoBongkarSusun, Tanggal, IdUsername, Note
      )
      OUTPUT INSERTED.*
      VALUES (
        @NoBongkarSusun, @Tanggal, @IdUsername, @Note
      );
    `;

    const insRes = await rqIns.query(insertSql);
    await tx.commit();

    return { header: insRes.recordset?.[0] || null };
  } catch (e) {
    try {
      await tx.rollback();
    } catch (_) {}
    throw e;
  }
}

// ===========================
//  UPDATE BongkarSusun_h
// ===========================
async function updateBongkarSusun(noBongkarSusun, payload) {
  if (!noBongkarSusun) {
    throw badReq('noBongkarSusun wajib diisi');
  }

  // Kumpulkan field yang akan di-update
  const setClauses = [];
  const params = [];

  if (Object.prototype.hasOwnProperty.call(payload, 'tanggal')) {
    setClauses.push('Tanggal = @Tanggal');
    params.push({ name: 'Tanggal', type: sql.Date, value: payload.tanggal });
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'idUsername')) {
    setClauses.push('IdUsername = @IdUsername');
    params.push({
      name: 'IdUsername',
      type: sql.Int,
      value: payload.idUsername,
    });
  }

  if (Object.prototype.hasOwnProperty.call(payload, 'note')) {
    setClauses.push('Note = @Note');
    params.push({
      name: 'Note',
      type: sql.VarChar(255),
      value: payload.note ?? null,
    });
  }

  if (setClauses.length === 0) {
    throw badReq('Tidak ada field yang diupdate');
  }

  const pool = await poolPromise;
  const request = pool.request();

  params.forEach((p) => {
    request.input(p.name, p.type, p.value);
  });

  request.input('NoBongkarSusun', sql.VarChar(50), noBongkarSusun);

  const sqlUpdate = `
    UPDATE dbo.BongkarSusun_h
    SET ${setClauses.join(', ')}
    OUTPUT INSERTED.*
    WHERE NoBongkarSusun = @NoBongkarSusun;
  `;

  const result = await request.query(sqlUpdate);

  if (result.recordset.length === 0) {
    throw badReq('BongkarSusun tidak ditemukan');
  }

  return { header: result.recordset[0] };
}

// ===========================
//  DELETE BongkarSusun_h
// ===========================
async function deleteBongkarSusun(noBongkarSusun) {
  if (!noBongkarSusun) {
    throw badReq('noBongkarSusun wajib diisi');
  }

  const pool = await poolPromise;
  const request = pool.request();
  request.input('NoBongkarSusun', sql.VarChar(50), noBongkarSusun);

  const sqlDelete = `
    DELETE FROM dbo.BongkarSusun_h
    WHERE NoBongkarSusun = @NoBongkarSusun;
  `;

  const result = await request.query(sqlDelete);

  if (result.rowsAffected[0] === 0) {
    throw badReq('BongkarSusun tidak ditemukan atau sudah dihapus');
  }

  return true;
}

module.exports = {
  getByDate,
  getAllBongkarSusun,
  createBongkarSusun,
  updateBongkarSusun,
  deleteBongkarSusun,
};

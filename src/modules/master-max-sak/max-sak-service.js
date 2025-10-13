const { sql, poolPromise } = require('../../core/config/db');

// GET list dengan pagination + optional filter IdBagian
exports.getAll = async ({ page, limit, idBagian }) => {
  const pool = await poolPromise;
  const reqData = pool.request();
  const reqCount = pool.request();

  const offset = (page - 1) * limit;

  let where = 'WHERE 1=1';
  if (Number.isInteger(idBagian)) {
    where += ' AND IdBagian = @IdBagian';
    reqData.input('IdBagian', sql.Int, idBagian);
    reqCount.input('IdBagian', sql.Int, idBagian);
  }

  const qData = `
    SELECT IdBagian, JlhSak, DefaultKG
    FROM MstMaxSak
    ${where}
    ORDER BY IdBagian ASC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;
  const qCount = `
    SELECT COUNT(*) AS total
    FROM MstMaxSak
    ${where};
  `;

  reqData.input('offset', sql.Int, offset).input('limit', sql.Int, limit);

  const [rData, rCount] = await Promise.all([reqData.query(qData), reqCount.query(qCount)]);
  return {
    data: rData.recordset,
    total: rCount.recordset[0]?.total ?? 0
  };
};

// GET one
exports.getOne = async (idBagian) => {
  const pool = await poolPromise;
  const r = await pool.request()
    .input('IdBagian', sql.Int, idBagian)
    .query(`
      SELECT IdBagian, JlhSak, DefaultKG
      FROM MstMaxSak
      WHERE IdBagian = @IdBagian
    `);
  return r.recordset[0] || null;
};

// CREATE
exports.create = async (payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const IdBagian  = parseInt(payload?.IdBagian, 10);
  const JlhSak    = parseInt(payload?.JlhSak, 10);
  const DefaultKG = payload?.DefaultKG != null ? Number(payload.DefaultKG) : null;

  // Validasi sederhana
  const bad = (m) => { const e = new Error(m); e.statusCode = 400; return e; };
  if (!Number.isInteger(IdBagian)) throw bad('IdBagian wajib (int)');
  if (!Number.isInteger(JlhSak))   throw bad('JlhSak wajib (int)');

  try {
    await tx.begin();

    // Cek duplikat IdBagian
    const exist = await new sql.Request(tx)
      .input('IdBagian', sql.Int, IdBagian)
      .query(`SELECT 1 FROM MstMaxSak WITH (UPDLOCK, HOLDLOCK) WHERE IdBagian = @IdBagian`);
    if (exist.recordset.length > 0) {
      const e = new Error(`IdBagian ${IdBagian} sudah ada`);
      e.statusCode = 409;
      throw e;
    }

    // Insert
    await new sql.Request(tx)
      .input('IdBagian', sql.Int, IdBagian)
      .input('JlhSak', sql.Int, JlhSak)
      .input('DefaultKG', sql.Decimal(18, 3), DefaultKG)
      .query(`
        INSERT INTO MstMaxSak (IdBagian, JlhSak, DefaultKG)
        VALUES (@IdBagian, @JlhSak, @DefaultKG)
      `);

    await tx.commit();
    return { IdBagian, JlhSak, DefaultKG };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};

// UPDATE
exports.update = async (idBagianParam, payload) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const IdBagian = parseInt(idBagianParam, 10);
  const JlhSak   = payload?.JlhSak != null ? parseInt(payload.JlhSak, 10) : null;
  const DefaultKG = payload?.DefaultKG != null ? Number(payload.DefaultKG) : null;

  const bad = (m) => { const e = new Error(m); e.statusCode = 400; return e; };
  if (!Number.isInteger(IdBagian)) throw bad('IdBagian tidak valid');
  if (JlhSak == null && DefaultKG == null) throw bad('Tidak ada field yang diupdate');

  try {
    await tx.begin();

    // Pastikan ada
    const exist = await new sql.Request(tx)
      .input('IdBagian', sql.Int, IdBagian)
      .query(`SELECT 1 FROM MstMaxSak WITH (UPDLOCK, HOLDLOCK) WHERE IdBagian = @IdBagian`);
    if (exist.recordset.length === 0) {
      const e = new Error('Data tidak ditemukan');
      e.statusCode = 404;
      throw e;
    }

    // Build dynamic set
    const sets = [];
    const rq = new sql.Request(tx).input('IdBagian', sql.Int, IdBagian);
    if (JlhSak != null)   { sets.push('JlhSak = @JlhSak'); rq.input('JlhSak', sql.Int, JlhSak); }
    if (DefaultKG != null){ sets.push('DefaultKG = @DefaultKG'); rq.input('DefaultKG', sql.Decimal(18, 3), DefaultKG); }

    await rq.query(`
      UPDATE MstMaxSak
      SET ${sets.join(', ')}
      WHERE IdBagian = @IdBagian
    `);

    await tx.commit();
    return { IdBagian, ...(JlhSak != null && { JlhSak }), ...(DefaultKG != null && { DefaultKG }) };
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};

// DELETE
exports.remove = async (idBagianParam) => {
  const pool = await poolPromise;
  const tx = new sql.Transaction(pool);

  const IdBagian = parseInt(idBagianParam, 10);
  const bad = (m) => { const e = new Error(m); e.statusCode = 400; return e; };
  if (!Number.isInteger(IdBagian)) throw bad('IdBagian tidak valid');

  try {
    await tx.begin();

    const del = await new sql.Request(tx)
      .input('IdBagian', sql.Int, IdBagian)
      .query(`DELETE FROM MstMaxSak WHERE IdBagian = @IdBagian`);

    await tx.commit();

    if (del.rowsAffected?.[0] === 0) {
      const e = new Error('Data tidak ditemukan');
      e.statusCode = 404;
      throw e;
    }
    return true;
  } catch (e) {
    try { await tx.rollback(); } catch (_) {}
    throw e;
  }
};

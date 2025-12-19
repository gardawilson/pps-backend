// src/core/shared/mesin-location.js
const { sql, poolPromise } = require('../config/db');
const { PRODUKSI_MESIN_SOURCES } = require('../config/produksi-mesin-config');

// helper: dapatkan sql.Request dari berbagai "runner" (copy pola dari log.js)
async function getRequest(runner) {
  const r = (typeof runner?.then === 'function') ? await runner : runner;
  if (r instanceof sql.Request) return r;
  if (r instanceof sql.Transaction) return new sql.Request(r);
  if (r?.request) return r.request();
  const pool = await poolPromise;
  return pool.request();
}

// Cari config berdasarkan prefix kode (pilih prefix terpanjang yang cocok)
function resolveProduksiSourceByPrefix(kode) {
  if (!kode) return null;

  let match = null;

  for (const s of PRODUKSI_MESIN_SOURCES) {
    if (kode.startsWith(s.prefix)) {
      if (!match || s.prefix.length > match.prefix.length) {
        match = s;
      }
    }
  }

  return match;
}

/**
 * Dapatkan IdMesin dari kode produksi (NoProduksi / NoPacking / NoCrusherProduksi, dst)
 * @param {Object} p
 * @param {string} p.kode - contoh: 'E.0000000001', 'G.0000000006', 'BH.0000000001'
 * @param {sql.Transaction|sql.Request|sql.ConnectionPool|Promise} [p.runner]
 * @returns {Promise<number|null>}
 */
async function getIdMesinFromKodeProduksi({ kode, runner } = {}) {
  if (!kode) return null;

  const source = resolveProduksiSourceByPrefix(kode);
  if (!source) {
    console.warn('[mesin-location] prefix tidak dikenal untuk kode:', kode);
    return null;
  }

  // SOURCE STATIC → tidak punya IdMesin
  if (source.staticBlok != null || source.staticIdLokasi != null) {
    return null;
  }

  const request = await getRequest(runner);

  const query = `
    SELECT TOP 1 ${source.idMesinColumn} AS IdMesin
    FROM dbo.${source.table} WITH (NOLOCK)
    WHERE ${source.codeColumn} = @Kode
  `;

  const res = await request
    .input('Kode', sql.VarChar, kode)
    .query(query);

  const row = res.recordset?.[0];
  return row?.IdMesin ?? null;
}

/**
 * Dapatkan Blok + IdLokasi dari IdMesin
 * @param {Object} p
 * @param {number} p.idMesin
 * @param {sql.Transaction|sql.Request|sql.ConnectionPool|Promise} [p.runner]
 * @returns {Promise<{Blok: string|null, IdLokasi: number|null}|null>}
 */
async function getBlokLokasiFromMesin({ idMesin, runner } = {}) {
  if (!idMesin) return null;

  const request = await getRequest(runner);

  const query = `
    SELECT TOP 1 Blok, IdLokasi
    FROM dbo.MstMesin WITH (NOLOCK)
    WHERE IdMesin = @IdMesin
  `;

  const res = await request
    .input('IdMesin', sql.Int, idMesin)
    .query(query);

  const row = res.recordset?.[0];
  if (!row) return null;

  return {
    Blok: row.Blok ?? null,
    IdLokasi: row.IdLokasi ?? null,
  };
}

/**
 * Shortcut: langsung dari kode produksi → Blok + IdLokasi
 * @param {Object} p
 * @param {string} p.kode
 * @param {sql.Transaction|sql.Request|sql.ConnectionPool|Promise} [p.runner]
 * @returns {Promise<{Blok: string|null, IdLokasi: number|null}|null>}
 */
async function getBlokLokasiFromKodeProduksi({ kode, runner } = {}) {
  if (!kode) return null;

  const source = resolveProduksiSourceByPrefix(kode);
  if (!source) {
    console.warn('[mesin-location] prefix tidak dikenal untuk kode:', kode);
    return null;
  }

  // ✅ STATIC mapping: BG. → langsung return tanpa DB
  if (source.staticBlok != null || source.staticIdLokasi != null) {
    return {
      Blok: source.staticBlok ?? null,
      IdLokasi: source.staticIdLokasi ?? null,
    };
  }

  const idMesin = await getIdMesinFromKodeProduksi({ kode, runner });
  if (!idMesin) return null;

  return getBlokLokasiFromMesin({ idMesin, runner });
}

module.exports = {
  getIdMesinFromKodeProduksi,
  getBlokLokasiFromMesin,
  getBlokLokasiFromKodeProduksi,
};

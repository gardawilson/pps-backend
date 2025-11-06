const overlapService = require('./production-overlap-service');

const ALLOWED_KINDS = new Set(['broker', 'crusher', 'washing', 'gilingan']);

function normalizeTime(t) {
  // Terima: "7:00", "07:00", "07:00:30"
  // Keluarkan: "HH:mm:ss"
  if (typeof t !== 'string') return '';
  const m = t.trim().match(/^(\d{1,2}):([0-5]\d)(?::([0-5]\d))?$/);
  if (!m) return '';
  let [ , hh, mm, ss ] = m;
  if (Number(hh) > 23) return '';
  hh = hh.padStart(2, '0');
  ss = ss ?? '00';
  return `${hh}:${mm}:${ss}`;
}

function isYmd(dateStr) {
  return /^\d{4}-\d{2}-\d{2}$/.test(dateStr);
}

async function checkOverlapGeneric(req, res) {
  const kind        = String(req.params.kind || '').trim();   // 'broker'|'crusher'|'washing'|'gilingan'
  const tglProduksi = String(req.query.date || '').trim();    // YYYY-MM-DD
  const idMesinStr  = String(req.query.idMesin || '').trim(); // number-like
  const excludeNo   = (req.query.exclude ? String(req.query.exclude).trim() : null);

  // Normalisasi waktu ke HH:mm:ss
  const hourStart = normalizeTime(String(req.query.start || '').trim());
  const hourEnd   = normalizeTime(String(req.query.end || '').trim());

  // Validasi dasar
  if (!ALLOWED_KINDS.has(kind)) {
    return res.status(400).json({ success: false, message: 'kind harus salah satu dari: broker|crusher|washing|gilingan' });
  }
  if (!tglProduksi || !isYmd(tglProduksi)) {
    return res.status(400).json({ success: false, message: 'date wajib format YYYY-MM-DD' });
  }
  if (!idMesinStr) {
    return res.status(400).json({ success: false, message: 'Parameter wajib: idMesin' });
  }
  const idMesin = Number(idMesinStr);
  if (!Number.isFinite(idMesin)) {
    return res.status(400).json({ success: false, message: 'idMesin harus numerik' });
  }
  if (!hourStart || !hourEnd) {
    return res.status(400).json({ success: false, message: 'start/end wajib format HH:mm atau HH:mm:ss' });
  }

  try {
    const result = await overlapService.checkOverlapGeneric({
      kind,
      tglProduksi,
      idMesin,
      hourStart,
      hourEnd,
      excludeNoProduksi: excludeNo
    });

    return res.status(200).json({
      success: true,
      message: 'Hasil pengecekan overlap',
      ...result, // { isOverlap, conflicts: [...] }
    });
  } catch (err) {
    console.error('Error checkOverlap Generic:', err);
    return res.status(500).json({
      success: false,
      message: 'Internal Server Error',
      error: err.message,
    });
  }
}

module.exports = { checkOverlapGeneric };

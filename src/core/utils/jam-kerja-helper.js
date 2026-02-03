/**
 * jamKerja can be:
 *  - number (8)
 *  - "HH:mm-HH:mm" => duration in hours (rounded)
 *  - "HH:mm" => take hour part
 *  - "8" => hour
 */
function parseJamToInt(jam) {
  if (jam == null) throw badReq("Format jamKerja tidak valid");
  if (typeof jam === "number") return Math.max(0, Math.round(jam));

  const s = String(jam).trim();

  const mRange = s.match(/^(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$/);
  if (mRange) {
    const sh = +mRange[1],
      sm = +mRange[2],
      eh = +mRange[3],
      em = +mRange[4];
    let mins = eh * 60 + em - (sh * 60 + sm);
    if (mins < 0) mins += 24 * 60;
    return Math.max(0, Math.round(mins / 60));
  }

  const mTime = s.match(/^(\d{1,2}):(\d{2})$/);
  if (mTime) return Math.max(0, parseInt(mTime[1], 10));

  const mHour = s.match(/^(\d{1,2})$/);
  if (mHour) return Math.max(0, parseInt(mHour[1], 10));

  throw badReq(
    'Format jamKerja tidak valid. Gunakan angka (mis. 8) atau "HH:mm-HH:mm"',
  );
}

// optional: if jamKerja empty, calculate from hourStart-hourEnd
function calcJamKerjaFromStartEnd(hourStart, hourEnd) {
  if (!hourStart || !hourEnd) return null;

  const norm = (s) => {
    const t = String(s).trim();
    if (/^\d{1,2}:\d{2}$/.test(t)) return `${t}:00`;
    return t;
  };

  const hs = norm(hourStart);
  const he = norm(hourEnd);

  const parse = (t) => {
    const m = String(t).match(/^(\d{1,2}):(\d{2}):(\d{2})$/);
    if (!m) return null;
    const h = +m[1],
      min = +m[2],
      sec = +m[3];
    return h * 3600 + min * 60 + sec;
  };

  const s1 = parse(hs);
  const s2 = parse(he);
  if (s1 == null || s2 == null) return null;

  let diff = s2 - s1;
  if (diff < 0) diff += 24 * 3600;
  return Math.max(0, Math.round(diff / 3600));
}

module.exports = { parseJamToInt, calcJamKerjaFromStartEnd };

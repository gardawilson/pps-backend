function normalize(v) {
  if (v === undefined) return undefined;
  if (v === null) return null;
  if (v instanceof Date) return v.toISOString();
  return String(v);
}

function setIfChanged({
  col,
  type,
  newVal,
  oldVal,
  req,
  sets,
  transform = (v) => v,
}) {
  if (newVal === undefined) return;

  const v = transform(newVal);

  const same =
    (v === null && oldVal === null) || normalize(v) === normalize(oldVal);

  if (!same) {
    sets.push(`${col} = @${col}`);
    req.input(col, type, v);
  }
}

module.exports = { setIfChanged };

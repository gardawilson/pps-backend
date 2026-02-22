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

function createSetIf(req, sets) {
  return (col, param, type, val) => {
    if (val !== undefined) {
      sets.push(`${col} = @${param}`);
      req.input(param, type, val);
    }
  };
}

module.exports = { setIfChanged, createSetIf };

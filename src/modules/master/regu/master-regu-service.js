// master-regu-service.js
const { poolPromise, sql } = require("../../../core/config/db");

async function listAll({
  q = "",
  orderBy = "NamaRegu",
  orderDir = "ASC",
  idBagian = null,
}) {
  const pool = await poolPromise;
  const request = pool.request();

  const allowedOrderBy = new Set([
    "IdRegu",
    "IdBagian",
    "NamaRegu",
    "KepalaRegu",
  ]);
  const orderCol = allowedOrderBy.has(orderBy) ? orderBy : "NamaRegu";
  const dir = orderDir === "DESC" ? "DESC" : "ASC";

  const conditions = [];

  if (q && q.trim().length > 0) {
    conditions.push("(a.NamaRegu LIKE @q OR b.NamaOperator LIKE @q)");
    request.input("q", `%${q}%`);
  }

  if (idBagian) {
    conditions.push("a.IdBagian = @idBagian");
    request.input("idBagian", idBagian);
  }

  const where =
    conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  const query = `
    SELECT
      a.IdRegu,
      a.IdBagian,
      a.NamaRegu,
      a.KepalaRegu,
      b.NamaOperator AS NamaKepalaRegu
    FROM [dbo].[MstRegu] a
    LEFT JOIN [dbo].[MstOperator] b ON a.KepalaRegu = b.IdOperator
    ${where}
    ORDER BY ${orderCol} ${dir};
  `;

  const result = await request.query(query);
  return result.recordset || [];
}

module.exports = { listAll };

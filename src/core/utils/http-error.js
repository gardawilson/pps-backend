function httpError(statusCode, message) {
  const e = new Error(message);
  e.statusCode = statusCode;
  return e;
}

function badReq(message) {
  return httpError(400, message);
}

function unauthorized(message = 'Unauthorized') {
  return httpError(401, message);
}

function forbidden(message = 'Forbidden') {
  return httpError(403, message);
}

function notFound(message = 'Not Found') {
  return httpError(404, message);
}

function conflict(message = 'Conflict') {
  return httpError(409, message);
}

module.exports = { httpError, badReq, unauthorized, forbidden, notFound, conflict };

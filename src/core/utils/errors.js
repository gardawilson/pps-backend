function badReq(msg) {
    const err = new Error(msg);
    err.statusCode = 400;
    return err;
  }
  
  module.exports = {
    badReq,
  };
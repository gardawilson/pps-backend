const service = require("./master-printer-service");

function getActorUsername(req) {
  return req.username || req.user?.username || null;
}

async function list(req, res) {
  try {
    const rows = await service.listAll();
    return res.status(200).json({
      success: true,
      data: rows,
    });
  } catch (error) {
    console.error("Error listing MstPrinter:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function upsert(req, res) {
  try {
    const data = await service.upsertByMacAddress({
      macAddress: req.body?.MacAddress,
      alias: req.body?.Alias,
      description: req.body?.Description,
      updatedBy: getActorUsername(req),
    });

    return res.status(200).json({
      success: true,
      message: "OK",
      data,
    });
  } catch (error) {
    const statusCode = error.statusCode || 500;
    console.error("Error upserting MstPrinter:", error);
    return res.status(statusCode).json({
      success: false,
      message: statusCode === 500 ? "Internal Server Error" : error.message,
      ...(statusCode === 500 ? { error: error.message } : {}),
    });
  }
}

async function remove(req, res) {
  try {
    const macAddress = decodeURIComponent(req.params.mac || "");
    await service.deleteByMacAddress(macAddress);

    return res.status(200).json({
      success: true,
      message: "Deleted",
    });
  } catch (error) {
    const statusCode = error.statusCode || 500;
    console.error("Error deleting MstPrinter:", error);
    return res.status(statusCode).json({
      success: false,
      message: statusCode === 500 ? "Internal Server Error" : error.message,
      ...(statusCode === 500 ? { error: error.message } : {}),
    });
  }
}

module.exports = {
  list,
  upsert,
  remove,
};

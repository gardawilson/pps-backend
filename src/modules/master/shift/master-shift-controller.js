const service = require("./master-shift-service");

async function getShiftHours(req, res) {
  const tanggal = String(req.query.tanggal || "").trim();
  const shiftRaw = String(req.query.shift || "").trim();
  const shift = Number(shiftRaw);

  if (!tanggal) {
    return res.status(400).json({
      success: false,
      message: "Query param tanggal wajib diisi (format YYYY-MM-DD)",
    });
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(tanggal)) {
    return res.status(400).json({
      success: false,
      message: "Format tanggal harus YYYY-MM-DD",
    });
  }

  if (!Number.isInteger(shift) || shift <= 0) {
    return res.status(400).json({
      success: false,
      message: "Query param shift wajib integer positif",
    });
  }

  try {
    const row = await service.getShiftHoursByDateAndShift({ tanggal, shift });

    if (!row) {
      return res.status(404).json({
        success: false,
        message: `Shift hour tidak ditemukan untuk tanggal ${tanggal} dan shift ${shift}`,
      });
    }

    return res.status(200).json({
      success: true,
      message: "Data shift hour berhasil diambil",
      data: {
        tanggal,
        shift,
        idShiftHourSet: row.IdShiftHourSet,
        validFrmDate: row.ValidFrmDate,
        hourStart: row.HourStart,
        hourEnd: row.HourEnd,
      },
    });
  } catch (error) {
    console.error("Error get shift hours:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

async function getCurrentShift(req, res) {
  try {
    const row = await service.getCurrentShift();

    if (!row) {
      return res.status(404).json({
        success: false,
        message: "Shift aktif saat ini tidak ditemukan",
      });
    }

    return res.status(200).json({
      success: true,
      message: "Data shift aktif saat ini berhasil diambil",
      data: {
        tanggal: row.CurrentDate,
        waktu: row.CurrentTime,
        shift: row.NoShift,
        idShiftHourSet: row.IdShiftHourSet,
        validFrmDate: row.ValidFrmDate,
        hourStart: row.HourStart,
        hourEnd: row.HourEnd,
      },
    });
  } catch (error) {
    console.error("Error get current shift:", error);
    return res.status(500).json({
      success: false,
      message: "Internal Server Error",
      error: error.message,
    });
  }
}

module.exports = { getShiftHours, getCurrentShift };

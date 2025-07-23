const { getProfileService, changePasswordService } = require('./profile-service');

const getProfile = async (req, res) => {
  const username = req.username; // ✅ FIXED

  try {
    const data = await getProfileService(username);
    if (!data) {
      return res.status(404).json({ success: false, message: 'Profil tidak ditemukan' });
    }

    res.status(200).json({
      success: true,
      message: 'Profil ditemukan',
      data
    });
  } catch (err) {
    console.error('Error fetching profile:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan di server' });
  }
};

const changePassword = async (req, res) => {
  const username = req.username; // ✅ FIXED
  const { oldPassword, newPassword, confirmPassword } = req.body;

  if (!oldPassword || !newPassword || !confirmPassword) {
    return res.status(400).json({ success: false, message: 'Semua password harus diisi.' });
  }

  if (newPassword !== confirmPassword) {
    return res.status(400).json({ success: false, message: 'Password baru dan konfirmasi tidak cocok.' });
  }

  try {
    await changePasswordService(username, oldPassword, newPassword);
    res.status(200).json({ success: true, message: 'Password berhasil diganti.' });
  } catch (err) {
    res.status(400).json({ success: false, message: err.message });
  }
};

module.exports = { getProfile, changePassword };

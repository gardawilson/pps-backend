const jwt = require('jsonwebtoken');
const authService = require('./auth-service');
const getUserPermissions = require('../../core/utils/get-user-permissions');

async function login(req, res) {
  const { username, password } = req.body;

  try {
    const user = await authService.verifyUser(username, password);
    if (!user) {
      return res.status(400).json({
        success: false,
        message: 'Username atau password salah',
      });
    }

    // 🔹 Ambil permission pakai helper (tanpa duplikasi query)
    const permissions = await getUserPermissions(user.IdUsername);

    const token = jwt.sign(
      {
        idUsername: user.IdUsername,
        username: user.Username,
      },
      process.env.SECRET_KEY,
      { expiresIn: '12h' }
    );

    res.status(200).json({
      success: true,
      message: 'Login berhasil',
      token,
      user: {
        idUsername: user.IdUsername,
        username: user.Username,
        fullName: `${user.FName ?? ''} ${user.LName ?? ''}`.trim(),
        permissions, // ✅ ambil dari helper, bukan query ulang
      },
    });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({
      success: false,
      message: 'Terjadi kesalahan di server',
    });
  }
}

module.exports = { login };

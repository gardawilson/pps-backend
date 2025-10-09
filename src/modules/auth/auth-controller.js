const jwt = require('jsonwebtoken');
const authService = require('./auth-service');

async function login(req, res) {
  const { username, password } = req.body;

  try {
    // 🔹 verifyUser sekarang return data user (bukan boolean)
    const user = await authService.verifyUser(username, password);

    if (!user) {
      return res.status(400).json({
        success: false,
        message: 'Username atau password salah',
      });
    }

    // 🔹 buat token yang berisi idUsername & username
    const token = jwt.sign(
      {
        idUsername: user.IdUsername,
        username: user.Username,
      },
      process.env.SECRET_KEY,
      { expiresIn: '12h' }
    );

    // 🔹 kirim response sukses
    res.status(200).json({
      success: true,
      message: 'Login berhasil',
      token,
      user: {
        idUsername: user.IdUsername,
        username: user.Username,
        fullName: `${user.FName ?? ''} ${user.LName ?? ''}`.trim(),
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

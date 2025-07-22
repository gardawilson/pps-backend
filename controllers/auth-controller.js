const jwt = require('jsonwebtoken');
const authService = require('../services/auth-service');

async function login(req, res) {
  const { username, password } = req.body;

  try {
    const isValid = await authService.verifyUser(username, password);

    if (isValid) {
      const token = jwt.sign({ username }, process.env.SECRET_KEY, {
        expiresIn: '12h',
      });

      res.status(200).json({
        success: true,
        message: 'Login berhasil',
        token,
      });
    } else {
      res.status(400).json({ success: false, message: 'Username atau password salah' });
    }
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ success: false, message: 'Terjadi kesalahan di server' });
  }
}

module.exports = { login };

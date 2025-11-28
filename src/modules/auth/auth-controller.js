const jwt = require('jsonwebtoken');
const authService = require('./auth-service');
const getUserPermissions = require('../../core/utils/get-user-permissions');

async function login(req, res) {
  const { username, password } = req.body;

  try {
    // 🔹 Validasi input
    if (!username || !password) {
      return res.status(400).json({
        success: false,
        message: 'Username dan password harus diisi',
        errorType: 'validation'
      });
    }

    // 🔹 Verifikasi user
    const verifyResult = await authService.verifyUser(username, password);
    
    if (!verifyResult.success) {
      // 🔹 Return error dengan status code yang sesuai
      const statusCode = verifyResult.errorType === 'user_not_found' 
        ? 404 
        : verifyResult.errorType === 'user_inactive'
        ? 403
        : 401; // wrong_password

      return res.status(statusCode).json({
        success: false,
        message: verifyResult.message,
        errorType: verifyResult.errorType
      });
    }

    const user = verifyResult.user;

    // 🔹 Ambil permission pakai helper
    const permissions = await getUserPermissions(user.IdUsername);

    // 🔹 Generate JWT token
    const token = jwt.sign(
      {
        idUsername: user.IdUsername,
        username: user.Username,
      },
      process.env.SECRET_KEY,
      { expiresIn: '12h' }
    );

    // 🔹 Success response
    res.status(200).json({
      success: true,
      message: 'Login berhasil',
      token,
      user: {
        idUsername: user.IdUsername,
        username: user.Username,
        fullName: `${user.FName ?? ''} ${user.LName ?? ''}`.trim(),
        permissions,
      },
    });
  } catch (err) {
    console.error('Login error:', err);
    
    // 🔹 Differentiate database errors
    if (err.name === 'ConnectionError') {
      return res.status(503).json({
        success: false,
        message: 'Database sedang tidak dapat diakses. Silakan coba lagi nanti.',
        errorType: 'database_connection'
      });
    }

    // 🔹 Generic server error
    res.status(500).json({
      success: false,
      message: 'Terjadi kesalahan di server',
      errorType: 'server_error'
    });
  }
}

module.exports = { login };
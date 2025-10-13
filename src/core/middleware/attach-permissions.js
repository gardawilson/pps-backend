// core/middleware/attach-permissions.js
const getUserPermissions = require('../utils/get-user-permissions');

const attachPermissions = async (req, res, next) => {
  try {
    const idUsername = req.idUsername;
    if (!idUsername) {
      return res.status(401).json({ success: false, message: 'Unauthorized (no idUsername)' });
    }

    const permissions = await getUserPermissions(idUsername);

    req.userPermissions = new Set(permissions);

    next();
  } catch (err) {
    console.error('attachPermissions error:', err);
    res.status(500).json({ success: false, message: 'Server error (attachPermissions)' });
  }
};

module.exports = attachPermissions;

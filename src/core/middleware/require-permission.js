// core/middleware/require-permission.js
function requirePermission(required) {
    const needed = Array.isArray(required) ? required : [required];
  
    return (req, res, next) => {
      const userPerms = req.userPermissions;
  
      if (!userPerms) {
        return res.status(500).json({ success: false, message: 'Permissions not attached' });
      }
  
      // super admin wildcard
      if (userPerms.has('*')) return next();
  
      const hasAll = needed.every(p => userPerms.has(p));
  
      if (!hasAll) {
        return res.status(403).json({
          success: false,
          message: 'Forbidden: insufficient permission',
          required: needed,
        });
      }
  
      next();
    };
  }
  
  module.exports = requirePermission;
  
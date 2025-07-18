require('dotenv').config();  // Menggunakan dotenv untuk memuat file .env

const jwt = require('jsonwebtoken');
const secretKey = process.env.SECRET_KEY;  

// Middleware untuk memverifikasi token dan mengekstrak username
const verifyToken = (req, res, next) => {
    const token = req.header('Authorization')?.replace('Bearer ', '');

    if (!token) {
        return res.status(401).json({ message: 'Access Denied, token required.' });
    }

    try {
        const decoded = jwt.verify(token, secretKey);  // Verifikasi dan decode token
        req.username = decoded.username;  // Menyimpan username ke dalam request object
        next();  // Melanjutkan ke route handler
    } catch (error) {
        return res.status(401).json({ message: 'Invalid or expired token.' });
    }
};

module.exports = verifyToken;

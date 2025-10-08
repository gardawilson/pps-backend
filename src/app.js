const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');

// Routes
const authRoutes = require('./modules/auth/auth-routes');
const stockOpnameRoutes = require('./modules/stock-opname/stock-opname-routes');
const profileRoutes = require('./modules/profile/profile-routes');
const mstLokasiRoutes = require('./modules/master-lokasi/master-lokasi-routes');
const detailLabelRoutes = require('./modules/label-detail/label-detail-routes');
const labelWashingRoutes = require('./modules/label/washing/label-washing-routes');
const plasticTypeRoutes = require('./modules/master-plastic/plastic-routes');
const produksiWashingRoutes = require('./modules/washing-produksi/washing-produksi-routes');
const blokRoutes = require('./modules/master-blok/master-blok-routes');
const labelRoutes = require('./modules/label/all/label-routes');





const app = express();

// 🌍 Global middleware
app.use(express.json({ limit: '10mb' }));
app.use(bodyParser.urlencoded({ extended: true }));

// 🩺 Health check
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'OK',
    message: 'Server is running',
    timestamp: new Date().toISOString()
  });
});

// 📌 API Routes
app.use('/api/auth', authRoutes);
app.use('/api/', stockOpnameRoutes);
app.use('/api/', profileRoutes);
app.use('/api/', mstLokasiRoutes);
app.use('/api/', detailLabelRoutes);
app.use('/api/', labelWashingRoutes);
app.use('/api/plastic-type', plasticTypeRoutes);
app.use('/api/washing-produksi', produksiWashingRoutes);
app.use('/api/blok', blokRoutes);
app.use('/api/', labelRoutes);


// ❌ Error handling
app.use((err, req, res, next) => {
  console.error('❌ Error:', err.stack);
  res.status(err.status || 500).json({
    success: false,
    message: err.message || 'Internal Server Error'
  });
});

// 🚫 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    message: 'Route not found'
  });
});

module.exports = app;

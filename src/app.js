const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');

// Routes
const authRoutes = require('./modules/auth/auth-routes');
const stockOpnameRoutes = require('./modules/stock-opname/stock-opname-routes');
const profileRoutes = require('./modules/profile/profile-routes');
const mstLokasiRoutes = require('./modules/master-lokasi/master-lokasi-routes');
const detailLabelRoutes = require('./modules/label-detail/label-detail-routes');
const labelWashingRoutes = require('./modules/label/washing/washing-routes');
const plasticTypeRoutes = require('./modules/master-plastic/plastic-routes');
const blokRoutes = require('./modules/master-blok/master-blok-routes');
const labelRoutes = require('./modules/label/all/label-routes');
const productionRoutes = require('./modules/production/washing/washing-production-routes');
const bongkarSusunRoutes = require('./modules/bongkar-susun/bongkar-susun-route');
const maxSak = require('./modules/master-max-sak/max-sak-routes');
const labelBrokerRoutes = require('./modules/label/broker/broker-routes');
const productionBrokerRoutes = require('./modules/production/broker/broker-production-routes');

const labelBonggolanRoutes = require('./modules/label/bonggolan/bonggolan-routes');

const productionInjectRoutes = require('./modules/production/inject/inject-production-routes');

const bonggolanTypeRoutes = require('./modules/jenis-bonggolan/jenis-bonggolan-routes');


const labelCrusherRoutes = require('./modules/label/crusher/crusher-routes');


const productionCrusherRoutes = require('./modules/production/crusher/crusher-production-routes');


const crusherTypeRoutes = require('./modules/master-crusher/master-crusher-routes');


const mstMesinRoutes = require('./modules/master-mesin/master-mesin-routes');
const mstOperatorRoutes = require('./modules/master-operator/master-operator-routes');


const checkOverlapRoutes = require('./modules/production/overlap/production-overlap-routes');


const labelGilinganRoutes = require('./modules/label/gilingan/gilingan-routes');

const labelMixerRoutes = require('./modules/label/mixer/mixer-routes');


const productionMixerRoutes = require('./modules/production/mixer/mixer-production-routes');

const mixerTypeRoutes = require('./modules/master-mixer/mixer-type-routes');


const gilinganTypeRoutes = require('./modules/master-gilingan/gilingan-type-routes');


const productionGilinganRoutes = require('./modules/production/gilingan/gilingan-production-routes');

const labelFurnitureWipRoutes = require('./modules/label/furniture-wip/furniture-wip-routes');

const productionHotStampRoutes = require('./modules/production/hot-stamp/hot-stamp-production-routes');

const productionKeyFittingRoutes = require('./modules/production/key-fitting/key-fitting-production-routes');

const productionSpannerRoutes = require('./modules/production/spanner/spanner-production-routes');

const productionReturnRoutes = require('./modules/production/return/return-production-routes');

const furnitureWipTypeRoutes = require('./modules/master-furniture-wip/furniture-wip-type-routes');

const labelPackingRoutes = require('./modules/label/packing/packing-routes');

const productionPackingRoutes = require('./modules/production/packing/packing-production-routes');


const packingTypeRoutes = require('./modules/master-packing/packing-master-routes');






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
app.use('/api/blok', blokRoutes);
app.use('/api/', labelRoutes);
app.use('/api/production', productionRoutes);
app.use('/api/bongkar-susun', bongkarSusunRoutes);
app.use('/api/max-sak', maxSak);
app.use('/api/', labelBrokerRoutes);
app.use('/api/production', productionBrokerRoutes);
app.use('/api/', labelBonggolanRoutes);
app.use('/api/production', productionInjectRoutes);
app.use('/api/bonggolan-type', bonggolanTypeRoutes);
app.use('/api/', labelCrusherRoutes);
app.use('/api/production', productionCrusherRoutes);
app.use('/api/crusher-type', crusherTypeRoutes);

app.use('/api/mst-mesin', mstMesinRoutes);

app.use('/api/mst-operator', mstOperatorRoutes);

app.use('/api/production', checkOverlapRoutes);

app.use('/api/', labelGilinganRoutes);

app.use('/api/', labelMixerRoutes);

app.use('/api/production', productionMixerRoutes);

app.use('/api/mixer-type', mixerTypeRoutes);

app.use('/api/gilingan-type', gilinganTypeRoutes);

app.use('/api/production', productionGilinganRoutes);

app.use('/api/', labelFurnitureWipRoutes);

app.use('/api/production', productionHotStampRoutes);
app.use('/api/production', productionKeyFittingRoutes);
app.use('/api/production', productionSpannerRoutes);
app.use('/api/production', productionReturnRoutes);

app.use('/api/furniture-wip-type', furnitureWipTypeRoutes);

app.use('/api/', labelPackingRoutes);

app.use('/api/production', productionPackingRoutes);


app.use('/api/packing-type', packingTypeRoutes);








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

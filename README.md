# PPS Backend

Backend API untuk Production Planning System (PPS) yang dibangun dengan Node.js dan Express.js.

## 📋 Deskripsi

PPS Backend adalah REST API yang mengelola data produksi, termasuk proses scanning barcode/QR code, manajemen input produksi, dan partial data untuk sistem produksi manufaktur. Sistem ini terintegrasi dengan SQL Server database dan mendukung operasi produksi real-time.

## 🚀 Fitur Utama

- **Authentication & Authorization** - Sistem autentikasi dengan JWT dan permission-based access control
- **Bongkar Susun Management** - Manajemen proses bongkar susun dengan validasi dan tracking
- **Label Management** - Pengelolaan berbagai tipe label produksi:
  - **All Labels** - Overview semua label
  - **Bonggolan** - Label untuk material bonggolan
  - **Broker** - Label proses broker
  - **Crusher** - Label proses crusher
  - **Furniture WIP** - Label work in progress furniture
- **Jenis Bonggolan** - Manajemen tipe-tipe bonggolan/material
- **Gilingan Module** - Manajemen proses gilingan/grinding
- **Mixer Module** - Manajemen proses mixing
- **Production Machine Config** - Konfigurasi mesin-mesin produksi
- **Transaction Management** - Guard dan konfigurasi tutup transaksi
- **WebSocket Support** - Real-time communication via Socket.IO
- **Machine Location Helper** - Utility untuk tracking lokasi mesin
- **Comprehensive Logging** - System logging untuk monitoring dan debugging

## 🛠️ Tech Stack

- **Runtime:** Node.js
- **Framework:** Express.js
- **Database:** SQL Server (mssql)
- **Real-time:** Socket.IO (WebSocket)
- **Authentication:** JWT (JSON Web Tokens)
- **Testing:** Jest
- **HTTP Client:** Axios (untuk testing)

## 📦 Prerequisites

- Node.js (v14 atau lebih tinggi)
- SQL Server
- npm atau yarn

## 🔧 Installation

1. Clone repository
```bash
git clone https://github.com/gardawilson/pps-backend.git
cd pps-backend
```

2. Install dependencies
```bash
npm install
```

3. Setup environment variables (buat file `.env`)
```env
# Database Configuration
DB_SERVER=your_server_address
DB_DATABASE=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=1433
DB_ENCRYPT=false
DB_TRUST_SERVER_CERTIFICATE=true

# Server Configuration
PORT=3000
NODE_ENV=development

# JWT Configuration
JWT_SECRET=your_super_secret_jwt_key
JWT_EXPIRES_IN=24h

# Socket.IO Configuration
SOCKET_CORS_ORIGIN=http://localhost:3000

# Logging
LOG_LEVEL=info
```

4. Jalankan server
```bash
npm start
```

## 📁 Struktur Project

```
pps-backend/
├── node_modules/
├── src/
│   ├── core/
│   │   ├── config/
│   │   │   ├── db.js                          # Database configuration
│   │   │   ├── produksi-mesin-config.js       # Production machine config
│   │   │   └── tutup-transaksi-config.js      # Transaction closure config
│   │   ├── middleware/
│   │   │   ├── attach-permissions.js          # Permission attachment middleware
│   │   │   ├── require-permission.js          # Permission validation middleware
│   │   │   └── verify-token.js                # JWT token verification
│   │   ├── shared/
│   │   │   ├── log.js                         # Logging utility
│   │   │   ├── mesin-location-helper.js       # Machine location helper
│   │   │   └── tutup-transaksi-guard.js       # Transaction guard utility
│   │   ├── socket/                            # WebSocket implementation
│   │   └── utils/                             # General utilities
│   └── modules/
│       ├── auth/                              # Authentication module
│       ├── bongkar-susun/
│       │   ├── _tests_/                       # Unit tests
│       │   ├── bongkar-susun-controller.js
│       │   ├── bongkar-susun-route.js
│       │   └── bongkar-susun-service.js
│       ├── jenis-bonggolan/                   # Material type module
│       ├── label/
│       │   ├── all/                           # All labels
│       │   ├── bonggolan/                     # Bonggolan labels
│       │   ├── broker/                        # Broker labels
│       │   ├── crusher/                       # Crusher labels
│       │   └── furniture-wip/
│       │       ├── _tests_/
│       │       ├── furniture-wip-controller.js
│       │       ├── furniture-wip-routes.js
│       │       └── furniture-wip-service.js
│       ├── gilingan/                          # Grinding module
│       └── mixer/                             # Mixer module
├── server.js                                  # Entry point aplikasi
├── package.json                               # Dependencies dan scripts
├── jest.config.cjs                            # Jest testing configuration
└── test.html                                  # API testing interface
```

## 🔌 API Endpoints

### Authentication
```
POST /api/auth/login
POST /api/auth/register
GET  /api/auth/verify
```

### Bongkar Susun
```
GET    /api/bongkar-susun
POST   /api/bongkar-susun
PUT    /api/bongkar-susun/:id
DELETE /api/bongkar-susun/:id
```

### Labels

#### Furniture WIP Labels
```
GET    /api/label/furniture-wip
POST   /api/label/furniture-wip
PUT    /api/label/furniture-wip/:id
DELETE /api/label/furniture-wip/:id
```

#### Bonggolan Labels
```
GET    /api/label/bonggolan
POST   /api/label/bonggolan
```

#### Broker Labels
```
GET    /api/label/broker
POST   /api/label/broker
```

#### Crusher Labels
```
GET    /api/label/crusher
POST   /api/label/crusher
```

#### All Labels
```
GET    /api/label/all
```

### Jenis Bonggolan (Material Types)
```
GET    /api/jenis-bonggolan
POST   /api/jenis-bonggolan
PUT    /api/jenis-bonggolan/:id
DELETE /api/jenis-bonggolan/:id
```

### Gilingan (Grinding)
```
GET    /api/gilingan
POST   /api/gilingan
```

### Mixer
```
GET    /api/mixer
POST   /api/mixer
```

### Example Request & Response

#### Create Furniture WIP Label
```http
POST /api/label/furniture-wip
Authorization: Bearer <token>
Content-Type: application/json

{
  "labelCode": "FW240108001",
  "productName": "Chair Component A",
  "quantity": 50,
  "machineId": "M001",
  "status": "in_progress"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Label berhasil dibuat",
  "data": {
    "id": 123,
    "labelCode": "FW240108001",
    "productName": "Chair Component A",
    "quantity": 50,
    "machineId": "M001",
    "status": "in_progress",
    "createdAt": "2024-01-08T10:30:00.000Z"
  }
}
```

## 🧪 Testing

Jalankan test suite:
```bash
npm test
```

Untuk development dengan watch mode:
```bash
npm run test:watch
```

Testing coverage:
```bash
npm run test:coverage
```

## 🔐 Authentication & Authorization

Sistem menggunakan JWT (JSON Web Tokens) untuk authentication dan permission-based authorization.

### Middleware Chain

1. **verify-token.js** - Memverifikasi JWT token dari request header
2. **attach-permissions.js** - Attach user permissions ke request object
3. **require-permission.js** - Validasi permission yang diperlukan untuk endpoint tertentu

### Protected Routes

Semua endpoint (kecuali `/auth/login` dan `/auth/register`) memerlukan JWT token:

```http
Authorization: Bearer <your-jwt-token>
```

### Permission System

Setiap endpoint dapat dibatasi berdasarkan permission tertentu menggunakan middleware:

```javascript
router.get('/sensitive-data', 
  verifyToken, 
  requirePermission('admin'), 
  controller.getData
);
```

## 🌐 Database Schema

### Furniture WIP Table
```sql
CREATE TABLE FurnitureWIP (
    id INT PRIMARY KEY IDENTITY,
    labelCode NVARCHAR(50) NOT NULL UNIQUE,
    productName NVARCHAR(200),
    quantity DECIMAL(18,2),
    machineId NVARCHAR(50),
    status NVARCHAR(50),
    createdAt DATETIME DEFAULT GETDATE(),
    updatedAt DATETIME DEFAULT GETDATE()
)
```

### Bongkar Susun Table
```sql
CREATE TABLE BongkarSusun (
    id INT PRIMARY KEY IDENTITY,
    labelCode NVARCHAR(50) NOT NULL,
    materialType NVARCHAR(100),
    quantity DECIMAL(18,2),
    location NVARCHAR(100),
    operatorId INT,
    status NVARCHAR(50),
    createdAt DATETIME DEFAULT GETDATE(),
    updatedAt DATETIME DEFAULT GETDATE()
)
```

### Jenis Bonggolan Table
```sql
CREATE TABLE JenisBonggolan (
    id INT PRIMARY KEY IDENTITY,
    kode NVARCHAR(50) NOT NULL UNIQUE,
    nama NVARCHAR(200),
    deskripsi NVARCHAR(500),
    aktif BIT DEFAULT 1,
    createdAt DATETIME DEFAULT GETDATE(),
    updatedAt DATETIME DEFAULT GETDATE()
)
```

### Label Tables
Terdapat beberapa tabel untuk berbagai tipe label:
- **LabelBonggolan** - Labels untuk material bonggolan
- **LabelBroker** - Labels untuk proses broker
- **LabelCrusher** - Labels untuk proses crusher
- **LabelAll** - View/table agregasi semua label

### Machine Configuration
```sql
CREATE TABLE MesinProduksi (
    id INT PRIMARY KEY IDENTITY,
    kode NVARCHAR(50) NOT NULL UNIQUE,
    nama NVARCHAR(200),
    lokasi NVARCHAR(200),
    tipe NVARCHAR(100),
    status NVARCHAR(50),
    createdAt DATETIME DEFAULT GETDATE(),
    updatedAt DATETIME DEFAULT GETDATE()
)
```

## 📱 Integration dengan Flutter App

Backend ini terintegrasi dengan Flutter Production Management App yang menggunakan MVVM architecture. Flutter app menangani:
- QR/Barcode scanning
- Full pallet, select, dan partial scanning modes
- Real-time data sync dengan backend via WebSocket
- Offline-first approach dengan local caching

## 🏗️ Architecture & Design Patterns

### Modular Architecture
Project menggunakan modular architecture dengan separation of concerns:
- **Core** - Infrastruktur dasar (config, middleware, shared utilities)
- **Modules** - Business logic modules yang independen

### Module Structure
Setiap module mengikuti pattern yang konsisten:
```
module-name/
├── module-name-controller.js    # Request handling & response
├── module-name-service.js       # Business logic
├── module-name-route.js         # Route definitions
└── _tests_/                     # Unit tests
```

### Service Layer Pattern
Business logic dipisahkan dari controllers untuk:
- Better testability
- Code reusability
- Easier maintenance

### Middleware Pipeline
Request flow: `Request → verify-token → attach-permissions → require-permission → Controller → Service → Database`

### Transaction Guard
Sistem `tutup-transaksi-guard.js` memastikan:
- Data integrity saat tutup transaksi
- Validasi sebelum closing
- Rollback mechanism jika diperlukan

## 🐛 Troubleshooting

### Connection Timeout
Jika mengalami timeout saat connect ke SQL Server:
```javascript
// Tambahkan di config database
options: {
  connectTimeout: 30000,
  requestTimeout: 30000,
  trustServerCertificate: true
}
```

### CORS Issues
Pastikan CORS sudah dikonfigurasi dengan benar:
```javascript
app.use(cors({
  origin: ['http://localhost:3000', 'your-flutter-app-domain'],
  credentials: true
}));
```

### WebSocket Connection Issues
Jika WebSocket tidak terkoneksi:
```javascript
// Client side
const socket = io('http://localhost:3000', {
  transports: ['websocket', 'polling'],
  reconnection: true,
  reconnectionAttempts: 5
});
```

### JWT Token Expired
Token expiration handling:
```javascript
// Response akan return 401 Unauthorized
// Client harus refresh token atau login ulang
```

### Permission Denied
Pastikan user memiliki permission yang sesuai:
```javascript
// Check di attach-permissions middleware
// Permission di-assign berdasarkan user role
```

## 🔌 WebSocket Events

Backend menggunakan Socket.IO untuk real-time communication:

### Server Events (Emit)
```javascript
// Label updates
socket.emit('label:created', labelData);
socket.emit('label:updated', labelData);
socket.emit('label:deleted', labelId);

// Production updates
socket.emit('production:status', statusData);
socket.emit('production:alert', alertData);

// Machine updates
socket.emit('machine:status', machineData);
```

### Client Events (Listen)
```javascript
// Subscribe to specific label updates
socket.on('label:subscribe', (labelType) => {
  // Client wants to receive updates for specific label type
});

// Unsubscribe
socket.on('label:unsubscribe', (labelType) => {
  // Stop receiving updates
});
```

## 📝 Development Guidelines

### Code Style
1. Gunakan ES6+ syntax (const, let, arrow functions, destructuring)
2. Follow clean code principles dan SOLID principles
3. Consistent naming conventions:
   - **Files**: kebab-case (furniture-wip-service.js)
   - **Functions/Variables**: camelCase (getUserData, labelCode)
   - **Classes**: PascalCase (LabelService, UserController)

### Module Development
Saat membuat module baru:
```bash
src/modules/new-module/
├── new-module-controller.js
├── new-module-service.js
├── new-module-route.js
└── _tests_/
    ├── new-module-controller.test.js
    └── new-module-service.test.js
```

### Error Handling
Gunakan try-catch dan return consistent error format:
```javascript
try {
  // Business logic
  return {
    success: true,
    message: 'Operation successful',
    data: result
  };
} catch (error) {
  console.error('Error:', error);
  return {
    success: false,
    message: error.message || 'Internal server error',
    error: process.env.NODE_ENV === 'development' ? error : undefined
  };
}
```

### Testing
1. Buat unit test untuk setiap service function
2. Test coverage minimal 70%
3. Mock database calls di unit tests
4. Gunakan Jest untuk testing framework

### Logging
Gunakan shared log utility:
```javascript
const { log } = require('../../core/shared/log');
log.info('Operation completed');
log.error('Error occurred', error);
```

### Database Transactions
Untuk operasi multiple tables, gunakan transaction:
```javascript
const transaction = new sql.Transaction(pool);
await transaction.begin();
try {
  // Multiple operations
  await transaction.commit();
} catch (error) {
  await transaction.rollback();
  throw error;
}
```

## 🔄 Version History

- **v1.0.0** - Initial release
  - Basic CRUD operations untuk label management
  - Authentication system dengan JWT
  - Database configuration
  
- **v1.1.0** - Module Expansion
  - Implementasi Bongkar Susun module
  - Jenis Bonggolan management
  - Permission-based authorization
  
- **v1.2.0** - Production Features
  - Furniture WIP label system
  - Gilingan dan Mixer modules
  - Machine location helper
  
- **v1.3.0** - Real-time & Testing
  - WebSocket integration dengan Socket.IO
  - Unit testing dengan Jest
  - Transaction guard system
  - Tutup transaksi configuration

## 👥 Contributors

- [Garda Wilson](https://github.com/gardawilson)

## 📄 License

*[Tambahkan license yang sesuai]*

## 🤝 Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📧 Contact

Garda Wilson - [@gardawilson](https://github.com/gardawilson)

Project Link: [https://github.com/gardawilson/pps-backend](https://github.com/gardawilson/pps-backend)

---

**Note:** Untuk informasi lebih detail tentang API endpoints, silakan refer ke file `test.html` atau gunakan tools seperti Postman untuk testing.

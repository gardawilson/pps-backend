# PPS Backend

Backend API untuk **Plastic Production System (PPS)** yang dibangun dengan Node.js, Express.js, dan SQL Server.

---

## Tech Stack

- **Runtime:** Node.js 20
- **Framework:** Express.js
- **Database:** SQL Server (mssql)
- **Real-time:** Socket.IO
- **Authentication:** JWT
- **Container:** Docker
- **CI/CD:** GitHub Actions + Self-hosted Runner

---

## Struktur Branch

| Branch       | Fungsi                        |
| ------------ | ----------------------------- |
| `main`       | Development sehari-hari       |
| `production` | Trigger auto deploy ke server |

---

## Setup Lokal

### 1. Clone repository

```bash
git clone https://github.com/gardawilson/pps-backend.git
cd pps-backend
```

### 2. Buat file `.env`

```env
# Database
DB_USER=sa
DB_PASSWORD=yourpassword
DB_SERVER=192.168.10.100
DB_PORT=1433
DB_DATABASE=PPS

# Security
SECRET_KEY=yoursecretkey

# Server
PORT=7500

# Update / APK Distribution
UPDATES_DIR=/app/deploy/pps_update
UPDATES_HOST_DIR=D:/deploy/pps_update
UPDATE_TOKEN=your-update-token
```

### 3. Jalankan dengan Docker

```bash
docker compose up -d --build
```

### 4. Test

```
http://localhost:7500/health
```

---

## Struktur Folder Deploy APK

Buat folder berikut di host (di luar Docker):

```
D:\deploy\pps_update\
├── tablet\
│   └── app-release.apk
└── mobile\
    └── app-release.apk
```

---

## Deployment ke Server

### Otomatis (via GitHub Actions)

Setiap push ke branch `production` akan otomatis trigger deploy ke server.

```bash
# Setelah coding di main
git checkout production
git merge main
git push origin production
```

GitHub Actions akan otomatis:

1. Pull kode terbaru di server
2. Rebuild Docker image
3. Restart container

### Manual (jika GitHub Actions gagal)

SSH atau RDP ke server, lalu:

```bash
cd D:\backend\pps_backend
git pull origin production
docker compose up -d --build
```

---

## Flow Deployment

```
coding → git push origin main
           ↓ (siap deploy)
       git merge main ke production
       git push origin production
           ↓
       GitHub Actions trigger
           ↓
       Self-hosted Runner di server
           ↓
       git pull + docker compose up -d --build
           ↓
       Backend live di 192.168.10.100:7500
```

---

## Perintah Docker Berguna

```bash
# Lihat status container
docker compose ps

# Lihat log realtime
docker compose logs -f app

# Restart container
docker compose restart app

# Stop semua
docker compose down
```

---

## Health Check

```
GET /health
```

Response:

```json
{
  "status": "OK",
  "message": "PPS Backend is healthy",
  "version": "1.0.0",
  "timestamp": "2026-01-01T00:00:00.000Z",
  "uptime": "120s"
}
```

---

## Struktur Module

Setiap module mengikuti pattern:

```
src/modules/nama-module/
├── nama-module-controller.js
├── nama-module-service.js
└── nama-module-route.js
```

---

## Contributors

- [Garda Wilson](https://github.com/gardawilson)

# Bongkar Susun V2

Dokumentasi ringkas untuk endpoint `bongkar-susun-v2`.

## Base Endpoint

- `GET /api/bongkar-susun-v2`
- `GET /api/bongkar-susun-v2/:noBongkarSusun`
- `GET /api/bongkar-susun-v2/label/:labelCode`
- `POST /api/bongkar-susun-v2`
- `DELETE /api/bongkar-susun-v2/:noBongkarSusun`

## Prefix Label

| Prefix | Kategori       |
| ------ | -------------- |
| `A.`   | `bahanBaku`    |
| `B.`   | `washing`      |
| `D.`   | `broker`       |
| `F.`   | `crusher`      |
| `V.`   | `gilingan`     |
| `H.`   | `mixer`        |
| `BB.`  | `furnitureWip` |
| `BA.`  | `barangJadi`   |
| `M.`   | `bonggolan`    |

## POST ` /api/bongkar-susun-v2`

Format umum:

```json
{
  "note": "opsional",
  "inputs": ["..."],
  "outputs": [{ "...": "..." }]
}
```

### 1) Bahan Baku

| Field                    | Format               | Keterangan                                            |
| ------------------------ | -------------------- | ----------------------------------------------------- |
| `inputs`                 | `["A.0000002509-1"]` | Label bahan baku, format `A.<NoBahanBaku>-<NoPallet>` |
| `outputs[].idJenis`      | number               | Harus sama dengan `IdJenisPlastik` input              |
| `outputs[].saks`         | array                | Daftar sak output                                     |
| `outputs[].saks[].noSak` | number               | Nomor sak                                             |
| `outputs[].saks[].berat` | number               | Berat per sak                                         |
| Balance                  | berat                | Berdasarkan total berat                               |

Contoh:

```json
{
  "note": "opsional",
  "inputs": ["A.0000002509-1"],
  "outputs": [
    {
      "idJenis": 137,
      "saks": [
        { "noSak": 1, "berat": 1000 },
        { "noSak": 2, "berat": 1000 }
      ]
    }
  ]
}
```

### 2) Washing

| Field                    | Format             | Keterangan                    |
| ------------------------ | ------------------ | ----------------------------- |
| `inputs`                 | `["B.0000013157"]` | Label washing                 |
| `outputs[].idJenis`      | number             | Harus sama dengan jenis input |
| `outputs[].saks`         | array              | Daftar sak output             |
| `outputs[].saks[].noSak` | number             | Nomor sak                     |
| `outputs[].saks[].berat` | number             | Berat per sak                 |
| Balance                  | berat              | Berdasarkan total berat       |

### 3) Broker

| Field                    | Format             | Keterangan                    |
| ------------------------ | ------------------ | ----------------------------- |
| `inputs`                 | `["D.0000001234"]` | Label broker                  |
| `outputs[].idJenis`      | number             | Harus sama dengan jenis input |
| `outputs[].saks`         | array              | Daftar sak output             |
| `outputs[].saks[].noSak` | number             | Nomor sak                     |
| `outputs[].saks[].berat` | number             | Berat per sak                 |
| Balance                  | berat              | Berdasarkan total berat       |

### 4) Crusher

| Field               | Format             | Keterangan                    |
| ------------------- | ------------------ | ----------------------------- |
| `inputs`            | `["F.0000005811"]` | Label crusher                 |
| `outputs[].idJenis` | number             | Harus sama dengan jenis input |
| `outputs[].berat`   | number             | Berat output                  |
| Balance             | berat              | Berdasarkan total berat       |

### 5) Gilingan

| Field               | Format             | Keterangan                    |
| ------------------- | ------------------ | ----------------------------- |
| `inputs`            | `["V.0000001234"]` | Label gilingan                |
| `outputs[].idJenis` | number             | Harus sama dengan jenis input |
| `outputs[].berat`   | number             | Berat output                  |
| Balance             | berat              | Berdasarkan total berat       |

### 6) Mixer

| Field                    | Format             | Keterangan                    |
| ------------------------ | ------------------ | ----------------------------- |
| `inputs`                 | `["H.0000025312"]` | Label mixer                   |
| `outputs[].idJenis`      | number             | Harus sama dengan jenis input |
| `outputs[].saks`         | array              | Daftar sak output             |
| `outputs[].saks[].noSak` | number             | Nomor sak                     |
| `outputs[].saks[].berat` | number             | Berat per sak                 |
| Balance                  | berat              | Berdasarkan total berat       |

### 7) Furniture WIP

| Field               | Format              | Keterangan                    |
| ------------------- | ------------------- | ----------------------------- |
| `inputs`            | `["BB.0000044552"]` | Label furniture WIP           |
| `outputs[].idJenis` | number              | Harus sama dengan jenis input |
| `outputs[].pcs`     | number              | Jumlah pcs output             |
| Balance             | pcs                 | Berdasarkan total pcs         |

### 8) Barang Jadi

| Field               | Format              | Keterangan                    |
| ------------------- | ------------------- | ----------------------------- |
| `inputs`            | `["BA.0000040338"]` | Label barang jadi             |
| `outputs[].idJenis` | number              | Harus sama dengan jenis input |
| `outputs[].pcs`     | number              | Jumlah pcs output             |
| Balance             | pcs                 | Berdasarkan total pcs         |

### 9) Bonggolan

| Field               | Format             | Keterangan                    |
| ------------------- | ------------------ | ----------------------------- |
| `inputs`            | `["M.0000011729"]` | Label bonggolan               |
| `outputs[].idJenis` | number             | Harus sama dengan jenis input |
| `outputs[].berat`   | number             | Berat output                  |
| Balance             | berat              | Berdasarkan total berat       |

## POST Response

Response umum:

```json
{
  "success": true,
  "data": {
    "noBongkarSusun": "BG.0000002279",
    "tanggal": "2026-04-24",
    "category": "bahanBaku",
    "totalBeratInput": 2000,
    "totalBeratOutput": 2000,
    "inputs": [],
    "outputs": [],
    "audit": {
      "actorId": 109,
      "requestId": "..."
    }
  }
}
```

Catatan:

- Untuk kategori berbasis berat, field total memakai `totalBeratInput` dan `totalBeratOutput`.
- Untuk kategori berbasis pcs, field total memakai `totalPcsInput` dan `totalPcsOutput`.
- Format `outputs` mengikuti kategori masing-masing.

## GET Label Info ` /api/bongkar-susun-v2/label/:labelCode`

### Bahan Baku

Format label:

- `A.<NoBahanBaku>-<NoPallet>`

Contoh:

- `A.0000002509-1`

Response:

```json
{
  "labelCode": "A.0000002509-1",
  "category": "bahanBaku",
  "noPallet": 1,
  "idJenis": 137,
  "namaJenis": "NAMA JENIS",
  "idWarehouse": 1,
  "namaWarehouse": "NAMA WAREHOUSE",
  "keterangan": null,
  "idStatus": 1,
  "statusText": "PASS",
  "moisture": null,
  "meltingIndex": null,
  "elasticity": null,
  "tenggelam": null,
  "density": null,
  "density2": null,
  "density3": null,
  "hasBeenPrinted": 0,
  "blok": "BSS",
  "idLokasi": 1,
  "sakActual": 10,
  "beratActual": 2010.07,
  "sakSisa": 4,
  "beratSisa": 1000,
  "isEmpty": false,
  "details": []
}
```

Jika semua sak pada pallet tersebut sudah memiliki `DateUsage`, label dianggap sudah terpakai.

### Washing

```json
{
  "labelCode": "B.0000013157",
  "category": "washing",
  "dateCreate": "2026-04-24T00:00:00.000Z",
  "idJenis": 46,
  "namaJenis": "PP GIL HITAM (CUCI)",
  "jumlahSak": 30,
  "totalBerat": 750,
  "hasBeenPrinted": 0,
  "createBy": "x",
  "mesin": "BG.0000002258",
  "shift": 1,
  "idWarehouse": 1,
  "namaWarehouse": "INJECT",
  "idStatus": 1,
  "statusText": "PASS"
}
```

### Broker

```json
{
  "labelCode": "D.0000001234",
  "category": "broker",
  "dateCreate": "2026-04-24T00:00:00.000Z",
  "idJenis": 18,
  "namaJenis": "NAMA BROKER",
  "jumlahSak": 10,
  "totalBerat": 999,
  "saks": [],
  "hasBeenPrinted": 0,
  "createBy": "x",
  "mesin": "MESIN A",
  "shift": 1,
  "idWarehouse": 1,
  "namaWarehouse": "WAREHOUSE A",
  "idStatus": 1,
  "statusText": "PASS"
}
```

### Crusher

```json
{
  "labelCode": "F.0000005811",
  "category": "crusher",
  "dateCreate": "2026-04-22T00:00:00.000Z",
  "idJenis": 28,
  "namaJenis": "BONGGOLAN CRUSHER HIJAU BROKER",
  "dateUsage": null,
  "berat": 500,
  "mesin": "BG.0000002252",
  "createBy": "x",
  "namaWarehouse": "INJECT",
  "shift": 0,
  "hasBeenPrinted": 0
}
```

### Gilingan

```json
{
  "labelCode": "V.0000001234",
  "category": "gilingan",
  "dateCreate": "2026-04-24T00:00:00.000Z",
  "idJenis": 12,
  "namaJenis": "NAMA GILINGAN",
  "totalBerat": 250,
  "dateUsage": null,
  "mesin": "MESIN A",
  "createBy": "x",
  "namaWarehouse": "WAREHOUSE A",
  "shift": 1,
  "hasBeenPrinted": 0
}
```

### Mixer

```json
{
  "labelCode": "H.0000025312",
  "category": "mixer",
  "dateCreate": "2026-04-24T00:00:00.000Z",
  "idJenis": 25,
  "namaJenis": "PP MIX CREAM LEMARI",
  "jumlahSak": 5,
  "totalBerat": 75,
  "saks": [],
  "dateUsage": null,
  "mesin": "MESIN A",
  "createBy": "x",
  "namaWarehouse": "WAREHOUSE A",
  "shift": 1,
  "hasBeenPrinted": 0
}
```

### Furniture WIP

```json
{
  "labelCode": "BB.0000044552",
  "category": "furnitureWip",
  "dateCreate": "2026-04-23T00:00:00.000Z",
  "idJenis": 177,
  "namaJenis": "CUP PANEL HITAM (MS 2802)",
  "pcs": 30,
  "hasBeenPrinted": 0,
  "createBy": "x",
  "mesin": "BG.0000002258",
  "shift": null
}
```

### Barang Jadi

```json
{
  "labelCode": "BA.0000040338",
  "category": "barangJadi",
  "dateCreate": "2025-11-11T00:00:00.000Z",
  "idJenis": 284,
  "namaJenis": "GRANDE PLASTIK KABINET PK 3003 3TX6P AVNGR (LP 008)",
  "pcs": 3,
  "hasBeenPrinted": 0,
  "createBy": "x",
  "mesin": "PACKING LEMARI",
  "shift": 1
}
```

### Bonggolan

```json
{
  "labelCode": "M.0000011729",
  "category": "bonggolan",
  "idJenis": 18,
  "namaJenis": "NAMA BONGGOLAN",
  "idWarehouse": null,
  "idStatus": true,
  "totalBerat": 999
}
```

## GET List ` /api/bongkar-susun-v2`

Response list sekarang memuat:

| Field              | Keterangan          |
| ------------------ | ------------------- |
| `NoBongkarSusun`   | Nomor transaksi     |
| `Tanggal`          | Tanggal transaksi   |
| `IdUsername`       | ID user pembuat     |
| `Username`         | Username pembuat    |
| `Note`             | Catatan             |
| `category`         | Kategori transaksi  |
| `inputLabelCount`  | Jumlah label input  |
| `outputLabelCount` | Jumlah label output |
| `balance`          | Status balance      |

Catatan:

- `bahanBaku` dihitung balance berdasarkan **berat**
- kategori lain mengikuti logika masing-masing

## GET Detail ` /api/bongkar-susun-v2/:noBongkarSusun`

Response detail:

```json
{
  "header": {},
  "inputs": [],
  "outputs": []
}
```

Catatan:

- `bahanBaku` detail dikelompokkan per `NoBahanBaku` + `NoPallet`
- `washing`, `broker`, dan `mixer` menampilkan detail sak
- kategori berbasis `pcs` tetap memakai `pcs`

## DELETE ` /api/bongkar-susun-v2/:noBongkarSusun`

Hapus transaksi bongkar susun, dengan guard agar tidak menghapus transaksi yang sudah menghasilkan output terpakai di proses lain.

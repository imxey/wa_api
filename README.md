# WA API

## Deploy (server)

App ada di `/var/www/wa-service`, dijalankan dengan PM2 sebagai proses `wa-api`
(pm2 terpasang di `node_modules`, jadi panggil lewat `npx pm2` dari folder app).

```sh
cd /var/www/wa-service
npx pm2 list              # status proses
npx pm2 logs wa-api       # log
npx pm2 restart wa-api    # restart
npx pm2 save              # simpan daftar proses
npx pm2 resurrect         # hidupkan lagi proses yang disimpan (setelah reboot)
```

Update:

```sh
git pull && npm install && npx pm2 restart wa-api
```

User server dipakai bersama: selalu sebut nama `wa-api`, jangan pakai `all`
(misalnya `pm2 restart all` / `pm2 stop all`) karena ikut mengenai proses orang lain.

## Manajemen akun

Daftar akun dan flag inbox disimpan di `auth/akun.json` (ikut volume `auth/`, tetap ada setelah restart).
Saat pertama jalan file ini dibuat otomatis dari `WA_ACCOUNTS` dan `INBOX_ACCOUNTS`; setelah itu env tersebut
diabaikan. Semua endpoint wajib header `X-Secret-Key` dan mengembalikan JSON.

Buat akun baru (belum login, QR muncul di `GET /status`). Nama: 1-32 karakter huruf kecil/angka/strip.
`201` sukses, `400` nama tidak valid, `409` nama sudah ada.

```sh
curl -X POST http://127.0.0.1:8086/akun \
  -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"akun": "admin9"}'
# {"success":true,"akun":"admin9","inbox":false,"dibuat_at":"2026-11-02T03:00:00.000Z"}
```

Hapus akun: logout dari WhatsApp lalu hapus sesinya. `/send` yang sedang berjalan di akun ini gagal
dengan error biasa (`500`), `/send` berikutnya `400 akun tidak dikenal`. `404` kalau akun tidak ada.

```sh
curl -X DELETE http://127.0.0.1:8086/akun/admin9 -H "X-Secret-Key: $SECRET_KEY"
# {"success":true}
```

Atur apakah chat masuk akun ini ditarik. `inbox: true` = chat masuk ditarik ke `GET /pesan-masuk`.
`inbox: false` = chat masuk hanya terlihat di HP. Semua akun tetap bisa kirim lewat `POST /send`.
Untuk akun inbox, pesan dari `/send` ke nomor yang sudah pernah chat ikut masuk antrean (`arah: "keluar"`,
`sumber: "aplikasi"`) supaya tampil di percakapan; broadcast/chat pertama ke nomor baru tidak, supaya daftar
chat tidak kebanjiran (catat sendiri di aplikasi pemanggil).

```sh
curl -X POST http://127.0.0.1:8086/akun/admin9/inbox \
  -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"inbox": true}'
# {"success":true,"akun":"admin9","inbox":true,"dibuat_at":"2026-11-02T03:00:00.000Z"}
```

`GET /status` menampilkan field `dibuat_at` per akun, di samping `akun`, `ready`, `inbox`, `nomor`, `qr`.

## Reply, forward, stiker, media

Item `GET /pesan-masuk` punya field tambahan:

- `jenis`: `teks`, `gambar`, `stiker`, `video`, `video_bulat` (video note), `audio`, `pesan_suara` (voice note), `dokumen`
- `media`: `true` = file-nya bisa diambil lewat `GET /media?cursor=<cursor>`
- `balasan_untuk`: `id_pesan` yang dibalas (reply), `kutipan`: potongan teksnya (`[gambar]` dsb. kalau tanpa teks)
- `diteruskan`: `true` kalau pesan ini hasil forward

Ambil file media (semua jenis di atas, dokumen ikut nama file di `Content-Disposition`):

```sh
curl -H "X-Secret-Key: $SECRET_KEY" "http://127.0.0.1:8086/media?cursor=1730000000000000" -o file
```

Reply: `POST /send` dengan `balas_id_pesan`. Respon berisi `id_pesan` pesan yang terkirim dan `kutipan_dimuat`.
Kalau pesan yang dibalas sudah tidak ada di memori gateway (restart / lebih lama dari 1000 item terakhir),
tetap dikirim sebagai reply dengan kutipan "Pesan tidak dapat dimuat..." dan `kutipan_dimuat: false`.

```sh
curl -X POST http://127.0.0.1:8086/send \
  -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"akun": "helpdesk", "nomor": "08123456789", "pesan": "Baik, kami cek", "balas_id_pesan": "3EB0ABC123"}'
# {"success":true,"message":"Pesan berhasil dikirim!","id_pesan":"3EB0DEF456","kutipan_dimuat":true}
```

Stiker: `POST /send` multipart dengan `stiker=true` dan file `image/webp` (file lain ditolak `400`).

```sh
curl -X POST http://127.0.0.1:8086/send -H "X-Secret-Key: $SECRET_KEY" \
  -F akun=helpdesk -F nomor=08123456789 -F stiker=true -F "file=@stiker.webp;type=image/webp"
```

Forward: `POST /teruskan`, hanya untuk pesan yang masih ada di memori gateway (`404` kalau tidak).
Media diteruskan tanpa diunduh/diupload ulang.

```sh
curl -X POST http://127.0.0.1:8086/teruskan \
  -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"akun": "helpdesk", "id_pesan": "3EB0ABC123", "nomor_tujuan": "08129876543"}'
# {"success":true,"id_pesan":"3EB0GHI789"}
```

## Status pesan (centang)

Pemetaan ke tampilan WhatsApp untuk pesan yang dikirim:

| Kondisi | Tampilan |
|---|---|
| Menunggu respon `POST /send` / `/teruskan` | jam |
| Respon `200` (simpan `id_pesan`-nya) | centang satu |
| `status: "diterima"` | centang dua abu-abu |
| `status: "dibaca"` / `"diputar"` (voice/video note) | centang dua biru |
| `status: "gagal"` | gagal kirim |

Perubahan status ditarik lewat `GET /status-pesan`, dengan pola cursor yang sama seperti `/pesan-masuk`
(simpan `cursor_terakhir`, ulangi selama `ada_lagi`). Berlaku untuk semua akun, termasuk broadcast dan
pesan yang diketik dari HP. Status tidak pernah mundur. Kalau penerima mematikan laporan dibaca, berhenti di
`diterima`. Disimpan di memori (5000 terakhir): yang belum ditarik saat gateway restart hilang.

```sh
curl -H "X-Secret-Key: $SECRET_KEY" "http://127.0.0.1:8086/status-pesan?sejak=0"
# {"status":[{"cursor":1730000000000001,"akun":"admin1","id_pesan":"3EB0DEF456","status":"dibaca",
#   "waktu":"2026-11-02T03:05:00.000Z"}],"cursor_terakhir":1730000000000001,"ada_lagi":false}
```

Tandai chat sudah dibaca (pelanggan melihat centang biru), misalnya saat admin membuka chat di aplikasi.
Menandai semua pesan masuk dari nomor itu yang masih ada di memori; yang sudah ditandai tidak dikirim ulang.
Kalau laporan dibaca di akun WA ini dimatikan, pelanggan tidak melihat centang biru.

```sh
curl -X POST http://127.0.0.1:8086/baca \
  -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"akun": "helpdesk", "nomor": "08123456789"}'
# {"success":true,"dibaca":3}
```

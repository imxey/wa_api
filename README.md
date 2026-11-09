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

Cek sebelum restart (tanpa koneksi WhatsApp). Dengan `SELFCHECK_DB_NAME` terisi, skenario antrean broadcast
ikut diuji ke database tes itu:

```sh
node index.js --self-check   # "self-check OK" + "broadcast self-check OK"
```

## Broadcast

Gateway menerima satu broadcast utuh lalu mengirimnya sendiri per akun, pelan-pelan, tahan restart
(antrean di MySQL). Semua endpoint wajib header `X-Secret-Key`, membalas JSON, error `4xx` berisi `{error}`.
`503` = database belum siap / belum diatur (endpoint lain tetap jalan).

### Aturan kirim

- Hanya pada jam `[jam_mulai, jam_selesai)` WIB; di luar itu menunggu jam mulai berikutnya.
- `batas_harian` per akun (terkirim hari ini di akun itu, semua broadcast); tercapai = akun itu lanjut besok.
- Jeda acak `jeda_min..jeda_maks` detik dari pesan terakhir akun itu (lintas broadcast, termasuk yang gagal).
- Istirahat acak 300-600 detik setiap `istirahat_setiap` pesan terkirim akun itu hari ini.
- Akun berbeda paralel, satu akun satu pesan dalam satu waktu. Urutan: broadcast tertua, lalu urutan penerima.
- Akun belum tersambung saat gilirannya, lebih dari 120 detik berturut-turut: broadcast itu `jeda`
  (catatan `Akun <akun> belum tersambung`). Putus sebentar / baru restart tidak menjeda.
- `gagal_beruntun` kegagalan berturut-turut dalam satu broadcast: broadcast `jeda` dengan catatan.
- Penerima dengan akun yang tidak ada di gateway: `gagal` `akun tidak ada`.
- Lampiran: gambar dikirim sebagai foto, selain itu dokumen; `pesan` jadi keterangannya.
  File disimpan di `auth/lampiran/<id>/`, dihapus 7 hari setelah broadcast selesai/dibatalkan.
- Restart di tengah kirim: penerima yang belum sempat dikirim kembali ke antrean. Yang sudah diserahkan ke
  WhatsApp jadi `gagal` "Proses berhenti saat mengirim, mungkin sudah terkirim" dengan `id_pesan`-nya, dan
  tidak dikirim ulang (juga tidak oleh `/lanjut`) supaya tidak dobel. Cocokkan `id_pesan` dengan `/status-pesan`.

### Buat broadcast

Multipart: field `data` (JSON) + opsional file `lampiran`. Tanpa lampiran boleh body JSON biasa.
Maks 5000 penerima, `id_eksternal` 1-64 karakter, `id_eksternal` penerima unik dalam satu broadcast,
`pesan` sudah jadi (tanpa variabel), boleh kosong hanya kalau ada lampiran.
Idempoten: `id_eksternal` yang sama dikirim ulang membalas broadcast yang sudah ada (`200`, bukan `201`).

```sh
curl -X POST http://127.0.0.1:8086/broadcast -H "X-Secret-Key: $SECRET_KEY" \
  -F 'data={"id_eksternal":"45","penerima":[
        {"id_eksternal":"901","akun":"admin1","nomor":"08123456789","pesan":"Yth. Bapak Budi, ..."},
        {"id_eksternal":"902","akun":"admin2","nomor":"08129876543","pesan":"Yth. Ibu Sari, ..."}]}' \
  -F "lampiran=@undangan.pdf;type=application/pdf"
# {"id":12,"id_eksternal":"45","total":2}
```

### Kendalikan broadcast

`<id>` boleh `id_eksternal` atau id gateway; `id_eksternal` dicocokkan lebih dulu (pakai `id_eksternal`).

```sh
curl -X POST -H "X-Secret-Key: $SECRET_KEY" http://127.0.0.1:8086/broadcast/45/jeda
curl -X POST -H "X-Secret-Key: $SECRET_KEY" http://127.0.0.1:8086/broadcast/45/lanjut   # yang gagal masuk antrean lagi
curl -X POST -H "X-Secret-Key: $SECRET_KEY" http://127.0.0.1:8086/broadcast/45/batal    # sisa antrean jadi dibatalkan
curl -X POST -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"penerima": ["901"]}' http://127.0.0.1:8086/broadcast/45/lewati                # atau {"akun": "admin1"}
# {"id":12,"id_eksternal":"45","status":"jeda","catatan":"Dijeda lewat API"}
```

- `lewati`: penerima `menunggu`/`gagal` jadi `dilewati`; broadcast yang tidak punya sisa jadi `selesai`.
- `lanjut` juga untuk broadcast `selesai` yang masih punya penerima gagal. `409` kalau lampirannya sudah dihapus.
- `409` untuk aksi pada broadcast `dibatalkan`, dan `jeda`/`batal` pada broadcast `selesai`.

Status broadcast: `berjalan`, `jeda`, `selesai`, `dibatalkan`.
Status penerima: `menunggu`, `mengirim`, `terkirim`, `gagal`, `dilewati`, `dibatalkan`.

```sh
curl -H "X-Secret-Key: $SECRET_KEY" http://127.0.0.1:8086/broadcast/45
# {"broadcast":{"id":12,"id_eksternal":"45","status":"berjalan","catatan":null,"lampiran_nama":"undangan.pdf",
#   "dibuat_at":"...","diubah_at":"..."},
#  "penerima":[{"id_eksternal":"901","akun":"admin1","nomor":"08123456789","status":"terkirim","error":null,
#   "id_pesan":"3EB0...","dikirim_at":"2026-11-06T02:00:15.000Z"}, ...]}
```

### Hasil kirim (feed)

Pola sama dengan `/status-pesan` (simpan `cursor_terakhir`, ulangi selama `ada_lagi`, `tunggu` maks 30).
Disimpan di MySQL (30 hari), jadi tidak hilang saat restart. Item dengan `penerima_id_eksternal: null` adalah
perubahan status broadcast (`jeda`, `berjalan`, `selesai`, `dibatalkan`), catatannya di `error`.
Status penerima di feed: `terkirim`, `gagal`, `dilewati`, `dibatalkan`, dan `menunggu` (setelah `/lanjut`).

```sh
curl -H "X-Secret-Key: $SECRET_KEY" "http://127.0.0.1:8086/hasil-kirim?sejak=0&tunggu=25"
# {"hasil":[{"cursor":1793971428266000,"broadcast_id_eksternal":"45","penerima_id_eksternal":"901",
#   "akun":"admin1","status":"terkirim","error":null,"id_pesan":"3EB0...","waktu":"..."}],
#  "cursor_terakhir":1793971428266000,"ada_lagi":false}
```

### Pengaturan dan statistik

Berlaku langsung tanpa restart. Boleh kirim sebagian field. Bilangan bulat >= 1, `jam_mulai` 0-23,
`jam_selesai` 1-24 dan lebih besar dari `jam_mulai`, `jeda_maks >= jeda_min`.

```sh
curl -H "X-Secret-Key: $SECRET_KEY" http://127.0.0.1:8086/pengaturan
curl -X PUT -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"batas_harian":150,"jeda_min":15,"jeda_maks":45,"istirahat_setiap":25,"jam_mulai":8,"jam_selesai":17,"gagal_beruntun":3}' \
  http://127.0.0.1:8086/pengaturan

curl -H "X-Secret-Key: $SECRET_KEY" http://127.0.0.1:8086/statistik
# {"per_akun":{"admin1":{"terkirim_hari_ini":42,"antrean":108,"terakhir_kirim":"2026-11-06T03:10:00.000Z"}},
#  "antrean_total":108}
```

### Migrasi (server PM2)

Gateway bisa dideploy lebih dulu: endpoint lama tidak berubah, dan tanpa database endpoint broadcast hanya
membalas `503`.

1. Buat database dan user MySQL:
   ```sql
   CREATE DATABASE wa_api CHARACTER SET utf8mb4;
   CREATE USER 'wa_api'@'localhost' IDENTIFIED BY '<password>';
   GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, REFERENCES ON wa_api.* TO 'wa_api'@'localhost';
   ```
2. Isi `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` di `.env`, ditambah `WA_DATA_KEY`
   (kunci enkripsi isi pesan di MySQL, buat sekali: `node -e "console.log('base64:'+require('crypto').randomBytes(32).toString('base64'))"`).
   Tanpa `WA_DATA_KEY` endpoint broadcast membalas 503. Jangan diganti selama masih ada antrean: pesan yang belum terkirim
   tidak bisa dibuka lagi. Folder `auth/` (sesi WhatsApp) jangan ikut backup tanpa enkripsi: isinya bisa membajak akun.
3. `git pull && npm install && npx pm2 restart wa-api`. Log harus berisi `[broadcast] siap`
   (tabel `broadcast`, `broadcast_penerima`, `pengaturan`, `hasil_kirim` dibuat otomatis).
4. Tambahkan path ini ke reverse proxy Apache (sama seperti path lain yang sudah diteruskan):
   `/broadcast`, `/broadcast/*`, `/hasil-kirim`, `/pengaturan`, `/statistik`.
5. Samakan `PUT /pengaturan` dengan pengaturan broadcast yang sekarang di aplikasi.
6. Baru setelah itu ubah aplikasi untuk memakai endpoint ini.

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

- `jenis`: `teks`, `gambar`, `stiker`, `video`, `video_bulat` (video note), `audio`, `pesan_suara` (voice note), `dokumen`, `polling`
  (`pesan` = pertanyaannya, field tambahan `pilihan` = daftar pilihannya)
- `media`: `true` = file-nya bisa diambil lewat `GET /media?cursor=<cursor>`
- `balasan_untuk`: `id_pesan` yang dibalas (reply), `kutipan`: potongan teksnya (`[gambar]` dsb. kalau tanpa teks)
- `diteruskan`: `true` kalau pesan ini hasil forward

Ambil file media (semua jenis di atas, dokumen ikut nama file di `Content-Disposition`):

```sh
curl -H "X-Secret-Key: $SECRET_KEY" "http://127.0.0.1:8086/media?cursor=1730000000000000" -o file
```

Reply: `POST /send` dengan `balas_id_pesan`. Respon berisi `id_pesan` pesan yang terkirim dan `kutipan_dimuat`.
Kalau pesan yang dibalas sudah tidak ada di memori gateway (restart / lebih lama dari 5000 item terakhir semua akun),
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

## Long-polling (`tunggu`)

`GET /pesan-masuk`, `GET /status-pesan` dan `GET /hasil-polling` menerima `tunggu=<detik>` (maks 30).
Kalau belum ada item baru setelah `sejak`, request ditahan sampai ada item baru (langsung dijawab) atau
waktunya habis (dijawab daftar kosong). Format respon sama. Tanpa `tunggu` perilakunya seperti biasa.

Dipakai oleh satu worker yang jalan terus di aplikasi pemanggil (1 proses per feed), bukan dari tiap
browser: request yang ditahan memakai 1 koneksi selama menunggu. Timeout HTTP di pemanggil harus lebih
lama dari `tunggu` (misalnya `tunggu=25`, timeout 35 detik). Batas 30 detik menjaga request tetap di bawah
timeout proxy Apache (default 60 detik).

```sh
curl -H "X-Secret-Key: $SECRET_KEY" "http://127.0.0.1:8086/pesan-masuk?sejak=1730000000000001&tunggu=25"
```

## Polling (vote)

Kirim polling ke chat pribadi (grup ditolak `400`), bisa dari semua akun. Body wajib JSON.
`pilihan`: 2-12 teks unik. `maks_pilihan`: `1` = pilih satu (default), `0` = boleh pilih banyak,
atau angka batasnya. Simpan `id_pesan` sebagai id polling.

```sh
curl -X POST http://127.0.0.1:8086/polling \
  -H "X-Secret-Key: $SECRET_KEY" -H "Content-Type: application/json" \
  -d '{"akun": "helpdesk", "nomor": "08123456789", "pertanyaan": "Jadwal pelatihan?", "pilihan": ["Senin", "Rabu", "Jumat"]}'
# {"success":true,"id_pesan":"3EB0POLL123"}
```

Vote ditarik lewat `GET /hasil-polling`, pola cursor sama seperti `/status-pesan`. Tiap item berisi
**pilihan lengkap terbaru** pemilih itu: ganti pilihan = item baru, `pilihan: []` = vote dibatalkan.
Di aplikasi cukup upsert per `(id_polling, pemilih)`, yang menang item dengan `cursor` terbesar.
`nomor_pemilih` bisa `null` kalau nomornya tersembunyi (`@lid`); pakai `jid_pemilih` sebagai identitas.

```sh
curl -H "X-Secret-Key: $SECRET_KEY" "http://127.0.0.1:8086/hasil-polling?sejak=0"
# {"hasil":[{"cursor":1730000000000005,"akun":"helpdesk","id_polling":"3EB0POLL123",
#   "nomor_pemilih":"628123456789","jid_pemilih":"628123456789@s.whatsapp.net","pilihan":["Rabu"],
#   "waktu":"2026-11-04T03:10:00.000Z"}],"cursor_terakhir":1730000000000005,"ada_lagi":false}
```

Batasan:

- Hanya vote untuk polling yang dikirim lewat `POST /polling`. Polling dari HP atau dari pelanggan tetap
  tampil di percakapan (`jenis: "polling"`), tapi vote-nya tidak bisa dibaca. Vote dari HP admin sendiri dilewati.
- Kunci 500 polling terakhir disimpan di `auth/polling.json` (tanpa nomor), jadi vote tetap bisa dibaca
  setelah restart. Polling yang lebih lama: vote-nya diabaikan.
- Feed vote di memori (5000 terakhir): vote yang belum ditarik saat gateway restart hilang.
- Kirim polling ke banyak nomor yang belum pernah chat = broadcast, risiko blokir sama.

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

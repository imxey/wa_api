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

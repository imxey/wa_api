// WhatsApp broadcast: the gateway takes a whole broadcast at once and sends it slowly per account.
// All queue state lives in MySQL so a restart picks up where it stopped. index.js hands over what it
// already has (accounts, toJid, queueSent, feed helpers) via mulai(); the old endpoints are untouched.
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const mysql = require("mysql2/promise");
const { generateMessageIDV2 } = require("baileys");

const MAKS_PENERIMA = 5000;
const MAKS_PESAN = 60000; // WhatsApp cuts text around 65k characters
const JAM = 3600_000;
const HARI = 24 * JAM;
const WIB = 7 * JAM; // Asia/Jakarta is UTC+7 with no DST, so a fixed offset is exact
// The boot-up connect and WhatsApp's own reconnects take seconds; only pause a broadcast for an account
// that stays down longer than this, or every deploy would pause everything
const TENGGANG_MS = 120_000;
const CEK_ULANG_MS = 15_000; // how often a not-ready account is checked during the grace period
const ISTIRAHAT_DETIK = [300, 600];
// Loops also wake on events (new broadcast, /lanjut, settings); this cap just bounds how stale they get
const TIDUR_MAKS = 60_000;
const SIMPAN_LAMPIRAN_HARI = 7;
const SIMPAN_HASIL_HARI = 30;
const DEFAULT = {
  batas_harian: 150,
  jeda_min: 15,
  jeda_maks: 45,
  istirahat_setiap: 25,
  jam_mulai: 8,
  jam_selesai: 17,
  gagal_beruntun: 3,
};
const ERR_TIDAK_PASTI = "Proses berhenti saat mengirim, mungkin sudah terkirim";

// Message text sits encrypted in MySQL (AES-256-GCM, key WA_DATA_KEY) so a DB dump/backup doesn't expose
// chats; rows that no longer need it (sent, skipped, cancelled) are emptied. "wa1:" marks encrypted text.
// Losing WA_DATA_KEY only loses messages still queued; keep a copy anyway.
const AWALAN = "wa1:";
function kunciData() {
  const k = Buffer.from(String(process.env.WA_DATA_KEY || "").replace(/^base64:/, ""), "base64");
  if (k.length !== 32) throw new Error("WA_DATA_KEY belum diisi / bukan 32 byte (format base64:...)");
  return k;
}
function tutup(teks) {
  if (!teks || teks.startsWith(AWALAN)) return teks;
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv("aes-256-gcm", kunciData(), iv);
  const isi = Buffer.concat([c.update(teks, "utf8"), c.final()]);
  return AWALAN + Buffer.concat([iv, c.getAuthTag(), isi]).toString("base64");
}
function buka(teks) {
  if (!teks || !teks.startsWith(AWALAN)) return teks; // rows queued before encryption
  const b = Buffer.from(teks.slice(AWALAN.length), "base64");
  const dc = crypto.createDecipheriv("aes-256-gcm", kunciData(), b.subarray(0, 12));
  dc.setAuthTag(b.subarray(12, 28));
  return Buffer.concat([dc.update(b.subarray(28)), dc.final()]).toString("utf8");
}
const KOSONGKAN_SELESAI = "UPDATE broadcast_penerima SET pesan = '' WHERE status IN ('terkirim','dilewati','dibatalkan') AND pesan <> ''";

let d = null; // deps from index.js
let pool = null;
let siap = false;
let pengaturan = { ...DEFAULT };
let versi = 0; // bumped on every settings change so a pending random gap gets re-rolled
let hasilTerakhir = 0; // newest committed /hasil-kirim cursor, so long-polls don't query on every wake
const loops = new Map(); // akun -> { bangun }
const belumSiap = new Map(); // akun -> ms since it was first seen not ready while it had a turn
const percobaan = new Map(); // akun -> ms of the last send attempt: failures are paced like sends
const jedaAcak = new Map(); // akun -> { kunci, detik }
// ponytail: in memory, a restart resets the streak; persist it if pauses after restarts matter
const gagalBeruntun = new Map(); // broadcast id -> consecutive failures

const SKEMA = [
  `CREATE TABLE IF NOT EXISTS broadcast (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    id_eksternal VARCHAR(64) NOT NULL,
    status ENUM('berjalan','jeda','selesai','dibatalkan') NOT NULL DEFAULT 'berjalan',
    catatan VARCHAR(255) NULL,
    lampiran_path VARCHAR(255) NULL,
    lampiran_nama VARCHAR(255) NULL,
    lampiran_mime VARCHAR(127) NULL,
    dibuat_at DATETIME(3) NOT NULL,
    diubah_at DATETIME(3) NOT NULL,
    UNIQUE KEY uniq_eksternal (id_eksternal),
    KEY status_diubah (status, diubah_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
  `CREATE TABLE IF NOT EXISTS broadcast_penerima (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    broadcast_id INT UNSIGNED NOT NULL,
    id_eksternal VARCHAR(64) NOT NULL,
    akun VARCHAR(64) NOT NULL,
    nomor VARCHAR(64) NOT NULL,
    pesan MEDIUMTEXT NOT NULL,
    status ENUM('menunggu','mengirim','terkirim','gagal','dilewati','dibatalkan') NOT NULL DEFAULT 'menunggu',
    error VARCHAR(255) NULL,
    id_pesan VARCHAR(64) NULL,
    dikirim_at DATETIME(3) NULL,
    diubah_at DATETIME(3) NOT NULL,
    UNIQUE KEY uniq_penerima (broadcast_id, id_eksternal),
    KEY antrean (akun, status, broadcast_id, id),
    KEY kirim_akun (akun, status, dikirim_at),
    KEY per_broadcast (broadcast_id, status),
    KEY kirim (status, dikirim_at),
    CONSTRAINT fk_penerima_broadcast FOREIGN KEY (broadcast_id) REFERENCES broadcast (id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
  `CREATE TABLE IF NOT EXISTS pengaturan (
    nama VARCHAR(32) PRIMARY KEY,
    nilai INT NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
  // Feed of GET /hasil-kirim. In the DB, not memory, so results not yet pulled survive a restart.
  // "cursor" is a reserved word in MySQL, hence kursor.
  `CREATE TABLE IF NOT EXISTS hasil_kirim (
    kursor BIGINT UNSIGNED PRIMARY KEY,
    broadcast_id_eksternal VARCHAR(64) NOT NULL,
    penerima_id_eksternal VARCHAR(64) NULL,
    akun VARCHAR(64) NULL,
    status VARCHAR(16) NOT NULL,
    error VARCHAR(255) NULL,
    id_pesan VARCHAR(64) NULL,
    waktu DATETIME(3) NOT NULL,
    KEY waktu (waktu)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
];

const sekarang = () => new Date(d.jam());
const log = (err) => console.error("[broadcast]", err?.message || err);

// Start of the Jakarta calendar day that contains ms, as a UTC timestamp
const awalHari = (ms) => Math.floor((ms + WIB) / HARI) * HARI - WIB;

// How long (ms) this account must wait before its next send, 0 = now. Pure, so the self-check drives
// the clock. terkirim = sent today (Jakarta day) by this account, terakhir = its last send (any day).
function rencana({ now, terkirim, terakhir, p, jedaDetik }) {
  const mulai = awalHari(now) + p.jam_mulai * JAM;
  const selesai = awalHari(now) + p.jam_selesai * JAM;
  if (terkirim >= p.batas_harian || now >= selesai) return mulai + HARI - now;
  if (now < mulai) return mulai - now;
  // A gap that ends after jam_selesai is fine: the next check lands outside the window and waits for tomorrow
  return Math.max(0, (terakhir || 0) + jedaDetik * 1000 - now);
}

// Rolled once per gap (keyed by the last send), so a loop woken early doesn't re-roll a longer wait
function jedaUntuk(akun, { terkirim, terakhir }) {
  const p = pengaturan;
  const kunci = `${terakhir}|${terkirim}|${versi}`;
  let j = jedaAcak.get(akun);
  if (j?.kunci !== kunci) {
    const istirahat = terkirim > 0 && terkirim % p.istirahat_setiap === 0;
    j = { kunci, detik: istirahat ? d.acak(...ISTIRAHAT_DETIK) : d.acak(p.jeda_min, p.jeda_maks) };
    jedaAcak.set(akun, j);
  }
  return j.detik;
}

function cekPengaturan(p) {
  for (const [k, v] of Object.entries(p)) if (!Number.isInteger(v)) return `${k} wajib bilangan bulat`;
  if (["batas_harian", "jeda_min", "jeda_maks", "istirahat_setiap", "gagal_beruntun"].some((k) => p[k] < 1)) {
    return "batas_harian, jeda_min, jeda_maks, istirahat_setiap, gagal_beruntun minimal 1";
  }
  if (p.jam_mulai < 0 || p.jam_selesai > 24 || p.jam_selesai <= p.jam_mulai) {
    return "jam_mulai/jam_selesai wajib 0-24 dan jam_selesai > jam_mulai";
  }
  if (p.jeda_maks < p.jeda_min) return "jeda_maks wajib >= jeda_min";
  return null;
}

// Every write goes through this one chain. No two transactions interleave, so a claim can't race a
// /batal, and feed rows commit in cursor order: a reader can never skip a cursor that commits late.
// ponytail: one writer for the whole gateway, plenty for ~30 accounts sending every few seconds each
let antre = Promise.resolve();
function tulis(fn) {
  const kerja = antre.then(async () => {
    const conn = await pool.getConnection();
    const feed = [];
    try {
      await conn.beginTransaction();
      const hasil = await fn(conn, feed);
      if (feed.length) await simpanFeed(conn, feed);
      await conn.commit();
      if (feed.length) {
        hasilTerakhir = feed.at(-1).kursor;
        d.wake();
      }
      return hasil;
    } catch (err) {
      await conn.rollback().catch(() => {});
      throw err;
    } finally {
      conn.release();
    }
  });
  antre = kerja.catch(() => {});
  return kerja;
}

async function simpanFeed(conn, feed) {
  const waktu = sekarang();
  for (const f of feed) f.kursor = d.nextCursor();
  for (let i = 0; i < feed.length; i += 1000) {
    await conn.query(
      "INSERT INTO hasil_kirim (kursor, broadcast_id_eksternal, penerima_id_eksternal, akun, status, error, id_pesan, waktu) VALUES ?",
      [feed.slice(i, i + 1000).map((f) => [f.kursor, f.bx, f.px, f.akun, f.status, f.error, f.id_pesan, waktu])],
    );
  }
}

async function penerimaDengan(conn, where, params) {
  const [rows] = await conn.query(
    `SELECT p.id, p.broadcast_id, p.id_eksternal, p.akun, p.id_pesan, b.id_eksternal AS bx
     FROM broadcast_penerima p JOIN broadcast b ON b.id = p.broadcast_id WHERE ${where} FOR UPDATE`,
    params,
  );
  return rows;
}

async function ubahPenerima(conn, feed, rows, status, error = null) {
  for (let i = 0; i < rows.length; i += 1000) {
    await conn.query("UPDATE broadcast_penerima SET status = ?, error = ?, diubah_at = ? WHERE id IN (?)", [
      status,
      error,
      sekarang(),
      rows.slice(i, i + 1000).map((r) => r.id),
    ]);
  }
  for (const r of rows) feed.push({ bx: r.bx, px: r.id_eksternal, akun: r.akun, status, error, id_pesan: r.id_pesan });
}

// Broadcast-level items in the feed have penerima_id_eksternal null and the note as error, so the app
// learns about a pause without polling GET /broadcast/<id>
async function ubahBroadcast(conn, feed, b, status, catatan = null) {
  await conn.query("UPDATE broadcast SET status = ?, catatan = ?, diubah_at = ? WHERE id = ?", [status, catatan, sekarang(), b.id]);
  feed.push({ bx: b.id_eksternal, px: null, akun: null, status, error: catatan, id_pesan: null });
  b.status = status;
}

async function selesaikanBilaHabis(conn, feed, broadcastId) {
  const [[b]] = await conn.query("SELECT id, id_eksternal, status FROM broadcast WHERE id = ? FOR UPDATE", [broadcastId]);
  if (!b || !["berjalan", "jeda"].includes(b.status)) return;
  // A paused broadcast keeps its failures for /lanjut; a running one is done once nothing is left to try
  const sisa = b.status === "jeda" ? ["menunggu", "mengirim", "gagal"] : ["menunggu", "mengirim"];
  const [[{ n }]] = await conn.query(
    "SELECT COUNT(*) AS n FROM broadcast_penerima WHERE broadcast_id = ? AND status IN (?)",
    [b.id, sisa],
  );
  if (!n) await ubahBroadcast(conn, feed, b, "selesai");
}

// id_eksternal first: the app's own ids are numbers too and would otherwise hit another broadcast's id
async function cariBroadcast(conn, ref) {
  const [[b]] = await conn.query("SELECT * FROM broadcast WHERE id_eksternal = ?", [ref]);
  if (b || !/^\d+$/.test(ref)) return b;
  const [[b2]] = await conn.query("SELECT * FROM broadcast WHERE id = ?", [ref]);
  return b2;
}

const BERIKUT = `SELECT p.id, p.broadcast_id, p.id_eksternal, p.akun, p.nomor, p.pesan, b.id_eksternal AS bx,
    b.lampiran_path, b.lampiran_nama, b.lampiran_mime
  FROM broadcast_penerima p JOIN broadcast b ON b.id = p.broadcast_id
  WHERE p.akun = ? AND p.status = 'menunggu' AND b.status = 'berjalan'
  ORDER BY p.broadcast_id, p.id LIMIT 1`;

async function statAkun(akun, now) {
  const [[hariIni]] = await pool.query(
    "SELECT COUNT(*) AS n FROM broadcast_penerima WHERE akun = ? AND status = 'terkirim' AND dikirim_at >= ?",
    [akun, new Date(awalHari(now))],
  );
  const [[akhir]] = await pool.query(
    "SELECT MAX(dikirim_at) AS t FROM broadcast_penerima WHERE akun = ? AND status = 'terkirim'",
    [akun],
  );
  return { terkirim: hariIni.n, terakhir: Math.max(akhir.t?.getTime() || 0, percobaan.get(akun) || 0) };
}

// Same rule for every attachment: a photo stays a photo, anything else goes as a document, and the
// message is the caption (POST /send's buildContent would send audio without its text)
async function isiPesan(p) {
  const pesan = buka(p.pesan);
  if (!p.lampiran_path) return { text: pesan };
  const buffer = await fs.promises.readFile(p.lampiran_path);
  if (p.lampiran_mime.startsWith("image/")) return { image: buffer, caption: pesan };
  return { document: buffer, mimetype: p.lampiran_mime, fileName: p.lampiran_nama, caption: pesan };
}

// One turn of an account's loop: send at most one message. Returns how long (ms) to sleep after.
async function langkah(akun) {
  const now = d.jam();
  const [[ada]] = await pool.query(BERIKUT, [akun]);
  if (!ada) return TIDUR_MAKS;
  const st = await statAkun(akun, now);
  const tunggu = rencana({ now, ...st, p: pengaturan, jedaDetik: jedaUntuk(akun, st) });
  if (tunggu > 0) return tunggu;

  const acc = d.accounts.get(akun);
  if (!acc?.ready || !acc.sock) {
    if (!belumSiap.has(akun)) belumSiap.set(akun, now);
    if (now - belumSiap.get(akun) < TENGGANG_MS) return CEK_ULANG_MS;
    // Nothing was claimed, so the recipient simply stays menunggu
    await tulis(async (conn, feed) => {
      const [[b]] = await conn.query("SELECT id, id_eksternal, status FROM broadcast WHERE id = ? FOR UPDATE", [ada.broadcast_id]);
      if (b?.status === "berjalan") await ubahBroadcast(conn, feed, b, "jeda", `Akun ${akun} belum tersambung`);
    });
    return 0;
  }
  belumSiap.delete(akun);

  // The id is stored BEFORE the send: after a crash, a row still "mengirim" with an id may have gone
  // out, one without never started (see pulihkan)
  const idPesan = generateMessageIDV2(acc.sock.user?.id);
  const p = await tulis(async (conn) => {
    const [[p]] = await conn.query(`${BERIKUT} FOR UPDATE`, [akun]);
    if (p) {
      await conn.query(
        "UPDATE broadcast_penerima SET status = 'mengirim', id_pesan = ?, diubah_at = ? WHERE id = ? AND status = 'menunggu'",
        [idPesan, sekarang(), p.id],
      );
    }
    return p;
  });
  if (!p) return 0;
  percobaan.set(akun, now);

  let error = null;
  try {
    const jid = d.toJid(p.nomor);
    const sent = await acc.sock.sendMessage(jid, await isiPesan(p), { messageId: idPesan });
    d.queueSent(akun, jid, sent); // same bookkeeping as POST /send (apiSentIds, chat queue)
  } catch (err) {
    error = String(err?.message || err).slice(0, 255);
  }

  // The message is out (or definitely not): keep retrying the bookkeeping rather than leave the row
  // "mengirim", which a restart would have to treat as uncertain
  for (;;) {
    try {
      await tulis((conn, feed) => catatKirim(conn, feed, p, idPesan, error));
      return 0;
    } catch (err) {
      log(err);
      await new Promise((r) => setTimeout(r, 10_000));
    }
  }
}

async function catatKirim(conn, feed, p, idPesan, error) {
  const item = { bx: p.bx, px: p.id_eksternal, akun: p.akun };
  if (error) {
    // A send that threw never reached WhatsApp as far as we can tell: drop the id so /lanjut may retry it
    await conn.query(
      "UPDATE broadcast_penerima SET status = 'gagal', error = ?, id_pesan = NULL, diubah_at = ? WHERE id = ?",
      [error, sekarang(), p.id],
    );
    feed.push({ ...item, status: "gagal", error, id_pesan: null });
    const n = (gagalBeruntun.get(p.broadcast_id) || 0) + 1;
    gagalBeruntun.set(p.broadcast_id, n);
    if (n >= pengaturan.gagal_beruntun) {
      gagalBeruntun.delete(p.broadcast_id);
      const [[b]] = await conn.query("SELECT id, id_eksternal, status FROM broadcast WHERE id = ? FOR UPDATE", [p.broadcast_id]);
      if (b?.status === "berjalan") {
        await ubahBroadcast(conn, feed, b, "jeda", `${n} kali gagal berturut-turut, terakhir: ${error}`.slice(0, 255));
      }
    }
  } else {
    const now = sekarang();
    await conn.query(
      "UPDATE broadcast_penerima SET status = 'terkirim', pesan = '', error = NULL, dikirim_at = ?, diubah_at = ? WHERE id = ?",
      [now, now, p.id],
    );
    feed.push({ ...item, status: "terkirim", error: null, id_pesan: idPesan });
    gagalBeruntun.delete(p.broadcast_id);
  }
  await selesaikanBilaHabis(conn, feed, p.broadcast_id);
}

// Accounts run in parallel, one loop each, and a loop sends one message at a time
async function jalankan(akun) {
  const loop = { bangun: () => {} };
  loops.set(akun, loop);
  while (d.accounts.has(akun)) {
    let ms;
    try {
      ms = await langkah(akun);
    } catch (err) {
      log(err);
      ms = 10_000;
    }
    if (ms > 0) {
      await new Promise((resolve) => {
        const t = setTimeout(resolve, Math.min(ms, TIDUR_MAKS));
        loop.bangun = () => {
          clearTimeout(t);
          resolve();
        };
      });
    }
    loop.bangun = () => {};
  }
  loops.delete(akun); // account deleted via DELETE /akun
}

const bangunkan = () => {
  for (const l of loops.values()) l.bangun();
};

function pastikanLoop() {
  for (const akun of d.accounts.keys()) if (!loops.has(akun)) jalankan(akun);
}

// Startup: a row left "mengirim" means the process died mid-send
async function pulihkan() {
  await tulis(async (conn, feed) => {
    // No id yet = the send never started: safe to queue again
    await conn.query(
      "UPDATE broadcast_penerima SET status = 'menunggu', diubah_at = ? WHERE status = 'mengirim' AND id_pesan IS NULL",
      [sekarang()],
    );
    // Id already handed to WhatsApp: it may well have gone out. Not resent (and /lanjut skips it); the app
    // can match the id against GET /status-pesan.
    const rows = await penerimaDengan(conn, "p.status = 'mengirim'", []);
    await ubahPenerima(conn, feed, rows, "gagal", ERR_TIDAK_PASTI);
  });
}

// Recipients of an account deleted from the gateway would wait forever. Not counted as a failure streak.
async function sapuAkunHilang() {
  const ada = [...d.accounts.keys()];
  await tulis(async (conn, feed) => {
    const rows = await penerimaDengan(conn, "p.status = 'menunggu' AND p.akun NOT IN (?)", [ada.length ? ada : [""]]);
    await ubahPenerima(conn, feed, rows, "gagal", "akun tidak ada");
    for (const id of new Set(rows.map((r) => r.broadcast_id))) await selesaikanBilaHabis(conn, feed, id);
  });
}

// Attachments of broadcasts finished/cancelled over 7 days ago, and feed rows nobody pulled in 30 days.
// lampiran_nama stays, so /lanjut can tell the file is gone.
async function bersihkan() {
  await tulis(async (conn) => {
    const [rows] = await conn.query(
      "SELECT id, lampiran_path FROM broadcast WHERE status IN ('selesai','dibatalkan') AND lampiran_path IS NOT NULL AND diubah_at < ? FOR UPDATE",
      [new Date(d.jam() - SIMPAN_LAMPIRAN_HARI * HARI)],
    );
    for (const r of rows) {
      fs.rmSync(path.dirname(r.lampiran_path), { recursive: true, force: true });
      await conn.query("UPDATE broadcast SET lampiran_path = NULL WHERE id = ?", [r.id]);
    }
    await conn.query("DELETE FROM hasil_kirim WHERE waktu < ?", [new Date(d.jam() - SIMPAN_HASIL_HARI * HARI)]);
    await conn.query(KOSONGKAN_SELESAI); // skipped/cancelled rows (sent ones are emptied right away)
  });
}

// Rows queued before encryption existed: those still to send (or retry) get encrypted, the rest emptied.
// Idempotent, runs at every start.
async function enkripsiLama() {
  await pool.query(KOSONGKAN_SELESAI);
  for (;;) {
    const [rows] = await pool.query("SELECT id, pesan FROM broadcast_penerima WHERE pesan <> '' AND pesan NOT LIKE 'wa1:%' LIMIT 500");
    if (!rows.length) return;
    for (const r of rows) await pool.query("UPDATE broadcast_penerima SET pesan = ? WHERE id = ?", [tutup(r.pesan), r.id]);
  }
}

const teks = (v, maks = 64) =>
  (typeof v === "string" || typeof v === "number") && String(v).length >= 1 && String(v).length <= maks ? String(v) : null;

async function buatBroadcast(data, file) {
  const bx = teks(data?.id_eksternal);
  if (!bx) return [400, { error: "Bad Request: id_eksternal wajib (1-64 karakter)" }];
  const daftar = data.penerima;
  if (!Array.isArray(daftar) || !daftar.length || daftar.length > MAKS_PENERIMA) {
    return [400, { error: `Bad Request: penerima wajib array 1-${MAKS_PENERIMA} item` }];
  }
  const rows = [];
  const ids = new Set();
  for (const [i, r] of daftar.entries()) {
    const px = teks(r?.id_eksternal);
    const akun = teks(r?.akun);
    const nomor = teks(r?.nomor);
    const pesan = typeof r?.pesan === "string" ? r.pesan : null;
    // Empty text only makes sense with an attachment (then it's just the file)
    if (!px || !akun || !nomor || pesan === null || pesan.length > MAKS_PESAN || (!pesan && !file)) {
      return [400, { error: `Bad Request: penerima[${i}] wajib id_eksternal, akun, nomor, pesan (maks ${MAKS_PESAN} karakter)` }];
    }
    if (ids.has(px)) return [400, { error: `Bad Request: id_eksternal penerima ganda: ${px}` }];
    ids.add(px);
    rows.push([px, akun, nomor, tutup(pesan)]);
  }
  const isiFile = file && Buffer.from(await file.arrayBuffer());

  const hasil = await tulis(async (conn) => {
    // Same id_eksternal again (app retried after a timeout): hand back the one already queued
    const [[ada]] = await conn.query(
      `SELECT b.id, b.id_eksternal, COUNT(p.id) AS total FROM broadcast b
       LEFT JOIN broadcast_penerima p ON p.broadcast_id = b.id WHERE b.id_eksternal = ? GROUP BY b.id`,
      [bx],
    );
    if (ada) return [200, ada];
    const now = sekarang();
    const [r] = await conn.query(
      "INSERT INTO broadcast (id_eksternal, status, lampiran_nama, lampiran_mime, dibuat_at, diubah_at) VALUES (?, 'berjalan', ?, ?, ?, ?)",
      [bx, file ? String(file.name || "lampiran").slice(-200) : null, file ? (file.type || "application/octet-stream").slice(0, 127) : null, now, now],
    );
    const id = r.insertId;
    // Chunked: one 5000-row insert with long texts could pass max_allowed_packet
    for (let i = 0; i < rows.length; i += 500) {
      await conn.query("INSERT INTO broadcast_penerima (broadcast_id, id_eksternal, akun, nomor, pesan, diubah_at) VALUES ?", [
        rows.slice(i, i + 500).map((x) => [id, ...x, now]),
      ]);
    }
    if (isiFile) {
      // Fixed file name on disk; the sender's name only goes into the DB, so it can't steer the path
      const dir = path.join(d.dirLampiran, String(id));
      fs.mkdirSync(dir, { recursive: true });
      const lokasi = path.join(dir, "file");
      fs.writeFileSync(lokasi, isiFile);
      await conn.query("UPDATE broadcast SET lampiran_path = ? WHERE id = ?", [lokasi, id]);
    }
    return [201, { id, id_eksternal: bx, total: rows.length }];
  });
  bangunkan();
  return hasil;
}

async function aksi(ref, nama, data) {
  const hasil = await tulis(async (conn, feed) => {
    const b = await cariBroadcast(conn, ref);
    if (!b) return [404, { error: "Broadcast tidak ada" }];
    if (b.status === "dibatalkan") return [409, { error: "Broadcast sudah dibatalkan" }];
    const extra = {};

    if (nama === "jeda") {
      if (b.status === "selesai") return [409, { error: "Broadcast sudah selesai" }];
      if (b.status === "berjalan") await ubahBroadcast(conn, feed, b, "jeda", "Dijeda lewat API");
    }
    if (nama === "lanjut") {
      if (b.lampiran_nama && !b.lampiran_path) return [409, { error: "Lampiran sudah dihapus, buat broadcast baru" }];
      // Only failures that never reached WhatsApp; one that kept its id may have gone out (see pulihkan)
      const rows = await penerimaDengan(conn, "p.broadcast_id = ? AND p.status = 'gagal' AND p.id_pesan IS NULL", [b.id]);
      gagalBeruntun.delete(b.id);
      if (b.status !== "selesai" || rows.length) {
        await ubahPenerima(conn, feed, rows, "menunggu");
        if (b.status !== "berjalan") await ubahBroadcast(conn, feed, b, "berjalan");
        await selesaikanBilaHabis(conn, feed, b.id);
      }
      extra.dikembalikan = rows.length;
    }
    if (nama === "batal") {
      if (b.status === "selesai") return [409, { error: "Broadcast sudah selesai" }];
      // A send already in flight still finishes and is recorded as terkirim/gagal
      const rows = await penerimaDengan(conn, "p.broadcast_id = ? AND p.status = 'menunggu'", [b.id]);
      await ubahPenerima(conn, feed, rows, "dibatalkan");
      await ubahBroadcast(conn, feed, b, "dibatalkan");
      extra.dibatalkan = rows.length;
    }
    if (nama === "lewati") {
      let where;
      if (Array.isArray(data?.penerima) && data.penerima.length) where = ["p.id_eksternal IN (?)", data.penerima.map(String)];
      else if (typeof data?.akun === "string" && data.akun) where = ["p.akun = ?", data.akun];
      else return [400, { error: "Bad Request: penerima (array id_eksternal) atau akun wajib" }];
      const rows = await penerimaDengan(conn, `p.broadcast_id = ? AND p.status IN ('menunggu','gagal') AND ${where[0]}`, [b.id, where[1]]);
      await ubahPenerima(conn, feed, rows, "dilewati");
      await selesaikanBilaHabis(conn, feed, b.id);
      extra.dilewati = rows.length;
    }

    const [[baru]] = await conn.query("SELECT status, catatan FROM broadcast WHERE id = ?", [b.id]);
    return [200, { id: b.id, id_eksternal: b.id_eksternal, ...baru, ...extra }];
  });
  bangunkan();
  return hasil;
}

async function detail(ref) {
  const b = await cariBroadcast(pool, ref);
  if (!b) return [404, { error: "Broadcast tidak ada" }];
  const [penerima] = await pool.query(
    "SELECT id_eksternal, akun, nomor, status, error, id_pesan, dikirim_at FROM broadcast_penerima WHERE broadcast_id = ? ORDER BY id",
    [b.id],
  );
  const { id, id_eksternal, status, catatan, lampiran_nama, dibuat_at, diubah_at } = b;
  return [200, { broadcast: { id, id_eksternal, status, catatan, lampiran_nama, dibuat_at, diubah_at }, penerima }];
}

async function bacaHasil(sejak) {
  const [rows] = await pool.query(
    `SELECT kursor AS \`cursor\`, broadcast_id_eksternal, penerima_id_eksternal, akun, status, error, id_pesan, waktu
     FROM hasil_kirim WHERE kursor > ? ORDER BY kursor LIMIT ?`,
    [sejak, d.PAGE_SIZE + 1],
  );
  const hasil = rows.slice(0, d.PAGE_SIZE);
  return { hasil, cursor_terakhir: hasil.at(-1)?.cursor ?? sejak, ada_lagi: rows.length > hasil.length };
}

// Same pattern as GET /status-pesan, including ?tunggu= long-polling
async function hasilKirim(req, res) {
  const q = new URL(req.url, "http://localhost").searchParams;
  const sejak = Number(q.get("sejak")) || 0;
  const deadline = Date.now() + Math.min(Number(q.get("tunggu")) || 0, d.MAX_WAIT_S) * 1000;
  while (!(hasilTerakhir > sejak) && Date.now() < deadline && !res.destroyed) {
    await d.waitForPush(res, deadline - Date.now());
  }
  return [200, await bacaHasil(sejak)];
}

async function simpanPengaturan(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return [400, { error: "Bad Request: body wajib objek JSON" }];
  const asing = Object.keys(data).filter((k) => !(k in DEFAULT));
  if (asing.length) return [400, { error: `Bad Request: tidak dikenal: ${asing.join(", ")}` }];
  // PHP forms often send numbers as strings
  const angka = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, typeof v === "string" && /^\d+$/.test(v) ? Number(v) : v]));
  const baru = { ...pengaturan, ...angka };
  const err = cekPengaturan(baru);
  if (err) return [400, { error: `Bad Request: ${err}` }];
  await tulis((conn) => conn.query("REPLACE INTO pengaturan (nama, nilai) VALUES ?", [Object.entries(baru)]));
  pengaturan = baru;
  versi++;
  bangunkan(); // takes effect now, no restart
  return [200, pengaturan];
}

async function statistik() {
  const per = {};
  const isi = (akun) => (per[akun] ??= { terkirim_hari_ini: 0, antrean: 0, terakhir_kirim: null });
  for (const akun of d.accounts.keys()) isi(akun);
  const [antrean] = await pool.query(
    "SELECT akun, COUNT(*) AS n FROM broadcast_penerima WHERE status IN ('menunggu','mengirim') GROUP BY akun",
  );
  const [hariIni] = await pool.query(
    "SELECT akun, COUNT(*) AS n FROM broadcast_penerima WHERE status = 'terkirim' AND dikirim_at >= ? GROUP BY akun",
    [new Date(awalHari(d.jam()))],
  );
  const [akhir] = await pool.query(
    "SELECT akun, MAX(dikirim_at) AS t FROM broadcast_penerima WHERE status = 'terkirim' GROUP BY akun",
  );
  let total = 0;
  for (const r of antrean) total += isi(r.akun).antrean = r.n;
  for (const r of hariIni) isi(r.akun).terkirim_hari_ini = r.n;
  for (const r of akhir) isi(r.akun).terakhir_kirim = r.t;
  return [200, { per_akun: per, antrean_total: total }];
}

async function bodyAtauNull(req) {
  try {
    return await d.readBody(req);
  } catch {
    return null;
  }
}

async function buatDariReq(req) {
  const body = await bodyAtauNull(req);
  let data = body;
  // multipart: the list in field "data" as JSON, the file in "lampiran"; plain JSON works without a file
  if (typeof body?.data === "string") {
    try {
      data = JSON.parse(body.data);
    } catch {
      data = null;
    }
  }
  if (!data) return [400, { error: "Bad Request: data wajib JSON" }];
  return buatBroadcast(data, body.lampiran instanceof File ? body.lampiran : null);
}

// Returns true when the request was one of ours (answered), false to let index.js carry on
async function tangani(req, res) {
  const url = req.url.split("?")[0];
  const m = url.match(/^\/broadcast(?:\/([^/]+)(?:\/(jeda|lanjut|batal|lewati))?)?$/);
  let handler = null;
  if (m && !m[1] && req.method === "POST") handler = () => buatDariReq(req);
  else if (m && m[1] && !m[2] && req.method === "GET") handler = () => detail(decodeURIComponent(m[1]));
  else if (m && m[2] && req.method === "POST") handler = async () => aksi(decodeURIComponent(m[1]), m[2], await bodyAtauNull(req));
  else if (url === "/hasil-kirim" && req.method === "GET") handler = () => hasilKirim(req, res);
  else if (url === "/pengaturan" && req.method === "GET") handler = async () => [200, pengaturan];
  else if (url === "/pengaturan" && req.method === "PUT") handler = async () => simpanPengaturan(await bodyAtauNull(req));
  else if (url === "/statistik" && req.method === "GET") handler = statistik;
  if (!handler) return false;

  if (!d.isValidSecret(req.headers["x-secret-key"])) {
    d.sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  } else if (!siap) {
    d.sendJson(res, 503, { error: process.env.DB_HOST ? "Database belum siap" : "Broadcast nonaktif: DB_HOST/DB_NAME belum diisi" });
  } else {
    const [kode, body] = await handler();
    d.sendJson(res, kode, body);
  }
  return true;
}

async function siapkan(database, hapusDulu = false) {
  pool ??= mysql.createPool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT) || 3306,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database,
    connectionLimit: 5,
    timezone: "Z", // DATETIME columns hold UTC
    supportBigNumbers: true,
  });
  if (hapusDulu) {
    for (const t of ["hasil_kirim", "broadcast_penerima", "broadcast", "pengaturan"]) await pool.query(`DROP TABLE IF EXISTS ${t}`);
  }
  for (const sql of SKEMA) await pool.query(sql);
  await pool.query("INSERT IGNORE INTO pengaturan (nama, nilai) VALUES ?", [Object.entries(DEFAULT)]);
  const [rows] = await pool.query("SELECT nama, nilai FROM pengaturan");
  pengaturan = { ...DEFAULT, ...Object.fromEntries(rows.filter((r) => r.nama in DEFAULT).map((r) => [r.nama, r.nilai])) };
  const [[{ k }]] = await pool.query("SELECT COALESCE(MAX(kursor), 0) AS k FROM hasil_kirim");
  hasilTerakhir = Number(k);
  // Cursors must keep increasing even if the server clock ever steps back
  d.seedCursor(hasilTerakhir);
}

function mulai(deps) {
  d = { jam: Date.now, acak: (a, b) => crypto.randomInt(a, b + 1), dirLampiran: "auth/lampiran", ...deps };
  if (!process.env.DB_HOST || !process.env.DB_NAME) {
    return console.log("[broadcast] nonaktif: DB_HOST/DB_NAME belum diisi, endpoint broadcast membalas 503");
  }
  try {
    kunciData();
  } catch (err) {
    return console.log(`[broadcast] nonaktif: ${err.message}, endpoint broadcast membalas 503`);
  }
  // The gateway must keep serving the old endpoints when MySQL is down, so this only retries
  (async function coba() {
    try {
      await siapkan(process.env.DB_NAME);
      await enkripsiLama();
      await pulihkan();
    } catch (err) {
      console.error("[broadcast] database belum siap, coba lagi 10 detik:", err.message);
      return setTimeout(coba, 10_000);
    }
    siap = true;
    console.log("[broadcast] siap");
    const rutin = () => {
      pastikanLoop(); // also picks up accounts made later via POST /akun
      sapuAkunHilang().catch(log);
    };
    rutin();
    setInterval(rutin, 60_000);
    bersihkan().catch(log);
    setInterval(() => bersihkan().catch(log), JAM);
  })();
}

// node index.js --self-check. The schedule rules always run; the DB scenarios need SELFCHECK_DB_NAME,
// an empty database just for this (its tables are dropped and recreated), with the same DB_* login.
async function selfCheck({ toJid }) {
  const assert = require("assert");
  const os = require("os");
  const p0 = DEFAULT;
  // Jakarta wall clock on 5 Jan 2026 (day = 1 is the 6th)
  const T = (jam, menit = 0, detik = 0, day = 0) => Date.UTC(2026, 0, 5 + day, jam - 7, menit, detik);
  const r = (now, terkirim = 0, terakhir = 0, jedaDetik = 15) => rencana({ now, terkirim, terakhir, p: p0, jedaDetik });
  assert.strictEqual(r(T(7)), JAM); // before jam_mulai: wait for 08:00
  assert.strictEqual(r(T(17)), 15 * JAM); // at jam_selesai: tomorrow 08:00
  assert.strictEqual(r(T(9), 150), 23 * JAM); // daily limit: tomorrow 08:00
  assert.strictEqual(r(T(9), 3, T(9) - 10_000), 5000); // 15 s gap, 10 s gone
  assert.strictEqual(r(T(9), 3, T(9) - 20_000), 0);
  assert.strictEqual(r(T(8), 0, T(16, 59, 59, -1)), 0); // yesterday's last send doesn't hold today
  assert.strictEqual(cekPengaturan({ ...p0, jeda_min: 50, jeda_maks: 10 }), "jeda_maks wajib >= jeda_min");
  assert.ok(cekPengaturan({ ...p0, jam_mulai: 17, jam_selesai: 8 }));
  assert.ok(cekPengaturan({ ...p0, batas_harian: 0 }));
  assert.ok(cekPengaturan({ ...p0, jam_selesai: 25 }));
  assert.strictEqual(cekPengaturan({ ...p0, jam_mulai: 0, jam_selesai: 24 }), null);

  // Stored message encryption (a throwaway key when none is set)
  process.env.WA_DATA_KEY ||= "base64:" + crypto.randomBytes(32).toString("base64");
  const rahasia = "Halo Bu 📌 NPSN 12345";
  assert.ok(tutup(rahasia).startsWith(AWALAN) && !tutup(rahasia).includes("NPSN"));
  assert.notStrictEqual(tutup(rahasia), tutup(rahasia)); // fresh IV each time
  assert.strictEqual(buka(tutup(rahasia)), rahasia);
  assert.strictEqual(tutup(tutup(rahasia)).length, tutup(rahasia).length); // never encrypted twice
  assert.strictEqual(buka("teks lama"), "teks lama");
  assert.strictEqual(tutup(""), "");

  const db = process.env.SELFCHECK_DB_NAME;
  if (!db) return console.log("broadcast self-check (jadwal) OK; skenario DB dilewati: isi SELFCHECK_DB_NAME");
  if (db === process.env.DB_NAME) throw new Error("SELFCHECK_DB_NAME tidak boleh sama dengan DB_NAME");

  let jam = T(9);
  const kirim = []; // what the fake sockets were asked to send
  const akun = (ready, rusak = false) => ({
    ready,
    sock: {
      user: { id: "628111:1@s.whatsapp.net" },
      async sendMessage(jid, content, opts) {
        if (rusak) throw new Error("tes gagal");
        kirim.push({ jid, content, id: opts.messageId });
        return { key: { id: opts.messageId } };
      },
    },
  });
  const accounts = new Map([
    ["a1", akun(true)],
    ["a2", akun(true)],
    ["a3", akun(true)],
    ["rusak", akun(true, true)],
    ["mati", akun(false)],
  ]);
  let kursor = 0;
  const dicatat = [];
  d = {
    accounts,
    toJid,
    queueSent: (a, jid, sent) => dicatat.push(sent.key.id),
    nextCursor: () => ++kursor,
    seedCursor: () => {},
    wake: () => {},
    PAGE_SIZE: 100,
    jam: () => jam,
    acak: (a) => a, // always the shortest gap: exact numbers to assert
    dirLampiran: fs.mkdtempSync(path.join(os.tmpdir(), "wa-broadcast-")),
  };
  try {
    await siapkan(db, true);
    assert.strictEqual((await simpanPengaturan({ jeda_maks: 5 }))[0], 400);
    assert.strictEqual((await simpanPengaturan({ halo: 1 }))[0], 400);
    const [kode, isi] = await simpanPengaturan({ batas_harian: 3, jeda_min: 15, jeda_maks: "15", istirahat_setiap: 2, gagal_beruntun: 2 });
    assert.strictEqual(kode, 200);
    assert.strictEqual(isi.jeda_maks, 15);

    const status = async (bx) => (await detail(bx))[1];
    const pn = (id, a, extra = {}) => ({ id_eksternal: id, akun: a, nomor: `08${id}`, pesan: `halo ${id}`, ...extra });

    // Idempotent: the same id_eksternal twice is one broadcast
    const b1 = { id_eksternal: "B1", penerima: [pn("1", "a1"), pn("2", "a1"), pn("3", "a1"), pn("4", "a1"), pn(5, "a2")] };
    const [k1, r1] = await buatBroadcast(b1, null);
    assert.deepStrictEqual([k1, r1.total], [201, 5]);
    const [k2, r2] = await buatBroadcast(b1, null);
    assert.deepStrictEqual([k2, r2.id, r2.total], [200, r1.id, 5]);
    assert.strictEqual((await status("B1")).penerima.length, 5);
    const [[mentah]] = await pool.query("SELECT pesan FROM broadcast_penerima WHERE id_eksternal = '1' AND broadcast_id = ?", [r1.id]);
    assert.ok(mentah.pesan.startsWith(AWALAN) && !mentah.pesan.includes("halo")); // stored encrypted
    assert.strictEqual((await buatBroadcast({ id_eksternal: "X", penerima: [pn("1", "a1"), pn("1", "a1")] }))[0], 400);
    assert.strictEqual((await buatBroadcast({ id_eksternal: "X", penerima: Array.from({ length: 5001 }, (_, i) => pn(String(i), "a1")) }))[0], 400);

    // Gap per account: a1 waits after its send, a2 is not held up by it
    assert.strictEqual(await langkah("a1"), 0);
    assert.deepStrictEqual(kirim.at(-1).content, { text: "halo 1" });
    assert.strictEqual(kirim.at(-1).jid, "6281@s.whatsapp.net"); // same number format as /send
    const [[terkirim]] = await pool.query("SELECT pesan FROM broadcast_penerima WHERE id_eksternal = '1' AND broadcast_id = ?", [r1.id]);
    assert.strictEqual(terkirim.pesan, ""); // emptied once sent
    assert.ok(dicatat.includes(kirim.at(-1).id)); // went through queueSent like /send
    assert.strictEqual(await langkah("a1"), 15_000);
    assert.strictEqual(await langkah("a2"), 0);
    assert.strictEqual(kirim.length, 2);
    jam += 15_000;
    assert.strictEqual(await langkah("a1"), 0);
    // Rest after every 2nd send today (shortest: 300 s), then the daily limit of 3
    assert.strictEqual(await langkah("a1"), 300_000);
    jam += 300_000;
    assert.strictEqual(await langkah("a1"), 0);
    assert.strictEqual(kirim.length, 4);
    assert.strictEqual(await langkah("a1"), T(8, 0, 0, 1) - jam);
    // Next day: wait for jam_mulai, then go on
    jam = T(7, 30, 0, 1);
    assert.strictEqual(await langkah("a1"), 30 * 60_000);
    jam = T(8, 0, 0, 1);
    assert.strictEqual(await langkah("a1"), 0);
    assert.strictEqual((await status("B1")).broadcast.status, "selesai");
    assert.strictEqual(await langkah("a1"), TIDUR_MAKS);
    const [, st] = await statistik();
    assert.strictEqual(st.per_akun.a1.terkirim_hari_ini, 1);
    assert.strictEqual(st.antrean_total, 0);

    // Failure streak pauses the broadcast; /lanjut queues the failures again; /lewati {akun} ends it
    await buatBroadcast({ id_eksternal: "B2", penerima: [pn("1", "rusak"), pn("2", "rusak"), pn("3", "rusak")] }, null);
    assert.strictEqual(await langkah("rusak"), 0);
    assert.strictEqual(await langkah("rusak"), 15_000); // failures are paced too
    jam += 15_000;
    assert.strictEqual(await langkah("rusak"), 0);
    let s2 = await status("B2");
    assert.strictEqual(s2.broadcast.status, "jeda");
    assert.match(s2.broadcast.catatan, /2 kali gagal/);
    assert.deepStrictEqual(s2.penerima.map((x) => x.status), ["gagal", "gagal", "menunggu"]);
    assert.strictEqual(await langkah("rusak"), TIDUR_MAKS); // paused: nothing to take
    const [, l2] = await aksi("B2", "lanjut", {});
    assert.deepStrictEqual([l2.status, l2.dikembalikan], ["berjalan", 2]);
    const [, w2] = await aksi(String((await status("B2")).broadcast.id), "lewati", { akun: "rusak" }); // by gateway id
    assert.deepStrictEqual([w2.dilewati, w2.status], [3, "selesai"]);

    // Account not connected: grace period first, then the broadcast is paused, recipient stays menunggu
    await buatBroadcast({ id_eksternal: "B3", penerima: [pn("1", "mati"), pn("2", "mati")] }, null);
    assert.strictEqual(await langkah("mati"), CEK_ULANG_MS);
    assert.strictEqual((await status("B3")).broadcast.status, "berjalan");
    jam += TENGGANG_MS + 1000;
    await langkah("mati");
    const s3 = await status("B3");
    assert.deepStrictEqual([s3.broadcast.status, s3.broadcast.catatan], ["jeda", "Akun mati belum tersambung"]);
    assert.deepStrictEqual(s3.penerima.map((x) => x.status), ["menunggu", "menunggu"]);
    // /lewati a list: a paused broadcast with nothing left becomes selesai
    assert.strictEqual((await aksi("B3", "lewati", { penerima: ["1"] }))[1].status, "jeda");
    assert.strictEqual((await aksi("B3", "lewati", { penerima: ["2"] }))[1].status, "selesai");
    assert.strictEqual((await aksi("B3", "lewati", {}))[0], 400);

    // /batal: the rest is cancelled and never sent
    await buatBroadcast({ id_eksternal: "B4", penerima: [pn("1", "a3"), pn("2", "a3")] }, null);
    const [, c4] = await aksi("B4", "batal", {});
    assert.deepStrictEqual([c4.status, c4.dibatalkan], ["dibatalkan", 2]);
    const n4 = kirim.length;
    assert.strictEqual(await langkah("a3"), TIDUR_MAKS);
    assert.strictEqual(kirim.length, n4);
    assert.strictEqual((await aksi("B4", "lanjut", {}))[0], 409);

    // Restart mid-send: no id = queued again and sent once; with an id = not resent, not even by /lanjut
    await buatBroadcast({ id_eksternal: "B5", penerima: [pn("71", "a3"), pn("72", "a3")] }, null);
    await pool.query("UPDATE broadcast_penerima p JOIN broadcast b ON b.id = p.broadcast_id SET p.status = 'mengirim' WHERE b.id_eksternal = 'B5'");
    await pool.query("UPDATE broadcast_penerima SET id_pesan = 'IDLAMA' WHERE id_eksternal = '72' AND akun = 'a3'");
    await pulihkan();
    let s5 = await status("B5");
    assert.deepStrictEqual(s5.penerima.map((x) => [x.status, x.id_pesan]), [["menunggu", null], ["gagal", "IDLAMA"]]);
    assert.strictEqual(await langkah("a3"), 0);
    assert.strictEqual(await langkah("a3"), TIDUR_MAKS);
    assert.strictEqual(kirim.filter((k) => k.jid === "62871@s.whatsapp.net").length, 1);
    assert.strictEqual(kirim.filter((k) => k.jid === "62872@s.whatsapp.net").length, 0);
    assert.strictEqual((await status("B5")).broadcast.status, "selesai");
    assert.strictEqual((await aksi("B5", "lanjut", {}))[1].dikembalikan, 0);
    assert.strictEqual(await langkah("a3"), TIDUR_MAKS);

    // Account deleted from the gateway
    await buatBroadcast({ id_eksternal: "B6", penerima: [pn("1", "hilang")] }, null);
    await sapuAkunHilang();
    const s6 = await status("B6");
    assert.deepStrictEqual([s6.penerima[0].status, s6.penerima[0].error, s6.broadcast.status], ["gagal", "akun tidak ada", "selesai"]);

    // Attachment: image as photo with the text as caption, anything else as a document
    const png = new File([Buffer.from("png")], "poster.png", { type: "image/png" });
    await buatBroadcast({ id_eksternal: "B7", penerima: [pn("1", "a2")] }, png);
    await langkah("a2");
    assert.deepStrictEqual(kirim.at(-1).content, { image: Buffer.from("png"), caption: "halo 1" });
    const pdf = new File([Buffer.from("pdf")], "surat.pdf", { type: "application/pdf" });
    await buatBroadcast({ id_eksternal: "B8", penerima: [pn("1", "a2", { pesan: "" })] }, pdf);
    jam += 15_000;
    await langkah("a2");
    assert.deepStrictEqual(kirim.at(-1).content, { document: Buffer.from("pdf"), mimetype: "application/pdf", fileName: "surat.pdf", caption: "" });
    // Cleanup a week later deletes the file, after which /lanjut refuses
    jam += 8 * HARI;
    await bersihkan();
    assert.strictEqual((await aksi("B8", "lanjut", {}))[0], 409);

    // Feed: cursor order, recipient and broadcast-level items, paging
    const f = await bacaHasil(0);
    assert.ok(f.hasil.every((x, i) => i === 0 || x.cursor > f.hasil[i - 1].cursor));
    assert.ok(f.hasil.some((x) => x.broadcast_id_eksternal === "B1" && x.penerima_id_eksternal === "1" && x.status === "terkirim" && x.id_pesan));
    assert.ok(f.hasil.some((x) => x.broadcast_id_eksternal === "B2" && x.penerima_id_eksternal === null && x.status === "jeda"));
    assert.ok(f.hasil.some((x) => x.broadcast_id_eksternal === "B5" && x.penerima_id_eksternal === "72" && x.error === ERR_TIDAK_PASTI));
    const f2 = await bacaHasil(f.hasil[2].cursor);
    assert.strictEqual(f2.hasil[0].cursor, f.hasil[3].cursor);
    assert.strictEqual(f2.ada_lagi, false);
    console.log("broadcast self-check OK");
  } finally {
    await pool?.end();
    pool = null;
    fs.rmSync(d.dirLampiran, { recursive: true, force: true });
  }
}

module.exports = { mulai, tangani, selfCheck };

require("dotenv").config();
const fs = require("fs");
const http = require("http");
const crypto = require("crypto");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  normalizeMessageContent,
  jidNormalizedUser,
  isPnUser,
  isLidUser,
} = require("baileys");
const QRCode = require("qrcode");

const PORT = process.env.PORT || 8086;

// First account is the default for requests without `akun`
const ACCOUNTS = (process.env.WA_ACCOUNTS || "utama")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
// Names become folder names under auth/, so no path tricks allowed
for (const id of ACCOUNTS) {
  if (!/^[a-z0-9_-]+$/i.test(id)) throw new Error(`Nama akun tidak valid: ${id}`);
}
const accounts = new Map(
  ACCOUNTS.map((id) => [id, { sock: null, qr: "", ready: false }]),
);

// Incoming chats wait here until the calling app pulls them via GET /pesan-masuk.
// Memory only, so nothing personal touches the disk; unpulled chats are lost on restart.
// ponytail: capped in-memory queue, persist it if restarts start losing chats
const QUEUE_SIZE = 1000;
const PAGE_SIZE = 100;
const queue = []; // oldest first
// Ids of messages sent through POST /send, so the echo of our own send is not queued as a chat
const apiSentIds = new Set();
let lastCursor = 0;
// Time-based so cursors keep increasing across restarts and the caller's saved cursor stays valid
const nextCursor = () => (lastCursor = Math.max(lastCursor + 1, Date.now() * 1000));

// DEBUG_INBOX=1: the newest few queued chats are also shown on the QR page
const INBOX_PREVIEW = 20;
const isDebugInbox = () => process.env.DEBUG_INBOX === "1";

// Chat text and names come from strangers, never put them in HTML raw
const escapeHtml = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

// Hash both sides so timingSafeEqual gets equal-length buffers
function matches(value, expected) {
  if (!expected || typeof value !== "string") return false;
  const hash = (s) => crypto.createHash("sha256").update(s).digest();
  return crypto.timingSafeEqual(hash(value), hash(expected));
}

const isValidSecret = (secret) => matches(secret, process.env.SECRET_KEY);

// Short passwords are too easy to guess even with the lockout below
const MIN_PASSWORD_LENGTH = 12;
if ((process.env.ADMIN_PASSWORD || "").length < MIN_PASSWORD_LENGTH) {
  console.log(`[auth] ADMIN_PASSWORD kosong/kurang dari ${MIN_PASSWORD_LENGTH} karakter, halaman QR dikunci`);
}
if ((process.env.SECRET_KEY || "").length < MIN_PASSWORD_LENGTH) {
  console.log(`[auth] PERINGATAN: SECRET_KEY kurang dari ${MIN_PASSWORD_LENGTH} karakter, ganti dengan yang acak dan panjang`);
}

// QR page uses Basic Auth with its own credentials, separate from the API key
function isAuthorizedPage(req) {
  if ((process.env.ADMIN_PASSWORD || "").length < MIN_PASSWORD_LENGTH) return false;
  const [scheme, encoded] = (req.headers.authorization || "").split(" ");
  if (scheme !== "Basic" || !encoded) return false;
  const decoded = Buffer.from(encoded, "base64").toString();
  const i = decoded.indexOf(":");
  return (
    matches(decoded.slice(0, i), process.env.ADMIN_USER) &&
    matches(decoded.slice(i + 1), process.env.ADMIN_PASSWORD)
  );
}

// Brute-force guard: every 401 counts against the caller's IP; too many and the IP is locked out.
// ponytail: in-memory, resets on restart; move to Redis if this ever runs as several instances
const MAX_FAILS = 10;
const WINDOW_MS = 15 * 60 * 1000;
const failures = new Map(); // ip -> { count, since, until }

function clientIp(req) {
  // Behind a reverse proxy every request comes from the proxy's IP, so trust the
  // entry it appends (the last one); earlier entries are client-controlled
  if (process.env.TRUST_PROXY === "1" && req.headers["x-forwarded-for"]) {
    return req.headers["x-forwarded-for"].split(",").pop().trim();
  }
  return req.socket.remoteAddress;
}

function lockedFor(ip) {
  const until = failures.get(ip)?.until || 0;
  return Math.max(0, Math.ceil((until - Date.now()) / 1000));
}

function recordFail(ip) {
  const now = Date.now();
  let f = failures.get(ip);
  if (!f || now - f.since > WINDOW_MS) f = { count: 0, since: now, until: 0 };
  if (++f.count >= MAX_FAILS) {
    f.until = now + WINDOW_MS;
    console.log(`[auth] ${ip} dikunci ${WINDOW_MS / 60000} menit setelah ${MAX_FAILS} gagal login`);
  }
  failures.set(ip, f);
}

// Drop stale entries so random IPs can't grow the map forever
setInterval(() => {
  const now = Date.now();
  for (const [ip, f] of failures) {
    if (f.until < now && now - f.since > WINDOW_MS) failures.delete(ip);
  }
}, WINDOW_MS).unref();

const phoneOf = (sock) => String(sock?.user?.id || "").split(/[:@]/)[0] || null;

function toJid(nomor) {
  if (String(nomor).includes("@")) return String(nomor);
  let jid = String(nomor).replace(/\D/g, "");
  if (jid.startsWith("0")) jid = "62" + jid.substring(1);
  return jid + "@s.whatsapp.net";
}

// Pick the WA message type from the file's mimetype; anything else goes as a document
async function buildContent(file, caption) {
  if (!file) return { text: caption };

  const buffer = Buffer.from(await file.arrayBuffer());
  const mimetype = file.type || "application/octet-stream";

  if (mimetype.startsWith("image/")) return { image: buffer, caption };
  if (mimetype.startsWith("video/")) return { video: buffer, caption };
  if (mimetype.startsWith("audio/")) return { audio: buffer, mimetype };
  return { document: buffer, mimetype, fileName: file.name || "file", caption };
}

// Accepts JSON or multipart/form-data (curl -F); Node's built-in Request parses both
async function readBody(req) {
  const request = new Request("http://localhost", {
    method: "POST",
    headers: req.headers,
    body: req,
    duplex: "half",
  });
  const type = req.headers["content-type"] || "";
  if (type.startsWith("multipart/form-data")) {
    // ponytail: whole upload buffered in memory, stream to disk if files get huge
    return Object.fromEntries(await request.formData());
  }
  return request.json();
}

function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

async function handleSend(req, res) {
  const data = await readBody(req);
  const nomor = data.nomor_tujuan || data.nomorTujuan || data.nomor;
  const msg = data.message || data.pesan || undefined;
  const secret =
    req.headers["x-secret-key"] ||
    data.kode_rahasia ||
    data.secret_key ||
    data.secretKey ||
    data.kodeRahasia;
  const file = data.file instanceof File ? data.file : null;
  const acc = accounts.get(data.akun || data.account || ACCOUNTS[0]);

  if (!isValidSecret(secret)) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  if (!acc) {
    return sendJson(res, 400, { error: "Bad Request: akun tidak dikenal" });
  }
  if (!nomor || (!msg && !file)) {
    return sendJson(res, 400, {
      error: "Bad Request: nomor and message or file are required",
    });
  }
  if (!acc.sock || !acc.ready) {
    return sendJson(res, 503, { error: "WhatsApp socket not ready" });
  }

  const sent = await acc.sock.sendMessage(toJid(nomor), await buildContent(file, msg));
  // Sent via API: the caller already logs it, so keep it out of the chat queue (no duplicates)
  if (sent?.key?.id) {
    apiSentIds.add(sent.key.id);
    if (apiSentIds.size > QUEUE_SIZE) apiSentIds.delete(apiSentIds.values().next().value);
  }
  sendJson(res, 200, { success: true, message: "Pesan berhasil dikirim!" });
}

async function renderQrPage(res) {
  const cards = await Promise.all(
    ACCOUNTS.map(async (id) => {
      const { qr, ready, sock } = accounts.get(id);
      const body = ready
        ? `<p>Tersambung: ${phoneOf(sock)}</p>
           <form method="post" action="/logout/${id}" onsubmit="return confirm('Logout ${id} dari WhatsApp?')">
             <button type="submit">Logout</button>
           </form>`
        : qr
          ? `<img src="${await QRCode.toDataURL(qr, { margin: 1, scale: 8 })}" alt="QR ${id}" />`
          : `<p>Menunggu QR dari WhatsApp...</p>`;
      return `<div class="card"><h2>${id}</h2>${body}</div>`;
    }),
  );

  const rows = queue
    .slice(-INBOX_PREVIEW)
    .reverse()
    .map(
      (p) => `<tr>
        <td>${escapeHtml(new Date(p.waktu).toLocaleString("id-ID", { timeZone: "Asia/Jakarta" }))}</td>
        <td>${escapeHtml(p.akun)}</td>
        <td>${escapeHtml(p.nama_pengirim)}<br />${escapeHtml(p.nomor_pengirim || p.jid_pengirim)}</td>
        <td>${escapeHtml(p.pesan)}</td>
      </tr>`,
    )
    .join("");
  const inboxHtml = isDebugInbox()
    ? `<h2>Pesan masuk terakhir</h2>
       <p class="note">Mode debug: ${INBOX_PREVIEW} pesan terakhir dari antrian, disimpan di memori, hilang saat restart.</p>
       <div class="inbox"><table>
         <tr><th>Waktu (WIB)</th><th>Akun</th><th>Pengirim</th><th>Pesan</th></tr>
         ${rows || `<tr><td colspan="4">Belum ada pesan masuk</td></tr>`}
       </table></div>`
    : "";

  res.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(`<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta http-equiv="refresh" content="5" />
    <title>WhatsApp QR Login</title>
    <style>
      body { margin: 0; min-height: 100vh; font-family: system-ui, sans-serif;
        background: #0f172a; color: #e2e8f0; text-align: center; }
      .grid { display: flex; flex-wrap: wrap; gap: 16px; justify-content: center; padding: 16px; }
      .card { background: #111827; padding: 24px; border-radius: 16px; }
      img { width: 320px; max-width: 100%; image-rendering: pixelated; background: #fff;
        padding: 12px; border-radius: 12px; box-sizing: border-box; }
      button { background: #dc2626; color: #fff; border: 0; padding: 8px 20px;
        border-radius: 8px; font: inherit; cursor: pointer; }
      .note { color: #94a3b8; font-size: 14px; }
      .inbox { max-width: 960px; margin: 0 auto 16px; padding: 0 16px; overflow-x: auto; }
      table { width: 100%; border-collapse: collapse; text-align: left; background: #111827; }
      th, td { padding: 8px 12px; border-bottom: 1px solid #1f2937; vertical-align: top;
        overflow-wrap: anywhere; }
    </style>
  </head>
  <body>
    <h1>WhatsApp QR</h1>
    <div class="grid">${cards.join("")}</div>
    ${inboxHtml}
    <p>Refresh otomatis tiap 5 detik.</p>
  </body>
</html>`);
}

// Status semua akun untuk aplikasi pemanggil (header X-Secret-Key). QR hanya untuk akun yang belum tersambung.
async function handleStatus(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const akun = await Promise.all(
    ACCOUNTS.map(async (id) => {
      const { qr, ready, sock } = accounts.get(id);
      return {
        akun: id,
        ready,
        nomor: ready ? phoneOf(sock) : null,
        qr: !ready && qr ? await QRCode.toDataURL(qr, { margin: 1, scale: 6 }) : null,
      };
    }),
  );
  sendJson(res, 200, { default: ACCOUNTS[0], akun });
}

// GET /pesan-masuk?sejak=<cursor>: queued chats after that cursor, oldest first, PAGE_SIZE per call.
// The caller saves cursor_terakhir and sends it back next time; repeat while ada_lagi is true.
async function handlePesanMasuk(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const sejak = Number(new URL(req.url, "http://localhost").searchParams.get("sejak")) || 0;
  const baru = queue.filter((p) => p.cursor > sejak);
  const pesan = baru.slice(0, PAGE_SIZE);
  sendJson(res, 200, {
    pesan,
    cursor_terakhir: pesan.at(-1)?.cursor ?? sejak,
    ada_lagi: baru.length > pesan.length,
  });
}

// POST /logout/<akun>: X-Secret-Key from the calling app, or the Logout button on the QR page
async function handleLogout(req, res) {
  const viaApi = isValidSecret(req.headers["x-secret-key"]);
  // Browsers resend Basic Auth on their own, so the button also requires same-origin (blocks CSRF)
  const viaPage =
    isAuthorizedPage(req) && req.headers["sec-fetch-site"] === "same-origin";
  if (!viaApi && !viaPage) {
    return sendJson(res, 401, { error: "Unauthorized" });
  }
  const acc = accounts.get(decodeURIComponent(req.url.slice("/logout/".length)));
  if (!acc) {
    return sendJson(res, 400, { error: "Bad Request: akun tidak dikenal" });
  }
  if (!acc.ready) {
    return sendJson(res, 409, { error: "Akun belum tersambung" });
  }

  // Ends with a loggedOut close, which wipes auth/<akun> and brings up a fresh QR
  await acc.sock.logout();
  if (viaApi) return sendJson(res, 200, { success: true });
  res.writeHead(303, { Location: "/" });
  res.end();
}

function startWebServer() {
  http
    .createServer(async (req, res) => {
      const ip = clientIp(req);
      const wait = lockedFor(ip);
      if (wait) {
        res.writeHead(429, { "Retry-After": String(wait) });
        return res.end("Too Many Requests: terlalu banyak percobaan login, coba lagi nanti");
      }
      // One hook covers every auth path (/send, /status, /pesan-masuk, /logout, QR page)
      res.on("finish", () => res.statusCode === 401 && recordFail(ip));

      try {
        if (req.method === "POST" && req.url === "/send") {
          return await handleSend(req, res);
        }
        if (req.method === "POST" && req.url.startsWith("/logout/")) {
          return await handleLogout(req, res);
        }
        if (req.method === "GET" && req.url === "/status") {
          return await handleStatus(req, res);
        }
        if (req.method === "GET" && req.url.split("?")[0] === "/pesan-masuk") {
          return await handlePesanMasuk(req, res);
        }
        if (!isAuthorizedPage(req)) {
          res.writeHead(401, { "WWW-Authenticate": 'Basic realm="WA API"' });
          return res.end("Unauthorized");
        }
        await renderQrPage(res);
      } catch (err) {
        sendJson(res, 500, { error: err.message });
      }
    })
    .listen(PORT, () => console.log(`WA API listening on :${PORT}`));
}

// Plain chat text only; media, reactions, stickers etc. return null and are skipped
function textOf(msg) {
  const m = normalizeMessageContent(msg.message);
  return m?.conversation || m?.extendedTextMessage?.text || null;
}

// Private chats may arrive as a hidden @lid id; resolve it back to a phone number when WA allows
async function senderNumber(sock, key) {
  const jid = isPnUser(key.remoteJid)
    ? key.remoteJid
    : isPnUser(key.remoteJidAlt)
      ? key.remoteJidAlt
      : await sock.signalRepository.lidMapping.getPNForLID(key.remoteJid);
  return jid ? jidNormalizedUser(jid).split("@")[0] : null;
}

async function connectToWhatsApp(id) {
  const acc = accounts.get(id);
  const { state, saveCreds } = await useMultiFileAuthState(`auth/${id}`);
  const { version, isLatest } = await fetchLatestBaileysVersion();
  console.log(`[wa:${id}] using WA v${version.join(".")}, isLatest: ${isLatest}`);

  const sock = makeWASocket({ version, auth: state, printQRInTerminal: false });
  acc.sock = sock;

  sock.ev.on("creds.update", saveCreds);

  // "notify" = new incoming messages; "append" is history sync after scanning the QR
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;
    for (const msg of messages) {
      const { key } = msg;
      // Private chats only: groups, status, broadcasts and channels are other JID kinds
      const isPrivate = isPnUser(key.remoteJid) || isLidUser(key.remoteJid);
      const pesan = textOf(msg);
      // fromMe = reply typed on the admin's own phone (kept); sends via POST /send are skipped
      if (!isPrivate || !pesan || apiSentIds.has(key.id)) continue;

      queue.push({
        cursor: nextCursor(),
        akun: id,
        // masuk = from the other person, keluar = reply from the admin's phone.
        // nomor_pengirim/jid_pengirim are always the OTHER person (the chat partner).
        arah: key.fromMe ? "keluar" : "masuk",
        id_pesan: key.id,
        nomor_pengirim: await senderNumber(sock, key),
        jid_pengirim: key.remoteJid,
        nama_pengirim: key.fromMe ? null : msg.pushName || null,
        waktu: new Date(Number(msg.messageTimestamp) * 1000).toISOString(),
        pesan,
      });
      if (queue.length > QUEUE_SIZE) queue.shift();
    }
  });

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) acc.qr = qr;

    if (connection === "close") {
      acc.ready = false;
      console.log(`[wa:${id}] closed:`, String(lastDisconnect?.error || ""));
      // Unlinked from the phone or via Logout: old creds are dead, wipe them so a fresh QR shows up
      if (
        lastDisconnect?.error?.output?.statusCode === DisconnectReason.loggedOut
      ) {
        fs.rmSync(`auth/${id}`, { recursive: true, force: true });
      }
      connectToWhatsApp(id);
    }
    if (connection === "open") {
      acc.qr = "";
      acc.ready = true;
      console.log(`[wa:${id}] ready`);
    }
  });
}

startWebServer();
ACCOUNTS.forEach(connectToWhatsApp);

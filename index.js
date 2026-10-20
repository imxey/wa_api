require("dotenv").config();
const http = require("http");
const crypto = require("crypto");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require("baileys");
const QRCode = require("qrcode");

const PORT = process.env.PORT || 8086;

let currentQr = "";
let waSocket = null;
let waReady = false;

// Hash both sides so timingSafeEqual gets equal-length buffers
function isValidSecret(secret) {
  const expected = process.env.SECRET_KEY;
  if (!expected || typeof secret !== "string") return false;
  const hash = (s) => crypto.createHash("sha256").update(s).digest();
  return crypto.timingSafeEqual(hash(secret), hash(expected));
}

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

  if (!isValidSecret(secret)) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  if (!nomor || (!msg && !file)) {
    return sendJson(res, 400, {
      error: "Bad Request: nomor and message or file are required",
    });
  }
  if (!waSocket || !waReady) {
    return sendJson(res, 503, { error: "WhatsApp socket not ready" });
  }

  await waSocket.sendMessage(toJid(nomor), await buildContent(file, msg));
  sendJson(res, 200, { success: true, message: "Pesan berhasil dikirim!" });
}

async function renderQrPage(res) {
  const qrImage = currentQr
    ? await QRCode.toDataURL(currentQr, { margin: 1, scale: 8 })
    : "";
  const body = waReady
    ? `<p>✅ WhatsApp connected</p>`
    : qrImage
      ? `<img src="${qrImage}" alt="QR Code" />`
      : `<p>Menunggu QR dari WhatsApp...</p>`;

  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(`<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta http-equiv="refresh" content="5" />
    <title>WhatsApp QR Login</title>
    <style>
      body { margin: 0; min-height: 100vh; display: grid; place-items: center;
        font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; }
      .card { text-align: center; background: #111827; padding: 24px; border-radius: 16px; }
      img { width: 320px; height: 320px; image-rendering: pixelated; background: #fff;
        padding: 12px; border-radius: 12px; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>WhatsApp QR</h1>
      ${body}
      <p>Refresh otomatis tiap 5 detik.</p>
    </div>
  </body>
</html>`);
}

function startWebServer() {
  http
    .createServer(async (req, res) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      res.setHeader(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization, X-Secret-Key",
      );

      try {
        if (req.method === "OPTIONS") {
          res.writeHead(200);
          return res.end();
        }
        if (req.method === "POST" && req.url === "/send") {
          return await handleSend(req, res);
        }
        await renderQrPage(res);
      } catch (err) {
        sendJson(res, 500, { error: err.message });
      }
    })
    .listen(PORT, () => console.log(`🌐 WA API listening on :${PORT}`));
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState("auth");
  const { version, isLatest } = await fetchLatestBaileysVersion();
  console.log(`using WA v${version.join(".")}, isLatest: ${isLatest}`);

  const sock = makeWASocket({ version, auth: state, printQRInTerminal: false });
  waSocket = sock;

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) currentQr = qr;

    if (connection === "close") {
      waReady = false;
      console.log("[wa] closed:", String(lastDisconnect?.error || ""));
      if (
        lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut
      ) {
        connectToWhatsApp();
      }
    }
    if (connection === "open") {
      currentQr = "";
      waReady = true;
      console.log("✅ WA API Ready!");
    }
  });
}

startWebServer();
connectToWhatsApp();

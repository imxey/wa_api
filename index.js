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
  downloadMediaMessage,
  jidNormalizedUser,
  isPnUser,
  isLidUser,
  decryptPollVote,
} = require("baileys");
const QRCode = require("qrcode");
const broadcast = require("./broadcast");

// node index.js --self-check: test the poll vote decoding and the broadcast scheduler, before anything
// below touches disk or WhatsApp
if (process.argv[2] === "--self-check") {
  selfCheck();
  broadcast.selfCheck({ toJid }).then(
    () => process.exit(0),
    (err) => {
      console.error(err);
      process.exit(1);
    },
  );
  return; // CommonJS allows a top-level return: nothing below may run while the async checks do
}

const PORT = process.env.PORT || 8086;
// Loopback by default so only the local reverse proxy (and SSH tunnel) can reach it
const HOST = process.env.HOST || "127.0.0.1";

const list = (s) =>
  (s || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);

// Account list + inbox flags, managed via /akun. Lives next to the sessions so the same volume keeps it.
// inbox=true: incoming chats are queued for GET /pesan-masuk. Every account can send via /send.
const STORE = "auth/akun.json";
// Names become folder names under auth/, so no path tricks allowed (no dot = can't clash with akun.json)
const NEW_NAME = /^[a-z0-9-]{1,32}$/;
const OLD_NAME = /^[a-z0-9_-]+$/i; // names migrated from env may still use these

// null when the file doesn't exist yet; a corrupt file throws (stop rather than silently start over)
function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (err) {
    if (err.code !== "ENOENT") throw err;
    return null;
  }
}

// Write-then-rename so a crash mid-write never leaves a half-written file
function writeJson(file, data) {
  fs.mkdirSync("auth", { recursive: true });
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(data, null, 2));
  fs.renameSync(`${file}.tmp`, file);
}

function loadStore() {
  const stored = readJson(STORE);
  if (stored) return stored;
  // First run: migrate the accounts from env. After this the env vars are ignored.
  const inbox = new Set(list(process.env.INBOX_ACCOUNTS));
  const now = new Date().toISOString();
  return list(process.env.WA_ACCOUNTS || "utama").map((akun) => ({ akun, inbox: inbox.has(akun), dibuat_at: now }));
}

// Insertion order = order in /status; the first account is the default for requests without `akun`
const accounts = new Map();
for (const { akun, inbox, dibuat_at } of loadStore()) {
  if (!OLD_NAME.test(akun)) throw new Error(`Nama akun tidak valid: ${akun}`);
  accounts.set(akun, { sock: null, qr: "", ready: false, inbox: !!inbox, dibuat_at });
}
const defaultAkun = () => accounts.keys().next().value ?? null;

function saveStore() {
  writeJson(STORE, [...accounts].map(([akun, a]) => ({ akun, inbox: a.inbox, dibuat_at: a.dibuat_at })));
}
saveStore();

// Polls sent via POST /polling. Votes arrive encrypted with the poll's secret, so the secret (and the
// option texts the vote hashes point to) must outlive a restart. No phone numbers are stored here.
const POLL_STORE = "auth/polling.json";
const POLL_STORE_SIZE = 500;
const polls = new Map(); // "akun|id" -> { akun, id, secret, pilihan, dibuat_at }
for (const p of readJson(POLL_STORE) || []) polls.set(`${p.akun}|${p.id}`, p);

function savePoll(p) {
  polls.set(`${p.akun}|${p.id}`, p);
  if (polls.size > POLL_STORE_SIZE) polls.delete(polls.keys().next().value);
  writeJson(POLL_STORE, [...polls.values()]);
}

// Incoming chats wait here until the calling app pulls them via GET /pesan-masuk.
// Memory only, so nothing personal touches the disk; unpulled chats are lost on restart.
// ponytail: capped in-memory queue, persist it if restarts start losing chats
// Shared by all accounts. Also how long a chat can still be quoted, forwarded or have its media fetched.
// Only message metadata is kept (no files), so 5000 stays in the tens of MB at most.
const QUEUE_SIZE = 5000;
const PAGE_SIZE = 100;
const queue = []; // oldest first
// Ids of messages sent through POST /send, so the echo of our own send is not queued as a chat
const apiSentIds = new Set();
// The WA message of every queued chat, needed to reply to (quote) or forward it. For media only the
// keys + CDN path are kept, the file itself is fetched from WhatsApp when GET /media asks for it,
// so no media is ever written to disk. cursor -> { akun, msg }
const waMsgs = new Map();
let lastCursor = 0;
// Time-based so cursors keep increasing across restarts and the caller's saved cursor stays valid
const nextCursor = () => (lastCursor = Math.max(lastCursor + 1, Date.now() * 1000));

// Long-poll requests (?tunggu=) that found nothing new wait here; any push to a feed wakes them
const waiters = new Set();
function wake() {
  for (const done of waiters) done();
}

function pushQueue(item, msg) {
  queue.push(item);
  if (msg) waMsgs.set(item.cursor, { akun: item.akun, msg });
  if (queue.length > QUEUE_SIZE) waMsgs.delete(queue.shift().cursor);
  wake();
}

const findMsg = (akun, id) => {
  const item = queue.find((p) => p.akun === akun && p.id_pesan === id);
  return item && waMsgs.get(item.cursor)?.msg;
};

// Quote text when the message being replied to is no longer in memory (restart, older than the queue)
const KUTIPAN_HILANG = "Pesan tidak dapat dimuat karena sudah tidak tersimpan di server";

// Receipts of messages we sent (API, broadcast and phone), pulled via GET /status-pesan.
// A 200 from /send already means one tick; the feed carries what comes after.
// ponytail: memory only like the chat queue; receipts not pulled before a restart are lost
const STATUS_SIZE = 5000; // bigger than the queue: one broadcast alone yields thousands
const STATUS_NAMES = { 0: "gagal", 2: "terkirim", 3: "diterima", 4: "dibaca", 5: "diputar" };
const statusFeed = []; // oldest first
const lastStatus = new Map(); // "akun|id" -> highest status seen

function recordStatus(akun, updates) {
  for (const { key, update } of updates) {
    const name = STATUS_NAMES[update.status];
    // fromMe = receipt for OUR message; the rest is e.g. the admin reading a chat on another device
    if (!key.fromMe || !name || !(isPnUser(key.remoteJid) || isLidUser(key.remoteJid))) continue;
    const k = `${akun}|${key.id}`;
    // WA repeats receipts and can deliver "read" before "delivered": only forward progress (or an error)
    if (update.status !== 0 && (lastStatus.get(k) ?? -1) >= update.status) continue;
    lastStatus.delete(k); // re-insert as newest so the cap below drops the oldest
    lastStatus.set(k, update.status);
    if (lastStatus.size > STATUS_SIZE) lastStatus.delete(lastStatus.keys().next().value);
    statusFeed.push({ cursor: nextCursor(), akun, id_pesan: key.id, status: name, waktu: new Date().toISOString() });
    if (statusFeed.length > STATUS_SIZE) statusFeed.shift();
    wake();
  }
}

// Decrypted votes on polls from POST /polling, pulled via GET /hasil-polling.
// ponytail: memory only like the status feed; votes not pulled before a restart are lost
const VOTE_SIZE = 5000;
const voteFeed = []; // oldest first

// A vote is encrypted with the poll's own secret and names the chosen options by their SHA-256.
// The voter's app signs it with either the phone-number or the @lid id of each side, so every pair
// is tried; a wrong pair fails the GCM check. Returns the chosen option texts ([] = vote withdrawn),
// or null when no pair opens it.
function decodeVote(poll, vote, creatorJids, voterJids) {
  const pollEncKey = Buffer.from(poll.secret, "base64");
  const norm = (jids) => [...new Set(jids.filter(Boolean).map(jidNormalizedUser))];
  for (const pollCreatorJid of norm(creatorJids)) {
    for (const voterJid of norm(voterJids)) {
      let selected;
      try {
        selected = decryptPollVote(vote, { pollCreatorJid, pollMsgId: poll.id, pollEncKey, voterJid }).selectedOptions;
      } catch {
        continue;
      }
      const hashes = new Set(selected.map((h) => Buffer.from(h).toString("hex")));
      return poll.pilihan.filter((p) => hashes.has(crypto.createHash("sha256").update(p).digest("hex")));
    }
  }
  return null;
}

function selfCheck() {
  const assert = require("assert");
  const { aesEncryptGCM, hmacSign, proto } = require("baileys");
  // Same steps as decryptPollVote, in reverse
  function encryptVote(poll, creator, voter, options) {
    const secret = Buffer.from(poll.secret, "base64");
    const sign = Buffer.concat([Buffer.from(poll.id), Buffer.from(creator), Buffer.from(voter), Buffer.from("Poll Vote"), Buffer.from([1])]);
    const key = hmacSign(sign, hmacSign(secret, new Uint8Array(32)));
    const encIv = crypto.randomBytes(12);
    const plain = proto.Message.PollVoteMessage.encode({
      selectedOptions: options.map((o) => crypto.createHash("sha256").update(o).digest()),
    }).finish();
    return { encIv, encPayload: aesEncryptGCM(plain, key, encIv, Buffer.from(`${poll.id}\u0000${voter}`)) };
  }

  const poll = { id: "3EB0POLL", secret: crypto.randomBytes(32).toString("base64"), pilihan: ["Senin", "Rabu", "Jumat"] };
  const me = ["628111:5@s.whatsapp.net", "999:5@lid"];
  const voter = ["123@lid", "628222@s.whatsapp.net"];

  // Signed with the @lid of the creator and the phone number of the voter: found by trying pairs
  const vote = encryptVote(poll, "999@lid", "628222@s.whatsapp.net", ["Rabu", "Jumat"]);
  assert.deepStrictEqual(decodeVote(poll, vote, me, voter), ["Rabu", "Jumat"]);
  assert.deepStrictEqual(decodeVote(poll, encryptVote(poll, "628111@s.whatsapp.net", "123@lid", []), me, voter), []);
  // Unknown option hash is dropped, not reported as a choice
  assert.deepStrictEqual(decodeVote(poll, encryptVote(poll, "999@lid", "123@lid", ["Sabtu"]), me, voter), []);
  // Wrong secret: unreadable
  assert.strictEqual(decodeVote({ ...poll, secret: crypto.randomBytes(32).toString("base64") }, vote, me, voter), null);
  console.log("self-check OK");
}

// Only polls sent via POST /polling can be read: their secret is in polls. A vote from the admin's own
// phone is skipped (the app records its own choices).
async function recordVote(akun, sock, msg, update) {
  const poll = polls.get(`${akun}|${update.pollCreationMessageKey?.id}`);
  if (!poll || msg.key.fromMe) return;
  const { key } = msg;
  const pilihan = decodeVote(poll, update.vote, [sock.user?.id, sock.user?.lid], [key.remoteJid, key.remoteJidAlt]);
  if (!pilihan) return console.log(`[wa:${akun}] vote polling ${poll.id} tidak bisa dibuka`);
  // Await BEFORE taking a cursor, same as the chat queue
  const nomor = await senderNumber(sock, key);
  voteFeed.push({
    cursor: nextCursor(),
    akun,
    id_polling: poll.id,
    nomor_pemilih: nomor,
    jid_pemilih: key.remoteJid,
    // The voter's full current choice; [] = vote withdrawn
    pilihan,
    waktu: new Date(Number(msg.messageTimestamp) * 1000).toISOString(),
  });
  if (voteFeed.length > VOTE_SIZE) voteFeed.shift();
  wake();
}

// Incoming messages already marked read via POST /baca, so reopening a chat doesn't resend receipts
const readIds = new Set();

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
  // TRUST_PROXY = number of trusted proxies in front of the app. Each proxy appends one entry
  // to the end of X-Forwarded-For, so the Nth from the end is the real client; entries
  // before that are client-controlled and can be forged
  const hops = Number(process.env.TRUST_PROXY) || 0;
  if (hops > 0 && req.headers["x-forwarded-for"]) {
    const ips = req.headers["x-forwarded-for"].split(",").map((s) => s.trim()).filter(Boolean);
    if (ips.length >= hops) return ips[ips.length - hops];
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
async function buildContent(file, caption, stiker) {
  if (!file) return { text: caption };

  const buffer = Buffer.from(await file.arrayBuffer());
  const mimetype = file.type || "application/octet-stream";

  if (stiker) return { sticker: buffer };
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
  const akunId = data.akun || data.account || defaultAkun();
  const acc = accounts.get(akunId);
  // Multipart sends booleans as strings
  const stiker = [true, "true", "1"].includes(data.stiker);
  const balasId = data.balas_id_pesan ? String(data.balas_id_pesan) : null;

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
  // WhatsApp only shows webp as a sticker; anything else arrives broken
  if (stiker && file?.type !== "image/webp") {
    return sendJson(res, 400, { error: "Bad Request: stiker wajib file image/webp" });
  }
  if (!acc.sock || !acc.ready) {
    return sendJson(res, 503, { error: "WhatsApp socket not ready" });
  }

  const jid = toJid(nomor);
  const found = balasId && findMsg(akunId, balasId);
  // Not in memory any more: still quote it (same id, so tapping it jumps to the original on the
  // recipient's phone), with a placeholder text since the original content is gone
  const quoted =
    balasId && (found || { key: { remoteJid: jid, fromMe: false, id: balasId }, message: { conversation: KUTIPAN_HILANG } });
  const sent = await acc.sock.sendMessage(jid, await buildContent(file, msg, stiker), quoted ? { quoted } : {});
  queueSent(akunId, jid, sent);
  sendJson(res, 200, {
    success: true,
    message: "Pesan berhasil dikirim!",
    id_pesan: sent?.key?.id ?? null,
    ...(balasId && { kutipan_dimuat: !!found }),
  });
}

// POST /teruskan {"akun", "id_pesan", "nomor_tujuan"}: forward a queued message (still in memory).
// Media is forwarded by reference, nothing is downloaded or re-uploaded.
async function handleTeruskan(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const data = await readBody(req);
  const akunId = data.akun || defaultAkun();
  const nomor = data.nomor_tujuan || data.nomor;
  const acc = accounts.get(akunId);
  if (!acc) {
    return sendJson(res, 400, { error: "Bad Request: akun tidak dikenal" });
  }
  if (!nomor || !data.id_pesan) {
    return sendJson(res, 400, { error: "Bad Request: id_pesan dan nomor_tujuan wajib" });
  }
  const msg = findMsg(akunId, String(data.id_pesan));
  if (!msg) {
    return sendJson(res, 404, { error: "Pesan tidak ada di memori" });
  }
  if (!acc.sock || !acc.ready) {
    return sendJson(res, 503, { error: "WhatsApp socket not ready" });
  }
  const jid = toJid(nomor);
  const sent = await acc.sock.sendMessage(jid, { forward: msg });
  queueSent(akunId, jid, sent);
  sendJson(res, 200, { success: true, id_pesan: sent?.key?.id ?? null });
}

// WhatsApp's own limit on poll options
const MAX_POLL_OPTIONS = 12;

// POST /polling {"akun", "nomor", "pertanyaan", "pilihan": [...], "maks_pilihan"}: send a poll to a
// private chat. Votes come back via GET /hasil-polling.
async function handlePolling(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const data = await readBody(req);
  const akunId = data.akun || defaultAkun();
  const acc = accounts.get(akunId);
  const nomor = data.nomor_tujuan || data.nomor;
  const { pertanyaan, pilihan } = data;
  // 1 = choose one, 0 = choose any number
  const maks = data.maks_pilihan ?? 1;
  if (!acc) {
    return sendJson(res, 400, { error: "Bad Request: akun tidak dikenal" });
  }
  if (!nomor || typeof pertanyaan !== "string" || !pertanyaan.trim()) {
    return sendJson(res, 400, { error: "Bad Request: nomor dan pertanyaan wajib" });
  }
  // Votes point to options by the hash of their text, so duplicates would be indistinguishable
  if (
    !Array.isArray(pilihan) ||
    pilihan.length < 2 ||
    pilihan.length > MAX_POLL_OPTIONS ||
    pilihan.some((p) => typeof p !== "string" || !p.trim()) ||
    new Set(pilihan).size !== pilihan.length
  ) {
    return sendJson(res, 400, { error: `Bad Request: pilihan wajib array 2-${MAX_POLL_OPTIONS} teks unik (kirim sebagai JSON)` });
  }
  if (!Number.isInteger(maks) || maks < 0 || maks > pilihan.length) {
    return sendJson(res, 400, { error: "Bad Request: maks_pilihan wajib angka 0 sampai jumlah pilihan" });
  }
  const jid = toJid(nomor);
  // Votes are only read from private chats (see messages.upsert)
  if (!isPnUser(jid) && !isLidUser(jid)) {
    return sendJson(res, 400, { error: "Bad Request: polling hanya untuk chat pribadi" });
  }
  if (!acc.sock || !acc.ready) {
    return sendJson(res, 503, { error: "WhatsApp socket not ready" });
  }
  const sent = await acc.sock.sendMessage(jid, { poll: { name: pertanyaan, values: pilihan, selectableCount: maks } });
  const secret = sent.message.messageContextInfo.messageSecret;
  savePoll({ akun: akunId, id: sent.key.id, secret: Buffer.from(secret).toString("base64"), pilihan, dibuat_at: new Date().toISOString() });
  queueSent(akunId, jid, sent);
  sendJson(res, 200, { success: true, id_pesan: sent.key.id });
}

function queueSent(akunId, jid, sent) {
  if (!sent?.key?.id) return;
  // The echo of an API send is never queued from messages.upsert (no duplicates)
  apiSentIds.add(sent.key.id);
  if (apiSentIds.size > QUEUE_SIZE) apiSentIds.delete(apiSentIds.values().next().value);
  // A message sent from the calling app goes into the chat queue so it shows up in the conversation
  // next to the replies from the phone. Only for numbers that already chatted this account: broadcasts
  // and first messages to new numbers would otherwise flood the chat list (the caller logs those itself).
  const nomor = jid.split("@")[0];
  const hasChat = queue.some((p) => p.akun === akunId && (p.nomor_pengirim === nomor || p.jid_pengirim === jid));
  const isi = contentOf(sent.message);
  if (!accounts.get(akunId)?.inbox || !hasChat || !isi) return;
  pushQueue({
    cursor: nextCursor(),
    akun: akunId,
    arah: "keluar",
    sumber: "aplikasi",
    id_pesan: sent.key.id,
    nomor_pengirim: nomor,
    jid_pengirim: jid,
    nama_pengirim: null,
    waktu: new Date().toISOString(),
    ...isi,
  }, sent);
}

async function status(res) {
  sendJson(res, 200, { success: true, message: "API is running!" });}

async function renderQrPage(res) {
  const cards = await Promise.all(
    [...accounts].map(async ([id, { qr, ready, sock }]) => {
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
    [...accounts].map(async ([id, { qr, ready, sock, inbox, dibuat_at }]) => ({
      akun: id,
      ready,
      // true = chat masuk ditarik ke GET /pesan-masuk; false = chat masuk hanya di HP
      inbox,
      nomor: ready ? phoneOf(sock) : null,
      qr: !ready && qr ? await QRCode.toDataURL(qr, { margin: 1, scale: 6 }) : null,
      dibuat_at,
    })),
  );
  sendJson(res, 200, { default: defaultAkun(), akun });
}

const akunJson = (id) => ({ akun: id, inbox: accounts.get(id).inbox, dibuat_at: accounts.get(id).dibuat_at });

// POST /akun {"akun": "<nama>"}: new, not yet logged-in session; its QR shows up in GET /status
async function handleBuatAkun(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const { akun } = await readBody(req);
  if (typeof akun !== "string" || !NEW_NAME.test(akun)) {
    return sendJson(res, 400, { error: "Bad Request: akun wajib 1-32 karakter huruf kecil/angka/strip" });
  }
  if (accounts.has(akun)) {
    return sendJson(res, 409, { error: "Akun sudah ada" });
  }
  // Leftover creds of an old account with this name would otherwise log in silently
  fs.rmSync(`auth/${akun}`, { recursive: true, force: true });
  accounts.set(akun, { sock: null, qr: "", ready: false, inbox: false, dibuat_at: new Date().toISOString() });
  saveStore();
  connect(akun);
  sendJson(res, 201, { success: true, ...akunJson(akun) });
}

// DELETE /akun/<akun>: unlink from WhatsApp and wipe the session. A /send already in flight on this
// account fails with a plain error (its socket is closed), later ones get "akun tidak dikenal".
async function handleHapusAkun(req, res, id) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const acc = accounts.get(id);
  if (!acc) {
    return sendJson(res, 404, { error: "Akun tidak ada" });
  }
  // Out of the map first: the close handler and a still-running connect see this and stop reconnecting
  accounts.delete(id);
  saveStore();
  const { sock } = acc;
  sock?.ev.removeAllListeners("creds.update"); // no more writes into the folder deleted below
  if (acc.ready) await sock.logout().catch(() => {}); // unlinks the device on the phone too
  else sock?.end(undefined);
  acc.ready = false;
  fs.rmSync(`auth/${id}`, { recursive: true, force: true });
  sendJson(res, 200, { success: true });
}

// POST /akun/<akun>/inbox {"inbox": true|false}: whether incoming chats are queued for GET /pesan-masuk
async function handleInbox(req, res, id) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const acc = accounts.get(id);
  if (!acc) {
    return sendJson(res, 404, { error: "Akun tidak ada" });
  }
  const { inbox } = await readBody(req);
  if (typeof inbox !== "boolean") {
    return sendJson(res, 400, { error: "Bad Request: inbox harus true atau false" });
  }
  acc.inbox = inbox;
  saveStore();
  sendJson(res, 200, { success: true, ...akunJson(id) });
}

// Kept under Apache's default 60s proxy timeout, or a waiting request ends as a 502
const MAX_WAIT_S = 30;

// Resolves on the next push to any feed, after ms, or when the caller hangs up
function waitForPush(res, ms) {
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      waiters.delete(done);
      res.off("close", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    waiters.add(done);
    res.on("close", done);
  });
}

// Items after ?sejak=<cursor>, oldest first, PAGE_SIZE per call. The caller saves cursor_terakhir
// and sends it back next time; repeat while ada_lagi is true.
// ?tunggu=<detik> (long-poll): when nothing is new yet, hold the request until something arrives
// in this list or the time is up (then an empty page), so the caller gets new items right away.
async function pageAfter(req, res, list) {
  const q = new URL(req.url, "http://localhost").searchParams;
  const sejak = Number(q.get("sejak")) || 0;
  const deadline = Date.now() + Math.min(Number(q.get("tunggu")) || 0, MAX_WAIT_S) * 1000;
  // Lists are in cursor order, so the last item tells whether anything is new. A push to another feed
  // also wakes us: just check again.
  while (!(list.at(-1)?.cursor > sejak) && Date.now() < deadline && !res.destroyed) {
    await waitForPush(res, deadline - Date.now());
  }
  const baru = list.filter((p) => p.cursor > sejak);
  const items = baru.slice(0, PAGE_SIZE);
  return [items, { cursor_terakhir: items.at(-1)?.cursor ?? sejak, ada_lagi: baru.length > items.length }];
}

// GET /pesan-masuk?sejak=<cursor>: queued chats
async function handlePesanMasuk(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const [pesan, info] = await pageAfter(req, res, queue);
  sendJson(res, 200, { pesan, ...info });
}

// GET /hasil-polling?sejak=<cursor>: votes on polls sent via POST /polling
async function handleHasilPolling(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const [hasil, info] = await pageAfter(req, res, voteFeed);
  sendJson(res, 200, { hasil, ...info });
}

// GET /status-pesan?sejak=<cursor>: tick changes of sent messages (diterima, dibaca, diputar, gagal)
async function handleStatusPesan(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const [status, info] = await pageAfter(req, res, statusFeed);
  sendJson(res, 200, { status, ...info });
}

// POST /baca {"akun", "nomor"}: mark that chat's queued incoming messages as read (blue ticks for the
// sender), e.g. when the admin opens the chat in the calling app
async function handleBaca(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const data = await readBody(req);
  const akunId = data.akun || defaultAkun();
  const acc = accounts.get(akunId);
  if (!acc) {
    return sendJson(res, 400, { error: "Bad Request: akun tidak dikenal" });
  }
  if (!data.nomor) {
    return sendJson(res, 400, { error: "Bad Request: nomor wajib" });
  }
  if (!acc.sock || !acc.ready) {
    return sendJson(res, 503, { error: "WhatsApp socket not ready" });
  }
  const jid = toJid(data.nomor);
  const nomor = jid.split("@")[0];
  const keys = queue
    .filter(
      (p) =>
        p.akun === akunId &&
        p.arah === "masuk" &&
        !readIds.has(`${akunId}|${p.id_pesan}`) &&
        (p.nomor_pengirim === nomor || p.jid_pengirim === jid || p.jid_pengirim === data.nomor),
    )
    .map((p) => waMsgs.get(p.cursor)?.msg.key)
    .filter(Boolean);
  // Sends "read-self" instead when this account has read receipts turned off: then no blue ticks
  if (keys.length) await acc.sock.readMessages(keys);
  for (const k of keys) readIds.add(`${akunId}|${k.id}`);
  while (readIds.size > QUEUE_SIZE) readIds.delete(readIds.values().next().value);
  sendJson(res, 200, { success: true, dibaca: keys.length });
}

// GET /media?cursor=<cursor>: file of one queued chat (photo, sticker, video, voice note, document...),
// downloaded from WhatsApp on the fly
async function handleMedia(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const cursor = Number(new URL(req.url, "http://localhost").searchParams.get("cursor"));
  const item = waMsgs.get(cursor);
  const sock = item && accounts.get(item.akun)?.sock;
  const m = item && normalizeMessageContent(item.msg.message);
  const media = m?.[Object.keys(MEDIA_TYPES).find((t) => m[t])];
  if (!sock || !media) {
    return sendJson(res, 404, { error: "Media tidak ada di memori" });
  }
  // Streamed: videos and documents can be far too big to hold in memory
  const stream = await downloadMediaMessage(item.msg, "stream", {}, {
    logger: sock.logger,
    reuploadRequest: sock.updateMediaMessage,
  });
  const headers = {
    "Content-Type": media.mimetype || "application/octet-stream",
    "Cache-Control": "private, max-age=86400",
  };
  // The name comes from the sender: percent-encoded so it can't break out of the header
  if (media.fileName) headers["Content-Disposition"] = `inline; filename*=UTF-8''${encodeURIComponent(media.fileName)}`;
  res.writeHead(200, headers);
  stream.on("error", () => res.destroy()).pipe(res);
}

// Profile photo URL per chat partner (null = hidden/none). Cached so a chat list refreshing every
// few seconds does not ask WhatsApp again and again (repeated lookups look bot-like).
const PHOTO_TTL_MS = 6 * 60 * 60 * 1000;
const photoUrls = new Map(); // "akun|jid" -> { url, at }

// GET /foto-profil?akun=<akun>&nomor=<nomor or jid>: small profile photo of someone who chatted the
// helpdesk. Only partners present in the chat queue, so this can't be used to look up any number.
async function handleFotoProfil(req, res) {
  if (!isValidSecret(req.headers["x-secret-key"])) {
    return sendJson(res, 401, { error: "Unauthorized: Invalid secret key" });
  }
  const q = new URL(req.url, "http://localhost").searchParams;
  const akun = q.get("akun");
  const nomor = String(q.get("nomor") || "");
  const sock = accounts.get(akun)?.ready && accounts.get(akun).sock;
  const item = queue.find(
    (p) => p.akun === akun && (p.nomor_pengirim === nomor.replace(/\D/g, "") || p.jid_pengirim === nomor),
  );
  if (!sock || !item || !nomor) {
    return sendJson(res, 404, { error: "Tidak ada" });
  }
  const jid = item.jid_pengirim || toJid(nomor);
  const key = `${akun}|${jid}`;
  let cached = photoUrls.get(key);
  if (!cached || Date.now() - cached.at > PHOTO_TTL_MS) {
    // Throws when the photo is hidden by privacy settings or not set
    const url = await sock.profilePictureUrl(jid, "preview").catch(() => null);
    cached = { url: url || null, at: Date.now() };
    photoUrls.set(key, cached);
    if (photoUrls.size > QUEUE_SIZE) photoUrls.delete(photoUrls.keys().next().value);
  }
  const foto = cached.url && (await fetch(cached.url).catch(() => null));
  if (!foto?.ok) {
    if (cached.url) photoUrls.delete(key); // expired CDN link: ask WhatsApp again next time
    return sendJson(res, 404, { error: "Tidak ada foto" });
  }
  res.writeHead(200, {
    "Content-Type": foto.headers.get("content-type") || "image/jpeg",
    "Cache-Control": "private, max-age=21600",
  });
  res.end(Buffer.from(await foto.arrayBuffer()));
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
        if (req.method === "GET" && req.url === "/health") {
          return await status(res);
        }

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
        if (req.method === "GET" && req.url.split("?")[0] === "/media") {
          return await handleMedia(req, res);
        }
        if (req.method === "GET" && req.url.split("?")[0] === "/foto-profil") {
          return await handleFotoProfil(req, res);
        }
        if (req.method === "GET" && req.url.split("?")[0] === "/status-pesan") {
          return await handleStatusPesan(req, res);
        }
        if (req.method === "POST" && req.url === "/baca") {
          return await handleBaca(req, res);
        }
        if (req.method === "POST" && req.url === "/teruskan") {
          return await handleTeruskan(req, res);
        }
        if (req.method === "POST" && req.url === "/polling") {
          return await handlePolling(req, res);
        }
        if (req.method === "GET" && req.url.split("?")[0] === "/hasil-polling") {
          return await handleHasilPolling(req, res);
        }
        if (req.method === "POST" && req.url === "/akun") {
          return await handleBuatAkun(req, res);
        }
        const akunPath = req.url.split("?")[0].match(/^\/akun\/([^/]+)(\/inbox)?$/);
        if (akunPath && req.method === "DELETE" && !akunPath[2]) {
          return await handleHapusAkun(req, res, decodeURIComponent(akunPath[1]));
        }
        if (akunPath && req.method === "POST" && akunPath[2]) {
          return await handleInbox(req, res, decodeURIComponent(akunPath[1]));
        }
        // /broadcast, /hasil-kirim, /pengaturan, /statistik
        if (await broadcast.tangani(req, res)) return;
        // QR page is for direct access only (localhost/SSH tunnel). Apache always adds
        // X-Forwarded-For, so anything proxied gets a plain 404 instead of a login prompt
        if (req.headers["x-forwarded-for"]) {
          return sendJson(res, 404, { error: "Not Found" });
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
    .listen(PORT, HOST, () => console.log(`WA API listening on ${HOST}:${PORT}`));
}

const MEDIA_TYPES = {
  imageMessage: "gambar",
  stickerMessage: "stiker",
  videoMessage: "video",
  ptvMessage: "video_bulat", // round video note
  audioMessage: "audio", // "pesan_suara" when it's a voice note (ptt)
  documentMessage: "dokumen",
};

// V3 = single choice, V2 = community announcement groups
const POLL_TYPES = ["pollCreationMessage", "pollCreationMessageV2", "pollCreationMessageV3"];

// Text, media (caption as text) or poll, plus reply/forward info; reactions, votes, edits, deletes etc.
// return null
function contentOf(message) {
  const m = normalizeMessageContent(message);
  const poll = POLL_TYPES.map((t) => m?.[t]).find(Boolean);
  if (poll) {
    return {
      jenis: "polling",
      media: false,
      pesan: poll.name || "",
      pilihan: (poll.options || []).map((o) => o.optionName),
      balasan_untuk: null,
      kutipan: null,
      diteruskan: false,
    };
  }
  const text = m?.conversation || m?.extendedTextMessage?.text;
  const type = !text && Object.keys(MEDIA_TYPES).find((t) => m?.[t]);
  if (!text && !type) return null;
  const media = type && m[type];
  const ctx = (media || m.extendedTextMessage)?.contextInfo;
  const quoted = ctx?.quotedMessage && contentOf(ctx.quotedMessage);
  return {
    jenis: text ? "teks" : type === "audioMessage" && media.ptt ? "pesan_suara" : MEDIA_TYPES[type],
    // true = GET /media?cursor= serves the file
    media: !text,
    pesan: text || media.caption || media.fileName || "",
    // id_pesan of the message this one replies to, plus a short preview of it
    balasan_untuk: ctx?.stanzaId || null,
    kutipan: quoted ? quoted.pesan || `[${quoted.jenis}]` : null,
    diteruskan: !!ctx?.isForwarded,
  };
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

// A failed connect (disk full, bad auth folder) must not kill the gateway: retry this account later
function connect(id) {
  const acc = accounts.get(id);
  connectToWhatsApp(id).catch((err) => {
    console.error(`[wa:${id}] gagal connect, coba lagi 10 detik:`, err);
    // Not if it was deleted (or deleted and re-created) meanwhile: that would open a second socket
    setTimeout(() => accounts.get(id) === acc && connect(id), 10_000);
  });
}

async function connectToWhatsApp(id) {
  const acc = accounts.get(id);
  if (!acc) return; // deleted before this (re)connect started
  const { state, saveCreds } = await useMultiFileAuthState(`auth/${id}`);
  const { version, isLatest } = await fetchLatestBaileysVersion();
  // Deleted (or deleted and re-created) while we were awaiting: leave it to the new one
  if (accounts.get(id) !== acc) return;
  console.log(`[wa:${id}] using WA v${version.join(".")}, isLatest: ${isLatest}`);

  const sock = makeWASocket({ version, auth: state, printQRInTerminal: false });
  acc.sock = sock;

  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("messages.update", (updates) => recordStatus(id, updates));

  // "notify" = new incoming messages; "append" is history sync after scanning the QR
  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;
    for (const msg of messages) {
      const { key } = msg;
      // Private chats only: groups, status, broadcasts and channels are other JID kinds
      const isPrivate = isPnUser(key.remoteJid) || isLidUser(key.remoteJid);
      // Poll votes count for every account, not only inbox ones
      const vote = isPrivate && normalizeMessageContent(msg.message)?.pollUpdateMessage;
      if (vote) {
        await recordVote(id, sock, msg, vote);
        continue;
      }
      if (!acc.inbox) continue;
      const isi = contentOf(msg.message);
      // fromMe = reply typed on the admin's own phone (kept); sends via POST /send are skipped
      if (!isPrivate || !isi || apiSentIds.has(key.id)) continue;

      // Await BEFORE taking a cursor: cursor + push must run with no await in between, or two
      // accounts can interleave, the queue gets out of cursor order and pulls skip/duplicate chats
      const nomorPengirim = await senderNumber(sock, key);
      // WA sometimes delivers the same message twice (decrypt retry): queue it once
      if (queue.some((p) => p.akun === id && p.id_pesan === key.id)) continue;
      pushQueue({
        cursor: nextCursor(),
        akun: id,
        // masuk = from the other person, keluar = reply from the admin's phone.
        // nomor_pengirim/jid_pengirim are always the OTHER person (the chat partner).
        arah: key.fromMe ? "keluar" : "masuk",
        sumber: "hp",
        id_pesan: key.id,
        nomor_pengirim: nomorPengirim,
        jid_pengirim: key.remoteJid,
        nama_pengirim: key.fromMe ? null : msg.pushName || null,
        waktu: new Date(Number(msg.messageTimestamp) * 1000).toISOString(),
        ...isi, // jenis, media, pesan, (pilihan), balasan_untuk, kutipan, diteruskan
      }, msg);
    }
  });

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) acc.qr = qr;

    if (connection === "close") {
      acc.ready = false;
      console.log(`[wa:${id}] closed:`, String(lastDisconnect?.error || ""));
      if (accounts.get(id) !== acc) return; // deleted via DELETE /akun: no reconnect
      // Unlinked from the phone or via Logout: old creds are dead, wipe them so a fresh QR shows up
      if (
        lastDisconnect?.error?.output?.statusCode === DisconnectReason.loggedOut
      ) {
        fs.rmSync(`auth/${id}`, { recursive: true, force: true });
      }
      connect(id);
    }
    if (connection === "open") {
      acc.qr = "";
      acc.ready = true;
      console.log(`[wa:${id}] ready`);
    }
  });
}

startWebServer();
for (const id of accounts.keys()) connect(id);
broadcast.mulai({
  accounts,
  toJid,
  queueSent,
  sendJson,
  readBody,
  isValidSecret,
  nextCursor,
  // Feed cursors stored in MySQL must stay ahead of the in-memory counter after a restart
  seedCursor: (c) => (lastCursor = Math.max(lastCursor, c)),
  wake,
  waitForPush,
  MAX_WAIT_S,
  PAGE_SIZE,
});

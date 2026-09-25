import os, asyncio, threading, hashlib, logging, json
from datetime import datetime
from flask import Flask, request, jsonify, session, render_template_string
from telethon import TelegramClient, events
from telethon.tl.types import (
    DocumentAttributeVideo, DocumentAttributeFilename,
    MessageMediaPhoto, MessageMediaDocument
)
from telethon.sessions import StringSession
import nest_asyncio
import sqlite3

nest_asyncio.apply()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

API_ID   = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SECRET_KEY = os.environ.get("SECRET_KEY", "changeme")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

CLIENT_KWARGS = dict(
    device_model="Samsung Galaxy S23",
    system_version="Android 13",
    app_version="9.6.7",
    lang_code="en",
    system_lang_code="en-US"
)

app = Flask(__name__)
app.secret_key = SECRET_KEY

DB_PATH = "/tmp/forwarder.db"
_loop = asyncio.new_event_loop()
_client = None
_jobs = {}

# ── Database ───────────────────────────────────────────────
def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with db() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS forwarded (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id      TEXT,
                message_id  INTEGER,
                file_hash   TEXT,
                media_type  TEXT,
                forwarded_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id          INTEGER PRIMARY KEY,
                session_str TEXT,
                saved_at    TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS duplicates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                file_hash   TEXT UNIQUE,
                first_msg_id INTEGER,
                source      TEXT,
                detected_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)

init_db()

# ── Session persistence ────────────────────────────────────
def save_session_to_db(session_str: str):
    with db() as c:
        c.execute("DELETE FROM sessions")
        c.execute("INSERT INTO sessions (session_str) VALUES (?)", (session_str,))
        c.commit()
    log.info("Session saved to DB")

def load_session_from_db() -> str:
    with db() as c:
        row = c.execute("SELECT session_str FROM sessions ORDER BY id DESC LIMIT 1").fetchone()
    return row["session_str"] if row else ""

def get_best_session() -> str:
    # Priority: env var > DB > empty
    if SESSION_STRING:
        return SESSION_STRING
    return load_session_from_db()

# ── File hash for duplicate detection ─────────────────────
def compute_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def is_duplicate(file_hash: str, source: str, msg_id: int) -> bool:
    with db() as c:
        row = c.execute(
            "SELECT id FROM duplicates WHERE file_hash = ?", (file_hash,)
        ).fetchone()
        if row:
            return True
        c.execute(
            "INSERT INTO duplicates (file_hash, first_msg_id, source) VALUES (?,?,?)",
            (file_hash, msg_id, source)
        )
        c.commit()
    return False

def already_forwarded_by_id(job_id: str, msg_id: int) -> bool:
    with db() as c:
        row = c.execute(
            "SELECT id FROM forwarded WHERE job_id=? AND message_id=?",
            (job_id, msg_id)
        ).fetchone()
    return row is not None

def record_forwarded(job_id, msg_id, file_hash, media_type):
    with db() as c:
        c.execute(
            "INSERT INTO forwarded (job_id, message_id, file_hash, media_type) VALUES (?,?,?,?)",
            (job_id, msg_id, file_hash or "", media_type)
        )
        c.commit()

# ── Telethon client ────────────────────────────────────────
def run_in_loop(coro):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=120)

def start_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

threading.Thread(target=start_loop, daemon=True).start()

async def get_client() -> TelegramClient:
    global _client
    if _client and _client.is_connected():
        # Auto-save session every time client is fetched
        ss = _client.session.save()
        if ss:
            save_session_to_db(ss)
        return _client
    best = get_best_session()
    if best:
        _client = TelegramClient(StringSession(best), API_ID, API_HASH, **CLIENT_KWARGS)
        await _client.connect()
        if await _client.is_user_authorized():
            ss = _client.session.save()
            save_session_to_db(ss)
            log.info("Restored session successfully")
            return _client
    _client = TelegramClient(StringSession(), API_ID, API_HASH, **CLIENT_KWARGS)
    await _client.connect()
    return _client

# ── Auth routes ────────────────────────────────────────────
@app.route("/send_code", methods=["POST"])
def send_code_route():
    phone = request.json.get("phone")
    async def _send():
        c = await get_client()
        r = await c.send_code_request(phone)
        return r.phone_code_hash
    try:
        h = run_in_loop(_send())
        session["phone"] = phone
        session["hash"] = h
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e))

@app.route("/sign_in", methods=["POST"])
def sign_in_route():
    code = request.json.get("code")
    pw   = request.json.get("password", "")
    async def _sign():
        c = await get_client()
        try:
            await c.sign_in(session["phone"], code, phone_code_hash=session["hash"])
        except Exception as e:
            if "two" in str(e).lower() or "password" in str(e).lower():
                if pw:
                    await c.sign_in(password=pw)
                else:
                    raise Exception("2FA_REQUIRED")
        ss = c.session.save()
        save_session_to_db(ss)
        return ss
    try:
        ss = run_in_loop(_sign())
        session["auth"] = True
        return jsonify(ok=True, session_string=ss)
    except Exception as e:
        return jsonify(ok=False, error=str(e))

@app.route("/check_auth")
def check_auth():
    async def _check():
        c = await get_client()
        return await c.is_user_authorized()
    try:
        ok = run_in_loop(_check())
        if ok:
            session["auth"] = True
        return jsonify(authed=ok)
    except:
        return jsonify(authed=False)

# ── Dialogs ────────────────────────────────────────────────
@app.route("/dialogs")
def get_dialogs():
    async def _get():
        c = await get_client()
        if not await c.is_user_authorized():
            raise Exception("Not authorized")
        dialogs = await c.get_dialogs(limit=200)
        result = []
        for d in dialogs:
            if not d.name:
                continue
            username = getattr(d.entity, 'username', None)
            identifier = f"@{username}" if username else str(d.id)
            result.append({
                "id": identifier,
                "name": d.name or "Unknown",
                "type": type(d.entity).__name__
            })
        return result
    try:
        return jsonify(dialogs=run_in_loop(_get()))
    except Exception as e:
        return jsonify(error=str(e)), 401

# ── Duplicate check route ──────────────────────────────────
@app.route("/scan_duplicates", methods=["POST"])
def scan_duplicates():
    """Scan source channel and return duplicate media before forwarding"""
    data = request.json
    source = data.get("source")
    media_types = data.get("media_types", ["video", "photo", "document"])
    limit = int(data.get("limit", 100))

    async def _scan():
        c = await get_client()
        entity = await c.get_entity(source)
        seen_hashes = {}
        duplicates_found = []
        async for msg in c.iter_messages(entity, limit=limit):
            if not msg.media:
                continue
            mtype = _get_media_type(msg)
            if mtype not in media_types:
                continue
            try:
                data_bytes = await c.download_media(msg, file=bytes)
                if not data_bytes:
                    continue
                fh = compute_hash(data_bytes)
                if fh in seen_hashes:
                    duplicates_found.append({
                        "msg_id": msg.id,
                        "duplicate_of": seen_hashes[fh],
                        "type": mtype,
                        "size": len(data_bytes),
                        "date": str(msg.date)
                    })
                else:
                    seen_hashes[fh] = msg.id
            except Exception as ex:
                log.warning(f"Could not scan msg {msg.id}: {ex}")
        return duplicates_found

    try:
        dupes = run_in_loop(_scan())
        return jsonify(ok=True, duplicates=dupes, count=len(dupes))
    except Exception as e:
        return jsonify(ok=False, error=str(e))

async def resolve_entity(client, identifier):
    identifier = str(identifier).strip()
    if identifier.startswith("@") or identifier.startswith("http") or identifier.startswith("+"):
        return await client.get_entity(identifier)
    try:
        numeric_id = int(identifier)
        async for dialog in client.iter_dialogs():
            if dialog.id == numeric_id or dialog.entity.id == numeric_id:
                return dialog.entity
        return await client.get_entity(numeric_id)
    except ValueError:
        return await client.get_entity(identifier)

def _get_media_type(msg) -> str:
    if isinstance(msg.media, MessageMediaPhoto):
        return "photo"
    if isinstance(msg.media, MessageMediaDocument):
        doc = msg.document
        for attr in (doc.attributes or []):
            if isinstance(attr, DocumentAttributeVideo):
                return "video"
        return "document"
    return "other"

# ── Forward job ────────────────────────────────────────────
@app.route("/start_job", methods=["POST"])
def start_job():
    data = request.json
    job_id = datetime.now().strftime("%Y%m%d%H%M%S")
    media_types = data.get("media_types", ["video", "photo", "document", "text"])
    skip_duplicates = data.get("skip_duplicates", True)

    _jobs[job_id] = {
        "status": "running",
        "done": 0,
        "skipped": 0,
        "duplicates_skipped": 0,
        "errors": 0,
        "logs": [],
        "media_types": media_types,
        "skip_duplicates": skip_duplicates
    }

    def _run():
        asyncio.run_coroutine_threadsafe(
            forward_job_async(job_id, data), _loop
        )

    threading.Thread(target=_run, daemon=True).start()
    return jsonify(ok=True, job_id=job_id)

async def forward_job_async(job_id: str, cfg: dict):
    job = _jobs[job_id]

    def log_msg(m):
        job["logs"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {m}")
        log.info(m)

    try:
        c = await get_client()
        src = await resolve_entity(c, cfg["source"])
        dst = await resolve_entity(c, cfg["dest"])
        limit = int(cfg.get("limit", 100))
        media_types = cfg.get("media_types", ["video", "photo", "document", "text"])
        skip_dupes = cfg.get("skip_duplicates", True)
        pin_first = cfg.get("pin_first", True)
        pin_every = int(cfg.get("pin_every", 200))
        pin_counter = 0
        first_pinned = False

        log_msg(f"Starting job: {cfg['source']} → {cfg['dest']} | types={media_types}")

        async for msg in c.iter_messages(src, limit=limit):
            if job["status"] == "cancelled":
                log_msg("Job cancelled by user")
                break

            # Message ID dedup
            if already_forwarded_by_id(job_id, msg.id):
                job["skipped"] += 1
                continue

            # Media type filter
            mtype = _get_media_type(msg) if msg.media else "text"
            if mtype not in media_types:
                job["skipped"] += 1
                continue

            file_hash = None

            # Content hash duplicate detection
            if skip_dupes and msg.media and mtype in ["video", "photo", "document"]:
                try:
                    raw = await c.download_media(msg, file=bytes)
                    if raw:
                        file_hash = compute_hash(raw)
                        if is_duplicate(file_hash, str(src.id), msg.id):
                            job["duplicates_skipped"] += 1
                            log_msg(f"⚠️ Duplicate detected — skipping msg {msg.id}")
                            continue
                except Exception as ex:
                    log_msg(f"Hash check failed for {msg.id}: {ex}")

            # Forward or re-upload
            try:
                sent = None
                caption = msg.text or ""

                if msg.media:
                    # Try direct forward first
                    try:
                        sent = await c.forward_messages(dst, msg.id, src)
                        log_msg(f"✅ Forwarded msg {msg.id} ({mtype})")
                    except Exception:
                        # Re-upload with metadata preserved
                        if not hasattr(locals(), 'raw') or raw is None:
                            raw = await c.download_media(msg, file=bytes)

                        orig_attrs = []
                        thumb = None

                        if msg.document:
                            orig_attrs = msg.document.attributes or []
                            if msg.document.thumbs:
                                thumb = await c.download_media(
                                    msg.document.thumbs[-1], file=bytes
                                )

                        import io
                        buf = io.BytesIO(raw)
                        buf.name = "media.mp4" if mtype == "video" else "media.jpg"

                        sent = await c.send_file(
                            dst,
                            file=buf,
                            caption=caption,
                            attributes=orig_attrs,
                            thumb=thumb,
                            supports_streaming=True
                        )
                        log_msg(f"✅ Re-uploaded msg {msg.id} ({mtype}) with metadata")
                else:
                    if "text" in media_types and msg.text:
                        sent = await c.send_message(dst, msg.text)
                        log_msg(f"✅ Sent text msg {msg.id}")

                # Auto-pin logic
                if sent:
                    pin_counter += 1
                    if pin_first and not first_pinned:
                        await c.pin_message(dst, sent.id, notify=False)
                        first_pinned = True
                        log_msg(f"📌 Pinned first message {sent.id}")
                    elif pin_counter % pin_every == 0:
                        await c.pin_message(dst, sent.id, notify=False)
                        log_msg(f"📌 Auto-pinned message {sent.id} (every {pin_every})")

                record_forwarded(job_id, msg.id, file_hash, mtype)
                job["done"] += 1

            except Exception as ex:
                job["errors"] += 1
                log_msg(f"❌ Error on msg {msg.id}: {ex}")

            await asyncio.sleep(1.5)

        job["status"] = "done"
        # Save session after job completes
        ss = c.session.save()
        if ss:
            save_session_to_db(ss)
        log_msg(f"── Job complete ── Forwarded: {job['done']} | Skipped: {job['skipped']} | Dupes: {job['duplicates_skipped']} | Errors: {job['errors']}")

    except Exception as e:
        job["status"] = "error"
        job["logs"].append(f"FATAL: {e}")
        log.error(e)

@app.route("/job_status/<job_id>")
def job_status(job_id):
    j = _jobs.get(job_id)
    if not j:
        return jsonify(error="Job not found"), 404
    return jsonify(j)

@app.route("/cancel_job/<job_id>", methods=["POST"])
def cancel_job(job_id):
    if job_id in _jobs:
        _jobs[job_id]["status"] = "cancelled"
        return jsonify(ok=True)
    return jsonify(ok=False)

# ── Main UI ────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TG Forwarder Pro</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0f0f1a; color: #e0e0e0; min-height: 100vh; }
  .container { max-width: 900px; margin: 0 auto; padding: 20px; }
  h1 { text-align: center; color: #7c83fd; margin-bottom: 24px; font-size: 1.8rem; }
  .card { background: #1a1a2e; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #2a2a4a; }
  .card h2 { color: #7c83fd; margin-bottom: 16px; font-size: 1.1rem; }
  input, select { width: 100%; padding: 10px 14px; background: #0f0f1a; border: 1px solid #3a3a5a; border-radius: 8px; color: #e0e0e0; margin-bottom: 12px; font-size: 0.95rem; }
  button { padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer; font-size: 0.95rem; font-weight: 600; transition: 0.2s; }
  .btn-primary { background: #7c83fd; color: #fff; width: 100%; margin-top: 4px; }
  .btn-primary:hover { background: #5c63dd; }
  .btn-danger { background: #e74c3c; color: #fff; }
  .btn-sm { padding: 6px 14px; font-size: 0.85rem; }
  .status { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; }
  .status.running { background: #27ae60; color: #fff; }
  .status.done { background: #2980b9; color: #fff; }
  .status.error { background: #e74c3c; color: #fff; }
  .status.cancelled { background: #7f8c8d; color: #fff; }
  .log-box { background: #0a0a14; border-radius: 8px; padding: 12px; font-family: monospace; font-size: 0.82rem; max-height: 220px; overflow-y: auto; color: #a0f0a0; border: 1px solid #2a2a4a; }
  .checkbox-group { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 14px; }
  .checkbox-group label { display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 0.9rem; }
  .checkbox-group input[type=checkbox] { width: auto; margin: 0; }
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 14px; }
  .stat { background: #0f0f1a; border-radius: 8px; padding: 10px; text-align: center; border: 1px solid #2a2a4a; }
  .stat .num { font-size: 1.4rem; font-weight: 700; color: #7c83fd; }
  .stat .lbl { font-size: 0.75rem; color: #888; }
  .dupe-list { max-height: 150px; overflow-y: auto; font-size: 0.82rem; }
  .dupe-item { padding: 6px; border-bottom: 1px solid #2a2a4a; display: flex; justify-content: space-between; }
  .hidden { display: none; }
  .session-badge { background: #27ae60; color: #fff; padding: 4px 10px; border-radius: 20px; font-size: 0.8rem; }
  .session-badge.offline { background: #e74c3c; }
  #auth-section select { margin-bottom: 12px; }
</style>
</head>
<body>
<div class="container">
  <h1>🚀 TG Forwarder Pro</h1>

  <!-- Auth Card -->
  <div class="card" id="auth-card">
    <h2>🔐 Authentication <span id="session-badge" class="session-badge offline">Checking...</span></h2>
    <div id="auth-section">
      <input type="tel" id="phone" placeholder="Phone number (+91...)" />
      <button class="btn-primary" onclick="sendCode()">Send Code</button>
      <div id="code-section" class="hidden">
        <input type="text" id="code" placeholder="Enter OTP code" />
        <input type="password" id="twofa" placeholder="2FA Password (if enabled)" />
        <button class="btn-primary" onclick="signIn()">Verify & Login</button>
      </div>
    </div>
    <div id="auth-info" class="hidden" style="color:#27ae60; font-size:0.9rem; margin-top:8px;"></div>
  </div>

  <!-- Forward Config Card -->
  <div class="card hidden" id="forward-card">
    <h2>⚙️ Forward Configuration</h2>
    <label style="font-size:0.85rem; color:#aaa; margin-bottom:4px; display:block;">Source Channel</label>
    <select id="source-select"><option value="">Loading dialogs...</option></select>
    <label style="font-size:0.85rem; color:#aaa; margin-bottom:4px; display:block;">Destination Channel</label>
    <select id="dest-select"><option value="">Loading dialogs...</option></select>
    <label style="font-size:0.85rem; color:#aaa; margin-bottom:4px; display:block;">Message Limit</label>
    <input type="number" id="limit" value="100" min="1" max="10000" />

    <label style="font-size:0.85rem; color:#aaa; margin-bottom:8px; display:block;">Content Types to Forward:</label>
    <div class="checkbox-group">
      <label><input type="checkbox" id="type-video" checked> 🎥 Videos</label>
      <label><input type="checkbox" id="type-photo" checked> 🖼️ Photos</label>
      <label><input type="checkbox" id="type-document" checked> 📄 Documents</label>
      <label><input type="checkbox" id="type-text"> 💬 Text Messages</label>
    </div>

    <label style="font-size:0.85rem; color:#aaa; margin-bottom:8px; display:block;">Options:</label>
    <div class="checkbox-group">
      <label><input type="checkbox" id="skip-dupes" checked> 🔍 Skip duplicate files (hash check)</label>
      <label><input type="checkbox" id="pin-first" checked> 📌 Pin first message</label>
    </div>
    <input type="number" id="pin-every" value="200" min="1" placeholder="Pin every N messages" />
    <small style="color:#888; display:block; margin-bottom:12px;">Auto-pin every N forwarded messages</small>

    <button class="btn-primary" onclick="scanDuplicates()" style="background:#e67e22; margin-bottom:8px;">🔍 Scan for Duplicates First</button>
    <button class="btn-primary" onclick="startJob()">▶️ Start Forwarding</button>
  </div>

  <!-- Duplicate Report Card -->
  <div class="card hidden" id="dupe-card">
    <h2>⚠️ Duplicate Report</h2>
    <div id="dupe-summary" style="margin-bottom:10px; font-size:0.9rem;"></div>
    <div class="dupe-list" id="dupe-list"></div>
    <button class="btn-primary" style="margin-top:12px;" onclick="startJob()">▶️ Continue (Skip Duplicates)</button>
  </div>

  <!-- Job Monitor Card -->
  <div class="card hidden" id="job-card">
    <h2>📊 Job Monitor <span id="job-status-badge" class="status running">running</span></h2>
    <div class="stats">
      <div class="stat"><div class="num" id="stat-done">0</div><div class="lbl">Forwarded</div></div>
      <div class="stat"><div class="num" id="stat-skipped">0</div><div class="lbl">Skipped</div></div>
      <div class="stat"><div class="num" id="stat-dupes">0</div><div class="lbl">Dupes Blocked</div></div>
      <div class="stat"><div class="num" id="stat-errors">0</div><div class="lbl">Errors</div></div>
    </div>
    <div class="log-box" id="log-box">Waiting for logs...</div>
    <button class="btn-danger btn-sm" style="margin-top:10px;" onclick="cancelJob()">⏹ Cancel Job</button>
  </div>

</div>

<script>
let currentJobId = null;
let pollInterval = null;
let dialogsCache = [];

async function api(url, method='GET', body=null) {
  const opts = { method, headers: {'Content-Type':'application/json'} };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(url, opts);
  return r.json();
}

async function checkAuth() {
  const r = await api('/check_auth');
  const badge = document.getElementById('session-badge');
  if (r.authed) {
    badge.textContent = '✅ Session Active';
    badge.className = 'session-badge';
    document.getElementById('auth-info').textContent = 'Session restored automatically — no login needed.';
    document.getElementById('auth-info').classList.remove('hidden');
    document.getElementById('auth-section').classList.add('hidden');
    document.getElementById('forward-card').classList.remove('hidden');
    loadDialogs();
  } else {
    badge.textContent = '❌ Not Logged In';
    badge.className = 'session-badge offline';
  }
}

async function sendCode() {
  const phone = document.getElementById('phone').value.trim();
  if (!phone) return alert('Enter phone number');
  const r = await api('/send_code', 'POST', {phone});
  if (r.ok) {
    document.getElementById('code-section').classList.remove('hidden');
    alert('OTP sent to your Telegram app');
  } else {
    alert('Error: ' + r.error);
  }
}

async function signIn() {
  const code = document.getElementById('code').value.trim();
  const password = document.getElementById('twofa').value.trim();
  const r = await api('/sign_in', 'POST', {code, password});
  if (r.ok) {
    document.getElementById('session-badge').textContent = '✅ Session Active';
    document.getElementById('session-badge').className = 'session-badge';
    document.getElementById('auth-section').classList.add('hidden');
    document.getElementById('auth-info').textContent = '✅ Logged in! Session saved permanently.';
    document.getElementById('auth-info').classList.remove('hidden');
    document.getElementById('forward-card').classList.remove('hidden');
    loadDialogs();
    if (r.session_string) {
      console.log('SESSION_STRING (save to Railway env):', r.session_string);
    }
  } else {
    alert('Login failed: ' + r.error);
  }
}

async function loadDialogs() {
  const r = await api('/dialogs');
  if (r.error) { alert('Could not load dialogs: ' + r.error); return; }
  dialogsCache = r.dialogs;
  const srcSel = document.getElementById('source-select');
  const dstSel = document.getElementById('dest-select');
  srcSel.innerHTML = '';
  dstSel.innerHTML = '';
  r.dialogs.forEach(d => {
    const o1 = new Option(`${d.name} (${d.type})`, d.id);
    const o2 = new Option(`${d.name} (${d.type})`, d.id);
    srcSel.add(o1);
    dstSel.add(o2);
  });
}

function getMediaTypes() {
  const types = [];
  if (document.getElementById('type-video').checked)    types.push('video');
  if (document.getElementById('type-photo').checked)    types.push('photo');
  if (document.getElementById('type-document').checked) types.push('document');
  if (document.getElementById('type-text').checked)     types.push('text');
  return types;
}

async function scanDuplicates() {
  const source = document.getElementById('source-select').value;
  const limit  = document.getElementById('limit').value;
  const types  = getMediaTypes().filter(t => t !== 'text');
  if (!source) return alert('Select a source channel');
  const r = await api('/scan_duplicates', 'POST', {source, limit, media_types: types});
  const card = document.getElementById('dupe-card');
  const summary = document.getElementById('dupe-summary');
  const list = document.getElementById('dupe-list');
  card.classList.remove('hidden');
  if (r.ok) {
    summary.textContent = `Found ${r.count} duplicate file(s) in source channel.`;
    list.innerHTML = r.duplicates.map(d =>
      `<div class="dupe-item"><span>Msg #${d.msg_id} (${d.type})</span><span>Duplicate of #${d.duplicate_of} | ${(d.size/1024/1024).toFixed(1)}MB</span></div>`
    ).join('') || '<div style="color:#888; padding:8px;">No duplicates found ✅</div>';
  } else {
    summary.textContent = 'Scan failed: ' + r.error;
  }
}

async function startJob() {
  const source = document.getElementById('source-select').value;
  const dest   = document.getElementById('dest-select').value;
  const limit  = document.getElementById('limit').value;
  const types  = getMediaTypes();
  if (!source || !dest) return alert('Select source and destination');
  if (types.length === 0) return alert('Select at least one content type');
  const r = await api('/start_job', 'POST', {
    source, dest, limit,
    media_types: types,
    skip_duplicates: document.getElementById('skip-dupes').checked,
    pin_first: document.getElementById('pin-first').checked,
    pin_every: document.getElementById('pin-every').value
  });
  if (r.ok) {
    currentJobId = r.job_id;
    document.getElementById('job-card').classList.remove('hidden');
    document.getElementById('dupe-card').classList.add('hidden');
    startPolling();
  } else {
    alert('Failed to start job');
  }
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(async () => {
    if (!currentJobId) return;
    const j = await api(`/job_status/${currentJobId}`);
    document.getElementById('stat-done').textContent    = j.done || 0;
    document.getElementById('stat-skipped').textContent = j.skipped || 0;
    document.getElementById('stat-dupes').textContent   = j.duplicates_skipped || 0;
    document.getElementById('stat-errors').textContent  = j.errors || 0;
    const badge = document.getElementById('job-status-badge');
    badge.textContent = j.status;
    badge.className = `status ${j.status}`;
    const lb = document.getElementById('log-box');
    lb.innerHTML = (j.logs || []).slice(-50).join('<br>');
    lb.scrollTop = lb.scrollHeight;
    if (['done','error','cancelled'].includes(j.status)) {
      clearInterval(pollInterval);
    }
  }, 2000);
}

async function cancelJob() {
  if (!currentJobId) return;
  await api(`/cancel_job/${currentJobId}`, 'POST');
}

// On page load, check if session already exists
checkAuth();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

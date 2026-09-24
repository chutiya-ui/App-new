# --- app.py ---

import asyncio
import io
import os
import threading
from flask import Flask, render_template_string, request, jsonify, session
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, DocumentAttributeFilename
from telethon.errors import FloodWaitError, SessionPasswordNeededError

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "sentinelflow-secret-2024")

API_ID   = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")

# --- persistent state ---
auth_clients: dict  = {}   # phone -> TelegramClient (kept alive during auth)
auth_sessions: dict = {}   # phone -> session string (post auth)
phone_hashes: dict  = {}   # phone -> phone_code_hash (required for sign_in)

# one shared event loop for all Telethon calls
_loop = asyncio.new_event_loop()

def _run(coro):
    """Submit a coroutine to the shared loop from any thread."""
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result(timeout=60)

def _start_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

_loop_thread = threading.Thread(target=_start_loop, daemon=True)
_loop_thread.start()

job = {
    "running":   False,
    "log":       [],
    "forwarded": 0,
    "failed":    0,
    "skipped":   0,
    "total":     0,
    "done":      False,
    "mode":      None,
}

# ─────────────────────────────────────────
#  HTML
# ─────────────────────────────────────────

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TG Channel Forwarder</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #0d1117;
  color: #e6edf3;
  font-family: 'Segoe UI', system-ui, sans-serif;
  min-height: 100vh;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding: 40px 16px;
}
.card {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 12px;
  padding: 32px;
  width: 100%;
  max-width: 680px;
}
h1 { font-size: 1.4rem; font-weight: 600; color: #58a6ff; margin-bottom: 6px; }
.subtitle { font-size: 0.85rem; color: #8b949e; margin-bottom: 28px; }
label {
  display: block;
  font-size: 0.8rem;
  color: #8b949e;
  margin-bottom: 6px;
  font-weight: 500;
  letter-spacing: 0.03em;
}
input[type=text], input[type=number], input[type=password], input[type=tel] {
  width: 100%;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  color: #e6edf3;
  font-size: 0.9rem;
  padding: 10px 14px;
  margin-bottom: 18px;
  outline: none;
  transition: border-color 0.15s;
}
input:focus { border-color: #58a6ff; }
.row { display: flex; gap: 16px; }
.row > div { flex: 1; }
.badge {
  display: inline-block;
  font-size: 0.7rem;
  padding: 2px 10px;
  border-radius: 20px;
  margin-bottom: 18px;
  font-weight: 600;
}
.badge-blue   { background: #1f3a5f; color: #58a6ff; }
.badge-green  { background: #1a3a2a; color: #3fb950; }
.badge-red    { background: #3a1a1a; color: #f85149; }
.badge-yellow { background: #3a2f0a; color: #d29922; }
button {
  width: 100%;
  padding: 12px;
  background: #238636;
  border: none;
  border-radius: 6px;
  color: #fff;
  font-size: 0.95rem;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.15s;
  margin-top: 4px;
}
button:hover:not(:disabled) { background: #2ea043; }
button:disabled { background: #21262d; color: #484f58; cursor: not-allowed; }
button.secondary {
  background: #21262d;
  border: 1px solid #30363d;
  color: #8b949e;
  margin-top: 10px;
}
button.secondary:hover:not(:disabled) { background: #30363d; color: #e6edf3; }
.stats { display: flex; gap: 12px; margin: 24px 0 16px; }
.stat {
  flex: 1;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 8px;
  padding: 12px;
  text-align: center;
}
.stat-num { font-size: 1.6rem; font-weight: 700; color: #58a6ff; }
.stat-num.green { color: #3fb950; }
.stat-num.red   { color: #f85149; }
.stat-num.gray  { color: #8b949e; }
.stat-label {
  font-size: 0.72rem;
  color: #8b949e;
  margin-top: 2px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.progress-wrap {
  background: #0d1117;
  border-radius: 4px;
  height: 6px;
  margin-bottom: 16px;
  overflow: hidden;
  border: 1px solid #21262d;
}
.progress-bar {
  height: 100%;
  background: #238636;
  transition: width 0.4s ease;
  border-radius: 4px;
}
#log-box {
  background: #0d1117;
  border: 1px solid #21262d;
  border-radius: 6px;
  height: 260px;
  overflow-y: auto;
  padding: 12px 14px;
  font-family: 'Courier New', monospace;
  font-size: 0.78rem;
  color: #8b949e;
}
.log-line { margin-bottom: 4px; line-height: 1.5; }
.log-line.ok   { color: #3fb950; }
.log-line.err  { color: #f85149; }
.log-line.info { color: #58a6ff; }
.log-line.warn { color: #d29922; }
.divider { border: none; border-top: 1px solid #21262d; margin: 24px 0; }
.auth-status {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 20px;
  font-size: 0.85rem;
}
.dot { width: 8px; height: 8px; border-radius: 50%; background: #f85149; flex-shrink: 0; }
.dot.green  { background: #3fb950; }
.dot.yellow { background: #d29922; }
.step-label {
  font-size: 0.75rem;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-bottom: 14px;
  font-weight: 600;
}
.hidden { display: none; }
.error-msg { color: #f85149; font-size: 0.82rem; margin-top: -12px; margin-bottom: 14px; }
</style>
</head>
<body>
<div class="card">
  <h1>⚡ TG Channel Forwarder</h1>
  <p class="subtitle">Smart forward detection — copies direct or re-uploads clean. No forward tag.</p>

  <div class="auth-status">
    <div class="dot" id="auth-dot"></div>
    <span id="auth-label">Not logged in</span>
    <button class="secondary" id="logout-btn"
      style="width:auto;padding:4px 12px;font-size:0.78rem;margin-top:0;"
      onclick="logout()">Logout</button>
  </div>

  <!-- STEP 1: PHONE -->
  <div id="step-phone">
    <div class="step-label">Step 1 — Telegram phone number</div>
    <label>PHONE NUMBER (with country code)</label>
    <input type="tel" id="phone" placeholder="+1234567890">
    <div class="error-msg hidden" id="phone-err"></div>
    <button id="phone-btn" onclick="sendPhone()">Send Code</button>
  </div>

  <!-- STEP 2: CODE -->
  <div id="step-code" class="hidden">
    <div class="step-label">Step 2 — Verification code</div>
    <label>CODE (from Telegram app)</label>
    <input type="text" id="code" placeholder="12345" maxlength="10">
    <div class="error-msg hidden" id="code-err"></div>
    <button id="code-btn" onclick="sendCode()">Verify Code</button>
    <button class="secondary" onclick="backToPhone()">← Back</button>
  </div>

  <!-- STEP 2B: 2FA -->
  <div id="step-2fa" class="hidden">
    <div class="step-label">Step 2B — Two-factor authentication</div>
    <label>TELEGRAM PASSWORD</label>
    <input type="password" id="twofa" placeholder="Your 2FA password">
    <div class="error-msg hidden" id="twofa-err"></div>
    <button id="twofa-btn" onclick="send2FA()">Submit Password</button>
  </div>

  <!-- STEP 3: FORWARDER -->
  <div id="step-forwarder" class="hidden">
    <hr class="divider">
    <form id="job-form">
      <div class="row">
        <div>
          <label>SOURCE CHANNEL</label>
          <input type="text" id="source" placeholder="@channel or invite link" required>
        </div>
        <div>
          <label>DESTINATION CHANNEL</label>
          <input type="text" id="dest" placeholder="@yourchannel" required>
        </div>
      </div>
      <label>MESSAGE LIMIT (0 = all)</label>
      <input type="number" id="limit" value="0" min="0">
      <div class="badge badge-blue" id="mode-badge">⏳ Mode detected after start</div>
      <button type="submit" id="start-btn">▶ Start Forwarding</button>
    </form>

    <div class="stats">
      <div class="stat">
        <div class="stat-num green" id="s-forwarded">0</div>
        <div class="stat-label">Forwarded</div>
      </div>
      <div class="stat">
        <div class="stat-num gray" id="s-skipped">0</div>
        <div class="stat-label">Skipped</div>
      </div>
      <div class="stat">
        <div class="stat-num red" id="s-failed">0</div>
        <div class="stat-label">Failed</div>
      </div>
      <div class="stat">
        <div class="stat-num" id="s-total">0</div>
        <div class="stat-label">Total</div>
      </div>
    </div>

    <div class="progress-wrap">
      <div class="progress-bar" id="progress" style="width:0%"></div>
    </div>
    <div id="log-box"></div>
  </div>
</div>

<script>
let polling     = null;
let logOffset   = 0;
let currentPhone = "";

window.addEventListener('DOMContentLoaded', async () => {
  const res  = await fetch('/auth/status');
  const data = await res.json();
  if (data.logged_in) {
    currentPhone = data.phone || "";
    showForwarder(data.phone);
  } else {
    showPhone();
  }
});

function setDot(state) {
  const dot   = document.getElementById('auth-dot');
  const label = document.getElementById('auth-label');
  dot.className = 'dot';
  if (state === 'green')  { dot.classList.add('green');  label.textContent = 'Logged in'; }
  if (state === 'yellow') { dot.classList.add('yellow'); label.textContent = 'Authenticating…'; }
  if (state === 'red')    {                              label.textContent = 'Not logged in'; }
}

function showPhone() {
  hide(['step-code','step-2fa','step-forwarder']);
  show(['step-phone']);
  setDot('red');
  document.getElementById('logout-btn').style.display = 'none';
}
function showCode()     { hide(['step-phone','step-2fa','step-forwarder']); show(['step-code']);     setDot('yellow'); }
function show2FA()      { hide(['step-phone','step-code','step-forwarder']); show(['step-2fa']);     setDot('yellow'); }
function showForwarder(phone) {
  hide(['step-phone','step-code','step-2fa']);
  show(['step-forwarder']);
  setDot('green');
  document.getElementById('auth-label').textContent = 'Logged in' + (phone ? ' · ' + phone : '');
  document.getElementById('logout-btn').style.display = '';
}
function backToPhone()  { hide(['step-code']); show(['step-phone']); setDot('red'); }
function hide(ids)      { ids.forEach(id => document.getElementById(id).classList.add('hidden')); }
function show(ids)      { ids.forEach(id => document.getElementById(id).classList.remove('hidden')); }
function showErr(id, m) { const e = document.getElementById(id); e.textContent = m; e.classList.remove('hidden'); }
function clearErr(id)   { document.getElementById(id).classList.add('hidden'); }

async function sendPhone() {
  clearErr('phone-err');
  const phone = document.getElementById('phone').value.trim();
  if (!phone) { showErr('phone-err', 'Enter your phone number.'); return; }
  currentPhone = phone;
  const btn = document.getElementById('phone-btn');
  btn.disabled = true; btn.textContent = 'Sending…';
  const res  = await fetch('/auth/send_code', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ phone }),
  });
  const data = await res.json();
  btn.disabled = false; btn.textContent = 'Send Code';
  data.ok ? showCode() : showErr('phone-err', data.error || 'Failed to send code.');
}

async function sendCode() {
  clearErr('code-err');
  const code = document.getElementById('code').value.trim();
  if (!code) { showErr('code-err', 'Enter the code.'); return; }
  const btn = document.getElementById('code-btn');
  btn.disabled = true; btn.textContent = 'Verifying…';
  const res  = await fetch('/auth/verify_code', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ phone: currentPhone, code }),
  });
  const data = await res.json();
  btn.disabled = false; btn.textContent = 'Verify Code';
  if (data.ok)        { showForwarder(currentPhone); }
  else if (data.need_2fa) { show2FA(); }
  else                { showErr('code-err', data.error || 'Invalid code.'); }
}

async function send2FA() {
  clearErr('twofa-err');
  const password = document.getElementById('twofa').value;
  if (!password) { showErr('twofa-err', 'Enter your 2FA password.'); return; }
  const btn = document.getElementById('twofa-btn');
  btn.disabled = true; btn.textContent = 'Submitting…';
  const res  = await fetch('/auth/verify_2fa', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ phone: currentPhone, password }),
  });
  const data = await res.json();
  btn.disabled = false; btn.textContent = 'Submit Password';
  data.ok ? showForwarder(currentPhone) : showErr('twofa-err', data.error || 'Wrong password.');
}

async function logout() {
  await fetch('/auth/logout', { method: 'POST' });
  showPhone();
}

function appendLog(lines) {
  const box = document.getElementById('log-box');
  lines.forEach(l => {
    const div = document.createElement('div');
    div.className = 'log-line ' + (l.level || '');
    div.textContent = l.text;
    box.appendChild(div);
  });
  box.scrollTop = box.scrollHeight;
}

function updateStats(data) {
  document.getElementById('s-forwarded').textContent = data.forwarded;
  document.getElementById('s-skipped').textContent   = data.skipped;
  document.getElementById('s-failed').textContent    = data.failed;
  document.getElementById('s-total').textContent     = data.total;
  const done = data.forwarded + data.skipped + data.failed;
  const pct  = data.total > 0 ? Math.min(100, Math.round(done / data.total * 100)) : 0;
  document.getElementById('progress').style.width = pct + '%';
  if (data.mode) {
    const badge = document.getElementById('mode-badge');
    if (data.mode === 'copy') {
      badge.className = 'badge badge-green';
      badge.textContent = '✓ Mode: Direct copy (forwarding allowed)';
    } else {
      badge.className = 'badge badge-red';
      badge.textContent = '⚠ Mode: Re-upload (forwarding restricted)';
    }
  }
}

async function poll() {
  try {
    const res  = await fetch('/status?offset=' + logOffset);
    const data = await res.json();
    if (data.new_logs && data.new_logs.length) {
      appendLog(data.new_logs);
      logOffset += data.new_logs.length;
    }
    updateStats(data);
    if (data.done) {
      clearInterval(polling);
      const btn = document.getElementById('start-btn');
      btn.disabled = false;
      btn.textContent = '▶ Run Again';
      appendLog([{ text: '── job complete ──', level: 'info' }]);
    }
  } catch(e) { console.error(e); }
}

document.getElementById('job-form').addEventListener('submit', async e => {
  e.preventDefault();
  logOffset = 0;
  document.getElementById('log-box').innerHTML = '';
  const btn = document.getElementById('start-btn');
  btn.disabled = true; btn.textContent = 'Running…';
  await fetch('/start', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({
      source: document.getElementById('source').value.trim(),
      dest:   document.getElementById('dest').value.trim(),
      limit:  parseInt(document.getElementById('limit').value) || 0,
      phone:  currentPhone,
    }),
  });
  polling = setInterval(poll, 1500);
});
</script>
</body>
</html>
"""

# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────

def log_push(text: str, level: str = "") -> None:
    job["log"].append({"text": text, "level": level})


def is_video(message) -> bool:
    if not isinstance(message.media, MessageMediaDocument):
        return False
    for attr in message.media.document.attributes:
        if type(attr).__name__ == "DocumentAttributeVideo":
            return True
    mime = getattr(message.media.document, "mime_type", "") or ""
    return mime.startswith("video/")


async def copy_message_buf(client: TelegramClient, message, dest_entity) -> None:
    buf = io.BytesIO()
    await client.download_media(message, file=buf)
    buf.seek(0)

    doc   = message.media.document
    mime  = getattr(doc, "mime_type", "video/mp4") or "video/mp4"
    fname = None
    for attr in doc.attributes:
        fname = getattr(attr, "file_name", None)
        if fname:
            break
    if not fname:
        ext   = mime.split("/")[-1] if "/" in mime else "mp4"
        fname = f"{message.id}.{ext}"

    uploaded = await client.upload_file(buf, file_name=fname)
    await client.send_file(
        dest_entity,
        uploaded,
        caption=message.text or "",
        attributes=[DocumentAttributeFilename(file_name=fname)],
    )

# ─────────────────────────────────────────
#  AUTH ROUTES — all run on shared _loop
# ─────────────────────────────────────────

@app.route("/auth/status")
def auth_status():
    phone = session.get("phone")
    if phone and phone in auth_sessions:
        return jsonify({"logged_in": True, "phone": phone})
    return jsonify({"logged_in": False})


@app.route("/auth/send_code", methods=["POST"])
def send_code_route():
    data  = request.get_json()
    phone = data.get("phone", "").strip()
    if not phone:
        return jsonify({"ok": False, "error": "Phone required"})

    async def _send():
        # clean up any stale client for this phone
        old = auth_clients.get(phone)
        if old:
            try:
                await old.disconnect()
            except Exception:
                pass

        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        result = await client.send_code_request(phone)
        auth_clients[phone]  = client
        phone_hashes[phone]  = result.phone_code_hash

    try:
        _run(_send())
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/auth/verify_code", methods=["POST"])
def verify_code_route():
    data  = request.get_json()
    phone = data.get("phone", "").strip()
    code  = data.get("code",  "").strip()

    if phone not in auth_clients:
        return jsonify({"ok": False, "error": "Session expired — send code again."})

    async def _verify():
        client     = auth_clients[phone]
        code_hash  = phone_hashes.get(phone)
        await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
        session_str           = client.session.save()
        auth_sessions[phone]  = session_str
        await client.disconnect()
        auth_clients.pop(phone, None)
        phone_hashes.pop(phone, None)

    try:
        _run(_verify())
        session["phone"] = phone
        return jsonify({"ok": True})
    except SessionPasswordNeededError:
        return jsonify({"ok": False, "need_2fa": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/auth/verify_2fa", methods=["POST"])
def verify_2fa_route():
    data     = request.get_json()
    phone    = data.get("phone",    "").strip()
    password = data.get("password", "")

    if phone not in auth_clients:
        return jsonify({"ok": False, "error": "Session expired — send code again."})

    async def _2fa():
        client = auth_clients[phone]
        await client.sign_in(password=password)
        session_str           = client.session.save()
        auth_sessions[phone]  = session_str
        await client.disconnect()
        auth_clients.pop(phone, None)
        phone_hashes.pop(phone, None)

    try:
        _run(_2fa())
        session["phone"] = phone
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/auth/logout", methods=["POST"])
def logout_route():
    phone = session.pop("phone", None)
    if phone:
        async def _disc():
            c = auth_clients.pop(phone, None)
            if c:
                try:
                    await c.disconnect()
                except Exception:
                    pass
        _run(_disc())
        auth_sessions.pop(phone, None)
        phone_hashes.pop(phone, None)
    return jsonify({"ok": True})

# ─────────────────────────────────────────
#  FORWARDER JOB
# ─────────────────────────────────────────

async def forward_job(session_string: str, source_id: str, dest_id: str, limit: int) -> None:
    job.update({
        "running":   True,
        "log":       [],
        "forwarded": 0,
        "failed":    0,
        "skipped":   0,
        "total":     0,
        "done":      False,
        "mode":      None,
    })

    try:
        async with TelegramClient(StringSession(session_string), API_ID, API_HASH) as client:
            log_push("Connected.", "info")

            try:
                source = await client.get_entity(source_id)
            except Exception as e:
                log_push(f"Could not resolve source: {e}", "err")
                return

            try:
                dest = await client.get_entity(dest_id)
            except Exception as e:
                log_push(f"Could not resolve destination: {e}", "err")
                return

            log_push(f"Source : {getattr(source, 'title', source_id)}", "info")
            log_push(f"Dest   : {getattr(dest,   'title', dest_id)}",   "info")

            no_forwards = getattr(source, "noforwards", False)
            if no_forwards:
                job["mode"] = "reupload"
                log_push("Forwarding restricted — re-upload mode.", "warn")
            else:
                job["mode"] = "copy"
                log_push("Forwarding allowed — direct copy mode.", "ok")

            msg_limit = limit if limit > 0 else None
            messages  = [m async for m in client.iter_messages(source, limit=msg_limit)]
            job["total"] = sum(1 for m in messages if m.media and is_video(m))
            log_push(f"Videos found: {job['total']}", "info")

            for message in messages:
                if not message.media or not is_video(message):
                    job["skipped"] += 1
                    continue

                try:
                    if no_forwards:
                        await copy_message_buf(client, message, dest)
                        log_push(f"[{job['forwarded']+1}] Re-uploaded msg {message.id}", "ok")
                    else:
                        await client.forward_messages(dest, message, source)
                        log_push(f"[{job['forwarded']+1}] Forwarded msg {message.id}", "ok")

                    job["forwarded"] += 1
                    await asyncio.sleep(2.0)

                except FloodWaitError as e:
                    log_push(f"FloodWait {e.seconds}s — pausing.", "warn")
                    await asyncio.sleep(e.seconds + 2)
                    try:
                        if no_forwards:
                            await copy_message_buf(client, message, dest)
                        else:
                            await client.forward_messages(dest, message, source)
                        job["forwarded"] += 1
                        log_push(f"Retry ok — msg {message.id}", "ok")
                    except Exception as retry_err:
                        log_push(f"Retry failed msg {message.id}: {retry_err}", "err")
                        job["failed"] += 1

                except Exception as e:
                    log_push(f"Failed msg {message.id}: {e}", "err")
                    job["failed"] += 1

            log_push(
                f"Done — forwarded: {job['forwarded']} | "
                f"failed: {job['failed']} | skipped: {job['skipped']}",
                "info"
            )

    except Exception as e:
        log_push(f"Fatal: {e}", "err")
    finally:
        job["running"] = False
        job["done"]    = True


def run_async_job(session_string: str, source: str, dest: str, limit: int) -> None:
    asyncio.run(forward_job(session_string, source, dest, limit))


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/start", methods=["POST"])
def start():
    if job["running"]:
        return jsonify({"error": "Job already running"}), 409

    data   = request.get_json()
    phone  = data.get("phone",  "").strip()
    source = data.get("source", "").strip()
    dest   = data.get("dest",   "").strip()
    limit  = int(data.get("limit", 0))

    if not source or not dest:
        return jsonify({"error": "Source and destination required"}), 400

    session_string = auth_sessions.get(phone)
    if not session_string:
        return jsonify({"error": "Not authenticated — log in first"}), 401

    t = threading.Thread(
        target=run_async_job,
        args=(session_string, source, dest, limit),
        daemon=True,
    )
    t.start()
    return jsonify({"status": "started"})


@app.route("/status")
def status():
    offset   = int(request.args.get("offset", 0))
    new_logs = job["log"][offset:]
    return jsonify({
        "running":   job["running"],
        "done":      job["done"],
        "forwarded": job["forwarded"],
        "failed":    job["failed"],
        "skipped":   job["skipped"],
        "total":     job["total"],
        "mode":      job.get("mode"),
        "new_logs":  new_logs,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

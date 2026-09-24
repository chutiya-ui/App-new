# --- app.py ---

import asyncio
import io
import os
import threading
import time
from flask import Flask, render_template_string, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument
from telethon.tl.functions.messages import SendMediaRequest
from telethon.tl.types import InputMediaUploadedDocument, DocumentAttributeFilename
from telethon.errors import FloodWaitError

API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

app = Flask(__name__)

# --- job state ---
job = {
    "running":   False,
    "log":       [],
    "forwarded": 0,
    "failed":    0,
    "skipped":   0,
    "total":     0,
    "done":      False,
}

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
  h1 {
    font-size: 1.4rem;
    font-weight: 600;
    color: #58a6ff;
    margin-bottom: 8px;
  }
  .subtitle {
    font-size: 0.85rem;
    color: #8b949e;
    margin-bottom: 28px;
  }
  label {
    display: block;
    font-size: 0.8rem;
    color: #8b949e;
    margin-bottom: 6px;
    font-weight: 500;
    letter-spacing: 0.03em;
  }
  input[type=text], input[type=number] {
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
    padding: 2px 8px;
    border-radius: 20px;
    margin-bottom: 18px;
    font-weight: 600;
  }
  .badge-blue  { background: #1f3a5f; color: #58a6ff; }
  .badge-green { background: #1a3a2a; color: #3fb950; }
  .badge-red   { background: #3a1a1a; color: #f85149; }
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
  button:hover { background: #2ea043; }
  button:disabled { background: #21262d; color: #484f58; cursor: not-allowed; }
  .stats {
    display: flex;
    gap: 12px;
    margin: 24px 0 16px;
  }
  .stat {
    flex: 1;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 12px;
    text-align: center;
  }
  .stat-num {
    font-size: 1.6rem;
    font-weight: 700;
    color: #58a6ff;
  }
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
  .log-line.ok     { color: #3fb950; }
  .log-line.err    { color: #f85149; }
  .log-line.info   { color: #58a6ff; }
  .log-line.warn   { color: #d29922; }
  #mode-badge { margin-top: -10px; }
</style>
</head>
<body>
<div class="card">
  <h1>⚡ TG Channel Forwarder</h1>
  <p class="subtitle">Smart forward detection — copies direct or re-uploads clean. No forward tag.</p>

  <form id="job-form">
    <div class="row">
      <div>
        <label>SOURCE CHANNEL</label>
        <input type="text" id="source" placeholder="@channel or invite link" required>
      </div>
      <div>
        <label>DESTINATION CHANNEL</label>
        <input type="text" id="dest" placeholder="@yourchannel or numeric ID" required>
      </div>
    </div>
    <label>MESSAGE LIMIT (0 = all)</label>
    <input type="number" id="limit" value="0" min="0">

    <div id="mode-badge" class="badge badge-blue">⏳ Detecting channel mode after start</div>
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
      <div class="stat-label">Total Seen</div>
    </div>
  </div>

  <div class="progress-wrap">
    <div class="progress-bar" id="progress" style="width:0%"></div>
  </div>

  <div id="log-box"></div>
</div>

<script>
let polling = null;
let logOffset = 0;

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

  const pct = data.total > 0
    ? Math.min(100, Math.round((data.forwarded + data.skipped + data.failed) / data.total * 100))
    : 0;
  document.getElementById('progress').style.width = pct + '%';

  if (data.mode) {
    const badge = document.getElementById('mode-badge');
    if (data.mode === 'copy') {
      badge.className = 'badge badge-green';
      badge.textContent = '✓ Mode: Direct copy (no restrictions)';
    } else if (data.mode === 'reupload') {
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
      document.getElementById('start-btn').disabled = false;
      document.getElementById('start-btn').textContent = '▶ Run Again';
      appendLog([{ text: '── job complete ──', level: 'info' }]);
    }
  } catch(e) {
    console.error(e);
  }
}

document.getElementById('job-form').addEventListener('submit', async e => {
  e.preventDefault();
  logOffset = 0;
  document.getElementById('log-box').innerHTML = '';
  document.getElementById('start-btn').disabled = true;
  document.getElementById('start-btn').textContent = 'Running…';

  const payload = {
    source: document.getElementById('source').value.trim(),
    dest:   document.getElementById('dest').value.trim(),
    limit:  parseInt(document.getElementById('limit').value) || 0,
  };

  await fetch('/start', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(payload),
  });

  polling = setInterval(poll, 1500);
});
</script>
</body>
</html>
"""


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


async def copy_message(client: TelegramClient, message, dest_entity) -> None:
    """Re-upload from memory buffer — no forward tag, no origin trace."""
    buf = io.BytesIO()
    await client.download_media(message, file=buf)
    buf.seek(0)

    doc  = message.media.document
    mime = getattr(doc, "mime_type", "video/mp4") or "video/mp4"

    fname = None
    for attr in doc.attributes:
        fname = getattr(attr, "file_name", None)
        if fname:
            break
    if not fname:
        ext   = mime.split("/")[-1] if "/" in mime else "mp4"
        fname = f"{message.id}.{ext}"

    uploaded = await client.upload_file(buf, file_name=fname)

    caption = message.text or ""

    await client.send_file(
        dest_entity,
        uploaded,
        caption=caption,
        attributes=[DocumentAttributeFilename(file_name=fname)],
    )


async def forward_job(source_id: str, dest_id: str, limit: int) -> None:
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
        async with TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH) as client:
            log_push("Connected to Telegram.", "info")

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

            source_title = getattr(source, "title", source_id)
            dest_title   = getattr(dest,   "title", dest_id)
            log_push(f"Source : {source_title}", "info")
            log_push(f"Dest   : {dest_title}",   "info")

            # --- forward restriction detection ---
            no_forwards = getattr(source, "noforwards", False)

            if no_forwards:
                job["mode"] = "reupload"
                log_push("Channel restricts forwarding — re-upload mode active.", "warn")
            else:
                job["mode"] = "copy"
                log_push("Channel allows forwarding — direct copy mode active.", "ok")

            msg_limit = limit if limit > 0 else None
            messages  = [m async for m in client.iter_messages(source, limit=msg_limit)]
            job["total"] = sum(1 for m in messages if m.media and is_video(m))
            log_push(f"Videos found: {job['total']}", "info")

            for message in messages:
                if not message.media or not is_video(message):
                    continue

                try:
                    if no_forwards:
                        await copy_message(client, message, dest)
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
                            await copy_message(client, message, dest)
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
        log_push(f"Fatal error: {e}", "err")
    finally:
        job["running"] = False
        job["done"]    = True


def run_async_job(source: str, dest: str, limit: int) -> None:
    asyncio.run(forward_job(source, dest, limit))


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/start", methods=["POST"])
def start():
    if job["running"]:
        return jsonify({"error": "Job already running"}), 409

    data   = request.get_json()
    source = data.get("source", "").strip()
    dest   = data.get("dest",   "").strip()
    limit  = int(data.get("limit", 0))

    if not source or not dest:
        return jsonify({"error": "Source and destination required"}), 400

    t = threading.Thread(target=run_async_job, args=(source, dest, limit), daemon=True)
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

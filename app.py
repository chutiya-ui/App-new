import os, asyncio, threading, hashlib, logging, io, urllib.request, json
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

API_ID         = int(os.environ.get("API_ID", "0"))
API_HASH       = os.environ.get("API_HASH", "")
SECRET_KEY     = os.environ.get("SECRET_KEY", "changeme")
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

DB_PATH          = "/tmp/forwarder.db"
_loop            = asyncio.new_event_loop()
_client          = None
_live_handlers   = {}
_running_jobs    = {}
PARALLEL_WORKERS = int(os.environ.get("PARALLEL_WORKERS", "4"))

# ── Database ───────────────────────────────────────────────
def db():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with db() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS forwarded (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key      TEXT,
                message_id   INTEGER,
                file_hash    TEXT,
                media_type   TEXT,
                forwarded_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(job_key, message_id)
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id          INTEGER PRIMARY KEY,
                session_str TEXT,
                saved_at    TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS duplicates (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                file_hash    TEXT UNIQUE,
                first_msg_id INTEGER,
                source       TEXT,
                detected_at  TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS jobs (
                job_key    TEXT PRIMARY KEY,
                source     TEXT,
                dest       TEXT,
                status     TEXT DEFAULT 'running',
                done       INTEGER DEFAULT 0,
                skipped    INTEGER DEFAULT 0,
                dupes      INTEGER DEFAULT 0,
                errors     INTEGER DEFAULT 0,
                config     TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS job_logs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key    TEXT,
                message    TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)

init_db()

# ── Self ping ──────────────────────────────────────────────
def self_ping():
    app_url = os.environ.get("APP_URL", "")
    if not app_url:
        return
    while True:
        try:
            urllib.request.urlopen(f"{app_url}/ping", timeout=10)
        except Exception as e:
            log.warning(f"Self-ping failed: {e}")
        threading.Event().wait(240)

threading.Thread(target=self_ping, daemon=True).start()

@app.route("/ping")
def ping():
    return jsonify(status="alive", time=str(datetime.now()))

# ── Session helpers ────────────────────────────────────────
def save_session_to_db(ss: str):
    try:
        with db() as c:
            c.execute("DELETE FROM sessions")
            c.execute("INSERT INTO sessions (session_str) VALUES (?)", (ss,))
            c.commit()
    except Exception as e:
        log.warning(f"save_session_to_db failed: {e}")

def load_session_from_db() -> str:
    try:
        with db() as c:
            row = c.execute(
                "SELECT session_str FROM sessions ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return row["session_str"] if row else ""
    except Exception:
        return ""

def get_best_session() -> str:
    return SESSION_STRING or load_session_from_db()

# ── Instant routes — no Telethon, no async ────────────────
@app.route("/ping")
def ping():
    return jsonify(status="alive", time=str(datetime.now()))

@app.route("/session_exists")
def session_exists():
    if not API_ID or API_ID == 0 or not API_HASH:
        return jsonify(exists=False, error="API_ID or API_HASH missing in Railway variables")
    ss = get_best_session()
    return jsonify(exists=bool(ss))

# ── Job DB helpers ─────────────────────────────────────────
def job_key_for(source: str, dest: str) -> str:
    return hashlib.md5(f"{source}|{dest}".encode()).hexdigest()[:12]

def db_create_or_resume_job(job_key, source, dest, config: dict):
    with db() as c:
        existing = c.execute(
            "SELECT job_key FROM jobs WHERE job_key=?", (job_key,)
        ).fetchone()
        if existing:
            c.execute(
                "UPDATE jobs SET status='running', updated_at=? WHERE job_key=?",
                (datetime.now().isoformat(), job_key)
            )
        else:
            c.execute(
                "INSERT INTO jobs (job_key,source,dest,status,config) VALUES (?,?,?,?,?)",
                (job_key, source, dest, "running", json.dumps(config))
            )
        c.commit()

def db_update_job(job_key, **kwargs):
    kwargs["updated_at"] = datetime.now().isoformat()
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [job_key]
    with db() as c:
        c.execute(f"UPDATE jobs SET {sets} WHERE job_key=?", vals)
        c.commit()

def db_get_job(job_key):
    with db() as c:
        row = c.execute(
            "SELECT * FROM jobs WHERE job_key=?", (job_key,)
        ).fetchone()
    return dict(row) if row else None

def db_all_jobs():
    with db() as c:
        rows = c.execute(
            "SELECT * FROM jobs ORDER BY updated_at DESC LIMIT 20"
        ).fetchall()
    return [dict(r) for r in rows]

def db_log(job_key, message):
    ts = datetime.now().strftime('%H:%M:%S')
    entry = f"[{ts}] {message}"
    with db() as c:
        c.execute(
            "INSERT INTO job_logs (job_key, message) VALUES (?,?)",
            (job_key, entry)
        )
        c.commit()
    log.info(message)

def db_get_logs(job_key, last_n=100):
    with db() as c:
        rows = c.execute(
            "SELECT message FROM job_logs WHERE job_key=? ORDER BY id DESC LIMIT ?",
            (job_key, last_n)
        ).fetchall()
    return [r["message"] for r in reversed(rows)]

def already_forwarded(job_key: str, msg_id: int) -> bool:
    with db() as c:
        row = c.execute(
            "SELECT id FROM forwarded WHERE job_key=? AND message_id=?",
            (job_key, msg_id)
        ).fetchone()
    return row is not None

def record_forwarded(job_key, msg_id, file_hash, media_type):
    with db() as c:
        c.execute(
            "INSERT OR IGNORE INTO forwarded "
            "(job_key, message_id, file_hash, media_type) VALUES (?,?,?,?)",
            (job_key, msg_id, file_hash or "", media_type)
        )
        c.commit()

def hash_already_seen(file_hash: str) -> bool:
    with db() as c:
        row = c.execute(
            "SELECT id FROM duplicates WHERE file_hash=?", (file_hash,)
        ).fetchone()
    return row is not None

def record_hash(file_hash: str, msg_id: int, source: str):
    with db() as c:
        c.execute(
            "INSERT OR IGNORE INTO duplicates (file_hash,first_msg_id,source) VALUES (?,?,?)",
            (file_hash, msg_id, source)
        )
        c.commit()

# ── Telethon loop ──────────────────────────────────────────
def run_in_loop(coro, timeout=30):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)

def start_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

threading.Thread(target=start_loop, daemon=True).start()

async def get_client() -> TelegramClient:
    global _client
    if _client and _client.is_connected():
        if await _client.is_user_authorized():
            ss = _client.session.save()
            if ss:
                save_session_to_db(ss)
            return _client
        await _client.disconnect()
        _client = None

    best = get_best_session()
    if best:
        _client = TelegramClient(
            StringSession(best), API_ID, API_HASH, **CLIENT_KWARGS
        )
        await _client.connect()
        if await _client.is_user_authorized():
            ss = _client.session.save()
            save_session_to_db(ss)
            log.info("Session restored")
            return _client
        await _client.disconnect()
        _client = None

    _client = TelegramClient(StringSession(), API_ID, API_HASH, **CLIENT_KWARGS)
    await _client.connect()
    return _client

async def resolve_entity(client, identifier):
    identifier = str(identifier).strip()
    if (identifier.startswith("@") or
            identifier.startswith("http") or
            identifier.startswith("+")):
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
        for attr in (msg.document.attributes or []):
            if isinstance(attr, DocumentAttributeVideo):
                return "video"
        return "document"
    return "other"

# ── Re-upload with full metadata ───────────────────────────
async def reupload_with_metadata(client, msg, dst, caption, raw=None):
    if raw is None:
        raw = await client.download_media(msg, file=bytes)
    if not raw:
        raise Exception("Empty download")

    orig_attrs = []
    thumb_buf  = None
    mtype      = _get_media_type(msg)

    if msg.document:
        orig_attrs = list(msg.document.attributes or [])
        if msg.document.thumbs:
            for thumb_idx in range(len(msg.document.thumbs) - 1, -1, -1):
                try:
                    thumb_bytes = await client.download_media(
                        msg.document, file=bytes, thumb=thumb_idx
                    )
                    if thumb_bytes:
                        thumb_buf = io.BytesIO(thumb_bytes)
                        thumb_buf.name = "thumb.jpg"
                        break
                except Exception:
                    continue

    buf = io.BytesIO(raw)
    if mtype == "video":
        buf.name = "video.mp4"
    elif mtype == "photo":
        buf.name = "photo.jpg"
    else:
        buf.name = "file"
        for attr in orig_attrs:
            if isinstance(attr, DocumentAttributeFilename):
                buf.name = attr.file_name
                break

    return await client.send_file(
        dst,
        file=buf,
        caption=caption,
        attributes=orig_attrs if orig_attrs else None,
        thumb=thumb_buf,
        supports_streaming=(mtype == "video"),
        force_document=False
    )

# ── Core: process one message ──────────────────────────────
def compute_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

async def process_message(client, msg, src, dst, job_key,
                           media_types, skip_dupes, counters, lock):
    mtype = _get_media_type(msg) if msg.media else "text"

    if mtype not in media_types:
        async with lock:
            counters["skipped"] += 1
        return

    raw       = None
    file_hash = None

    try:
        caption = msg.text or ""

        if msg.media:
            forwarded_directly = False
            try:
                await client.forward_messages(dst, msg.id, src)
                forwarded_directly = True
                db_log(job_key, f"✅ Forwarded {msg.id} ({mtype})")
            except Exception:
                pass

            if not forwarded_directly:
                raw = await client.download_media(msg, file=bytes)
                if raw and skip_dupes and mtype in ["video", "photo", "document"]:
                    file_hash = compute_hash(raw)
                    if hash_already_seen(file_hash):
                        record_forwarded(job_key, msg.id, file_hash, mtype)
                        async with lock:
                            counters["dupes"] += 1
                        db_log(job_key, f"⚠️ Duplicate skipped {msg.id}")
                        return
                    else:
                        record_hash(file_hash, msg.id, str(src.id))
                await reupload_with_metadata(client, msg, dst, caption, raw=raw)
                db_log(job_key, f"✅ Re-uploaded {msg.id} ({mtype})")
        else:
            if "text" in media_types and msg.text:
                await client.send_message(dst, msg.text)
                db_log(job_key, f"✅ Text {msg.id}")

        record_forwarded(job_key, msg.id, file_hash, mtype)
        async with lock:
            counters["done"] += 1

    except Exception as ex:
        async with lock:
            counters["errors"] += 1
        db_log(job_key, f"❌ Error {msg.id}: {ex}")

# ── Forward job ────────────────────────────────────────────
async def forward_job_async(job_key: str, cfg: dict):
    try:
        c           = await get_client()
        src         = await resolve_entity(c, cfg["source"])
        dst         = await resolve_entity(c, cfg["dest"])
        limit       = int(cfg.get("limit", 100))
        media_types = cfg.get("media_types", ["video", "photo", "document", "text"])
        skip_dupes  = cfg.get("skip_duplicates", True)
        pin_first   = cfg.get("pin_first", True)
        pin_every   = int(cfg.get("pin_every", 200))
        workers     = int(cfg.get("workers", PARALLEL_WORKERS))

        job      = db_get_job(job_key)
        counters = {
            "done":    job["done"],
            "skipped": job["skipped"],
            "dupes":   job["dupes"],
            "errors":  job["errors"]
        }
        lock         = asyncio.Lock()
        pin_counter  = 0
        first_pinned = False

        action = "Resuming" if counters["done"] > 0 else "Starting"
        db_log(job_key,
               f"{action} | {cfg['source']} → {cfg['dest']} | "
               f"workers={workers} | types={media_types}")

        pending = []
        async for msg in c.iter_messages(src, limit=limit):
            job = db_get_job(job_key)
            if job["status"] == "cancelled":
                db_log(job_key, "Cancelled before processing")
                return
            if not already_forwarded(job_key, msg.id):
                pending.append(msg)

        db_log(job_key, f"📋 {len(pending)} messages to process")

        for i in range(0, len(pending), workers):
            job = db_get_job(job_key)
            if job["status"] == "cancelled":
                db_log(job_key, "Cancelled mid-batch")
                break

            batch = pending[i: i + workers]
            await asyncio.gather(*[
                process_message(
                    c, msg, src, dst, job_key,
                    media_types, skip_dupes, counters, lock
                )
                for msg in batch
            ])

            db_update_job(
                job_key,
                done=counters["done"],
                skipped=counters["skipped"],
                dupes=counters["dupes"],
                errors=counters["errors"]
            )

            if pin_first and not first_pinned and counters["done"] > 0:
                try:
                    async for last_msg in c.iter_messages(dst, limit=1):
                        await c.pin_message(dst, last_msg.id, notify=False)
                        first_pinned = True
                        db_log(job_key, "📌 Pinned first message")
                        break
                except Exception:
                    pass

            pin_counter += len(batch)
            if pin_every > 0 and pin_counter % pin_every < workers:
                try:
                    async for last_msg in c.iter_messages(dst, limit=1):
                        await c.pin_message(dst, last_msg.id, notify=False)
                        db_log(job_key, f"📌 Auto-pinned at {pin_counter}")
                        break
                except Exception:
                    pass

            db_log(job_key,
                   f"📦 Batch {i // workers + 1} done — "
                   f"✅{counters['done']} ⚠️{counters['dupes']} ❌{counters['errors']}")

            await asyncio.sleep(0.5)

        job = db_get_job(job_key)
        if job["status"] != "cancelled":
            db_update_job(job_key, status="done")

        ss = c.session.save()
        if ss:
            save_session_to_db(ss)

        db_log(job_key,
               f"── Complete ── Done:{counters['done']} "
               f"Skipped:{counters['skipped']} "
               f"Dupes:{counters['dupes']} "
               f"Errors:{counters['errors']}")

    except Exception as e:
        db_update_job(job_key, status="error")
        db_log(job_key, f"FATAL: {e}")
        log.error(e)
    finally:
        _running_jobs.pop(job_key, None)

# ── Auth routes ────────────────────────────────────────────
@app.route("/send_code", methods=["POST"])
def send_code_route():
    phone = request.json.get("phone")
    log.info(f"send_code called for {phone}")

    async def _send():
        c = await get_client()
        r = await c.send_code_request(phone)
        return r.phone_code_hash

    try:
        h = run_in_loop(_send(), timeout=30)
        session["phone"] = phone
        session["hash"]  = h
        return jsonify(ok=True)
    except Exception as e:
        log.error(f"send_code error: {e}")
        return jsonify(ok=False, error=str(e))

@app.route("/sign_in", methods=["POST"])
def sign_in_route():
    code = request.json.get("code")
    pw   = request.json.get("password", "")

    async def _sign():
        c = await get_client()
        try:
            await c.sign_in(
                session["phone"], code,
                phone_code_hash=session["hash"]
            )
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
        ok = run_in_loop(_check(), timeout=15)
        if ok:
            session["auth"] = True
        return jsonify(authed=ok)
    except Exception as e:
        log.error(f"check_auth error: {e}")
        return jsonify(authed=False, error=str(e))

# ── Dialogs ────────────────────────────────────────────────
@app.route("/dialogs")
def get_dialogs():
    async def _get():
        c = await get_client()
        if not await c.is_user_authorized():
            raise Exception("Not authorized")
        dialogs = await c.get_dialogs(limit=200)
        result  = []
        for d in dialogs:
            if not d.name:
                continue
            username   = getattr(d.entity, 'username', None)
            identifier = f"@{username}" if username else str(d.id)
            result.append({
                "id":   identifier,
                "name": d.name,
                "type": type(d.entity).__name__
            })
        return result

    try:
        return jsonify(dialogs=run_in_loop(_get(), timeout=30))
    except Exception as e:
        log.error(f"Dialogs error: {e}")
        return jsonify(error=str(e)), 401

# ── Job routes ─────────────────────────────────────────────
@app.route("/start_job", methods=["POST"])
def start_job():
    data    = request.json
    source  = data.get("source")
    dest    = data.get("dest")
    job_key = job_key_for(source, dest)

    if job_key in _running_jobs:
        return jsonify(ok=True, job_key=job_key, resumed=True)

    db_create_or_resume_job(job_key, source, dest, data)

    def _run():
        _running_jobs[job_key] = True
        asyncio.run_coroutine_threadsafe(
            forward_job_async(job_key, data), _loop
        )

    threading.Thread(target=_run, daemon=True).start()
    return jsonify(ok=True, job_key=job_key)

@app.route("/start_multi_job", methods=["POST"])
def start_multi_job():
    data    = request.json
    sources = data.get("sources", [])
    dest    = data.get("dest", "")
    if not sources or not dest:
        return jsonify(ok=False, error="sources and dest required")

    launched = []
    for source in sources:
        cfg     = {**data, "source": source}
        job_key = job_key_for(source, dest)
        if job_key in _running_jobs:
            launched.append({"job_key": job_key, "source": source, "resumed": True})
            continue
        db_create_or_resume_job(job_key, source, dest, cfg)

        def _run(jk=job_key, c=cfg):
            _running_jobs[jk] = True
            asyncio.run_coroutine_threadsafe(
                forward_job_async(jk, c), _loop
            )

        threading.Thread(target=_run, daemon=True).start()
        launched.append({"job_key": job_key, "source": source, "resumed": False})

    return jsonify(ok=True, jobs=launched)

@app.route("/job_status/<job_key>")
def job_status(job_key):
    j = db_get_job(job_key)
    if not j:
        return jsonify(error="Job not found"), 404
    j["logs"]       = db_get_logs(job_key, 80)
    j["is_running"] = job_key in _running_jobs
    return jsonify(j)

@app.route("/all_jobs")
def all_jobs():
    jobs = db_all_jobs()
    for j in jobs:
        j["is_running"] = j["job_key"] in _running_jobs
    return jsonify(jobs=jobs)

@app.route("/cancel_job/<job_key>", methods=["POST"])
def cancel_job(job_key):
    db_update_job(job_key, status="cancelled")
    _running_jobs.pop(job_key, None)
    return jsonify(ok=True)

# ── Live sync ──────────────────────────────────────────────
@app.route("/start_live", methods=["POST"])
def start_live():
    data        = request.json
    source      = data.get("source")
    dest        = data.get("dest")
    media_types = data.get("media_types", ["video", "photo", "document", "text"])

    async def _start():
        c      = await get_client()
        src    = await resolve_entity(c, source)
        dst    = await resolve_entity(c, dest)
        src_id = src.id

        if src_id in _live_handlers:
            c.remove_event_handler(_live_handlers[src_id])
            del _live_handlers[src_id]

        @c.on(events.NewMessage(chats=src))
        async def handler(event):
            msg   = event.message
            mtype = _get_media_type(msg) if msg.media else "text"
            if mtype not in media_types:
                return
            try:
                if msg.media:
                    try:
                        await c.forward_messages(dst, msg.id, src)
                    except Exception:
                        await reupload_with_metadata(c, msg, dst, msg.text or "")
                elif "text" in media_types and msg.text:
                    await c.send_message(dst, msg.text)
            except Exception as ex:
                log.error(f"LIVE error {msg.id}: {ex}")

        _live_handlers[src_id] = handler
        return src_id

    try:
        src_id = run_in_loop(_start())
        return jsonify(ok=True, src_id=str(src_id))
    except Exception as e:
        return jsonify(ok=False, error=str(e))

@app.route("/stop_live", methods=["POST"])
def stop_live():
    src_id = int(request.json.get("src_id", 0))

    async def _stop():
        c = await get_client()
        if src_id in _live_handlers:
            c.remove_event_handler(_live_handlers[src_id])
            del _live_handlers[src_id]
            return True
        return False

    try:
        return jsonify(ok=run_in_loop(_stop()))
    except Exception as e:
        return jsonify(ok=False, error=str(e))

@app.route("/live_status")
def live_status():
    return jsonify(active=[str(k) for k in _live_handlers.keys()])

# ── Scan duplicates ────────────────────────────────────────
@app.route("/scan_duplicates", methods=["POST"])
def scan_duplicates():
    data        = request.json
    source      = data.get("source")
    media_types = data.get("media_types", ["video", "photo", "document"])
    limit       = int(data.get("limit", 100))

    async def _scan():
        c      = await get_client()
        entity = await resolve_entity(c, source)
        seen   = {}
        dupes  = []
        async for msg in c.iter_messages(entity, limit=limit):
            if not msg.media:
                continue
            mtype = _get_media_type(msg)
            if mtype not in media_types:
                continue
            try:
                raw = await client.download_media(msg, file=bytes)
                if not raw:
                    continue
                fh = compute_hash(raw)
                if fh in seen:
                    dupes.append({
                        "msg_id":       msg.id,
                        "duplicate_of": seen[fh],
                        "type":         mtype,
                        "size":         len(raw),
                        "date":         str(msg.date)
                    })
                else:
                    seen[fh] = msg.id
            except Exception as ex:
                log.warning(f"Scan failed {msg.id}: {ex}")
        return dupes

    try:
        dupes = run_in_loop(_scan(), timeout=120)
        return jsonify(ok=True, duplicates=dupes, count=len(dupes))
    except Exception as e:
        return jsonify(ok=False, error=str(e))

# ── UI ─────────────────────────────────────────────────────
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
.container { max-width: 960px; margin: 0 auto; padding: 20px; }
h1 { text-align: center; color: #7c83fd; margin-bottom: 24px; font-size: 1.8rem; }
.card { background: #1a1a2e; border-radius: 12px; padding: 20px; margin-bottom: 20px; border: 1px solid #2a2a4a; }
.card h2 { color: #7c83fd; margin-bottom: 16px; font-size: 1.1rem; }
input, select { width: 100%; padding: 10px 14px; background: #0f0f1a; border: 1px solid #3a3a5a; border-radius: 8px; color: #e0e0e0; margin-bottom: 12px; font-size: 0.95rem; }
button { padding: 10px 20px; border: none; border-radius: 8px; cursor: pointer; font-size: 0.9rem; font-weight: 600; transition: 0.2s; }
.btn-primary  { background: #7c83fd; color: #fff; }
.btn-primary:hover { background: #5c63dd; }
.btn-danger   { background: #e74c3c; color: #fff; }
.btn-warn     { background: #e67e22; color: #fff; }
.btn-live     { background: #c0392b; color: #fff; }
.btn-green    { background: #27ae60; color: #fff; }
.btn-full     { width: 100%; margin-top: 6px; }
.btn-sm       { padding: 6px 14px; font-size: 0.83rem; }
.btn-group    { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
.status { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; }
.status.running   { background: #27ae60; color: #fff; }
.status.done      { background: #2980b9; color: #fff; }
.status.error     { background: #e74c3c; color: #fff; }
.status.cancelled { background: #7f8c8d; color: #fff; }
.log-box { background: #0a0a14; border-radius: 8px; padding: 12px; font-family: monospace; font-size: 0.8rem; max-height: 280px; overflow-y: auto; color: #a0f0a0; border: 1px solid #2a2a4a; white-space: pre-wrap; }
.checkbox-group { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 14px; }
.checkbox-group label { display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 0.9rem; }
.checkbox-group input[type=checkbox] { width: auto; margin: 0; }
.stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 14px; }
.stat { background: #0f0f1a; border-radius: 8px; padding: 10px; text-align: center; border: 1px solid #2a2a4a; }
.stat .num { font-size: 1.4rem; font-weight: 700; color: #7c83fd; }
.stat .lbl { font-size: 0.75rem; color: #888; }
.dupe-list  { max-height: 150px; overflow-y: auto; font-size: 0.82rem; }
.dupe-item  { padding: 6px; border-bottom: 1px solid #2a2a4a; display: flex; justify-content: space-between; }
.hidden     { display: none; }
.badge      { display: inline-block; padding: 4px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: 600; }
.badge.green { background: #27ae60; color: #fff; }
.badge.red   { background: #e74c3c; color: #fff; }
.job-row    { padding: 10px; border-bottom: 1px solid #2a2a4a; display: flex; justify-content: space-between; align-items: center; cursor: pointer; border-radius: 8px; }
.job-row:hover { background: #0f0f1a; }
.source-tag { display: inline-block; background: #2a2a4a; border-radius: 6px; padding: 3px 8px; font-size: 0.82rem; margin: 2px; }
.multi-source-list { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 10px; min-height: 36px; border: 1px dashed #3a3a5a; border-radius: 8px; padding: 6px; }
select[multiple] { height: 140px; }
</style>
</head>
<body>
<div class="container">
  <h1>🚀 TG Forwarder Pro</h1>

  <div class="card" id="auth-card">
    <h2>🔐 Authentication <span id="session-badge" class="badge red">Checking...</span></h2>
    <div id="auth-section" class="hidden">
      <input type="tel" id="phone" placeholder="Phone number (+91...)" />
      <button class="btn-primary btn-full" onclick="sendCode()">Send Code</button>
      <div id="code-section" class="hidden">
        <input type="text"     id="code"  placeholder="Enter OTP code" />
        <input type="password" id="twofa" placeholder="2FA Password (if enabled)" />
        <button class="btn-primary btn-full" onclick="signIn()">Verify & Login</button>
      </div>
    </div>
    <div id="auth-info" class="hidden"
         style="color:#27ae60;font-size:0.9rem;margin-top:8px;"></div>
  </div>

  <div class="card hidden" id="forward-card">
    <h2>⚙️ Forward Configuration</h2>
    <label style="font-size:0.85rem;color:#aaa;display:block;margin-bottom:4px;">
      Source Channel(s) <small style="color:#666;">(hold Ctrl/Cmd for multiple)</small>
    </label>
    <select id="source-select" multiple><option value="">Loading...</option></select>
    <div class="multi-source-list" id="selected-sources-display">
      <span style="color:#555;font-size:0.82rem;">Selected sources appear here</span>
    </div>
    <label style="font-size:0.85rem;color:#aaa;display:block;margin-bottom:4px;">Destination Channel</label>
    <select id="dest-select"><option value="">Loading...</option></select>
    <label style="font-size:0.85rem;color:#aaa;display:block;margin-bottom:4px;">Message Limit (per source)</label>
    <input type="number" id="limit" value="100" min="1" max="10000" />
    <label style="font-size:0.85rem;color:#aaa;display:block;margin-bottom:4px;">Parallel Workers (1–8)</label>
    <input type="number" id="workers" value="4" min="1" max="8" />
    <label style="font-size:0.85rem;color:#aaa;display:block;margin-bottom:8px;">Content Types:</label>
    <div class="checkbox-group">
      <label><input type="checkbox" id="type-video"    checked> 🎥 Videos</label>
      <label><input type="checkbox" id="type-photo"    checked> 🖼️ Photos</label>
      <label><input type="checkbox" id="type-document" checked> 📄 Documents</label>
      <label><input type="checkbox" id="type-text">             💬 Text</label>
    </div>
    <div class="checkbox-group">
      <label><input type="checkbox" id="skip-dupes" checked> 🔍 Skip duplicates</label>
      <label><input type="checkbox" id="pin-first"  checked> 📌 Pin first message</label>
    </div>
    <input type="number" id="pin-every" value="200" min="1" placeholder="Pin every N messages" />
    <small style="color:#888;display:block;margin-bottom:14px;">Auto-pin every N forwarded messages</small>
    <div class="btn-group">
      <button class="btn-warn btn-sm"    onclick="scanDuplicates()">🔍 Scan Dupes</button>
      <button class="btn-primary btn-sm" onclick="startJob()">▶️ Start</button>
      <button class="btn-green btn-sm"   onclick="startMultiJob()">⚡ Multi-Source</button>
      <button class="btn-live btn-sm"    onclick="showLive()">🔴 Live Sync</button>
    </div>
  </div>

  <div class="card hidden" id="active-job-banner" style="border-color:#27ae60;">
    <h2>⚡ Active Job <span id="banner-status" class="status running">running</span></h2>
    <div style="font-size:0.85rem;color:#aaa;margin-bottom:10px;" id="banner-info"></div>
    <div class="stats">
      <div class="stat"><div class="num" id="stat-done">0</div><div class="lbl">Forwarded</div></div>
      <div class="stat"><div class="num" id="stat-skipped">0</div><div class="lbl">Skipped</div></div>
      <div class="stat"><div class="num" id="stat-dupes">0</div><div class="lbl">Dupes</div></div>
      <div class="stat"><div class="num" id="stat-errors">0</div><div class="lbl">Errors</div></div>
    </div>
    <div class="log-box" id="log-box">Loading logs...</div>
    <div class="btn-group" style="margin-top:10px;">
      <button class="btn-danger btn-sm" onclick="cancelJob()">⏹ Cancel Job</button>
    </div>
  </div>

  <div class="card hidden" id="multi-monitor-card">
    <h2>⚡ Multi-Source Jobs</h2>
    <div id="multi-job-list"></div>
  </div>

  <div class="card hidden" id="dupe-card">
    <h2>⚠️ Duplicate Report</h2>
    <div id="dupe-summary" style="margin-bottom:10px;font-size:0.9rem;"></div>
    <div class="dupe-list" id="dupe-list"></div>
    <button class="btn-primary btn-full" style="margin-top:12px;" onclick="startJob()">
      ▶️ Continue (Skip Duplicates)
    </button>
  </div>

  <div class="card hidden" id="history-card">
    <h2>📋 Job History <small style="color:#888;font-size:0.8rem;">(click to monitor)</small></h2>
    <div id="history-list"></div>
  </div>

  <div class="card hidden" id="live-card">
    <h2>🔴 Live Sync Mode</h2>
    <p style="font-size:0.85rem;color:#aaa;margin-bottom:14px;">
      Forwards every new message instantly as it arrives.
    </p>
    <div id="live-status-box" style="margin-bottom:12px;font-size:0.9rem;color:#27ae60;"></div>
    <button class="btn-live btn-full" style="margin-bottom:8px;" onclick="startLive()">🔴 Start Live Sync</button>
    <button class="btn-full" style="background:#7f8c8d;color:#fff;" onclick="stopLive()">⏹ Stop Live Sync</button>
  </div>
</div>

<script>
let currentJobKey     = null;
let pollInterval      = null;
let multiPollInterval = null;
let currentLiveSrcId  = null;
let activeMultiJobs   = [];

async function api(url, method = 'GET', body = null) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(url, opts);
  return r.json();
}

async function checkAuth() {
  const badge = document.getElementById('session-badge');
  badge.textContent = 'Checking...';
  badge.className   = 'badge red';

  try {
    // Step 1: instant check — no Telethon involved
    const exists = await api('/session_exists');

    if (!exists.exists) {
      badge.textContent = '❌ Not logged in';
      badge.className   = 'badge red';
      document.getElementById('auth-section').classList.remove('hidden');
      if (exists.error) {
        document.getElementById('auth-info').textContent = '⚠️ ' + exists.error;
        document.getElementById('auth-info').classList.remove('hidden');
      }
      return;
    }

    // Step 2: connect to Telegram — race with 14s timeout
    badge.textContent = 'Connecting...';
    const result = await Promise.race([
      api('/check_auth'),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('timeout')), 14000)
      )
    ]);

    if (result.authed) {
      badge.textContent = '✅ Session Active';
      badge.className   = 'badge green';
      document.getElementById('auth-section').classList.add('hidden');
      document.getElementById('auth-info').textContent = '✅ Session restored automatically.';
      document.getElementById('auth-info').classList.remove('hidden');
      document.getElementById('forward-card').classList.remove('hidden');
      loadDialogs();
      loadHistory();
    } else {
      badge.textContent = '❌ Session expired';
      badge.className   = 'badge red';
      document.getElementById('auth-section').classList.remove('hidden');
    }

  } catch (e) {
    badge.textContent = '⚠️ Tap to retry';
    badge.className   = 'badge red';
    badge.style.cursor = 'pointer';
    badge.onclick = () => checkAuth();
    document.getElementById('auth-section').classList.remove('hidden');
    console.warn('checkAuth failed:', e.message);
  }
}

async function sendCode() {
  const phone = document.getElementById('phone').value.trim();
  if (!phone) return alert('Enter phone number');
  const r = await api('/send_code', 'POST', { phone });
  if (r.ok) {
    document.getElementById('code-section').classList.remove('hidden');
    alert('OTP sent to Telegram');
  } else {
    alert('Error: ' + r.error);
  }
}

async function signIn() {
  const code     = document.getElementById('code').value.trim();
  const password = document.getElementById('twofa').value.trim();
  const r = await api('/sign_in', 'POST', { code, password });
  if (r.ok) {
    document.getElementById('session-badge').textContent = '✅ Session Active';
    document.getElementById('session-badge').className   = 'badge green';
    document.getElementById('auth-section').classList.add('hidden');
    document.getElementById('auth-info').textContent =
      '✅ Logged in! Copy session string from browser console (F12) and save to Railway as SESSION_STRING.';
    document.getElementById('auth-info').classList.remove('hidden');
    document.getElementById('forward-card').classList.remove('hidden');
    console.log('SESSION_STRING:', r.session_string);
    loadDialogs();
    loadHistory();
  } else {
    alert('Login failed: ' + r.error);
  }
}

async function loadDialogs() {
  const r = await api('/dialogs');
  if (r.error) {
    if (r.error.includes('authorized') || r.error.includes('401')) {
      document.getElementById('auth-section').classList.remove('hidden');
      document.getElementById('forward-card').classList.add('hidden');
    } else {
      alert('Could not load dialogs: ' + r.error);
    }
    return;
  }
  const srcSel = document.getElementById('source-select');
  const dstSel = document.getElementById('dest-select');
  srcSel.innerHTML = '';
  dstSel.innerHTML = '<option value="">-- Select destination --</option>';
  r.dialogs.forEach(d => {
    srcSel.add(new Option(`${d.name} (${d.type})`, d.id));
    dstSel.add(new Option(`${d.name} (${d.type})`, d.id));
  });
  srcSel.addEventListener('change', updateSelectedDisplay);
}

function updateSelectedDisplay() {
  const sel      = document.getElementById('source-select');
  const display  = document.getElementById('selected-sources-display');
  const selected = Array.from(sel.selectedOptions);
  if (selected.length === 0) {
    display.innerHTML = '<span style="color:#555;font-size:0.82rem;">Selected sources appear here</span>';
    return;
  }
  display.innerHTML = selected.map(o =>
    `<span class="source-tag">${o.text}</span>`
  ).join('');
}

async function loadHistory() {
  const r = await api('/all_jobs');
  if (!r.jobs || r.jobs.length === 0) return;
  const running = r.jobs.find(j => j.status === 'running');
  if (running && !currentJobKey) {
    currentJobKey = running.job_key;
    showJobBanner(running);
    startPolling();
  }
  const card = document.getElementById('history-card');
  const list = document.getElementById('history-list');
  card.classList.remove('hidden');
  list.innerHTML = r.jobs.map(j => `
    <div class="job-row" onclick="resumeMonitor('${j.job_key}')">
      <div>
        <div style="font-size:0.88rem;font-weight:600;">${j.source} → ${j.dest}</div>
        <div style="font-size:0.76rem;color:#888;">${j.updated_at}</div>
      </div>
      <div style="text-align:right;">
        <span class="status ${j.status}">${j.status}</span>
        <div style="font-size:0.76rem;color:#aaa;margin-top:4px;">
          ✅${j.done} ⏭${j.skipped} 🔁${j.dupes} ❌${j.errors}
        </div>
      </div>
    </div>
  `).join('');
}

function showJobBanner(j) {
  document.getElementById('active-job-banner').classList.remove('hidden');
  document.getElementById('banner-info').textContent = `${j.source} → ${j.dest}`;
}

function resumeMonitor(job_key) {
  currentJobKey = job_key;
  document.getElementById('active-job-banner').classList.remove('hidden');
  startPolling();
}

function getMediaTypes() {
  const t = [];
  if (document.getElementById('type-video').checked)    t.push('video');
  if (document.getElementById('type-photo').checked)    t.push('photo');
  if (document.getElementById('type-document').checked) t.push('document');
  if (document.getElementById('type-text').checked)     t.push('text');
  return t;
}

function getJobConfig() {
  return {
    dest:            document.getElementById('dest-select').value,
    limit:           document.getElementById('limit').value,
    workers:         document.getElementById('workers').value,
    media_types:     getMediaTypes(),
    skip_duplicates: document.getElementById('skip-dupes').checked,
    pin_first:       document.getElementById('pin-first').checked,
    pin_every:       document.getElementById('pin-every').value
  };
}

async function scanDuplicates() {
  const sel    = document.getElementById('source-select');
  const source = sel.selectedOptions[0]?.value;
  const limit  = document.getElementById('limit').value;
  const types  = getMediaTypes().filter(t => t !== 'text');
  if (!source) return alert('Select a source channel');
  const r = await api('/scan_duplicates', 'POST', { source, limit, media_types: types });
  const card    = document.getElementById('dupe-card');
  const summary = document.getElementById('dupe-summary');
  const list    = document.getElementById('dupe-list');
  card.classList.remove('hidden');
  if (r.ok) {
    summary.textContent = `Found ${r.count} duplicate file(s).`;
    list.innerHTML = r.duplicates.map(d =>
      `<div class="dupe-item">
        <span>Msg #${d.msg_id} (${d.type})</span>
        <span>Dup of #${d.duplicate_of} | ${(d.size/1024/1024).toFixed(1)} MB</span>
      </div>`
    ).join('') || '<div style="color:#888;padding:8px;">No duplicates ✅</div>';
  } else {
    summary.textContent = 'Scan failed: ' + r.error;
  }
}

async function startJob() {
  const sel    = document.getElementById('source-select');
  const source = sel.selectedOptions[0]?.value;
  const cfg    = getJobConfig();
  if (!source || !cfg.dest) return alert('Select source and destination');
  if (cfg.media_types.length === 0) return alert('Select at least one content type');
  const r = await api('/start_job', 'POST', { ...cfg, source });
  if (r.ok) {
    currentJobKey = r.job_key;
    showJobBanner({ source, dest: cfg.dest });
    document.getElementById('dupe-card').classList.add('hidden');
    startPolling();
    loadHistory();
  } else {
    alert('Failed to start job');
  }
}

async function startMultiJob() {
  const sel     = document.getElementById('source-select');
  const sources = Array.from(sel.selectedOptions).map(o => o.value);
  const cfg     = getJobConfig();
  if (sources.length === 0) return alert('Select at least one source channel');
  if (!cfg.dest) return alert('Select a destination channel');
  if (sources.length === 1) return startJob();
  const r = await api('/start_multi_job', 'POST', { ...cfg, sources });
  if (r.ok) {
    activeMultiJobs = r.jobs.map(j => j.job_key);
    document.getElementById('multi-monitor-card').classList.remove('hidden');
    startMultiPolling();
    loadHistory();
    alert(`⚡ Started ${r.jobs.length} parallel jobs!`);
  } else {
    alert('Failed: ' + r.error);
  }
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(async () => {
    if (!currentJobKey) return;
    const j = await api(`/job_status/${currentJobKey}`);
    if (j.error) return;
    document.getElementById('stat-done').textContent    = j.done    || 0;
    document.getElementById('stat-skipped').textContent = j.skipped || 0;
    document.getElementById('stat-dupes').textContent   = j.dupes   || 0;
    document.getElementById('stat-errors').textContent  = j.errors  || 0;
    const badge = document.getElementById('banner-status');
    badge.textContent = j.status;
    badge.className   = `status ${j.status}`;
    const lb = document.getElementById('log-box');
    lb.innerHTML = (j.logs || []).join('\n');
    lb.scrollTop = lb.scrollHeight;
    if (['done', 'error', 'cancelled'].includes(j.status)) {
      clearInterval(pollInterval);
      loadHistory();
    }
  }, 2000);
}

function startMultiPolling() {
  if (multiPollInterval) clearInterval(multiPollInterval);
  multiPollInterval = setInterval(async () => {
    if (activeMultiJobs.length === 0) { clearInterval(multiPollInterval); return; }
    const statuses = await Promise.all(activeMultiJobs.map(k => api(`/job_status/${k}`)));
    const container = document.getElementById('multi-job-list');
    container.innerHTML = statuses.map((j, i) => `
      <div style="padding:10px;border-bottom:1px solid #2a2a4a;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <span style="font-size:0.88rem;font-weight:600;">${j.source || activeMultiJobs[i]}</span>
          <span class="status ${j.status}">${j.status}</span>
        </div>
        <div style="font-size:0.78rem;color:#aaa;margin-top:4px;">
          ✅ ${j.done||0} forwarded &nbsp; 🔁 ${j.dupes||0} dupes &nbsp; ❌ ${j.errors||0} errors
        </div>
        <div style="margin-top:6px;">
          <button class="btn-danger btn-sm" onclick="cancelSpecific('${activeMultiJobs[i]}')">⏹ Cancel</button>
        </div>
      </div>
    `).join('');
    const allDone = statuses.every(j => ['done','error','cancelled'].includes(j.status));
    if (allDone) { clearInterval(multiPollInterval); loadHistory(); }
  }, 2000);
}

async function cancelSpecific(job_key) {
  await api(`/cancel_job/${job_key}`, 'POST');
}

async function cancelJob() {
  if (!currentJobKey) return;
  if (!confirm('Cancel this job?')) return;
  await api(`/cancel_job/${currentJobKey}`, 'POST');
  document.getElementById('banner-status').textContent = 'cancelled';
  document.getElementById('banner-status').className   = 'status cancelled';
  clearInterval(pollInterval);
  loadHistory();
}

function showLive() {
  document.getElementById('live-card').classList.remove('hidden');
}

async function startLive() {
  const sel    = document.getElementById('source-select');
  const source = sel.selectedOptions[0]?.value;
  const dest   = document.getElementById('dest-select').value;
  const types  = getMediaTypes();
  if (!source || !dest) return alert('Select source and destination first');
  const r = await api('/start_live', 'POST', { source, dest, media_types: types });
  if (r.ok) {
    currentLiveSrcId = r.src_id;
    document.getElementById('live-status-box').innerHTML =
      `✅ Live sync active<br><small>${source} → ${dest}</small>`;
  } else {
    alert('Failed: ' + r.error);
  }
}

async function stopLive() {
  if (!currentLiveSrcId) return alert('No active live sync');
  const r = await api('/stop_live', 'POST', { src_id: currentLiveSrcId });
  if (r.ok) {
    currentLiveSrcId = null;
    document.getElementById('live-status-box').textContent = '⏹ Stopped.';
  }
}

checkAuth();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

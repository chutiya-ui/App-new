"""
TG Forwarder Pro v3
───────────────────
Flask dashboard + Telethon engine. Everything runs on the server's own event
loop, so transfers, live syncs and scans keep going after you close the page.
State lives in SQLite on a Railway volume, so jobs resume after a redeploy.

Railway variables:
  API_ID, API_HASH       from https://my.telegram.org                 (required)
  APP_PASSWORD           password for this dashboard (else set on first visit)
  SECRET_KEY             any long random text                         (recommended)
  SESSION_STRING         optional - or log in from the dashboard
  MAX_CONCURRENT_JOBS    default 3
Attach a Railway volume (any mount path) so data survives redeploys.
Start command (ONE worker - the Telegram engine must live in one process):
  gunicorn app:app --workers 1 --threads 16 --timeout 180 --bind 0.0.0.0:$PORT
"""
import os, io, re, csv, json, time, uuid, hmac, base64, shutil, hashlib, logging, asyncio
import threading, sqlite3, secrets, mimetypes, urllib.request, concurrent.futures
from contextlib import closing
from datetime import datetime, timezone, timedelta
from functools import wraps

from flask import Flask, request, jsonify, session, Response, send_file
from werkzeug.security import generate_password_hash, check_password_hash

from telethon import TelegramClient, events, utils as tgu
from telethon.errors import (FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError,
                             PhoneCodeExpiredError, PasswordHashInvalidError,
                             ChatForwardsRestrictedError)
from telethon.sessions import StringSession
from telethon.tl import types as T, functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tgfwd")
logging.getLogger("telethon").setLevel(logging.WARNING)

# ═════════════════════════════ Config ═════════════════════════════
API_ID       = int(os.environ.get("API_ID", "0") or 0)
API_HASH     = os.environ.get("API_HASH", "")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
VOLUME       = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")
DATA_DIR     = os.environ.get("DATA_DIR") or VOLUME or "/tmp/tgfwd"
PERSISTENT   = bool(os.environ.get("DATA_DIR") or VOLUME)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH      = os.environ.get("DB_PATH") or os.path.join(DATA_DIR, "forwarder.db")
TMP_DIR      = os.path.join(DATA_DIR, "tmp")
AVATAR_DIR   = os.path.join(DATA_DIR, "avatars")
for _d in (TMP_DIR, AVATAR_DIR):
    os.makedirs(_d, exist_ok=True)
DIALOG_LIMIT = int(os.environ.get("DIALOG_LIMIT", "500") or 500)
BOOT_TIME    = time.time()

CLIENT_KWARGS = dict(device_model="Samsung Galaxy S23", system_version="Android 13",
                     app_version="9.6.7", lang_code="en", system_lang_code="en-US")


def _load_secret():
    s = os.environ.get("SECRET_KEY", "")
    if s and s != "changeme":
        return s
    p = os.path.join(DATA_DIR, ".secret_key")
    if os.path.exists(p):
        return open(p).read().strip()
    s = secrets.token_hex(32)
    with open(p, "w") as fh:
        fh.write(s)
    return s


SECRET_KEY = _load_secret()

try:
    from cryptography.fernet import Fernet, InvalidToken
    _fernet = Fernet(base64.urlsafe_b64encode(hashlib.sha256(SECRET_KEY.encode()).digest()))
except Exception:                                   # cryptography not installed
    _fernet, InvalidToken = None, Exception

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                  SESSION_COOKIE_SECURE=bool(os.environ.get("RAILWAY_PUBLIC_DOMAIN")),
                  PERMANENT_SESSION_LIFETIME=timedelta(days=30))

TYPES = ["video", "photo", "document", "audio", "voice", "gif", "sticker", "round", "text"]
SPEEDS = {"safe": (10, 4.0), "normal": (25, 2.0), "fast": (50, 1.0)}
STATE_KEYS = ("cursor", "processed", "done", "skipped", "dupes", "errors", "bytes",
              "first_pinned", "last_pin_at")
DEFAULT_CFG = dict(
    types=["video", "photo", "document"], range="all", latest_n=500, from_id=0, to_id=0,
    date_from="", date_to="",
    skip_dupes=True, check_dest=True,
    pin_first=True, pin_every=200, hide_pin_notice=True,
    mode="copy", caption_mode="keep", caption_template="{caption}", strip_links=False,
    strip_mentions=False, footer="", replace_rules="",
    min_size_mb=0.0, max_size_mb=0.0, min_duration=0, include_words="", exclude_words="",
    extensions="",
    speed="normal", batch_size=0, delay=0.0, silent=True, keep_live=False, notify=True,
)


class NotConnected(Exception):
    pass


class ApiError(Exception):
    pass


# ═════════════════════════════ Small helpers ═════════════════════════════
def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_int(v, d=0):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return d


def safe_float(v, d=0.0):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return d


def as_bool(v):
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)


def short(e):
    return f"{type(e).__name__}: {e}"[:220]


def human(n):
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.0f} {u}" if u in ("B", "KB") else f"{n:.1f} {u}"
        n /= 1024


def fmt_dur(s):
    s = int(s or 0)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    return f"{s // 3600}h {(s % 3600) // 60:02d}m"


def peer_key(e):
    try:
        return tgu.get_peer_id(e)
    except Exception:
        return getattr(e, "id", 0)


def display(e):
    if isinstance(e, T.User) and e.is_self:
        return "Saved Messages"
    try:
        n = tgu.get_display_name(e)
    except Exception:
        n = ""
    return n or getattr(e, "title", None) or str(getattr(e, "id", "?"))


def link_base(e):
    u = getattr(e, "username", None)
    if u:
        return f"https://t.me/{u}/"
    if isinstance(e, T.Channel):
        return f"https://t.me/c/{e.id}/"
    return ""


class _SafeDict(dict):
    def __missing__(self, k):
        return "{" + k + "}"


def _fmt(tpl, vals):
    try:
        return tpl.format_map(_SafeDict(vals))
    except (ValueError, IndexError):
        return tpl


# ═════════════════════════════ Database ═════════════════════════════
def db():
    c = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def q(sql, args=(), one=False):
    with closing(db()) as c:
        rows = c.execute(sql, args).fetchall()
    if one:
        return dict(rows[0]) if rows else None
    return [dict(r) for r in rows]


def x(sql, args=()):
    with closing(db()) as c:
        cur = c.execute(sql, args)
        c.commit()
        return cur.rowcount


def xm(sql, seq):
    with closing(db()) as c:
        c.executemany(sql, seq)
        c.commit()


def init_db():
    with closing(db()) as c:
        c.execute("PRAGMA journal_mode=WAL")
        cols = {r["name"] for r in c.execute("PRAGMA table_info(jobs)")}
        if cols and "kind" not in cols:          # table from v1/v2 -> keep as legacy
            c.execute("ALTER TABLE jobs RENAME TO jobs_legacy")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS jobs (
            job_key TEXT PRIMARY KEY, kind TEXT, source TEXT, source_name TEXT,
            dest TEXT, dest_name TEXT, status TEXT, config TEXT,
            cursor INTEGER DEFAULT 0, start_id INTEGER DEFAULT 0, top_id INTEGER DEFAULT 0,
            processed INTEGER DEFAULT 0, done INTEGER DEFAULT 0, skipped INTEGER DEFAULT 0,
            dupes INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, bytes INTEGER DEFAULT 0,
            first_pinned INTEGER DEFAULT 0, last_pin_at INTEGER DEFAULT 0,
            result TEXT, error TEXT, started_at TEXT, finished_at TEXT,
            created_at TEXT, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS job_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_key TEXT, level TEXT,
            message TEXT, created_at TEXT);
        CREATE INDEX IF NOT EXISTS ix_logs ON job_logs(job_key, id);
        CREATE TABLE IF NOT EXISTS failed (
            job_key TEXT, msg_id INTEGER, error TEXT, at TEXT,
            PRIMARY KEY (job_key, msg_id));
        CREATE TABLE IF NOT EXISTS fingerprints (
            scope TEXT, fp TEXT, msg_id INTEGER, mtype TEXT, size INTEGER, name TEXT,
            PRIMARY KEY (scope, fp));
        CREATE TABLE IF NOT EXISTS index_state (scope TEXT PRIMARY KEY, last_id INTEGER, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS live_syncs (
            id TEXT PRIMARY KEY, source TEXT, source_name TEXT, dest TEXT, dest_name TEXT,
            config TEXT, active INTEGER DEFAULT 1, error TEXT,
            cursor INTEGER DEFAULT 0, processed INTEGER DEFAULT 0, done INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0, dupes INTEGER DEFAULT 0, errors INTEGER DEFAULT 0,
            bytes INTEGER DEFAULT 0, first_pinned INTEGER DEFAULT 0, last_pin_at INTEGER DEFAULT 0,
            last_at TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS scan_groups (
            job_key TEXT, fp TEXT, mtype TEXT, size INTEGER, name TEXT,
            keep_id INTEGER, dup_ids TEXT, PRIMARY KEY (job_key, fp));
        CREATE TABLE IF NOT EXISTS catalog (
            job_key TEXT, msg_id INTEGER, mtype TEXT, size INTEGER, name TEXT,
            duration INTEGER, date TEXT, caption TEXT, PRIMARY KEY (job_key, msg_id));
        """)
        c.commit()


init_db()


def kv_get(k, default=None):
    r = q("SELECT v FROM kv WHERE k=?", (k,), one=True)
    return r["v"] if r else default


def kv_set(k, v):
    if v is None:
        x("DELETE FROM kv WHERE k=?", (k,))
    else:
        x("INSERT OR REPLACE INTO kv (k, v) VALUES (?,?)", (k, str(v)))


def db_log(key, message, level="info"):
    try:
        x("INSERT INTO job_logs (job_key, level, message, created_at) VALUES (?,?,?,?)",
          (key, level, message, now()))
    except Exception as e:
        log.warning(f"log write failed: {e}")
    log.info(f"[{key}] {message}")


def get_job(key):
    return q("SELECT * FROM jobs WHERE job_key=?", (key,), one=True)


def db_update_job(key, **kw):
    kw["updated_at"] = now()
    sets = ", ".join(f"{k}=?" for k in kw)
    x(f"UPDATE jobs SET {sets} WHERE job_key=?", list(kw.values()) + [key])


def get_live(lid):
    return q("SELECT * FROM live_syncs WHERE id=?", (lid,), one=True)


def fp_exists(scope, fp):
    return q("SELECT 1 AS y FROM fingerprints WHERE scope=? AND fp=?", (scope, fp), one=True) is not None


def fp_add(scope, fp, msg_id, mtype, size, name):
    x("INSERT OR IGNORE INTO fingerprints (scope, fp, msg_id, mtype, size, name) VALUES (?,?,?,?,?,?)",
      (scope, fp, msg_id, mtype, size or 0, name or ""))


# ── Telegram session storage (encrypted with SECRET_KEY) ──
def save_session(ss):
    if ss is None:
        kv_set("session", None)
    elif _fernet:
        kv_set("session", "f1:" + _fernet.encrypt(ss.encode()).decode())
    else:
        kv_set("session", "b64:" + base64.b64encode(ss.encode()).decode())


def load_session():
    v = kv_get("session")
    if v:
        try:
            if v.startswith("f1:") and _fernet:
                return _fernet.decrypt(v[3:].encode()).decode()
            if v.startswith("b64:"):
                return base64.b64decode(v[4:]).decode()
        except (InvalidToken, ValueError):
            log.warning("Stored session can't be decrypted (SECRET_KEY changed?)")
    if kv_get("env_session_off") == "1":
        return ""
    return os.environ.get("SESSION_STRING", "").strip()


# ═════════════════════════════ Config normalising ═════════════════════════════
def _parse_date(s, end=False):
    try:
        d = datetime.strptime(str(s).strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return d + timedelta(days=1) - timedelta(seconds=1) if end else d
    except (ValueError, TypeError):
        return None


def norm_cfg(d):
    d = dict(d or {})
    if "types" not in d and "media_types" in d:
        d["types"] = d["media_types"]
    c = dict(DEFAULT_CFG)
    for k in DEFAULT_CFG:
        if k in d and d[k] is not None:
            c[k] = d[k]
    c["types"] = [t for t in (c["types"] or []) if t in TYPES]
    for k in ("latest_n", "from_id", "to_id", "pin_every", "batch_size", "min_duration"):
        c[k] = max(0, safe_int(c[k], DEFAULT_CFG[k]))
    for k in ("min_size_mb", "max_size_mb", "delay"):
        c[k] = max(0.0, safe_float(c[k], DEFAULT_CFG[k]))
    for k in ("skip_dupes", "check_dest", "pin_first", "hide_pin_notice", "strip_links",
              "strip_mentions", "silent", "keep_live", "notify"):
        c[k] = as_bool(c[k])
    for k in ("caption_template", "footer", "replace_rules", "include_words", "exclude_words",
              "extensions", "date_from", "date_to"):
        c[k] = str(c[k] or "")
    if c["range"] not in ("all", "latest", "ids", "dates"):
        c["range"] = "all"
    if c["mode"] not in ("copy", "forward"):
        c["mode"] = "copy"
    if c["caption_mode"] not in ("keep", "remove", "replace"):
        c["caption_mode"] = "keep"
    if c["speed"] not in SPEEDS:
        c["speed"] = "normal"
    return c


def derive(c):
    """Pre-computed filter helpers (never stored)."""
    def words(s):
        return [w.strip().lower() for w in re.split(r"[,\n]", s) if w.strip()]
    rules = []
    for line in c["replace_rules"].splitlines():
        if "=>" in line:
            old, new = line.split("=>", 1)
            if old.strip():
                rules.append((old.strip(), new.strip()))
    dated = c["range"] == "dates"
    return dict(inc=words(c["include_words"]), exc=words(c["exclude_words"]),
                exts=[e.lstrip(".") for e in words(c["extensions"])], rules=rules,
                dfrom=_parse_date(c["date_from"]) if dated else None,
                dto=_parse_date(c["date_to"], end=True) if dated else None)


def speed_of(c):
    bs, delay = SPEEDS[c["speed"]]
    if c["batch_size"]:
        bs = max(1, min(100, c["batch_size"]))
    if c["delay"]:
        delay = c["delay"]
    return bs, delay


# ═════════════════════════════ Message analysis ═════════════════════════════
def classify(msg):
    if isinstance(msg, T.MessageService) or getattr(msg, "action", None):
        return "service"
    media = getattr(msg, "media", None)
    if media is None or isinstance(media, T.MessageMediaWebPage):
        return "text" if (getattr(msg, "message", "") or "").strip() else "empty"
    if isinstance(media, T.MessageMediaPhoto):
        return "photo" if media.photo else "other"
    if isinstance(media, T.MessageMediaDocument) and isinstance(media.document, T.Document):
        vid = aud = None
        anim = stk = False
        for a in media.document.attributes or []:
            if isinstance(a, T.DocumentAttributeSticker):
                stk = True
            elif isinstance(a, T.DocumentAttributeAnimated):
                anim = True
            elif isinstance(a, T.DocumentAttributeVideo):
                vid = a
            elif isinstance(a, T.DocumentAttributeAudio):
                aud = a
        if stk:
            return "sticker"
        if anim:
            return "gif"
        if vid:
            return "round" if getattr(vid, "round_message", False) else "video"
        if aud:
            return "voice" if aud.voice else "audio"
        if (media.document.mime_type or "").startswith("video/"):
            return "video"
        return "document"
    return "other"          # polls, contacts, locations, dice ...


def media_info(msg):
    info = {"size": 0, "name": "", "ext": "", "mime": "", "duration": 0, "w": 0, "h": 0}
    media = getattr(msg, "media", None)
    if isinstance(media, T.MessageMediaPhoto) and isinstance(media.photo, T.Photo):
        best = 0
        for s in media.photo.sizes or []:
            sz = getattr(s, "size", 0) or 0
            if isinstance(s, T.PhotoSizeProgressive):
                sz = max(s.sizes or [0])
            if isinstance(sz, int) and sz >= best:
                best = sz
                info["w"], info["h"] = getattr(s, "w", 0) or 0, getattr(s, "h", 0) or 0
        info.update(size=best, ext=".jpg", mime="image/jpeg")
    elif isinstance(media, T.MessageMediaDocument) and isinstance(media.document, T.Document):
        d = media.document
        info.update(size=d.size or 0, mime=d.mime_type or "")
        for a in d.attributes or []:
            if isinstance(a, T.DocumentAttributeFilename):
                info["name"] = a.file_name or ""
            elif isinstance(a, (T.DocumentAttributeVideo, T.DocumentAttributeAudio)):
                info["duration"] = int(getattr(a, "duration", 0) or 0)
                if isinstance(a, T.DocumentAttributeVideo):
                    info["w"], info["h"] = a.w or 0, a.h or 0
        ext = os.path.splitext(info["name"])[1].lower()
        info["ext"] = ext or (mimetypes.guess_extension(info["mime"]) or "")
    return info


def fingerprint(msg, mtype, info):
    """Content key WITHOUT downloading. A re-posted/forwarded file keeps its exact
    byte size, type and duration; photos keep byte size + resolution."""
    if mtype == "text":
        t = re.sub(r"\s+", " ", msg.message or "").strip().lower()
        return "t:" + hashlib.sha1(t.encode()).hexdigest()[:24] if len(t) >= 30 else None
    media = msg.media
    if mtype == "photo":
        if info["size"]:
            return f"p:{info['w']}x{info['h']}:{info['size']}"
        return f"pid:{getattr(media.photo, 'id', msg.id)}"
    if info["size"]:
        fp = f"d:{info['size']}:{info['mime']}:{info['duration']}"
        return fp + (":" + info["name"] if info["size"] < 65536 else "")
    doc = getattr(media, "document", None)
    return f"did:{doc.id}" if doc else None


def passes(msg, mtype, info, cfg, dv):
    if mtype != "text":
        mb = (info["size"] or 0) / 1048576
        if cfg["min_size_mb"] and mb < cfg["min_size_mb"]:
            return False
        if cfg["max_size_mb"] and mb > cfg["max_size_mb"]:
            return False
        if (cfg["min_duration"] and mtype in ("video", "round", "audio", "voice")
                and info["duration"] < cfg["min_duration"]):
            return False
        if dv["exts"] and mtype == "document" and info["ext"].lstrip(".") not in dv["exts"]:
            return False
    d = getattr(msg, "date", None)
    if d and dv["dfrom"] and d < dv["dfrom"]:
        return False
    if d and dv["dto"] and d > dv["dto"]:
        return False
    if dv["inc"] or dv["exc"]:
        hay = f"{msg.message or ''} {info['name']}".lower()
        if dv["inc"] and not any(w in hay for w in dv["inc"]):
            return False
        if dv["exc"] and any(w in hay for w in dv["exc"]):
            return False
    return True


URL_RE     = re.compile(r"(?:https?://|www\.)\S+|(?<![\w@/])t\.me/\S+", re.I)
MENTION_RE = re.compile(r"(?<![\w@])@[A-Za-z0-9_]{4,}")


def build_caption(msg, mtype, info, cfg, dv, n, src_name):
    orig = msg.message or ""
    text = orig
    vals = dict(caption=orig, filename=info["name"], name=os.path.splitext(info["name"])[0],
                size=human(info["size"]) if info["size"] else "", duration=fmt_dur(info["duration"]),
                date=msg.date.strftime("%Y-%m-%d") if getattr(msg, "date", None) else "",
                n=n, source=src_name, id=msg.id)
    if mtype != "text":
        if cfg["caption_mode"] == "remove":
            text = ""
        elif cfg["caption_mode"] == "replace":
            text = _fmt(cfg["caption_template"], vals)
    if cfg["strip_links"]:
        text = URL_RE.sub("", text)
    if cfg["strip_mentions"]:
        text = MENTION_RE.sub("", text)
    for old, new in dv["rules"]:
        text = text.replace(old, new)
    footer = _fmt(cfg["footer"].strip(), vals) if cfg["footer"].strip() else ""
    if footer:
        text = f"{text.rstrip()}\n\n{footer}" if text.strip() else footer
    if text != orig:
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        if mtype != "text" and len(text) > 1024:
            text = text[:1023] + "…"
    return text, text != orig


# ═════════════════════════════ Event loop + Telegram client ═════════════════════════════
_loop = asyncio.new_event_loop()


def _run_loop():
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


threading.Thread(target=_run_loop, daemon=True, name="tg-loop").start()


def run(coro, timeout=60):
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    try:
        return fut.result(timeout)
    except concurrent.futures.TimeoutError:
        fut.cancel()
        raise ApiError("Telegram took too long to answer — try again")


def spawn(coro):
    return asyncio.run_coroutine_threadsafe(coro, _loop)


_client       = None
_me           = None
_client_lock  = None     # created inside the loop
_ent          = {}       # identifier -> entity cache
_dialogs_warm = False
_pending      = {}       # phone logins in progress
_tasks        = {}       # job_key -> asyncio.Task
_active_fwd   = set()    # forward jobs holding a concurrency slot
_run_stats    = {}       # job_key -> live speed info (memory only)
_lives        = {}       # live id -> runtime (handler, buffer, lock)
_dialog_cache = {"at": 0, "data": None}
_summary_cache = {}


def make_client(ss):
    c = TelegramClient(StringSession(ss), API_ID, API_HASH, flood_sleep_threshold=60, **CLIENT_KWARGS)
    c.parse_mode = None           # captions are sent exactly as written
    return c


async def _lock():
    global _client_lock
    if _client_lock is None:
        _client_lock = asyncio.Lock()
    return _client_lock


async def get_client():
    global _client, _me
    async with await _lock():
        if _client is not None:
            if not _client.is_connected():
                try:
                    await _client.connect()
                except Exception as e:
                    log.warning(f"reconnect failed: {e}")
            if _client.is_connected():
                return _client
        ss = load_session()
        if not ss:
            raise NotConnected("Telegram isn't connected yet — open the Account tab and log in")
        if not API_ID or not API_HASH:
            raise NotConnected("API_ID / API_HASH are missing in your Railway variables")
        c = make_client(ss)
        await c.connect()
        if not await c.is_user_authorized():
            await c.disconnect()
            raise NotConnected("The saved Telegram session has expired — log in again")
        _client = c
        _me = await c.get_me()
        log.info(f"Telegram connected as {display(_me)}")
        return c


async def adopt_client(c, ss):
    """Make a freshly logged-in client the main one, then resume everything."""
    global _client, _me, _dialogs_warm
    old = _client
    _client = c
    _me = await c.get_me()
    save_session(ss)
    kv_set("env_session_off", None)
    _ent.clear()
    _dialogs_warm = False
    _dialog_cache.update(at=0, data=None)
    _summary_cache.clear()
    _lives.clear()
    if old is not None and old is not c:
        try:
            await old.disconnect()
        except Exception:
            pass
    asyncio.ensure_future(resume_all())


async def logout_client(terminate=False):
    global _client, _me
    for key, t in list(_tasks.items()):
        t.cancel()
    c = _client
    _client, _me = None, None
    _lives.clear()
    if c is not None:
        try:
            if terminate:
                await c.log_out()
            else:
                await c.disconnect()
        except Exception:
            pass
    save_session(None)
    kv_set("env_session_off", "1")
    x("UPDATE jobs SET status='paused' WHERE status IN ('running','queued')")


async def warm_dialogs(c):
    global _dialogs_warm
    async for d in c.iter_dialogs(limit=DIALOG_LIMIT):
        _ent[str(d.id)] = d.entity
    _dialogs_warm = True


async def resolve(c, ident):
    ident = str(ident or "").strip()
    if not ident:
        raise ApiError("No chat selected")
    if ident.lower() in ("me", "self", "saved"):
        ident = "me"
    if ident in _ent:
        return _ent[ident]
    target = int(ident) if re.fullmatch(r"-?\d+", ident) else ident
    try:
        e = await c.get_entity(target)
    except (ValueError, TypeError):
        if not _dialogs_warm:
            await warm_dialogs(c)
        if ident in _ent:
            return _ent[ident]
        try:
            e = await c.get_entity(target)
        except Exception:
            raise ApiError(f"Can't open chat {ident} — make sure this account has joined it")
    _ent[ident] = e
    return e


async def notify(text):
    if kv_get("notify", "1") == "0":
        return
    try:
        c = await get_client()
        await c.send_message("me", text)
    except Exception as e:
        log.warning(f"notify failed: {e}")


# ═════════════════════════════ Transfer engine ═════════════════════════════
class Item:
    __slots__ = ("msg", "mtype", "info", "fp", "caption", "changed", "route")

    def __init__(self, msg, mtype, info, fp, caption, changed):
        self.msg, self.mtype, self.info, self.fp = msg, mtype, info, fp
        self.caption, self.changed, self.route = caption, changed, "native"


class Ctx:
    """Everything one transfer (job or live sync) needs."""

    def __init__(self, kind, key, client, src, dst, cfg, state):
        self.kind, self.key, self.c, self.src, self.dst = kind, key, client, src, dst
        self.set_cfg(cfg)
        self.state = {k: int(state.get(k) or 0) for k in STATE_KEYS}
        self.protected = bool(getattr(src, "noforwards", False))
        self.scope = f"chat:{peer_key(dst)}"
        self.src_name, self.dst_name = display(src), display(dst)
        self.pin_broken = False

    def set_cfg(self, cfg):
        self.cfg = norm_cfg(cfg)
        self.dv = derive(self.cfg)

    def log(self, m, level="info"):
        db_log(self.key, m, level)

    def persist(self):
        s = self.state
        if self.kind == "job":
            db_update_job(self.key, **{k: s[k] for k in STATE_KEYS})
        else:
            sets = ", ".join(f"{k}=?" for k in STATE_KEYS)
            x(f"UPDATE live_syncs SET {sets}, last_at=? WHERE id=?",
              [s[k] for k in STATE_KEYS] + [now(), self.key[5:]])

    def fail(self, msg_id, err):
        x("INSERT OR REPLACE INTO failed (job_key, msg_id, error, at) VALUES (?,?,?,?)",
          (self.key, msg_id, err, now()))
        self.log(f"❌ #{msg_id}: {err}", "error")

    async def flood(self, make, tries=4):
        for a in range(tries):
            try:
                return await make()
            except FloodWaitError as e:
                if a == tries - 1:
                    raise
                wait = e.seconds + 2
                _run_stats.setdefault(self.key, {})["flood_until"] = time.time() + wait
                self.log(f"⏳ Telegram asked us to slow down — waiting {fmt_dur(wait)}", "warn")
                await asyncio.sleep(wait)


async def _safe(coro):
    try:
        r = await coro
        return r, (None if r is not None else "nothing was sent")
    except Exception as e:
        return None, short(e)


async def forward_one(ctx, it):
    res = await ctx.flood(lambda: ctx.c.forward_messages(
        ctx.dst, [it.msg.id], ctx.src, drop_author=(ctx.cfg["mode"] == "copy"),
        drop_media_captions=(it.route == "native_drop"), silent=ctx.cfg["silent"]))
    return res[0] if isinstance(res, list) else res


async def send_custom_one(ctx, it):
    c, cfg = ctx.c, ctx.cfg
    ents = None if it.changed else it.msg.entities
    if it.mtype == "text":
        return await ctx.flood(lambda: c.send_message(
            ctx.dst, it.caption, formatting_entities=ents, silent=cfg["silent"],
            link_preview=isinstance(it.msg.media, T.MessageMediaWebPage)))
    try:
        return await ctx.flood(lambda: c.send_file(
            ctx.dst, it.msg.media, caption=it.caption, formatting_entities=ents,
            silent=cfg["silent"], supports_streaming=(it.mtype == "video")))
    except ChatForwardsRestrictedError:
        return await reupload(ctx, it)


async def reupload(ctx, it):
    """Protected source: download to the volume (not RAM), upload, delete."""
    c, cfg = ctx.c, ctx.cfg
    ents = None if it.changed else it.msg.entities
    if it.mtype == "text":
        return await ctx.flood(lambda: c.send_message(ctx.dst, it.caption, formatting_entities=ents,
                                                      silent=cfg["silent"]))
    size = it.info["size"] or 0
    free = shutil.disk_usage(TMP_DIR).free
    if size and size * 1.1 + 100 * 1048576 > free:
        raise Exception(f"not enough server disk ({human(size)} needed, {human(free)} free)")
    path = None
    try:
        target = os.path.join(TMP_DIR, uuid.uuid4().hex + (it.info["ext"] or ""))
        path = await c.download_media(it.msg, file=target)
        if not path:
            raise Exception("download returned nothing")
        doc = getattr(it.msg.media, "document", None)
        attrs, thumb = None, None
        if isinstance(doc, T.Document):
            attrs = list(doc.attributes or [])
            if doc.thumbs:
                try:
                    tb = await c.download_media(it.msg, file=bytes, thumb=-1)
                    if tb:
                        thumb = io.BytesIO(tb)
                        thumb.name = "thumb.jpg"
                except Exception:
                    thumb = None
        return await ctx.flood(lambda: c.send_file(
            ctx.dst, path, caption=it.caption, formatting_entities=ents, attributes=attrs,
            thumb=thumb, silent=cfg["silent"], supports_streaming=(it.mtype == "video"),
            force_document=(it.mtype == "document"), voice_note=(it.mtype == "voice"),
            video_note=(it.mtype == "round")))
    finally:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass


async def send_custom(ctx, its):
    """Changed captions: re-send by file reference (no download). Albums stay albums."""
    out, i = [], 0
    while i < len(its):
        it, g = its[i], its[i].msg.grouped_id
        if g and it.mtype in ("photo", "video"):
            j = i
            while j < len(its) and its[j].msg.grouped_id == g and its[j].mtype in ("photo", "video"):
                j += 1
            grp = its[i:j]
            if len(grp) > 1:
                try:
                    sent = await ctx.flood(lambda: ctx.c.send_file(
                        ctx.dst, [g_.msg.media for g_ in grp], caption=[g_.caption for g_ in grp],
                        silent=ctx.cfg["silent"]))
                    sent = list(sent) if isinstance(sent, list) else [sent]
                    out += [(s, None if s else "not sent") for s in (sent + [None] * len(grp))[:len(grp)]]
                except Exception:
                    for g_ in grp:
                        out.append(await _safe(send_custom_one(ctx, g_)))
                i = j
                continue
        out.append(await _safe(send_custom_one(ctx, it)))
        i += 1
    return out


async def deliver(ctx, items):
    """Send items in order. Consecutive unchanged items go in ONE request
    (fast, and albums stay together)."""
    out, why = [None] * len(items), {}
    segs = []
    for i, it in enumerate(items):
        if segs and segs[-1][0] == it.route and len(segs[-1][1]) < 100:
            segs[-1][1].append(i)
        else:
            segs.append((it.route, [i]))
    for route, idxs in segs:
        grp = [items[i] for i in idxs]
        if route in ("native", "native_drop") and not ctx.protected:
            try:
                res = await ctx.flood(lambda: ctx.c.forward_messages(
                    ctx.dst, [g.msg.id for g in grp], ctx.src,
                    drop_author=(ctx.cfg["mode"] == "copy"),
                    drop_media_captions=(route == "native_drop"), silent=ctx.cfg["silent"]))
                res = res if isinstance(res, list) else [res]
                for i, r in zip(idxs, res):
                    out[i] = r
                    if r is None:
                        why[i] = "message was deleted or is unavailable"
                continue
            except ChatForwardsRestrictedError:
                ctx.protected = True
                ctx.log("🔒 Source blocks forwarding — switching to download & re-upload", "warn")
            except FloodWaitError as e:
                for i in idxs:
                    why[i] = short(e)
                continue
            except Exception as e:
                ctx.log(f"↪️ Batch forward failed ({short(e)}) — retrying one by one", "warn")
                for i in idxs:
                    out[i], why[i] = await _safe(forward_one(ctx, items[i]))
                    if out[i] is None:
                        out[i], why[i] = await _safe(reupload(ctx, items[i]))
                continue
        if route == "custom" and not ctx.protected:
            for i, (r, w) in zip(idxs, await send_custom(ctx, grp)):
                out[i], why[i] = r, w
            continue
        for i in idxs:                                  # protected source
            out[i], why[i] = await _safe(reupload(ctx, items[i]))
    for i, r in enumerate(out):
        if r is None:
            ctx.fail(items[i].msg.id, why.get(i) or "not sent")
    return out


async def pin(ctx, message_id, why):
    if ctx.pin_broken:
        return False
    try:
        svc = await ctx.flood(lambda: ctx.c.pin_message(ctx.dst, message_id, notify=False))
        ctx.log(f"📌 {why}: pinned message {message_id}")
        if ctx.cfg["hide_pin_notice"] and svc is not None and getattr(svc, "id", None):
            try:
                await ctx.c.delete_messages(ctx.dst, svc.id)
            except Exception:
                pass
        return True
    except Exception as e:
        ctx.pin_broken = True
        ctx.log(f"❌ Pin failed ({why}): {short(e)} — make this account an admin with the "
                f"'Pin messages' right in the destination. Pinning is paused for this run.", "error")
        return False


async def count_and_pin(ctx, sent):
    st, cfg = ctx.state, ctx.cfg
    st["done"] += 1
    n = st["done"]
    if cfg["pin_first"] and not st["first_pinned"]:
        if await pin(ctx, sent.id, "First message"):
            st["first_pinned"] = 1
    pe = cfg["pin_every"]
    if pe > 0 and n % pe == 0 and n != st["last_pin_at"]:
        if await pin(ctx, sent.id, f"Every {pe} (#{n})"):
            st["last_pin_at"] = n


def route_for(ctx, it):
    if ctx.protected:
        return "reupload"
    if not it.changed:
        return "native"
    if it.mtype != "text" and it.caption == "" and ctx.cfg["mode"] == "copy":
        return "native_drop"
    return "custom"


async def process_chunk(ctx, msgs, advance=True):
    st, cfg, dv = ctx.state, ctx.cfg, ctx.dv
    items, seen = [], set()
    for m in msgs:
        mtype = classify(m)
        if mtype not in cfg["types"]:               # text / service / unticked types
            st["skipped"] += 1
            continue
        info = media_info(m)
        if not passes(m, mtype, info, cfg, dv):
            st["skipped"] += 1
            continue
        fp = fingerprint(m, mtype, info)
        if cfg["skip_dupes"] and fp and (fp in seen or fp_exists(ctx.scope, fp)):
            st["dupes"] += 1
            continue
        if fp:
            seen.add(fp)
        cap, changed = build_caption(m, mtype, info, cfg, dv, st["done"] + len(items) + 1, ctx.src_name)
        if mtype == "text" and not cap.strip():
            st["skipped"] += 1
            continue
        it = Item(m, mtype, info, fp, cap, changed)
        it.route = route_for(ctx, it)
        items.append(it)
    sent_ok = []
    if items:
        sent = await deliver(ctx, items)
        for it, s in zip(items, sent):
            if s is None:
                st["errors"] += 1
                continue
            sent_ok.append(it.msg.id)
            if it.fp:
                fp_add(ctx.scope, it.fp, getattr(s, "id", 0), it.mtype, it.info["size"], it.info["name"])
            st["bytes"] += it.info["size"] or 0
            await count_and_pin(ctx, s)
    if advance:
        st["processed"] += len(msgs)
        if msgs:
            st["cursor"] = max(st["cursor"], max(m.id for m in msgs))
    ctx.persist()
    return sent_ok


async def index_chat(c, entity, scope, key=None, should_stop=None):
    """Remember fingerprints of what's already in a chat (incremental)."""
    row = q("SELECT last_id FROM index_state WHERE scope=?", (scope,), one=True)
    last, n, batch = (row["last_id"] if row else 0), 0, []

    def flush():
        if batch:
            xm("INSERT OR IGNORE INTO fingerprints (scope, fp, msg_id, mtype, size, name) "
               "VALUES (?,?,?,?,?,?)", batch)
            batch.clear()
        x("INSERT OR REPLACE INTO index_state (scope, last_id, updated_at) VALUES (?,?,?)",
          (scope, last, now()))

    async for m in c.iter_messages(entity, reverse=True, min_id=last, wait_time=0.3):
        last = max(last, m.id)
        mtype = classify(m)
        if mtype in ("service", "empty", "other"):
            continue
        info = media_info(m)
        fp = fingerprint(m, mtype, info)
        if fp:
            batch.append((scope, fp, m.id, mtype, info["size"] or 0, info["name"] or ""))
        n += 1
        if len(batch) >= 500:
            flush()
            if key and n % 5000 < 500:
                db_log(key, f"🔎 Checked {n:,} messages already in the destination…")
            if should_stop and should_stop():
                break
    flush()
    return n


# ═════════════════════════════ Forward jobs ═════════════════════════════
def max_jobs():
    return max(1, min(10, safe_int(kv_get("max_jobs") or os.environ.get("MAX_CONCURRENT_JOBS", 3), 3)))


def job_status(key):
    j = q("SELECT status FROM jobs WHERE job_key=?", (key,), one=True)
    return j["status"] if j else None


def launch(key):
    """Start (or re-attach) the background task for a job. Safe to call twice."""
    t = _tasks.get(key)
    if t and not t.done():
        return

    async def _go():
        j = get_job(key)
        coro = {"forward": run_forward, "scan": run_scan, "retry": run_retry}.get(j["kind"] if j else "")
        if coro:
            _tasks[key] = asyncio.current_task()
            try:
                await coro(key)
            finally:
                _tasks.pop(key, None)
                _active_fwd.discard(key)
                _run_stats.pop(key, None)
    spawn(_go())


async def _wait_slot(key):
    told = False
    while True:
        if job_status(key) not in ("queued", "running"):
            return False
        if key in _active_fwd or len(_active_fwd) < max_jobs():
            _active_fwd.add(key)
            return True
        if not told:
            db_log(key, f"⏸ Waiting — {max_jobs()} transfers are already running")
            told = True
        await asyncio.sleep(3)


async def _msg_at_offset(c, src, n):
    """ID of the n-th newest message (for 'latest N')."""
    last = None
    async for m in c.iter_messages(src, limit=n, wait_time=0.3):
        last = m
    return last.id if last else 0


async def _compute_range(c, src, cfg):
    top_msgs = await c.get_messages(src, limit=1)
    top = top_msgs[0].id if top_msgs else 0
    start = 0
    if cfg["range"] == "latest":
        first = await _msg_at_offset(c, src, max(1, cfg["latest_n"]))
        start = max(0, first - 1)
    elif cfg["range"] == "ids":
        start = max(0, cfg["from_id"] - 1)
        if cfg["to_id"]:
            top = min(top, cfg["to_id"])
    elif cfg["range"] == "dates":
        dv = derive(cfg)
        if dv["dfrom"]:
            before = await c.get_messages(src, limit=1, offset_date=dv["dfrom"])
            start = before[0].id if before else 0
        if dv["dto"]:
            upto = await c.get_messages(src, limit=1, offset_date=dv["dto"] + timedelta(seconds=1))
            top = upto[0].id if upto else 0
    return start, top


async def run_forward(key):
    if not await _wait_slot(key):
        return
    j = get_job(key)
    db_update_job(key, status="running", error=None, started_at=j["started_at"] or now())
    ctx = None
    try:
        c = await get_client()
        src, dst = await resolve(c, j["source"]), await resolve(c, j["dest"])
        cfg = norm_cfg(json.loads(j["config"] or "{}"))
        if not j["top_id"]:
            db_log(key, "🧭 Working out which messages to copy…")
            start, top = await _compute_range(c, src, cfg)
            db_update_job(key, start_id=start, top_id=top, cursor=max(j["cursor"] or 0, start))
            j = get_job(key)
        ctx = Ctx("job", key, c, src, dst, cfg, j)
        cfg_raw = j["config"]
        text_state = "copied" if "text" in cfg["types"] else "skipped"
        db_log(key, f"▶️ {'Resuming' if j['processed'] else 'Starting'}: {ctx.src_name} → {ctx.dst_name} · "
                    f"types {', '.join(t for t in cfg['types'] if t != 'text') or 'none'} · text {text_state} · "
                    f"pin first {'on' if cfg['pin_first'] else 'off'}, every {cfg['pin_every'] or 'off'}")
        if ctx.protected:
            db_log(key, "🔒 Source has forwarding restricted — files will be downloaded and re-uploaded (slower)", "warn")
        if cfg["skip_dupes"] and cfg["check_dest"]:
            db_log(key, "🔎 Checking what's already in the destination so nothing is sent twice…")
            n = await index_chat(c, dst, ctx.scope, key, lambda: job_status(key) not in ("running",))
            db_log(key, f"🔎 Destination checked ({n:,} new messages indexed)")
        top = j["top_id"]
        stats = _run_stats.setdefault(key, {})
        stats.update(t0=time.time(), p0=ctx.state["processed"])
        while True:
            st = job_status(key)
            if st != "running":
                db_log(key, "⏸ Paused" if st == "paused" else "⏹ Stopped")
                break
            jj = get_job(key)
            if jj["config"] != cfg_raw:                    # settings edited while running
                cfg_raw = jj["config"]
                ctx.set_cfg(json.loads(cfg_raw or "{}"))
                db_log(key, "⚙️ New settings applied")
            bs, delay = speed_of(ctx.cfg)
            msgs = await c.get_messages(src, limit=bs, min_id=ctx.state["cursor"], reverse=True)
            msgs = [m for m in msgs if m and m.id <= top]
            if not msgs:
                db_update_job(key, status="done", finished_at=now())
                break
            await process_chunk(ctx, msgs)
            el = time.time() - stats["t0"]
            if el > 0:
                stats["rate"] = (ctx.state["processed"] - stats["p0"]) / el
            await asyncio.sleep(delay)
        if job_status(key) == "done":
            s = ctx.state
            db_log(key, f"✅ Finished — {s['done']:,} sent · {s['skipped']:,} skipped · "
                        f"{s['dupes']:,} duplicates · {s['errors']:,} failed · {human(s['bytes'])}")
            if ctx.cfg["notify"]:
                await notify(f"✅ Transfer finished\n{ctx.src_name} → {ctx.dst_name}\n"
                             f"Sent {s['done']:,} · Duplicates {s['dupes']:,} · Failed {s['errors']:,}")
            if ctx.cfg["keep_live"]:
                lid = create_live(j["source"], ctx.src_name, j["dest"], ctx.dst_name,
                                  ctx.cfg, cursor=top, carry=s)
                db_log(key, "🔴 Live sync started — new posts will keep coming across")
                await attach_live(lid)
    except asyncio.CancelledError:
        raise
    except NotConnected as e:
        db_update_job(key, status="queued", error=str(e))
        db_log(key, f"⚠️ {e} — the job will continue when Telegram is connected", "warn")
    except Exception as e:
        log.exception("job failed")
        db_update_job(key, status="error", error=short(e))
        db_log(key, f"💥 Stopped with an error: {short(e)}", "error")
        if ctx and ctx.cfg["notify"]:
            await notify(f"💥 Transfer stopped: {ctx.src_name} → {ctx.dst_name}\n{short(e)}")


async def run_retry(key):
    """Re-send messages that failed in a forward job."""
    j = get_job(key)
    parent = get_job(j["result"]) if j["result"] else None
    if not parent:
        db_update_job(key, status="error", error="original job not found")
        return
    db_update_job(key, status="running", started_at=now())
    try:
        c = await get_client()
        src, dst = await resolve(c, parent["source"]), await resolve(c, parent["dest"])
        ctx = Ctx("job", parent["job_key"], c, src, dst, json.loads(parent["config"] or "{}"), parent)
        ids = [r["msg_id"] for r in q("SELECT msg_id FROM failed WHERE job_key=? ORDER BY msg_id", (parent["job_key"],))]
        db_log(key, f"🔁 Retrying {len(ids):,} failed messages")
        fixed = 0
        for i in range(0, len(ids), 50):
            if job_status(key) != "running":
                break
            chunk = [m for m in await c.get_messages(src, ids=ids[i:i + 50]) if m]
            before = ctx.state["errors"]
            ok = await process_chunk(ctx, chunk, advance=False)
            ctx.state["errors"] = max(0, before - len(ok))
            ctx.persist()
            if ok:
                xm("DELETE FROM failed WHERE job_key=? AND msg_id=?", [(parent["job_key"], m) for m in ok])
            fixed += len(ok)
            missing = set(ids[i:i + 50]) - {m.id for m in chunk}
            if missing:
                xm("DELETE FROM failed WHERE job_key=? AND msg_id=?", [(parent["job_key"], m) for m in missing])
        db_update_job(key, status="done", finished_at=now(), done=fixed)
        db_log(key, f"✅ Retry finished — {fixed:,} sent")
    except Exception as e:
        db_update_job(key, status="error", error=short(e))
        db_log(key, f"💥 {short(e)}", "error")


def new_job(kind, source, source_name, dest, dest_name, cfg, key=None, result=None):
    key = key or uuid.uuid4().hex[:10]
    x("INSERT INTO jobs (job_key, kind, source, source_name, dest, dest_name, status, config, result, "
      "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
      (key, kind, source, source_name, dest, dest_name, "queued", json.dumps(cfg), result, now(), now()))
    launch(key)
    return key


# ═════════════════════════════ Duplicate scan / chat report ═════════════════════════════
async def run_scan(key):
    j = get_job(key)
    db_update_job(key, status="running", started_at=now(), error=None)
    try:
        c = await get_client()
        ent = await resolve(c, j["source"])
        cfg = json.loads(j["config"] or "{}")
        limit = safe_int(cfg.get("limit"), 0) or None
        types = [t for t in (cfg.get("types") or TYPES) if t in TYPES]
        x("DELETE FROM scan_groups WHERE job_key=?", (key,))
        x("DELETE FROM catalog WHERE job_key=?", (key,))
        groups, by_type, largest = {}, {}, []
        n = media = 0
        cat = []
        first_date = last_date = None
        db_log(key, f"🔎 Scanning {display(ent)}{' (latest ' + format(limit, ',') + ')' if limit else ''}…")
        async for m in c.iter_messages(ent, limit=limit, reverse=not limit, wait_time=0.3):
            n += 1
            mtype = classify(m)
            if mtype in ("service", "empty", "other"):
                continue
            d = m.date.isoformat() if m.date else ""
            first_date = first_date or d
            last_date = d
            info = media_info(m)
            bt = by_type.setdefault(mtype, {"count": 0, "bytes": 0})
            bt["count"] += 1
            bt["bytes"] += info["size"] or 0
            if mtype != "text":
                media += 1
                cat.append((key, m.id, mtype, info["size"] or 0, info["name"], info["duration"], d,
                            (m.message or "")[:200]))
                if len(largest) < 15 or (info["size"] or 0) > largest[-1][0]:
                    largest.append(((info["size"] or 0), m.id, mtype, info["name"]))
                    largest.sort(reverse=True)
                    del largest[15:]
            if mtype in types:
                fp = fingerprint(m, mtype, info)
                if fp:
                    g = groups.setdefault(fp, {"ids": [], "mtype": mtype, "size": info["size"], "name": info["name"]})
                    g["ids"].append(m.id)
            if len(cat) >= 500:
                xm("INSERT OR REPLACE INTO catalog VALUES (?,?,?,?,?,?,?,?)", cat)
                cat = []
            if n % 2000 == 0:
                db_update_job(key, processed=n)
                db_log(key, f"🔎 Scanned {n:,} messages…")
                if job_status(key) != "running":
                    db_log(key, "⏹ Scan stopped")
                    return
        if cat:
            xm("INSERT OR REPLACE INTO catalog VALUES (?,?,?,?,?,?,?,?)", cat)
        rows, dup_msgs, dup_bytes = [], 0, 0
        for fp, g in groups.items():
            if len(g["ids"]) > 1:
                ids = sorted(g["ids"])
                rows.append((key, fp, g["mtype"], g["size"] or 0, g["name"] or "", ids[0], json.dumps(ids[1:])))
                dup_msgs += len(ids) - 1
                dup_bytes += (g["size"] or 0) * (len(ids) - 1)
        if rows:
            xm("INSERT OR REPLACE INTO scan_groups VALUES (?,?,?,?,?,?,?)", rows)
        result = dict(chat=display(ent), scanned=n, media=media, by_type=by_type,
                      largest=[dict(size=s, id=i, type=t, name=nm) for s, i, t, nm in largest],
                      groups=len(rows), dup_messages=dup_msgs, dup_bytes=dup_bytes,
                      first_date=min(first_date or "", last_date or "") or None,
                      last_date=max(first_date or "", last_date or "") or None,
                      link=link_base(ent))
        db_update_job(key, status="done", finished_at=now(), processed=n, dupes=dup_msgs,
                      bytes=sum(v["bytes"] for v in by_type.values()), result=json.dumps(result))
        db_log(key, f"✅ Scan finished — {n:,} messages, {len(rows):,} duplicate groups, "
                    f"{dup_msgs:,} extra copies wasting {human(dup_bytes)}")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("scan failed")
        db_update_job(key, status="error", error=short(e))
        db_log(key, f"💥 Scan failed: {short(e)}", "error")


async def delete_duplicates(key, fps=None):
    j = get_job(key)
    c = await get_client()
    ent = await resolve(c, j["source"])
    rows = q("SELECT * FROM scan_groups WHERE job_key=?", (key,))
    ids = []
    for r in rows:
        if fps is None or r["fp"] in fps:
            ids += json.loads(r["dup_ids"])
    deleted = 0
    for i in range(0, len(ids), 100):
        part = ids[i:i + 100]
        try:
            await c.delete_messages(ent, part)
            deleted += len(part)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 2)
            await c.delete_messages(ent, part)
            deleted += len(part)
        except Exception as e:
            db_log(key, f"❌ Delete failed: {short(e)} — you need the 'Delete messages' right", "error")
            break
        await asyncio.sleep(1)
    if deleted:
        if fps is None:
            x("DELETE FROM scan_groups WHERE job_key=?", (key,))
        else:
            xm("DELETE FROM scan_groups WHERE job_key=? AND fp=?", [(key, f) for f in fps])
    db_log(key, f"🗑 Deleted {deleted:,} duplicate messages (kept the oldest copy of each)")
    return deleted


# ═════════════════════════════ Live sync ═════════════════════════════
def create_live(source, source_name, dest, dest_name, cfg, cursor=0, carry=None):
    lid = uuid.uuid4().hex[:8]
    carry = carry or {}
    x("INSERT INTO live_syncs (id, source, source_name, dest, dest_name, config, active, cursor, "
      "done, first_pinned, last_pin_at, created_at, last_at) VALUES (?,?,?,?,?,?,1,?,?,?,?,?,?)",
      (lid, source, source_name, dest, dest_name, json.dumps(cfg), cursor,
       carry.get("done", 0), carry.get("first_pinned", 0), carry.get("last_pin_at", 0), now(), None))
    return lid


class LiveRT:
    def __init__(self, ctx, src):
        self.ctx, self.src = ctx, src
        self.lock = asyncio.Lock()
        self.buf = {}
        self.timer = None
        self.handler = None


async def attach_live(lid):
    row = get_live(lid)
    if not row or not row["active"] or lid in _lives:
        return
    c = await get_client()
    src, dst = await resolve(c, row["source"]), await resolve(c, row["dest"])
    ctx = Ctx("live", f"live:{lid}", c, src, dst, json.loads(row["config"] or "{}"), row)
    rt = LiveRT(ctx, src)
    if not ctx.state["cursor"]:
        top = await c.get_messages(src, limit=1)
        ctx.state["cursor"] = top[0].id if top else 0
        ctx.persist()

    async def on_new(event):
        rt.buf[event.message.id] = event.message
        if rt.timer:
            rt.timer.cancel()
        rt.timer = asyncio.get_event_loop().call_later(2.0, lambda: asyncio.ensure_future(flush_live(lid)))

    c.add_event_handler(on_new, events.NewMessage(chats=src))
    rt.handler = on_new
    _lives[lid] = rt
    x("UPDATE live_syncs SET error=NULL WHERE id=?", (lid,))
    db_log(ctx.key, f"🔴 Watching {ctx.src_name} → {ctx.dst_name}")
    asyncio.ensure_future(poll_live(lid))       # catch up on anything missed while offline


async def flush_live(lid):
    rt = _lives.get(lid)
    if not rt:
        return
    async with rt.lock:
        msgs = sorted((m for m in rt.buf.values() if m.id > rt.ctx.state["cursor"]), key=lambda m: m.id)
        rt.buf.clear()
        if not msgs:
            return
        row = get_live(lid)
        if row:
            rt.ctx.set_cfg(json.loads(row["config"] or "{}"))
        try:
            await process_chunk(rt.ctx, msgs)
        except Exception as e:
            db_log(rt.ctx.key, f"❌ {short(e)}", "error")


async def poll_live(lid):
    """Safety net: fetch anything newer than the cursor (missed updates, downtime)."""
    rt = _lives.get(lid)
    if not rt:
        return
    try:
        while True:
            msgs = await rt.ctx.c.get_messages(rt.src, limit=50, min_id=rt.ctx.state["cursor"], reverse=True)
            if not msgs:
                return
            for m in msgs:
                rt.buf[m.id] = m
            await flush_live(lid)
            if len(msgs) < 50:
                return
            await asyncio.sleep(2)
    except Exception as e:
        log.warning(f"live poll {lid}: {e}")


async def detach_live(lid):
    rt = _lives.pop(lid, None)
    if rt and rt.handler:
        try:
            rt.ctx.c.remove_event_handler(rt.handler)
        except Exception:
            pass


# ═════════════════════════════ Boot, watchdog, keep-alive ═════════════════════════════
async def resume_all():
    try:
        await get_client()
    except Exception as e:
        log.info(f"Not resuming yet: {e}")
        return
    for j in q("SELECT job_key FROM jobs WHERE status IN ('running','queued')"):
        launch(j["job_key"])
    for r in q("SELECT id FROM live_syncs WHERE active=1"):
        try:
            await attach_live(r["id"])
        except Exception as e:
            x("UPDATE live_syncs SET error=? WHERE id=?", (short(e), r["id"]))


async def watchdog():
    await asyncio.sleep(3)
    x("UPDATE jobs SET status='queued' WHERE status='running'")     # previous process died mid-run
    await resume_all()
    tick = 0
    while True:
        await asyncio.sleep(60)
        tick += 1
        try:
            if not load_session():
                continue
            await get_client()
            for j in q("SELECT job_key FROM jobs WHERE status IN ('running','queued')"):
                launch(j["job_key"])
            for r in q("SELECT id FROM live_syncs WHERE active=1"):
                if r["id"] not in _lives:
                    await attach_live(r["id"])
                elif tick % 5 == 0:
                    asyncio.ensure_future(poll_live(r["id"]))
            if tick % 60 == 0:                                        # hourly housekeeping
                x("DELETE FROM job_logs WHERE id < (SELECT COALESCE(MAX(id),0) - 200000 FROM job_logs)")
                for f in os.listdir(TMP_DIR):
                    p = os.path.join(TMP_DIR, f)
                    if time.time() - os.path.getmtime(p) > 6 * 3600:
                        os.remove(p)
        except Exception as e:
            log.warning(f"watchdog: {e}")


def _self_ping():
    url = os.environ.get("APP_URL", "")
    if not url and os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
        url = "https://" + os.environ["RAILWAY_PUBLIC_DOMAIN"]
    if not url or os.environ.get("SELF_PING", "1") == "0":
        return
    while True:
        time.sleep(240)
        try:
            urllib.request.urlopen(url.rstrip("/") + "/ping", timeout=10)
        except Exception:
            pass


_started = False


def start_background():
    global _started
    if _started or os.environ.get("TGFWD_NO_BOOT"):
        return
    _started = True
    spawn(watchdog())
    threading.Thread(target=_self_ping, daemon=True).start()


start_background()


# ═════════════════════════════ Web: auth ═════════════════════════════
_login_fails = {}


def _pw_hash():
    return kv_get("app_pw")


def needs_setup():
    return not APP_PASSWORD and not _pw_hash()


def check_pw(pw):
    if APP_PASSWORD:
        return hmac.compare_digest(str(pw), APP_PASSWORD)
    h = _pw_hash()
    return bool(h) and check_password_hash(h, str(pw))


def _auth_ver():
    return hashlib.sha256((APP_PASSWORD or _pw_hash() or "").encode()).hexdigest()[:12]


def authed():
    return session.get("ok") == _auth_ver() and not needs_setup()


def api(fn):
    @wraps(fn)
    def w(*a, **k):
        if not authed():
            return jsonify(ok=False, error="Please sign in", auth=True), 401
        if request.method != "GET" and request.headers.get("X-Requested-With") != "tgfwd":
            return jsonify(ok=False, error="Bad request"), 400
        try:
            r = fn(*a, **k)
            return r if isinstance(r, (Response, tuple)) else jsonify(r)
        except NotConnected as e:
            return jsonify(ok=False, error=str(e), tg=False), 409
        except ApiError as e:
            return jsonify(ok=False, error=str(e)), 400
        except FloodWaitError as e:
            return jsonify(ok=False, error=f"Telegram rate limit — try again in {fmt_dur(e.seconds)}"), 429
        except Exception as e:
            log.exception("api error")
            return jsonify(ok=False, error=short(e)), 500
    return w


def body():
    return request.get_json(force=True, silent=True) or {}


@app.after_request
def _headers(r):
    r.headers["X-Frame-Options"] = "DENY"
    r.headers["X-Content-Type-Options"] = "nosniff"
    r.headers["Referrer-Policy"] = "no-referrer"
    return r


@app.route("/ping")
@app.route("/healthz")
def ping():
    return jsonify(status="alive", uptime=int(time.time() - BOOT_TIME))


@app.route("/api/auth/state")
def auth_state():
    return jsonify(ok=True, setup=needs_setup(), authed=authed(), env_pw=bool(APP_PASSWORD))


@app.route("/api/auth/setup", methods=["POST"])
def auth_setup():
    if not needs_setup():
        return jsonify(ok=False, error="A password is already set"), 400
    pw = str(body().get("password", ""))
    if len(pw) < 8:
        return jsonify(ok=False, error="Use at least 8 characters")
    kv_set("app_pw", generate_password_hash(pw))
    session.permanent = True
    session["ok"] = _auth_ver()
    return jsonify(ok=True)


@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "?").split(",")[0].strip()
    fails = [t for t in _login_fails.get(ip, []) if time.time() - t < 900]
    if len(fails) >= 8:
        return jsonify(ok=False, error="Too many attempts — wait 15 minutes"), 429
    if check_pw(body().get("password", "")):
        _login_fails.pop(ip, None)
        session.permanent = True
        session["ok"] = _auth_ver()
        return jsonify(ok=True)
    fails.append(time.time())
    _login_fails[ip] = fails
    return jsonify(ok=False, error="Wrong password"), 401


@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify(ok=True)


@app.route("/api/auth/password", methods=["POST"])
@api
def auth_change():
    d = body()
    if APP_PASSWORD:
        raise ApiError("The password comes from the APP_PASSWORD variable — change it in Railway")
    if not check_pw(d.get("current", "")):
        raise ApiError("Current password is wrong")
    if len(str(d.get("new", ""))) < 8:
        raise ApiError("Use at least 8 characters")
    kv_set("app_pw", generate_password_hash(str(d["new"])))
    session["ok"] = _auth_ver()
    return dict(ok=True)


# ═════════════════════════════ Web: status + Telegram login ═════════════════════════════
def _me_dict():
    if not _me:
        return None
    return dict(name=tgu.get_display_name(_me) or "You", username=_me.username, phone=("+" + _me.phone[:3] + "•••" + _me.phone[-2:]) if _me.phone else None,
                id=_me.id)


@app.route("/api/status")
@api
def status():
    connected, err = False, None
    if load_session():
        try:
            run(get_client(), 25)
            connected = True
        except Exception as e:
            err = str(e)
    du = shutil.disk_usage(DATA_DIR)
    counts = {r["status"]: r["n"] for r in q("SELECT status, COUNT(*) AS n FROM jobs WHERE kind='forward' GROUP BY status")}
    tot = q("SELECT COALESCE(SUM(done),0) AS d, COALESCE(SUM(bytes),0) AS b FROM jobs WHERE kind='forward'", one=True)
    live = q("SELECT COALESCE(SUM(done),0) AS d, COUNT(*) AS n, COALESCE(SUM(active),0) AS a FROM live_syncs", one=True)
    return dict(ok=True, connected=connected, error=err, me=_me_dict(), persistent=PERSISTENT,
                data_dir=DATA_DIR, api_ok=bool(API_ID and API_HASH), uptime=int(time.time() - BOOT_TIME),
                disk=dict(free=du.free, total=du.total), db_size=os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
                jobs=counts, running=len(_active_fwd), max_jobs=max_jobs(),
                total_sent=tot["d"] + live["d"], total_bytes=tot["b"],
                live=dict(total=live["n"], active=live["a"], attached=len(_lives)),
                notify=kv_get("notify", "1") != "0", env_password=bool(APP_PASSWORD))


@app.route("/api/tg/session", methods=["POST"])
@api
def tg_session():
    ss = str(body().get("session_string", "")).strip()
    if not ss:
        raise ApiError("Paste a session string first")
    if not API_ID or not API_HASH:
        raise ApiError("API_ID / API_HASH are missing in your Railway variables")

    async def go():
        c = make_client(ss)
        await c.connect()
        if not await c.is_user_authorized():
            await c.disconnect()
            raise ApiError("That session string is invalid or expired")
        await adopt_client(c, ss)
    run(go(), 40)
    return dict(ok=True, me=_me_dict())


@app.route("/api/tg/send_code", methods=["POST"])
@api
def tg_send_code():
    phone = re.sub(r"[^\d+]", "", str(body().get("phone", "")))
    if len(phone) < 8:
        raise ApiError("Enter your phone number with country code, e.g. +91…")
    if not API_ID or not API_HASH:
        raise ApiError("API_ID / API_HASH are missing in your Railway variables")

    async def go():
        old = _pending.pop("login", None)
        if old:
            try:
                await old["client"].disconnect()
            except Exception:
                pass
        c = make_client("")
        await c.connect()
        sent = await c.send_code_request(phone)
        _pending["login"] = dict(client=c, phone=phone, hash=sent.phone_code_hash, at=time.time())
    run(go(), 40)
    return dict(ok=True)


@app.route("/api/tg/sign_in", methods=["POST"])
@api
def tg_sign_in():
    d = body()
    p = _pending.get("login")
    if not p:
        raise ApiError("Request a new code first")

    async def go():
        c = p["client"]
        try:
            if d.get("password"):
                await c.sign_in(password=str(d["password"]))
            else:
                code = re.sub(r"\D", "", str(d.get("code", "")))
                await c.sign_in(p["phone"], code, phone_code_hash=p["hash"])
        except SessionPasswordNeededError:
            return "password"
        except (PhoneCodeInvalidError,):
            raise ApiError("That code is wrong — check the Telegram app and try again")
        except PhoneCodeExpiredError:
            raise ApiError("That code has expired — request a new one")
        except PasswordHashInvalidError:
            raise ApiError("Wrong two-step verification password")
        _pending.pop("login", None)
        await adopt_client(c, c.session.save())
        return "ok"
    r = run(go(), 40)
    if r == "password":
        return dict(ok=True, need_password=True)
    return dict(ok=True, me=_me_dict())


@app.route("/api/tg/logout", methods=["POST"])
@api
def tg_logout():
    run(logout_client(terminate=as_bool(body().get("terminate"))), 30)
    return dict(ok=True)


@app.route("/api/tg/export", methods=["POST"])
@api
def tg_export():
    if not check_pw(body().get("password", "")):
        raise ApiError("Enter your dashboard password to reveal the session string")
    ss = load_session()
    if not ss:
        raise ApiError("No Telegram session saved")
    return dict(ok=True, session_string=ss)


# ═════════════════════════════ Web: chats, avatars, summary ═════════════════════════════
def _kind(e):
    if isinstance(e, T.User):
        if e.is_self:
            return "saved"
        return "bot" if e.bot else "user"
    if isinstance(e, T.Channel):
        return "channel" if e.broadcast else "group"
    return "group"


def _rights(e):
    if isinstance(e, T.User):
        return True, True, False
    creator = bool(getattr(e, "creator", False))
    ar = getattr(e, "admin_rights", None)
    admin = creator or ar is not None
    if isinstance(e, T.Channel) and e.broadcast:
        can_post = creator or bool(ar and ar.post_messages)
    else:
        br = getattr(e, "banned_rights", None) or getattr(e, "default_banned_rights", None)
        can_post = creator or admin or not (br and br.send_messages)
    dbr = getattr(e, "default_banned_rights", None)
    can_pin = creator or bool(ar and ar.pin_messages) or (_kind(e) == "group" and dbr is not None and not dbr.pin_messages)
    return can_post, can_pin, admin


def dialog_dict(d):
    e = d.entity
    can_post, can_pin, admin = _rights(e)
    return dict(id=str(d.id), name=display(e) if _kind(e) == "saved" else (d.name or display(e)),
                kind=_kind(e), username=getattr(e, "username", None),
                members=getattr(e, "participants_count", None), unread=d.unread_count,
                last=d.date.isoformat() if d.date else None, can_post=can_post, can_pin=can_pin,
                admin=admin, protected=bool(getattr(e, "noforwards", False)),
                archived=d.folder_id == 1, has_photo=bool(getattr(e, "photo", None)) and
                not isinstance(getattr(e, "photo", None), (T.ChatPhotoEmpty, T.UserProfilePhotoEmpty)))


@app.route("/api/dialogs")
@api
def dialogs():
    fresh = request.args.get("refresh") == "1"
    if not fresh and _dialog_cache["data"] and time.time() - _dialog_cache["at"] < 300:
        return dict(ok=True, dialogs=_dialog_cache["data"], cached=True)

    async def go():
        global _dialogs_warm
        c = await get_client()
        out = []
        async for d in c.iter_dialogs(limit=DIALOG_LIMIT):
            _ent[str(d.id)] = d.entity
            out.append(dialog_dict(d))
        _dialogs_warm = True
        if not any(o["kind"] == "saved" for o in out):
            me = await c.get_me()
            _ent[str(me.id)] = me
            out.insert(0, dict(id=str(me.id), name="Saved Messages", kind="saved", username=None, members=None,
                               unread=0, last=None, can_post=True, can_pin=True, admin=False, protected=False,
                               archived=False, has_photo=False))
        return out
    data = run(go(), 90)
    _dialog_cache.update(at=time.time(), data=data)
    return dict(ok=True, dialogs=data)


@app.route("/api/avatar/<cid>")
@api
def avatar(cid):
    if not re.fullmatch(r"-?\d+", cid):
        return Response(status=404)
    path = os.path.join(AVATAR_DIR, cid.replace("-", "m") + ".jpg")
    if not os.path.exists(path) or time.time() - os.path.getmtime(path) > 7 * 86400:
        async def go():
            c = await get_client()
            e = await resolve(c, cid)
            return await c.download_profile_photo(e, file=path, download_big=False)
        try:
            if not run(go(), 20):
                return Response(status=404)
        except Exception:
            return Response(status=404)
    r = send_file(path, mimetype="image/jpeg", max_age=86400)
    return r


FILTERS = [("photo", T.InputMessagesFilterPhotos), ("video", T.InputMessagesFilterVideo),
           ("document", T.InputMessagesFilterDocument), ("audio", T.InputMessagesFilterMusic),
           ("voice", T.InputMessagesFilterVoice), ("gif", T.InputMessagesFilterGif),
           ("round", T.InputMessagesFilterRoundVideo), ("links", T.InputMessagesFilterUrl)]


async def chat_summary(cid):
    c = await get_client()
    e = await resolve(c, cid)
    total = (await c.get_messages(e, limit=0)).total
    counts = {}
    for name, flt in FILTERS:
        try:
            counts[name] = (await c.get_messages(e, limit=0, filter=flt)).total
        except Exception:
            counts[name] = None
    first = await c.get_messages(e, limit=1, reverse=True)
    last = await c.get_messages(e, limit=1)
    can_post, can_pin, admin = _rights(e)
    media = sum(v for k, v in counts.items() if v and k != "links")
    info = dict(id=str(cid), name=display(e), kind=_kind(e), username=getattr(e, "username", None),
                members=getattr(e, "participants_count", None), total=total, counts=counts,
                text_other=max(0, total - media), first_date=first[0].date.isoformat() if first else None,
                last_date=last[0].date.isoformat() if last else None, last_id=last[0].id if last else 0,
                can_post=can_post, can_pin=can_pin, admin=admin, protected=bool(getattr(e, "noforwards", False)),
                link=link_base(e))
    if isinstance(e, T.Channel):
        try:
            full = await c(F.channels.GetFullChannelRequest(e))
            info["about"] = (full.full_chat.about or "")[:300]
            info["members"] = full.full_chat.participants_count or info["members"]
        except Exception:
            pass
    return info


@app.route("/api/summary/<cid>")
@api
def summary(cid):
    hit = _summary_cache.get(cid)
    if hit and time.time() - hit[0] < 300 and request.args.get("refresh") != "1":
        s = dict(hit[1])
    else:
        s = run(chat_summary(cid), 60)
        _summary_cache[cid] = (time.time(), s)
        s = dict(s)
    scope = f"chat:{cid}"
    idx = q("SELECT COUNT(*) AS n FROM fingerprints WHERE scope=?", (scope,), one=True)
    s["indexed"] = idx["n"]
    s["as_source"] = q("SELECT job_key, dest_name, status, done, cursor, top_id, updated_at FROM jobs "
                       "WHERE kind='forward' AND source=? ORDER BY updated_at DESC LIMIT 5", (cid,))
    s["as_dest"] = q("SELECT job_key, source_name, status, done, updated_at FROM jobs "
                     "WHERE kind='forward' AND dest=? ORDER BY updated_at DESC LIMIT 5", (cid,))
    s["live"] = q("SELECT id, dest_name, source_name, active FROM live_syncs WHERE source=? OR dest=?", (cid, cid))
    s["last_scan"] = q("SELECT job_key, result, finished_at FROM jobs WHERE kind='scan' AND source=? AND status='done' "
                       "ORDER BY finished_at DESC LIMIT 1", (cid,), one=True)
    if s["last_scan"] and s["last_scan"]["result"]:
        s["last_scan"]["result"] = json.loads(s["last_scan"]["result"])
    return dict(ok=True, summary=s)


@app.route("/api/estimate", methods=["POST"])
@api
def estimate():
    d = body()
    cfg = norm_cfg(d.get("config"))
    out = []
    for sid in d.get("sources") or []:
        hit = _summary_cache.get(str(sid))
        s = hit[1] if hit else run(chat_summary(str(sid)), 60)
        _summary_cache[str(sid)] = (time.time(), s)
        n = sum((s["counts"].get(t) or 0) for t in cfg["types"] if t in s["counts"])
        if "text" in cfg["types"]:
            n += s["text_other"]
        if cfg["range"] == "latest":
            n = min(n, cfg["latest_n"])
        out.append(dict(id=str(sid), name=s["name"], estimate=n, total=s["total"], protected=s["protected"]))
    return dict(ok=True, sources=out)


# ═════════════════════════════ Web: jobs ═════════════════════════════
def job_view(j, detail=False):
    j = dict(j)
    j["config"] = json.loads(j["config"] or "{}")
    if j.get("result"):
        try:
            j["result"] = json.loads(j["result"])
        except (ValueError, TypeError):
            pass
    span = max(0, (j["top_id"] or 0) - (j["start_id"] or 0))
    if j["kind"] == "forward":
        j["pct"] = 100.0 if j["status"] == "done" else (
            round(min(100.0, max(0.0, ((j["cursor"] or 0) - (j["start_id"] or 0)) / span * 100)), 1) if span else 0.0)
    elif j["kind"] == "scan":
        j["pct"] = 100.0 if j["status"] == "done" else None
    rs = _run_stats.get(j["job_key"], {})
    j["rate"] = round(rs.get("rate", 0), 2)
    left = max(0, (j["top_id"] or 0) - (j["cursor"] or 0))
    j["eta"] = int(left / rs["rate"]) if rs.get("rate") and j["status"] == "running" else None
    fu = rs.get("flood_until", 0)
    j["flood_wait"] = int(fu - time.time()) if fu > time.time() else 0
    j["active"] = j["job_key"] in _tasks
    j["failed_count"] = q("SELECT COUNT(*) AS n FROM failed WHERE job_key=?", (j["job_key"],), one=True)["n"]
    return j


@app.route("/api/jobs")
@api
def jobs_list():
    kind = request.args.get("kind")
    sql = "SELECT * FROM jobs" + (" WHERE kind=?" if kind else "") + " ORDER BY " \
          "CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END, updated_at DESC LIMIT 200"
    rows = q(sql, (kind,) if kind else ())
    return dict(ok=True, jobs=[job_view(r) for r in rows])


@app.route("/api/jobs", methods=["POST"])
@api
def jobs_create():
    d = body()
    sources = [str(s) for s in (d.get("sources") or ([d["source"]] if d.get("source") else []))]
    dest = str(d.get("dest") or "")
    cfg = norm_cfg(d.get("config"))
    if not sources or not dest:
        raise ApiError("Pick at least one source and a destination")
    if dest in sources:
        raise ApiError("The destination can't also be a source")
    if not cfg["types"]:
        raise ApiError("Tick at least one content type")
    kv_set("last_cfg", json.dumps(cfg))

    async def names():
        c = await get_client()
        out = {}
        for s in sources + [dest]:
            out[s] = display(await resolve(c, s))
        return out
    nm = run(names(), 60)
    made = []
    for s in sources:
        old = q("SELECT job_key, status FROM jobs WHERE kind='forward' AND source=? AND dest=? "
                "AND status IN ('running','queued','paused') LIMIT 1", (s, dest), one=True)
        if old:
            db_update_job(old["job_key"], config=json.dumps(cfg),
                          status="queued" if old["status"] == "paused" else old["status"])
            launch(old["job_key"])
            made.append(dict(job_key=old["job_key"], source=nm[s], updated=True))
            continue
        made.append(dict(job_key=new_job("forward", s, nm[s], dest, nm[dest], cfg), source=nm[s], updated=False))
    return dict(ok=True, jobs=made)


@app.route("/api/jobs/<key>")
@api
def job_detail(key):
    j = get_job(key)
    if not j:
        raise ApiError("Job not found")
    after = safe_int(request.args.get("after"), 0)
    if after:
        logs = q("SELECT id, level, message, created_at FROM job_logs WHERE job_key=? AND id>? ORDER BY id LIMIT 500",
                 (key, after))
    else:
        logs = list(reversed(q("SELECT id, level, message, created_at FROM job_logs WHERE job_key=? "
                               "ORDER BY id DESC LIMIT 300", (key,))))
    v = job_view(j, True)
    if j["kind"] == "scan":
        v["groups"] = q("SELECT fp, mtype, size, name, keep_id, dup_ids FROM scan_groups WHERE job_key=? "
                        "ORDER BY size * (LENGTH(dup_ids) - LENGTH(REPLACE(dup_ids, ',', '')) + 1) DESC LIMIT 300", (key,))
        for g in v["groups"]:
            g["dup_ids"] = json.loads(g["dup_ids"])
    return dict(ok=True, job=v, logs=logs)


@app.route("/api/jobs/<key>/<action>", methods=["POST"])
@api
def job_action(key, action):
    j = get_job(key)
    if not j:
        raise ApiError("Job not found")
    st = j["status"]
    if action == "pause":
        if st in ("running", "queued"):
            db_update_job(key, status="paused")
    elif action == "resume":
        if st in ("paused", "error", "stopped"):
            db_update_job(key, status="queued", error=None)
            launch(key)
    elif action == "stop":
        db_update_job(key, status="stopped", finished_at=now())
        t = _tasks.get(key)
        if t and j["kind"] == "scan":
            spawn(_cancel(t))
    elif action == "continue":            # grab anything posted since the job finished
        if j["kind"] != "forward":
            raise ApiError("Only transfers can continue")
        db_update_job(key, status="queued", top_id=0, finished_at=None, error=None)
        db_log(key, "🔄 Checking the source for new posts…")
        launch(key)
    elif action == "retry":
        n = q("SELECT COUNT(*) AS n FROM failed WHERE job_key=?", (key,), one=True)["n"]
        if not n:
            raise ApiError("Nothing failed in this job")
        rk = new_job("retry", j["source"], j["source_name"], j["dest"], j["dest_name"], {}, result=key)
        return dict(ok=True, job_key=rk)
    elif action == "live":
        cfg = norm_cfg(json.loads(j["config"] or "{}"))
        lid = create_live(j["source"], j["source_name"], j["dest"], j["dest_name"], cfg,
                          cursor=j["cursor"] or 0, carry=j)
        spawn(attach_live(lid))
        return dict(ok=True, live_id=lid)
    elif action == "delete":
        if st in ("running", "queued"):
            raise ApiError("Pause or stop the job first")
        for tbl in ("jobs", "job_logs", "failed", "scan_groups", "catalog"):
            x(f"DELETE FROM {tbl} WHERE job_key=?", (key,))
    elif action == "config":
        cfg = norm_cfg({**json.loads(j["config"] or "{}"), **(body().get("config") or {})})
        db_update_job(key, config=json.dumps(cfg))
        db_log(key, "⚙️ Settings updated from the dashboard")
    else:
        raise ApiError("Unknown action")
    return dict(ok=True)


async def _cancel(t):
    t.cancel()


@app.route("/api/jobs/<key>/logs.txt")
@api
def job_logs_txt(key):
    rows = q("SELECT created_at, level, message FROM job_logs WHERE job_key=? ORDER BY id", (key,))
    txt = "\n".join(f"{r['created_at']} [{r['level']}] {r['message']}" for r in rows)
    return Response(txt, mimetype="text/plain",
                    headers={"Content-Disposition": f"attachment; filename=job-{key}-log.txt"})


@app.route("/api/jobs/<key>/failed.csv")
@api
def job_failed_csv(key):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["message_id", "error", "time"])
    for r in q("SELECT msg_id, error, at FROM failed WHERE job_key=? ORDER BY msg_id", (key,)):
        w.writerow([r["msg_id"], r["error"], r["at"]])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=job-{key}-failed.csv"})


# ═════════════════════════════ Web: scans ═════════════════════════════
@app.route("/api/scan", methods=["POST"])
@api
def scan_create():
    d = body()
    cid = str(d.get("chat") or "")
    if not cid:
        raise ApiError("Pick a chat to scan")
    types = [t for t in (d.get("types") or ["video", "photo", "document", "audio"]) if t in TYPES]

    async def nm():
        c = await get_client()
        return display(await resolve(c, cid))
    name = run(nm(), 30)
    running = q("SELECT job_key FROM jobs WHERE kind='scan' AND source=? AND status IN ('running','queued')",
                (cid,), one=True)
    if running:
        return dict(ok=True, job_key=running["job_key"], existing=True)
    key = new_job("scan", cid, name, "", "", dict(types=types, limit=safe_int(d.get("limit"), 0)))
    return dict(ok=True, job_key=key)


@app.route("/api/scan/<key>/delete", methods=["POST"])
@api
def scan_delete(key):
    j = get_job(key)
    if not j or j["kind"] != "scan":
        raise ApiError("Scan not found")
    fps = body().get("fps")
    n = run(delete_duplicates(key, set(fps) if fps else None), 1800)
    return dict(ok=True, deleted=n)


@app.route("/api/scan/<key>/catalog.csv")
@api
def scan_catalog(key):
    j = get_job(key)
    base = ""
    if j and j.get("result"):
        try:
            base = json.loads(j["result"]).get("link") or ""
        except ValueError:
            pass
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["message_id", "type", "size_bytes", "size", "file_name", "duration_s", "date", "caption", "link"])
    for r in q("SELECT * FROM catalog WHERE job_key=? ORDER BY msg_id", (key,)):
        w.writerow([r["msg_id"], r["mtype"], r["size"], human(r["size"]), r["name"], r["duration"], r["date"],
                    r["caption"], (base + str(r["msg_id"])) if base else ""])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=catalog-{key}.csv"})


@app.route("/api/scan/<key>/duplicates.csv")
@api
def scan_dupes_csv(key):
    j = get_job(key)
    base = ""
    if j and j.get("result"):
        try:
            base = json.loads(j["result"]).get("link") or ""
        except ValueError:
            pass
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["type", "size", "file_name", "kept_message", "duplicate_messages", "wasted"])
    for r in q("SELECT * FROM scan_groups WHERE job_key=?", (key,)):
        ids = json.loads(r["dup_ids"])
        w.writerow([r["mtype"], human(r["size"]), r["name"], (base + str(r["keep_id"])) if base else r["keep_id"],
                    " ".join((base + str(i)) if base else str(i) for i in ids), human(r["size"] * len(ids))])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=duplicates-{key}.csv"})


# ═════════════════════════════ Web: live sync ═════════════════════════════
@app.route("/api/live")
@api
def live_list():
    rows = q("SELECT * FROM live_syncs ORDER BY created_at DESC")
    for r in rows:
        r["config"] = json.loads(r["config"] or "{}")
        r["attached"] = r["id"] in _lives
    return dict(ok=True, live=rows)


@app.route("/api/live", methods=["POST"])
@api
def live_create():
    d = body()
    src, dest = str(d.get("source") or ""), str(d.get("dest") or "")
    cfg = norm_cfg(d.get("config"))
    if not src or not dest or src == dest:
        raise ApiError("Pick a source and a different destination")
    if not cfg["types"]:
        raise ApiError("Tick at least one content type")
    if q("SELECT id FROM live_syncs WHERE source=? AND dest=? AND active=1", (src, dest), one=True):
        raise ApiError("A live sync for this pair is already running")

    async def go():
        c = await get_client()
        s, t = await resolve(c, src), await resolve(c, dest)
        lid = create_live(src, display(s), dest, display(t), cfg)
        await attach_live(lid)
        return lid
    return dict(ok=True, id=run(go(), 60))


@app.route("/api/live/<lid>/<action>", methods=["POST"])
@api
def live_action(lid, action):
    if not get_live(lid):
        raise ApiError("Live sync not found")
    if action == "pause":
        x("UPDATE live_syncs SET active=0 WHERE id=?", (lid,))
        run(detach_live(lid), 20)
    elif action == "resume":
        x("UPDATE live_syncs SET active=1 WHERE id=?", (lid,))
        run(attach_live(lid), 60)
    elif action == "delete":
        run(detach_live(lid), 20)
        x("DELETE FROM live_syncs WHERE id=?", (lid,))
        x("DELETE FROM job_logs WHERE job_key=?", (f"live:{lid}",))
        x("DELETE FROM failed WHERE job_key=?", (f"live:{lid}",))
    elif action == "config":
        row = get_live(lid)
        cfg = norm_cfg({**json.loads(row["config"] or "{}"), **(body().get("config") or {})})
        x("UPDATE live_syncs SET config=? WHERE id=?", (json.dumps(cfg), lid))
    else:
        raise ApiError("Unknown action")
    return dict(ok=True)


@app.route("/api/live/<lid>/logs")
@api
def live_logs(lid):
    rows = list(reversed(q("SELECT id, level, message, created_at FROM job_logs WHERE job_key=? "
                           "ORDER BY id DESC LIMIT 200", (f"live:{lid}",))))
    return dict(ok=True, logs=rows)


# ═════════════════════════════ Web: settings ═════════════════════════════
@app.route("/api/settings", methods=["GET", "POST"])
@api
def settings():
    if request.method == "POST":
        d = body()
        if "max_jobs" in d:
            kv_set("max_jobs", max(1, min(10, safe_int(d["max_jobs"], 3))))
        if "notify" in d:
            kv_set("notify", "1" if as_bool(d["notify"]) else "0")
    last = kv_get("last_cfg")
    return dict(ok=True, max_jobs=max_jobs(), notify=kv_get("notify", "1") != "0",
                defaults=DEFAULT_CFG, last_cfg=json.loads(last) if last else None)


@app.route("/api/presets", methods=["GET", "POST"])
@api
def presets():
    data = json.loads(kv_get("presets") or "{}")
    if request.method == "POST":
        d = body()
        name = str(d.get("name") or "").strip()[:40]
        if not name:
            raise ApiError("Give the preset a name")
        if d.get("delete"):
            data.pop(name, None)
        else:
            data[name] = norm_cfg(d.get("config"))
        kv_set("presets", json.dumps(data))
    return dict(ok=True, presets=data)


@app.route("/api/fingerprints/clear", methods=["POST"])
@api
def fp_clear():
    cid = str(body().get("chat") or "")
    if cid:
        x("DELETE FROM fingerprints WHERE scope=?", (f"chat:{cid}",))
        x("DELETE FROM index_state WHERE scope=?", (f"chat:{cid}",))
    else:
        x("DELETE FROM fingerprints")
        x("DELETE FROM index_state")
    return dict(ok=True)


@app.route("/api/backup.db")
@api
def backup():
    path = os.path.join(TMP_DIR, "backup.db")
    with closing(db()) as src, closing(sqlite3.connect(path)) as dst:
        src.backup(dst)
    with closing(sqlite3.connect(path)) as c:
        c.execute("DELETE FROM kv WHERE k IN ('session','app_pw')")      # no secrets in backups
        c.commit()
    return send_file(path, as_attachment=True, download_name=f"tgfwd-backup-{datetime.now():%Y%m%d}.db")


# ═════════════════════════════ UI ═════════════════════════════
@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")



HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay — Telegram transfer console</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,500;12..96,700;12..96,800&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#E8EDF2; --panel:#FFFFFF; --panel2:#F4F7FA; --line:#D5DDE6; --line2:#E6ECF2;
  --ink:#15233B; --ink2:#4A5A72; --ink3:#8594A8;
  --blue:#2474C9; --blue-soft:#DCEAF8; --amber:#E39B2D; --amber-soft:#FBEBD2;
  --green:#1F9D6B; --green-soft:#D8F1E6; --red:#D2453D; --red-soft:#F8DEDC; --violet:#6E5BD6;
  --shadow:0 1px 2px rgba(21,35,59,.06),0 4px 16px rgba(21,35,59,.06);
  --r:14px; --r-sm:9px;
  --f-display:'Bricolage Grotesque',system-ui,sans-serif;
  --f-body:'IBM Plex Sans',system-ui,sans-serif;
  --f-mono:'IBM Plex Mono',ui-monospace,monospace;
}
@media (prefers-color-scheme:dark){:root{
  --bg:#0E1622; --panel:#16212F; --panel2:#1B2839; --line:#2A3A4F; --line2:#223246;
  --ink:#E6EDF6; --ink2:#A7B6CA; --ink3:#6F8199;
  --blue:#5AA2EE; --blue-soft:#1D3450; --amber:#F0B254; --amber-soft:#3A2E1A;
  --green:#3CC28C; --green-soft:#173529; --red:#F06B62; --red-soft:#3D2020; --violet:#9C8CF2;
  --shadow:0 1px 2px rgba(0,0,0,.3),0 6px 18px rgba(0,0,0,.25);
}}
*{box-sizing:border-box;margin:0;padding:0}
html,body{background:var(--bg);color:var(--ink);font:15px/1.5 var(--f-body);-webkit-font-smoothing:antialiased}
button,input,select,textarea{font:inherit;color:inherit}
a{color:var(--blue);text-decoration:none}
a:hover{text-decoration:underline}
:focus-visible{outline:2px solid var(--blue);outline-offset:2px;border-radius:6px}
.mono{font-family:var(--f-mono)}
.muted{color:var(--ink2)} .faint{color:var(--ink3)} .small{font-size:13px} .tiny{font-size:12px}
.hidden{display:none!important}
svg.i{width:18px;height:18px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round;flex:none}
svg.i.sm{width:15px;height:15px}

/* ── shell ── */
.top{position:sticky;top:0;z-index:30;background:color-mix(in srgb,var(--bg) 88%,transparent);backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}
.top-in{max-width:1180px;margin:0 auto;padding:12px 20px;display:flex;align-items:center;gap:18px}
.brand{display:flex;align-items:center;gap:10px;font:800 21px/1 var(--f-display);letter-spacing:-.02em}
.brand-mark{width:30px;height:30px;border-radius:9px;background:var(--ink);display:grid;place-items:center;color:var(--bg)}
.brand small{font:500 12px var(--f-mono);color:var(--ink3);letter-spacing:0}
.nav{display:flex;gap:2px;overflow-x:auto;scrollbar-width:none;flex:1}
.nav::-webkit-scrollbar{display:none}
.nav button{display:flex;align-items:center;gap:7px;border:0;background:none;padding:8px 12px;border-radius:9px;color:var(--ink2);font-weight:500;cursor:pointer;white-space:nowrap}
.nav button:hover{background:var(--panel2);color:var(--ink)}
.nav button.on{background:var(--panel);color:var(--ink);box-shadow:var(--shadow)}
.nav .count{font:500 11px var(--f-mono);background:var(--blue);color:#fff;border-radius:10px;padding:0 6px;line-height:17px}
.conn{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--ink2);white-space:nowrap;cursor:pointer;border:1px solid var(--line);background:var(--panel);padding:6px 11px;border-radius:20px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--ink3);flex:none}
.dot.ok{background:var(--green);box-shadow:0 0 0 3px var(--green-soft)}
.dot.bad{background:var(--red);box-shadow:0 0 0 3px var(--red-soft)}
.dot.live{background:var(--red);animation:pulse 1.6s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 color-mix(in srgb,var(--red) 55%,transparent)}100%{box-shadow:0 0 0 8px transparent}}
main{max-width:1180px;margin:0 auto;padding:24px 20px 120px}
.view{display:none}.view.on{display:block;animation:rise .25s ease}
@keyframes rise{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}

h1{font:800 34px/1.1 var(--f-display);letter-spacing:-.03em}
h2{font:700 20px/1.2 var(--f-display);letter-spacing:-.015em}
h3{font:700 15px/1.3 var(--f-body)}
.head{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;margin-bottom:20px;flex-wrap:wrap}
.head p{color:var(--ink2);margin-top:6px;max-width:640px}
.eyebrow{font:500 12px var(--f-mono);text-transform:uppercase;letter-spacing:.08em;color:var(--ink3);margin-bottom:6px}

.card{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);box-shadow:var(--shadow)}
.pad{padding:20px}
.grid{display:grid;gap:16px}
.g2{grid-template-columns:repeat(2,minmax(0,1fr))}
.g3{grid-template-columns:repeat(3,minmax(0,1fr))}
.g4{grid-template-columns:repeat(4,minmax(0,1fr))}
@media (max-width:900px){.g3,.g4{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media (max-width:860px){#chats-grid{grid-template-columns:1fr!important}}
@media (max-width:620px){.g2,.g3,.g4{grid-template-columns:1fr}.top-in{flex-wrap:wrap;gap:10px}.nav{order:3;flex-basis:100%}h1{font-size:28px}}
.row{display:flex;align-items:center;gap:10px}
.row.wrap{flex-wrap:wrap}
.sp{flex:1}
.stack>*+*{margin-top:12px}

/* ── controls ── */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid var(--line);background:var(--panel);color:var(--ink);padding:8px 14px;border-radius:var(--r-sm);font-weight:600;font-size:14px;cursor:pointer;transition:.15s;white-space:nowrap}
.btn:hover{border-color:var(--ink3)}
.btn:disabled{opacity:.5;cursor:not-allowed}
.btn.pri{background:var(--ink);border-color:var(--ink);color:var(--bg)}
.btn.pri:hover{filter:brightness(1.15)}
.btn.blue{background:var(--blue);border-color:var(--blue);color:#fff}
.btn.danger{color:var(--red)} .btn.danger:hover{border-color:var(--red);background:var(--red-soft)}
.btn.ghost{border-color:transparent;background:none}
.btn.ghost:hover{background:var(--panel2)}
.btn.sm{padding:5px 10px;font-size:13px;border-radius:8px}
.btn.lg{padding:12px 20px;font-size:15px}
.icon-btn{border:0;background:none;padding:6px;border-radius:8px;cursor:pointer;color:var(--ink2);display:inline-grid;place-items:center}
.icon-btn:hover{background:var(--panel2);color:var(--ink)}
label.f{display:block;font-size:13px;font-weight:600;color:var(--ink2);margin-bottom:6px}
.in,select.in,textarea.in{width:100%;border:1px solid var(--line);background:var(--panel2);border-radius:var(--r-sm);padding:9px 12px;outline:none;transition:.15s}
.in:focus{border-color:var(--blue);background:var(--panel);box-shadow:0 0 0 3px var(--blue-soft)}
textarea.in{min-height:80px;resize:vertical;font-family:var(--f-mono);font-size:13px}
.hint{font-size:12.5px;color:var(--ink3);margin-top:6px;line-height:1.45}
.seg{display:inline-flex;background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:3px;gap:2px;flex-wrap:wrap}
.seg button{border:0;background:none;padding:6px 12px;border-radius:7px;font-weight:500;font-size:13.5px;color:var(--ink2);cursor:pointer}
.seg button.on{background:var(--panel);color:var(--ink);box-shadow:var(--shadow)}
.chipset{display:flex;flex-wrap:wrap;gap:8px}
.tchip{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);background:var(--panel);padding:7px 12px 7px 10px;border-radius:30px;cursor:pointer;font-weight:500;font-size:14px;user-select:none;transition:.12s}
.tchip .box{width:16px;height:16px;border-radius:5px;border:1.5px solid var(--ink3);display:grid;place-items:center}
.tchip.on{border-color:var(--ink);background:var(--ink);color:var(--bg)}
.tchip.on .box{border-color:var(--bg);background:var(--bg)}
.tchip.on .box::after{content:"";width:8px;height:8px;border-radius:2px;background:var(--ink)}
.tchip .n{font:500 11.5px var(--f-mono);opacity:.7}
.tchip.text-chip:not(.on){border-style:dashed}
.switch{display:flex;align-items:flex-start;gap:12px;cursor:pointer;padding:6px 0}
.switch input{display:none}
.switch .tr{width:36px;height:21px;border-radius:20px;background:var(--line);position:relative;flex:none;transition:.15s;margin-top:1px}
.switch .tr::after{content:"";position:absolute;left:3px;top:3px;width:15px;height:15px;border-radius:50%;background:#fff;transition:.15s;box-shadow:0 1px 2px rgba(0,0,0,.2)}
.switch input:checked+.tr{background:var(--green)}
.switch input:checked+.tr::after{left:18px}
.switch b{display:block;font-weight:600;font-size:14px}
.switch span.d{display:block;font-size:12.5px;color:var(--ink3)}
.badge{display:inline-flex;align-items:center;gap:4px;font:500 11.5px var(--f-mono);padding:2px 7px;border-radius:6px;background:var(--panel2);color:var(--ink2);border:1px solid var(--line2);white-space:nowrap}
.badge.blue{background:var(--blue-soft);color:var(--blue);border-color:transparent}
.badge.amber{background:var(--amber-soft);color:var(--amber);border-color:transparent}
.badge.green{background:var(--green-soft);color:var(--green);border-color:transparent}
.badge.red{background:var(--red-soft);color:var(--red);border-color:transparent}
.st{font:600 11.5px var(--f-mono);text-transform:uppercase;letter-spacing:.05em;padding:3px 8px;border-radius:6px}
.st.running{background:var(--green-soft);color:var(--green)}
.st.queued{background:var(--blue-soft);color:var(--blue)}
.st.paused{background:var(--amber-soft);color:var(--amber)}
.st.done{background:var(--panel2);color:var(--ink2)}
.st.error{background:var(--red-soft);color:var(--red)}
.st.stopped{background:var(--panel2);color:var(--ink3)}
.note{display:flex;gap:12px;padding:12px 14px;border-radius:var(--r-sm);background:var(--blue-soft);color:var(--ink);font-size:13.5px;line-height:1.5}
.note.warn{background:var(--amber-soft)} .note.bad{background:var(--red-soft)} .note.good{background:var(--green-soft)}
.note svg{margin-top:2px;flex:none}

/* ── signature: the route ticket ── */
.route{display:grid;grid-template-columns:minmax(0,1fr) minmax(90px,1.2fr) minmax(0,1fr);align-items:center;gap:14px}
.stop{display:flex;align-items:center;gap:10px;min-width:0}
.stop.end{flex-direction:row-reverse;text-align:right}
.stop .nm{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.stop .sub{font:500 11.5px var(--f-mono);color:var(--ink3);text-transform:uppercase;letter-spacing:.05em}
.stop>div{min-width:0}
.track{position:relative;height:22px}
.track::before{content:"";position:absolute;left:0;right:0;top:50%;border-top:2px dashed var(--line)}
.track .fill{position:absolute;left:0;top:calc(50% - 1px);height:2px;background:var(--ink);transition:width .6s ease}
.track .pkt{position:absolute;top:50%;width:12px;height:12px;border-radius:3px;background:var(--blue);transform:translate(-50%,-50%) rotate(45deg);transition:left .6s ease;box-shadow:0 0 0 4px var(--blue-soft)}
.running .track .pkt{animation:bob 1.2s ease-in-out infinite}
@keyframes bob{50%{transform:translate(-50%,-70%) rotate(45deg)}}
.track .pins{position:absolute;inset:0}
.track .pins i{position:absolute;top:50%;width:6px;height:6px;border-radius:50%;background:var(--amber);transform:translate(-50%,-50%)}
.av{width:38px;height:38px;border-radius:50%;flex:none;display:grid;place-items:center;font:700 14px var(--f-display);color:#fff;overflow:hidden;background:var(--ink3)}
.av img{width:100%;height:100%;object-fit:cover}
.av.sm{width:30px;height:30px;font-size:12px}
.av.lg{width:54px;height:54px;font-size:20px}

/* ── transfers ── */
.job{padding:18px 20px}
.job+.job{border-top:1px solid var(--line2)}
.job-top{display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap}
.nums{display:flex;gap:22px;flex-wrap:wrap;margin-top:14px}
.num b{display:block;font:700 20px/1.1 var(--f-display);letter-spacing:-.02em}
.num span{font:500 11px var(--f-mono);text-transform:uppercase;letter-spacing:.06em;color:var(--ink3)}
.num.red b{color:var(--red)} .num.amber b{color:var(--amber)} .num.green b{color:var(--green)}
.acts{display:flex;gap:6px;flex-wrap:wrap;margin-top:14px}
.bar{height:6px;background:var(--panel2);border-radius:6px;overflow:hidden;border:1px solid var(--line2)}
.bar i{display:block;height:100%;background:var(--ink);border-radius:6px;transition:width .6s}
.empty{text-align:center;padding:48px 20px;color:var(--ink2)}
.empty svg{width:34px;height:34px;color:var(--ink3);margin-bottom:10px}
.empty h3{color:var(--ink);margin-bottom:4px}

/* ── stat tiles ── */
.tile{padding:16px 18px}
.tile .k{font:500 11.5px var(--f-mono);text-transform:uppercase;letter-spacing:.07em;color:var(--ink3)}
.tile .v{font:800 28px/1.15 var(--f-display);letter-spacing:-.03em;margin-top:6px}
.tile .s{font-size:12.5px;color:var(--ink2);margin-top:2px}

/* ── form sections ── */
.sec{display:grid;grid-template-columns:220px minmax(0,1fr);gap:28px;padding:22px 24px}
.sec+.sec{border-top:1px solid var(--line2)}
.sec-h h3{font:700 16px var(--f-display)}
.sec-h p{font-size:13px;color:var(--ink3);margin-top:4px}
@media (max-width:760px){.sec{grid-template-columns:1fr;gap:12px;padding:18px}}
.picked{display:flex;flex-direction:column;gap:8px}
.pick-row{display:flex;align-items:center;gap:12px;border:1px solid var(--line);border-radius:12px;padding:10px 12px;background:var(--panel2)}
.pick-row .nm{font-weight:600}
.pick-empty{display:flex;align-items:center;justify-content:center;gap:8px;border:1.5px dashed var(--line);border-radius:12px;padding:16px;color:var(--ink2);cursor:pointer;font-weight:500;background:none;width:100%}
.pick-empty:hover{border-color:var(--blue);color:var(--blue)}
.mini-counts{display:flex;gap:10px;flex-wrap:wrap;font:500 12px var(--f-mono);color:var(--ink2);margin-top:3px}
details.adv summary{cursor:pointer;font-weight:600;color:var(--ink2);list-style:none;display:flex;align-items:center;gap:6px}
details.adv summary::-webkit-details-marker{display:none}
details.adv[open] summary svg{transform:rotate(90deg)}
.launch{position:sticky;bottom:12px;z-index:5;display:flex;align-items:center;gap:14px;padding:14px 18px;margin-top:16px;flex-wrap:wrap}
.launch .est{font:700 18px var(--f-display)}

/* ── picker modal ── */
.scrim{position:fixed;inset:0;background:rgba(10,18,30,.45);z-index:50;display:flex;align-items:flex-start;justify-content:center;padding:6vh 16px 16px;animation:fade .15s}
@keyframes fade{from{opacity:0}}
.modal{width:100%;max-width:620px;max-height:86vh;display:flex;flex-direction:column;overflow:hidden}
.modal.wide{max-width:900px}
.modal-h{padding:16px 18px;border-bottom:1px solid var(--line2);display:flex;align-items:center;gap:12px}
.modal-b{overflow:auto;flex:1}
.modal-f{padding:12px 18px;border-top:1px solid var(--line2);display:flex;gap:10px;align-items:center}
.searchbox{position:relative;flex:1}
.searchbox svg{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--ink3)}
.searchbox input{padding-left:36px}
.filters{display:flex;flex-wrap:wrap;gap:6px;padding:10px 18px;border-bottom:1px solid var(--line2)}
.filters button{border:1px solid var(--line);background:var(--panel);border-radius:20px;padding:4px 11px;font-size:13px;cursor:pointer;white-space:nowrap;color:var(--ink2)}
.filters button.on{background:var(--ink);border-color:var(--ink);color:var(--bg)}
.chat{display:flex;align-items:center;gap:12px;padding:10px 18px;cursor:pointer;border-bottom:1px solid var(--line2)}
.chat:hover{background:var(--panel2)}
.chat.sel{background:var(--blue-soft)}
.chat.dim{opacity:.55}
.chat .nm{font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat .meta{font-size:12.5px;color:var(--ink3);display:flex;gap:8px;flex-wrap:wrap}
.chat .ck{width:22px;height:22px;border-radius:50%;border:1.5px solid var(--line);display:grid;place-items:center;flex:none}
.chat.sel .ck{background:var(--blue);border-color:var(--blue);color:#fff}

/* ── logs ── */
.logs{font:12.5px/1.6 var(--f-mono);background:var(--panel2);border:1px solid var(--line2);border-radius:10px;padding:10px 12px;max-height:420px;overflow:auto;white-space:pre-wrap;word-break:break-word}
.logs .l-error{color:var(--red)} .logs .l-warn{color:var(--amber)}
.logs time{color:var(--ink3);margin-right:8px}

/* ── summary panel ── */
.sum-h{display:flex;gap:14px;align-items:center}
.typebars{display:grid;gap:8px;margin-top:14px}
.tb{display:grid;grid-template-columns:92px minmax(0,1fr) 70px;gap:10px;align-items:center;font-size:13px}
.tb .b{height:8px;background:var(--panel2);border-radius:6px;overflow:hidden}
.tb .b i{display:block;height:100%;background:var(--blue);border-radius:6px}
.tb .c{font-family:var(--f-mono);text-align:right;color:var(--ink2)}
.kv{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;font-size:13.5px}
.kv dt{color:var(--ink3)} .kv dd{font-weight:500}
.dgroup{display:flex;align-items:center;gap:12px;padding:10px 16px;border-bottom:1px solid var(--line2)}
.dgroup input{width:16px;height:16px;accent-color:var(--blue)}
.toast-wrap{position:fixed;bottom:18px;left:50%;transform:translateX(-50%);z-index:80;display:flex;flex-direction:column;gap:8px;align-items:center}
.toast{background:var(--ink);color:var(--bg);padding:10px 16px;border-radius:10px;font-size:14px;box-shadow:var(--shadow);max-width:92vw;animation:rise .2s}
.toast.bad{background:var(--red);color:#fff}
.gate{min-height:100vh;display:grid;place-items:center;padding:20px}
.gate .card{width:100%;max-width:400px;padding:28px}
.steps{counter-reset:s;display:grid;gap:10px}
.steps li{list-style:none;display:flex;gap:10px;font-size:13.5px;color:var(--ink2)}
.steps li::before{counter-increment:s;content:counter(s);font:600 12px var(--f-mono);width:22px;height:22px;border-radius:50%;background:var(--panel2);border:1px solid var(--line);display:grid;place-items:center;flex:none;color:var(--ink)}
.spin{width:16px;height:16px;border:2px solid var(--line);border-top-color:var(--blue);border-radius:50%;animation:rot .7s linear infinite;display:inline-block}
@keyframes rot{to{transform:rotate(360deg)}}
</style>
</head>
'''

HTML += r'''<body>
<svg width="0" height="0" style="position:absolute" aria-hidden="true">
 <symbol id="i-home" viewBox="0 0 24 24"><path d="M3 11l9-7 9 7"/><path d="M5 10v10h14V10"/></symbol>
 <symbol id="i-plus" viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></symbol>
 <symbol id="i-send" viewBox="0 0 24 24"><path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4z"/></symbol>
 <symbol id="i-live" viewBox="0 0 24 24"><circle cx="12" cy="12" r="2"/><path d="M16.2 7.8a6 6 0 010 8.4M7.8 16.2a6 6 0 010-8.4M19 5a10 10 0 010 14M5 19A10 10 0 015 5"/></symbol>
 <symbol id="i-copy" viewBox="0 0 24 24"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a1 1 0 01-1-1V4a1 1 0 011-1h10a1 1 0 011 1v1"/></symbol>
 <symbol id="i-list" viewBox="0 0 24 24"><path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/></symbol>
 <symbol id="i-user" viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0116 0"/></symbol>
 <symbol id="i-search" viewBox="0 0 24 24"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></symbol>
 <symbol id="i-x" viewBox="0 0 24 24"><path d="M18 6L6 18M6 6l12 12"/></symbol>
 <symbol id="i-check" viewBox="0 0 24 24"><path d="M20 6L9 17l-5-5"/></symbol>
 <symbol id="i-pause" viewBox="0 0 24 24"><path d="M9 5v14M15 5v14"/></symbol>
 <symbol id="i-play" viewBox="0 0 24 24"><path d="M7 4l13 8-13 8z"/></symbol>
 <symbol id="i-stop" viewBox="0 0 24 24"><rect x="6" y="6" width="12" height="12" rx="1.5"/></symbol>
 <symbol id="i-refresh" viewBox="0 0 24 24"><path d="M21 12a9 9 0 11-3-6.7L21 8"/><path d="M21 3v5h-5"/></symbol>
 <symbol id="i-trash" viewBox="0 0 24 24"><path d="M3 6h18M8 6V4h8v2M6 6l1 14h10l1-14"/></symbol>
 <symbol id="i-log" viewBox="0 0 24 24"><path d="M14 3H6a1 1 0 00-1 1v16a1 1 0 001 1h12a1 1 0 001-1V8z"/><path d="M14 3v5h5M9 13h6M9 17h6"/></symbol>
 <symbol id="i-down" viewBox="0 0 24 24"><path d="M12 3v12M6 11l6 6 6-6M4 21h16"/></symbol>
 <symbol id="i-chev" viewBox="0 0 24 24"><path d="M9 6l6 6-6 6"/></symbol>
 <symbol id="i-pin" viewBox="0 0 24 24"><path d="M12 17v5M9 3h6l-1 6 4 4H6l4-4z"/></symbol>
 <symbol id="i-lock" viewBox="0 0 24 24"><rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 018 0v4"/></symbol>
 <symbol id="i-info" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 8h.01M11 12h1v5h1"/></symbol>
 <symbol id="i-alert" viewBox="0 0 24 24"><path d="M12 3l10 18H2z"/><path d="M12 10v4M12 18h.01"/></symbol>
 <symbol id="i-swap" viewBox="0 0 24 24"><path d="M7 4L3 8l4 4M3 8h14M17 20l4-4-4-4M21 16H7"/></symbol>
 <symbol id="i-db" viewBox="0 0 24 24"><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/></symbol>
 <symbol id="i-star" viewBox="0 0 24 24"><path d="M12 3l2.8 5.8 6.2.9-4.5 4.4 1 6.2L12 17.4 6.5 20.3l1-6.2L3 9.7l6.2-.9z"/></symbol>
 <symbol id="i-video" viewBox="0 0 24 24"><rect x="2" y="6" width="14" height="12" rx="2"/><path d="M16 10l6-3v10l-6-3"/></symbol>
 <symbol id="i-photo" viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="16" rx="2"/><circle cx="9" cy="10" r="2"/><path d="M21 16l-5-5-9 9"/></symbol>
 <symbol id="i-document" viewBox="0 0 24 24"><path d="M14 3H6v18h12V7z"/><path d="M14 3v4h4"/></symbol>
 <symbol id="i-logo" viewBox="0 0 24 24"><path d="M4 12h9M10 7l5 5-5 5"/><circle cx="19" cy="12" r="2"/></symbol>
</svg>

<!-- ═══ gate: dashboard password ═══ -->
<div id="gate" class="gate hidden">
 <div class="card">
  <div class="brand" style="margin-bottom:22px"><span class="brand-mark"><svg class="i"><use href="#i-logo"/></svg></span>Relay</div>
  <div id="gate-setup" class="hidden stack">
   <h2>Set a dashboard password</h2>
   <p class="muted small">Anyone with this link could control your Telegram account, so the dashboard is locked. Pick a password you'll remember — it's stored hashed on your Railway volume.</p>
   <div><label class="f" for="g-new">New password</label><input class="in" id="g-new" type="password" autocomplete="new-password" placeholder="At least 8 characters"></div>
   <button class="btn pri lg" style="width:100%" id="g-setup-btn">Set password and continue</button>
  </div>
  <div id="gate-login" class="hidden stack">
   <h2>Sign in</h2>
   <p class="muted small">Enter the dashboard password.</p>
   <div><label class="f" for="g-pw">Password</label><input class="in" id="g-pw" type="password" autocomplete="current-password"></div>
   <button class="btn pri lg" style="width:100%" id="g-login-btn">Sign in</button>
  </div>
  <p id="gate-err" class="small" style="color:var(--red);margin-top:12px"></p>
 </div>
</div>

<!-- ═══ app ═══ -->
<div id="app" class="hidden">
<header class="top"><div class="top-in">
 <div class="brand"><span class="brand-mark"><svg class="i"><use href="#i-logo"/></svg></span>Relay <small>v3</small></div>
 <nav class="nav" id="nav">
  <button data-v="home" class="on"><svg class="i sm"><use href="#i-home"/></svg>Overview</button>
  <button data-v="new"><svg class="i sm"><use href="#i-plus"/></svg>New transfer</button>
  <button data-v="jobs"><svg class="i sm"><use href="#i-send"/></svg>Transfers <span class="count hidden" id="nav-jobs"></span></button>
  <button data-v="live"><svg class="i sm"><use href="#i-live"/></svg>Live sync <span class="count hidden" id="nav-live"></span></button>
  <button data-v="dupes"><svg class="i sm"><use href="#i-copy"/></svg>Duplicates</button>
  <button data-v="chats"><svg class="i sm"><use href="#i-list"/></svg>Chats</button>
  <button data-v="account"><svg class="i sm"><use href="#i-user"/></svg>Account</button>
 </nav>
 <button class="conn" id="conn" title="Telegram connection"><span class="dot" id="conn-dot"></span><span id="conn-txt">Checking…</span></button>
</div></header>

<main>
<!-- ─── Overview ─── -->
<section class="view on" id="v-home">
 <div class="head"><div><div class="eyebrow">Overview</div><h1 id="hello">Your transfer console</h1>
  <p>Everything here runs on the server. Close this tab whenever you like — transfers, live syncs and scans keep going and pick up again after a redeploy.</p></div>
  <button class="btn pri lg" data-go="new"><svg class="i"><use href="#i-plus"/></svg>New transfer</button></div>
 <div id="home-warn" class="stack" style="margin-bottom:16px"></div>
 <div class="grid g4" style="margin-bottom:16px">
  <div class="card tile"><div class="k">Messages sent</div><div class="v" id="t-sent">—</div><div class="s">all transfers + live syncs</div></div>
  <div class="card tile"><div class="k">Data moved</div><div class="v" id="t-bytes">—</div><div class="s">files copied between chats</div></div>
  <div class="card tile"><div class="k">Running now</div><div class="v" id="t-run">—</div><div class="s" id="t-run-s">transfers</div></div>
  <div class="card tile"><div class="k">Live syncs</div><div class="v" id="t-live">—</div><div class="s">watching for new posts</div></div>
 </div>
 <div class="card"><div class="pad row"><h2>Active transfers</h2><span class="sp"></span><button class="btn sm ghost" data-go="jobs">See all<svg class="i sm"><use href="#i-chev"/></svg></button></div>
  <div id="home-jobs"></div></div>
</section>

<!-- ─── New transfer ─── -->
<section class="view" id="v-new">
 <div class="head"><div><div class="eyebrow">New transfer</div><h1>Move content between chats</h1>
  <p>Pick where from and where to, choose what to bring, and start. It runs in the background on Railway.</p></div>
  <div class="row"><select class="in" id="preset-sel" style="width:auto;min-width:170px"><option value="">Load a preset…</option></select>
  <button class="btn sm" id="preset-save">Save as preset</button></div></div>

 <div class="card">
  <div class="sec"><div class="sec-h"><h3>Route</h3><p>Sources are copied in their original order, oldest first. Several sources run as separate transfers into the same destination.</p></div>
   <div class="stack">
    <div><label class="f">From</label><div class="picked" id="src-picked"></div>
     <button class="pick-empty" id="src-add"><svg class="i"><use href="#i-plus"/></svg>Choose source chats</button></div>
    <div><label class="f">To</label><div class="picked" id="dst-picked"></div>
     <button class="pick-empty" id="dst-add"><svg class="i"><use href="#i-plus"/></svg>Choose the destination</button></div>
    <div id="route-notes" class="stack"></div>
   </div></div>

  <div class="sec"><div class="sec-h"><h3>What to bring</h3><p>Numbers show how many of each the first source contains. Text messages are skipped unless you tick Text.</p></div>
   <div class="stack">
    <div class="chipset" id="types"></div>
    <div class="row wrap small faint"><button class="btn sm ghost" id="types-media">All media</button><button class="btn sm ghost" id="types-files">Only files, videos, photos</button><button class="btn sm ghost" id="types-none">Clear</button></div>
   </div></div>

  <div class="sec"><div class="sec-h"><h3>Which messages</h3><p>How far back to go.</p></div>
   <div class="stack">
    <div class="seg" id="range-seg"><button data-r="all" class="on">Everything</button><button data-r="latest">Latest N</button><button data-r="dates">Date range</button><button data-r="ids">Message IDs</button></div>
    <div id="range-latest" class="hidden"><label class="f">Number of newest messages</label><input class="in" id="latest_n" type="number" min="1" value="500" style="max-width:200px"></div>
    <div id="range-dates" class="hidden grid g2"><div><label class="f">From date</label><input class="in" id="date_from" type="date"></div><div><label class="f">To date</label><input class="in" id="date_to" type="date"></div></div>
    <div id="range-ids" class="hidden grid g2"><div><label class="f">From message ID</label><input class="in" id="from_id" type="number" min="1" placeholder="e.g. 1"></div><div><label class="f">To message ID (optional)</label><input class="in" id="to_id" type="number" min="0" placeholder="latest"></div></div>
    <div><label class="f">After copying the history</label>
     <div class="seg" id="after-seg"><button data-a="stop" class="on">Stop</button><button data-a="live">Keep syncing new posts</button><button data-a="liveonly">Skip history — only new posts</button></div></div>
   </div></div>

  <div class="sec"><div class="sec-h"><h3>Duplicates</h3><p>Matched by exact file size, type and length — no downloading needed, so it's fast even for huge channels.</p></div>
   <div class="stack">
    <label class="switch"><input type="checkbox" id="skip_dupes" checked><span class="tr"></span><span><b>Skip anything already sent</b><span class="d">Never sends the same file into the destination twice — across all transfers and live syncs.</span></span></label>
    <label class="switch"><input type="checkbox" id="check_dest" checked><span class="tr"></span><span><b>Also check what's already in the destination</b><span class="d">Reads the destination first so files posted earlier (by hand or another tool) are skipped too.</span></span></label>
   </div></div>

  <div class="sec"><div class="sec-h"><h3>Pinning</h3><p>Needs admin with the “Pin messages” right in the destination.</p></div>
   <div class="stack">
    <label class="switch"><input type="checkbox" id="pin_first" checked><span class="tr"></span><span><b>Pin the first message</b></span></label>
    <div class="row"><span class="small muted">Pin every</span><input class="in" id="pin_every" type="number" min="0" value="200" style="width:110px"><span class="small muted">messages sent (0 = off)</span></div>
    <label class="switch"><input type="checkbox" id="hide_pin_notice" checked><span class="tr"></span><span><b>Hide “pinned a message” notices</b><span class="d">Deletes the service line Telegram posts after each pin.</span></span></label>
   </div></div>

  <div class="sec"><div class="sec-h"><h3>Look &amp; captions</h3><p>Copy mode removes “Forwarded from”. Albums stay grouped.</p></div>
   <div class="stack">
    <div class="seg" id="mode-seg"><button data-m="copy" class="on">Copy (no “Forwarded from”)</button><button data-m="forward">Forward (show original)</button></div>
    <div class="seg" id="cap-seg"><button data-c="keep" class="on">Keep captions</button><button data-c="remove">Remove captions</button><button data-c="replace">Use a template</button></div>
    <div id="cap-tpl" class="hidden"><label class="f">Caption template</label><textarea class="in" id="caption_template">{caption}</textarea>
     <div class="hint">Placeholders: <span class="mono">{caption} {filename} {name} {size} {duration} {date} {n} {source}</span></div></div>
    <details class="adv"><summary><svg class="i sm"><use href="#i-chev"/></svg>Clean up and brand captions</summary>
     <div class="stack" style="margin-top:12px">
      <label class="switch"><input type="checkbox" id="strip_links"><span class="tr"></span><span><b>Remove links</b><span class="d">Strips http(s)://, www. and t.me links.</span></span></label>
      <label class="switch"><input type="checkbox" id="strip_mentions"><span class="tr"></span><span><b>Remove @mentions</b></span></label>
      <div><label class="f">Replace words (one rule per line)</label><textarea class="in" id="replace_rules" placeholder="@oldchannel => @mychannel"></textarea></div>
      <div><label class="f">Footer added to every message</label><input class="in" id="footer" placeholder="Join @mychannel"></div>
     </div></details>
   </div></div>

  <div class="sec"><div class="sec-h"><h3>Filters</h3><p>Optional. Leave blank to bring everything of the ticked types.</p></div>
   <details class="adv"><summary><svg class="i sm"><use href="#i-chev"/></svg>Size, length, file type and keywords</summary>
    <div class="grid g2" style="margin-top:12px">
     <div><label class="f">Min size (MB)</label><input class="in" id="min_size_mb" type="number" min="0" step="0.1" placeholder="0"></div>
     <div><label class="f">Max size (MB)</label><input class="in" id="max_size_mb" type="number" min="0" step="0.1" placeholder="no limit"></div>
     <div><label class="f">Min video/audio length (seconds)</label><input class="in" id="min_duration" type="number" min="0" placeholder="0"></div>
     <div><label class="f">Only these file extensions</label><input class="in" id="extensions" placeholder="pdf, zip, apk"></div>
     <div><label class="f">Must contain one of</label><input class="in" id="include_words" placeholder="1080p, episode"></div>
     <div><label class="f">Skip if it contains</label><input class="in" id="exclude_words" placeholder="promo, ad"></div>
    </div></details></div>

  <div class="sec"><div class="sec-h"><h3>Speed</h3><p>Faster risks Telegram's rate limits; the app waits them out automatically.</p></div>
   <div class="stack">
    <div class="seg" id="speed-seg"><button data-s="safe">Gentle</button><button data-s="normal" class="on">Balanced</button><button data-s="fast">Fast</button></div>
    <label class="switch"><input type="checkbox" id="silent" checked><span class="tr"></span><span><b>Send silently</b><span class="d">Destination members don't get a notification for each message.</span></span></label>
    <label class="switch"><input type="checkbox" id="notify" checked><span class="tr"></span><span><b>Message me in Saved Messages when it's done</b></span></label>
   </div></div>
 </div>

 <div class="card launch">
  <div><div class="est" id="est-txt">Choose a route to see an estimate</div><div class="small muted" id="est-sub">Nothing is sent until you press Start.</div></div>
  <span class="sp"></span>
  <button class="btn" id="est-btn"><svg class="i sm"><use href="#i-search"/></svg>Estimate</button>
  <button class="btn pri lg" id="start-btn"><svg class="i"><use href="#i-play"/></svg>Start transfer</button>
 </div>
</section>

<!-- ─── Transfers ─── -->
<section class="view" id="v-jobs">
 <div class="head"><div><div class="eyebrow">Transfers</div><h1>Transfers</h1><p>Pause, resume or change settings at any time. Finished transfers can fetch posts added since.</p></div>
  <div class="seg" id="jobs-filter"><button data-f="active" class="on">Active</button><button data-f="all">All</button><button data-f="done">Finished</button></div></div>
 <div class="card" id="jobs-list"></div>
</section>

<!-- ─── Live ─── -->
<section class="view" id="v-live">
 <div class="head"><div><div class="eyebrow">Live sync</div><h1>Live sync</h1><p>New posts in a source are copied within seconds. They survive restarts and catch up on anything posted while the server was down.</p></div>
  <button class="btn pri" id="live-new"><svg class="i sm"><use href="#i-plus"/></svg>New live sync</button></div>
 <div class="card" id="live-list"></div>
</section>

<!-- ─── Duplicates ─── -->
<section class="view" id="v-dupes">
 <div class="head"><div><div class="eyebrow">Duplicates</div><h1>Find duplicate files</h1><p>Scan any chat — your own storage channel or a source — to see what's repeated, how much space it wastes, and remove extra copies if you're an admin.</p></div></div>
 <div class="card pad" style="margin-bottom:16px">
  <div class="grid g2" style="align-items:end">
   <div><label class="f">Chat to scan</label><div class="picked" id="scan-picked"></div><button class="pick-empty" id="scan-add"><svg class="i"><use href="#i-plus"/></svg>Choose a chat</button></div>
   <div class="stack"><div><label class="f">How much</label><div class="seg" id="scan-seg"><button data-l="0" class="on">Whole chat</button><button data-l="5000">Latest 5,000</button><button data-l="1000">Latest 1,000</button></div></div>
    <div class="chipset" id="scan-types"></div></div>
  </div>
  <div class="row" style="margin-top:16px"><span class="sp"></span><button class="btn pri" id="scan-btn"><svg class="i sm"><use href="#i-search"/></svg>Start scan</button></div>
 </div>
 <div class="card" id="scan-list"></div>
</section>

<!-- ─── Chats ─── -->
<section class="view" id="v-chats">
 <div class="head"><div><div class="eyebrow">Chats</div><h1>Your chats</h1><p>Open any chat for a summary: what it holds, how big it is, your rights there and what's been transferred.</p></div>
  <button class="btn" id="chats-refresh"><svg class="i sm"><use href="#i-refresh"/></svg>Refresh list</button></div>
 <div class="grid" style="grid-template-columns:minmax(0,380px) minmax(0,1fr);align-items:start" id="chats-grid">
  <div class="card" style="overflow:hidden"><div style="padding:12px"><div class="searchbox"><svg class="i sm"><use href="#i-search"/></svg><input class="in" id="chats-q" placeholder="Search chats"></div></div>
   <div class="filters" id="chats-filters"></div><div id="chats-list" style="max-height:70vh;overflow:auto"></div></div>
  <div class="card pad" id="chat-sum"><div class="empty"><svg class="i"><use href="#i-list"/></svg><h3>Pick a chat</h3><p>Its summary shows up here.</p></div></div>
 </div>
</section>

<!-- ─── Account ─── -->
<section class="view" id="v-account">
 <div class="head"><div><div class="eyebrow">Account</div><h1>Account &amp; settings</h1></div></div>
 <div class="grid g2" style="align-items:start">
  <div class="card pad stack" id="tg-card"></div>
  <div class="stack">
   <div class="card pad stack"><h2>Server</h2><dl class="kv" id="srv-kv"></dl><div id="srv-note"></div></div>
   <div class="card pad stack"><h2>Behaviour</h2>
    <div class="row"><span class="small muted">Run up to</span><input class="in" id="max_jobs" type="number" min="1" max="10" style="width:80px"><span class="small muted">transfers at the same time (others wait in line)</span></div>
    <label class="switch"><input type="checkbox" id="g-notify"><span class="tr"></span><span><b>Notifications in Saved Messages</b><span class="d">A short message when a transfer finishes or fails.</span></span></label>
    <div class="row"><button class="btn sm" id="save-settings">Save</button></div></div>
   <div class="card pad stack"><h2>Data</h2>
    <p class="small muted">Download a backup of your jobs, logs and duplicate memory (without your session or password). “Forget duplicates” lets files be sent again.</p>
    <div class="row wrap"><a class="btn sm" id="backup-link" href="/api/backup.db"><svg class="i sm"><use href="#i-down"/></svg>Download backup</a><button class="btn sm danger" id="fp-clear">Forget all duplicate memory</button></div></div>
   <div class="card pad stack" id="pw-card"><h2>Dashboard password</h2>
    <div class="grid g2"><input class="in" id="pw-cur" type="password" placeholder="Current password"><input class="in" id="pw-new" type="password" placeholder="New password"></div>
    <div class="row"><button class="btn sm" id="pw-save">Change password</button><span class="sp"></span><button class="btn sm ghost" id="signout">Sign out of dashboard</button></div></div>
  </div>
 </div>
</section>
</main>
</div>
<div id="modal-root"></div>
<div class="toast-wrap" id="toasts"></div>
'''

HTML += r'''<script>
"use strict";
const $ = (s, r) => (r || document).querySelector(s);
const $$ = (s, r) => Array.from((r || document).querySelectorAll(s));
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ic = (n, cls) => `<svg class="i ${cls||''}"><use href="#i-${n}"/></svg>`;
const fmtN = n => (n == null ? '—' : Number(n).toLocaleString());
function human(b){ b = Number(b||0); const u=['B','KB','MB','GB','TB']; let i=0; while(b>=1024&&i<4){b/=1024;i++} return (i<2?b.toFixed(0):b.toFixed(1))+' '+u[i]; }
function dur(s){ s=Math.max(0,Math.round(s||0)); if(s<60) return s+'s'; if(s<3600) return Math.floor(s/60)+'m '+String(s%60).padStart(2,'0')+'s'; if(s<86400) return Math.floor(s/3600)+'h '+String(Math.floor(s%3600/60)).padStart(2,'0')+'m'; return Math.floor(s/86400)+'d '+Math.floor(s%86400/3600)+'h'; }
function ago(iso){ if(!iso) return '—'; const s=(Date.now()-new Date(iso).getTime())/1000; if(s<60) return 'just now'; if(s<3600) return Math.floor(s/60)+' min ago'; if(s<86400) return Math.floor(s/3600)+' h ago'; if(s<86400*30) return Math.floor(s/86400)+' days ago'; return new Date(iso).toLocaleDateString(); }
function dateStr(iso){ return iso ? new Date(iso).toLocaleDateString(undefined,{year:'numeric',month:'short',day:'numeric'}) : '—'; }
function toast(msg, bad){ const t=document.createElement('div'); t.className='toast'+(bad?' bad':''); t.textContent=msg; $('#toasts').appendChild(t); setTimeout(()=>t.remove(), bad?6000:3500); }

async function api(url, method, body){
  const o = {method: method||'GET', headers:{'X-Requested-With':'tgfwd'}, credentials:'same-origin'};
  if (body !== undefined){ o.headers['Content-Type']='application/json'; o.body=JSON.stringify(body); }
  let r; try { r = await fetch(url, o); } catch(e){ throw new Error('Network problem — is the server awake?'); }
  let j = {}; try { j = await r.json(); } catch(e){ j = {ok:false, error:'Server returned '+r.status}; }
  if (r.status===401 && j.auth){ showGate(); throw new Error('Please sign in'); }
  if (!r.ok || j.ok===false){ const e = new Error(j.error||('Error '+r.status)); e.tg = j.tg===false; throw e; }
  return j;
}
async function act(btn, fn){ if(btn) btn.disabled=true; try { return await fn(); } catch(e){ toast(e.message, true); } finally { if(btn) btn.disabled=false; } }

/* ── avatars ── */
const PALETTE=['#2474C9','#1F9D6B','#E39B2D','#D2453D','#6E5BD6','#0E8A9A','#B5487E','#5C6B82'];
function avatar(c, size){ const id=String(c&&c.id||'0'); let h=0; for(const ch of id) h=(h*31+ch.charCodeAt(0))>>>0;
  const nm=(c&&c.name)||'?'; const ini=(c&&c.kind==='saved')?'★':(nm.replace(/[^\p{L}\p{N}]/gu,'').slice(0,1).toUpperCase()||'#');
  const img=(c&&c.has_photo)?`<img loading="lazy" alt="" src="/api/avatar/${encodeURIComponent(id)}" onerror="this.remove()">`:'';
  return `<span class="av ${size||''}" style="background:${PALETTE[h%PALETTE.length]}">${esc(ini)}${img}</span>`; }
const KIND={channel:'Channel',group:'Group',user:'Person',bot:'Bot',saved:'Saved Messages'};

/* ── state ── */
const S = { status:null, dialogs:null, dmap:{}, summaries:{}, view:'home', jobsFilter:'active', presets:{}, lastCfg:null };
function chatById(id){ return S.dmap[String(id)] || null; }
async function loadDialogs(refresh){
  const r = await api('/api/dialogs'+(refresh?'?refresh=1':''));
  S.dialogs = r.dialogs; S.dmap = {}; r.dialogs.forEach(d=>S.dmap[d.id]=d); return r.dialogs;
}
async function getSummary(id, refresh){
  if(!refresh && S.summaries[id]) return S.summaries[id];
  const r = await api('/api/summary/'+encodeURIComponent(id)+(refresh?'?refresh=1':'')); S.summaries[id]=r.summary; return r.summary;
}

/* ── gate ── */
async function showGate(){
  $('#app').classList.add('hidden'); $('#gate').classList.remove('hidden');
  const st = await fetch('/api/auth/state').then(r=>r.json()).catch(()=>({}));
  $('#gate-setup').classList.toggle('hidden', !st.setup); $('#gate-login').classList.toggle('hidden', !!st.setup);
  setTimeout(()=>($(st.setup?'#g-new':'#g-pw')||{}).focus&&$(st.setup?'#g-new':'#g-pw').focus(), 50);
}
async function gateSubmit(url, pw){
  $('#gate-err').textContent='';
  try { await api(url,'POST',{password:pw}); $('#gate').classList.add('hidden'); startApp(); }
  catch(e){ $('#gate-err').textContent=e.message; }
}
$('#g-setup-btn').onclick=()=>gateSubmit('/api/auth/setup', $('#g-new').value);
$('#g-login-btn').onclick=()=>gateSubmit('/api/auth/login', $('#g-pw').value);
$('#g-pw').onkeydown=e=>{ if(e.key==='Enter') $('#g-login-btn').click(); };
$('#g-new').onkeydown=e=>{ if(e.key==='Enter') $('#g-setup-btn').click(); };

/* ── router ── */
function go(v){ S.view=v; $$('#nav button').forEach(b=>b.classList.toggle('on', b.dataset.v===v)); $$('.view').forEach(s=>s.classList.toggle('on', s.id==='v-'+v));
  window.scrollTo({top:0}); if(location.hash!=='#'+v) history.replaceState(null,'','#'+v); refreshView(); }
$$('#nav button').forEach(b=>b.onclick=()=>go(b.dataset.v));
document.addEventListener('click', e=>{ const g=e.target.closest('[data-go]'); if(g){ e.preventDefault(); go(g.dataset.go); } });
$('#conn').onclick=()=>go('account');

/* ── status ── */
async function loadStatus(){
  try { S.status = await api('/api/status'); } catch(e){ return; }
  const s=S.status, dot=$('#conn-dot');
  dot.className='dot '+(s.connected?'ok':'bad');
  $('#conn-txt').textContent = s.connected ? (s.me ? s.me.name : 'Connected') : 'Telegram not connected';
  $('#t-sent').textContent=fmtN(s.total_sent); $('#t-bytes').textContent=human(s.total_bytes);
  $('#t-run').textContent=s.running; $('#t-run-s').textContent='of '+s.max_jobs+' allowed at once';
  $('#t-live').textContent=s.live.active;
  const nq=(s.jobs.running||0)+(s.jobs.queued||0); const nj=$('#nav-jobs'); nj.textContent=nq; nj.classList.toggle('hidden', !nq);
  const nl=$('#nav-live'); nl.textContent=s.live.active; nl.classList.toggle('hidden', !s.live.active);
  if (s.me) $('#hello').textContent = 'Hi, '+s.me.name.split(' ')[0];
  const w=[];
  if(!s.api_ok) w.push(note('bad','alert','<b>API_ID and API_HASH are missing.</b> Add them in Railway → Variables (get them at my.telegram.org), then redeploy.'));
  if(!s.connected) w.push(note('warn','user',`<b>Telegram isn't connected.</b> ${esc(s.error||'')} <a href="#account" data-go="account">Connect your account →</a>`));
  if(!s.persistent) w.push(note('warn','db','<b>No Railway volume attached.</b> Jobs, progress and duplicate memory are wiped on every redeploy. In Railway, right-click the service → Attach volume (any mount path) and redeploy.'));
  $('#home-warn').innerHTML=w.join('');
  if(S.view==='account') renderAccount();
}
function note(kind, icon, html){ return `<div class="note ${kind}">${ic(icon,'sm')}<div>${html}</div></div>`; }

/* ── chat picker ── */
const PF=[['all','All'],['channel','Channels'],['group','Groups'],['user','People'],['bot','Bots'],['post','I can post'],['admin','I\'m admin']];
function openPicker(opts){
  const sel = new Set(opts.selected||[]); let filter=opts.dest?'post':'all', qstr='';
  const root=$('#modal-root');
  root.innerHTML=`<div class="scrim"><div class="card modal" role="dialog" aria-modal="true" aria-label="${esc(opts.title)}">
   <div class="modal-h"><h2 style="flex:1">${esc(opts.title)}</h2><button class="icon-btn" data-x>${ic('x')}</button></div>
   <div style="padding:12px 18px 0"><div class="searchbox">${ic('search','sm')}<input class="in" id="pk-q" placeholder="Search by name or @username" autocomplete="off"></div></div>
   <div class="filters" id="pk-f">${PF.map(f=>`<button data-f="${f[0]}" class="${f[0]===filter?'on':''}">${f[1]}</button>`).join('')}</div>
   <div class="modal-b" id="pk-list"><div class="empty"><span class="spin"></span></div></div>
   <div class="modal-f"><span class="small muted" id="pk-count"></span><span class="sp"></span><button class="btn ghost" data-x>Cancel</button>${opts.multi?'<button class="btn pri" id="pk-done">Done</button>':''}</div>
  </div></div>`;
  const close=()=>root.innerHTML='';
  $$('[data-x]',root).forEach(b=>b.onclick=close); $('.scrim',root).onclick=e=>{ if(e.target.classList.contains('scrim')) close(); };
  const onKey=e=>{ if(e.key==='Escape'){ close(); document.removeEventListener('keydown',onKey);} }; document.addEventListener('keydown',onKey);
  function draw(){
    const L=$('#pk-list', root); if(!L) return;
    if(!S.dialogs){ return; }
    const qq=qstr.toLowerCase();
    let list=S.dialogs.filter(d=>!(opts.exclude||[]).includes(d.id));
    list=list.filter(d=> filter==='all'||(filter==='post'?d.can_post:filter==='admin'?(d.admin||d.kind==='saved'):d.kind===filter));
    if(qq) list=list.filter(d=>(d.name||'').toLowerCase().includes(qq)||(d.username||'').toLowerCase().includes(qq.replace('@',''))||d.id.includes(qq));
    $('#pk-count',root).textContent = opts.multi ? (sel.size+' selected') : (list.length+' chats');
    if(!list.length){ L.innerHTML=`<div class="empty">${ic('search')}<h3>No chats match</h3><p class="small">Try another filter — or join the chat in Telegram first.</p></div>`; return; }
    L.innerHTML=list.slice(0,400).map(d=>{
      const tags=[KIND[d.kind]||d.kind]; if(d.members) tags.push(fmtN(d.members)+' members'); if(d.username) tags.push('@'+d.username);
      const b=[]; if(d.admin) b.push('<span class="badge blue">admin</span>'); if(d.protected) b.push('<span class="badge amber">'+ic('lock','sm')+' protected</span>');
      if(opts.dest && !d.can_post) b.push('<span class="badge red">can\'t post</span>'); if(opts.dest && d.can_post && !d.can_pin && d.kind!=='saved') b.push('<span class="badge">no pin right</span>');
      return `<div class="chat ${sel.has(d.id)?'sel':''} ${opts.dest&&!d.can_post?'dim':''}" data-id="${esc(d.id)}" tabindex="0">${avatar(d,'sm')}
        <div style="flex:1;min-width:0"><div class="nm">${esc(d.name)}</div><div class="meta">${tags.map(esc).join(' · ')}</div></div>
        <div class="row" style="gap:5px">${b.join('')}</div>${opts.multi?`<span class="ck">${sel.has(d.id)?ic('check','sm'):''}</span>`:''}</div>`; }).join('');
    $$('.chat',L).forEach(el=>{ const pick=()=>{ const id=el.dataset.id;
      if(opts.multi){ sel.has(id)?sel.delete(id):sel.add(id); draw(); } else { close(); opts.onDone([id]); } };
      el.onclick=pick; el.onkeydown=e=>{ if(e.key==='Enter') pick(); }; });
  }
  $('#pk-q',root).oninput=e=>{ qstr=e.target.value; draw(); }; setTimeout(()=>$('#pk-q',root)&&$('#pk-q',root).focus(),30);
  $$('#pk-f button',root).forEach(b=>b.onclick=()=>{ filter=b.dataset.f; $$('#pk-f button',root).forEach(x=>x.classList.toggle('on',x===b)); draw(); });
  if(opts.multi) $('#pk-done',root).onclick=()=>{ close(); opts.onDone(Array.from(sel)); };
  if(S.dialogs) draw(); else loadDialogs().then(draw).catch(e=>{ const L=$('#pk-list',root); if(L) L.innerHTML=`<div class="empty">${ic('alert')}<h3>Couldn't load your chats</h3><p class="small">${esc(e.message)}</p></div>`; });
}
function modal(title, html, wide){ const root=$('#modal-root');
  root.innerHTML=`<div class="scrim"><div class="card modal ${wide?'wide':''}" role="dialog" aria-modal="true"><div class="modal-h"><h2 style="flex:1;min-width:0">${title}</h2><button class="icon-btn" data-x>${ic('x')}</button></div><div class="modal-b">${html}</div></div></div>`;
  const close=()=>{ root.innerHTML=''; S.modalTick=null; };
  $$('[data-x]',root).forEach(b=>b.onclick=close); $('.scrim',root).onclick=e=>{ if(e.target.classList.contains('scrim')) close(); };
  return root; }
function confirmBox(title, text, okLabel, danger){ return new Promise(res=>{ const root=modal(esc(title), `<div class="pad stack"><p class="muted">${text}</p><div class="row"><span class="sp"></span><button class="btn ghost" id="cf-no">Cancel</button><button class="btn ${danger?'danger':'pri'}" id="cf-ok">${esc(okLabel)}</button></div></div>`);
  $('#cf-no',root).onclick=()=>{ root.innerHTML=''; res(false); }; $('#cf-ok',root).onclick=()=>{ root.innerHTML=''; res(true); }; }); }
'''

HTML += r'''
/* ═══ New transfer form ═══ */
const TYPES=[['video','Videos','video'],['photo','Photos','photo'],['document','Files','document'],['audio','Music','audio'],['voice','Voice notes','voice'],['gif','GIFs','gif'],['round','Video notes','round'],['sticker','Stickers',null],['text','Text',null]];
const F = { sources:[], dest:null, types:new Set(['video','photo','document']), range:'all', after:'stop', mode:'copy', cap:'keep', speed:'normal' };
function drawTypes(counts){
  $('#types').innerHTML=TYPES.map(t=>{ const n=counts&&t[2]?counts[t[2]]:(counts&&t[0]==='text'?counts.__text:null);
    return `<span class="tchip ${F.types.has(t[0])?'on':''} ${t[0]==='text'?'text-chip':''}" data-t="${t[0]}" role="checkbox" tabindex="0" aria-checked="${F.types.has(t[0])}"><span class="box"></span>${t[1]}${n!=null?` <span class="n">${fmtN(n)}</span>`:''}</span>`; }).join('');
  $$('#types .tchip').forEach(el=>{ const tg=()=>{ const t=el.dataset.t; F.types.has(t)?F.types.delete(t):F.types.add(t); drawTypes(counts); estimateSoon(); }; el.onclick=tg; el.onkeydown=e=>{ if(e.key===' '||e.key==='Enter'){ e.preventDefault(); tg(); } }; });
}
function typeCounts(){ const s=F.sources[0]&&S.summaries[F.sources[0]]; if(!s) return null; return Object.assign({__text:s.text_other}, s.counts); }
$('#types-media').onclick=()=>{ F.types=new Set(['video','photo','document','audio','voice','gif','round']); drawTypes(typeCounts()); estimateSoon(); };
$('#types-files').onclick=()=>{ F.types=new Set(['video','photo','document']); drawTypes(typeCounts()); estimateSoon(); };
$('#types-none').onclick=()=>{ F.types=new Set(); drawTypes(typeCounts()); estimateSoon(); };
function segBind(id, key, attr, cb){ $$('#'+id+' button').forEach(b=>b.onclick=()=>{ F[key]=b.dataset[attr]; $$('#'+id+' button').forEach(x=>x.classList.toggle('on',x===b)); cb&&cb(); }); }
function segSet(id, attr, val){ $$('#'+id+' button').forEach(x=>x.classList.toggle('on', x.dataset[attr]===String(val))); }
segBind('range-seg','range','r',()=>{ ['latest','dates','ids'].forEach(r=>$('#range-'+r).classList.toggle('hidden',F.range!==r)); estimateSoon(); });
segBind('after-seg','after','a');
segBind('mode-seg','mode','m');
segBind('cap-seg','cap','c',()=>$('#cap-tpl').classList.toggle('hidden',F.cap!=='replace'));
segBind('speed-seg','speed','s');

function pickRow(id, removable, which){
  const d=chatById(id)||{id, name:id, kind:'channel'}; const s=S.summaries[id];
  let meta=`<span>${esc(KIND[d.kind]||'')}</span>`;
  if(s){ meta=`<span>${fmtN(s.total)} messages</span><span>${ic('video','sm')} ${fmtN(s.counts.video)}</span><span>${ic('photo','sm')} ${fmtN(s.counts.photo)}</span><span>${ic('document','sm')} ${fmtN(s.counts.document)}</span>`; }
  else meta+=' <span class="spin" style="width:11px;height:11px"></span>';
  return `<div class="pick-row">${avatar(d,'sm')}<div style="flex:1;min-width:0"><div class="nm">${esc(d.name)}</div><div class="mini-counts">${meta}</div></div>
   ${d.protected?'<span class="badge amber">'+ic('lock','sm')+' protected</span>':''}
   <button class="icon-btn" data-sum="${esc(id)}" title="Summary">${ic('info','sm')}</button>
   ${removable?`<button class="icon-btn" data-rm="${esc(id)}" data-w="${which}" title="Remove">${ic('x','sm')}</button>`:''}</div>`;
}
function drawRoute(){
  $('#src-picked').innerHTML=F.sources.map(id=>pickRow(id,true,'src')).join('');
  $('#src-add').innerHTML=ic('plus')+(F.sources.length?'Add or change sources':'Choose source chats');
  $('#dst-picked').innerHTML=F.dest?pickRow(F.dest,true,'dst'):'';
  $('#dst-add').classList.toggle('hidden', !!F.dest);
  $$('[data-rm]').forEach(b=>b.onclick=()=>{ if(b.dataset.w==='src') F.sources=F.sources.filter(x=>x!==b.dataset.rm); else F.dest=null; drawRoute(); estimateSoon(); });
  $$('[data-sum]').forEach(b=>b.onclick=()=>showSummaryModal(b.dataset.sum));
  const n=[]; const dd=F.dest&&chatById(F.dest);
  if(dd && !dd.can_post) n.push(note('bad','alert','This account can\'t post in <b>'+esc(dd.name)+'</b>. Make it an admin with “Post messages”.'));
  if(dd && dd.can_post && !dd.can_pin && dd.kind!=='saved' && (($('#pin_first')||{}).checked || +($('#pin_every')||{}).value>0)) n.push(note('warn','pin','You can post in <b>'+esc(dd.name)+'</b> but not pin. Give the account the “Pin messages” right, or turn pinning off below.'));
  F.sources.forEach(id=>{ const d=chatById(id); if(d&&d.protected) n.push(note('warn','lock','<b>'+esc(d.name)+'</b> blocks forwarding. Files will be downloaded and re-uploaded — this works, but it\'s slower and uses server disk while each file is in transit.')); });
  $('#route-notes').innerHTML=n.join('');
  F.sources.concat(F.dest?[F.dest]:[]).forEach(id=>{ if(!S.summaries[id]) getSummary(id).then(()=>{ drawRoute(); drawTypes(typeCounts()); estimateSoon(); }).catch(()=>{}); });
  drawTypes(typeCounts());
}
$('#src-add').onclick=()=>openPicker({title:'Choose source chats', multi:true, selected:F.sources, exclude:F.dest?[F.dest]:[], onDone:ids=>{ F.sources=ids; drawRoute(); estimateSoon(); }});
$('#dst-add').onclick=()=>openPicker({title:'Choose the destination', dest:true, exclude:F.sources, onDone:ids=>{ F.dest=ids[0]; drawRoute(); estimateSoon(); }});
['pin_first','pin_every'].forEach(id=>$('#'+id).addEventListener('change',drawRoute));

const CFG_INPUTS=['latest_n','date_from','date_to','from_id','to_id','caption_template','replace_rules','footer','min_size_mb','max_size_mb','min_duration','extensions','include_words','exclude_words','pin_every'];
const CFG_SWITCH=['skip_dupes','check_dest','pin_first','hide_pin_notice','strip_links','strip_mentions','silent','notify'];
function readCfg(){ const c={types:Array.from(F.types), range:F.range, mode:F.mode, caption_mode:F.cap, speed:F.speed, keep_live:F.after!=='stop'};
  CFG_INPUTS.forEach(k=>c[k]=$('#'+k).value); CFG_SWITCH.forEach(k=>c[k]=$('#'+k).checked);
  if(c.pin_every==='') c.pin_every=0; return c; }
function writeCfg(c){ if(!c) return; F.types=new Set(c.types||[]); F.range=c.range||'all'; F.mode=c.mode||'copy'; F.cap=c.caption_mode||'keep'; F.speed=c.speed||'normal'; F.after=c.keep_live?'live':'stop';
  CFG_INPUTS.forEach(k=>{ if(c[k]!=null) $('#'+k).value = (c[k]===0 && !['pin_every','latest_n'].includes(k)) ? '' : c[k]; }); CFG_SWITCH.forEach(k=>{ if(c[k]!=null) $('#'+k).checked=!!c[k]; });
  segSet('range-seg','r',F.range); segSet('mode-seg','m',F.mode); segSet('cap-seg','c',F.cap); segSet('speed-seg','s',F.speed); segSet('after-seg','a',F.after);
  ['latest','dates','ids'].forEach(r=>$('#range-'+r).classList.toggle('hidden',F.range!==r)); $('#cap-tpl').classList.toggle('hidden',F.cap!=='replace'); drawTypes(typeCounts()); }

let estT=null; function estimateSoon(){ clearTimeout(estT); estT=setTimeout(estimate, 500); }
async function estimate(){
  if(!F.sources.length){ $('#est-txt').textContent='Choose a route to see an estimate'; $('#est-sub').textContent='Nothing is sent until you press Start.'; return; }
  if(!F.types.size){ $('#est-txt').textContent='Tick at least one content type'; return; }
  try { const r=await api('/api/estimate','POST',{sources:F.sources, config:readCfg()});
    const tot=r.sources.reduce((a,s)=>a+s.estimate,0);
    const rate = {safe:2.5, normal:10, fast:25}[F.speed] * (r.sources.some(s=>s.protected)?0.15:1);
    $('#est-txt').textContent = (F.range==='dates'||F.range==='ids'?'Up to ':'About ')+fmtN(tot)+' messages';
    $('#est-sub').textContent = (F.after==='liveonly'?'History skipped — only new posts will be copied. ':'Roughly '+dur(tot/rate)+' at this speed, before duplicate skipping. ')+(r.sources.length>1?r.sources.length+' transfers will run side by side.':'');
  } catch(e){ $('#est-sub').textContent=e.message; }
}
$('#est-btn').onclick=estimate;
$('#start-btn').onclick=()=>act($('#start-btn'), async()=>{
  if(!F.sources.length||!F.dest) throw new Error('Pick at least one source and a destination');
  if(!F.types.size) throw new Error('Tick at least one content type');
  const cfg=readCfg();
  if(F.after==='liveonly'){
    for(const s of F.sources) await api('/api/live','POST',{source:s, dest:F.dest, config:cfg});
    toast('Live sync started — new posts will be copied'); go('live'); return; }
  const r=await api('/api/jobs','POST',{sources:F.sources, dest:F.dest, config:cfg});
  const upd=r.jobs.filter(j=>j.updated).length;
  toast(upd?`Updated ${upd} running transfer(s) with the new settings`:`Started ${r.jobs.length} transfer(s). You can close this tab.`);
  go('jobs');
});
async function loadPresets(){ try { const r=await api('/api/presets'); S.presets=r.presets;
  $('#preset-sel').innerHTML='<option value="">Load a preset…</option>'+Object.keys(r.presets).map(n=>`<option>${esc(n)}</option>`).join(''); } catch(e){} }
$('#preset-sel').onchange=e=>{ const p=S.presets[e.target.value]; if(p){ writeCfg(p); toast('Preset “'+e.target.value+'” loaded'); estimateSoon(); } };
$('#preset-save').onclick=()=>{ const root=modal('Save settings as a preset',`<div class="pad stack"><input class="in" id="ps-name" placeholder="e.g. Movies only, no captions" maxlength="40"><div class="row"><span class="sp"></span><button class="btn pri" id="ps-ok">Save preset</button></div></div>`);
  $('#ps-name',root).focus(); $('#ps-ok',root).onclick=()=>act($('#ps-ok',root), async()=>{ await api('/api/presets','POST',{name:$('#ps-name',root).value, config:readCfg()}); root.innerHTML=''; toast('Preset saved'); loadPresets(); }); };

/* ═══ Job cards ═══ */
function routeTicket(j){
  const s=chatById(j.source)||{id:j.source,name:j.source_name,kind:'channel'}, d=chatById(j.dest)||{id:j.dest,name:j.dest_name,kind:'channel'};
  const pct=j.pct==null?0:j.pct; const pe=j.config&&j.config.pin_every; let pins='';
  if(pe>0 && j.top_id){ const est=Math.max(j.done, Math.round(j.done/Math.max(pct,1)*100)); for(let k=pe;k<=est && k/pe<30;k+=pe) pins+=`<i style="left:${Math.min(100,k/est*100)}%"></i>`; }
  return `<div class="route"><div class="stop">${avatar(s)}<div><div class="sub">From</div><div class="nm">${esc(j.source_name||s.name)}</div></div></div>
   <div class="track"><span class="fill" style="width:${pct}%"></span><span class="pins">${pins}</span><span class="pkt" style="left:${pct}%"></span></div>
   <div class="stop end">${avatar(d)}<div><div class="sub">To</div><div class="nm">${esc(j.dest_name||d.name)}</div></div></div></div>`;
}
function jobCard(j){
  const st=j.status; const c=j.config||{}; const types=(c.types||[]).filter(t=>t!=='text');
  const extra=[]; if(j.flood_wait>0) extra.push(`<span class="badge amber">Telegram cooldown ${dur(j.flood_wait)}</span>`);
  if(st==='running' && j.rate) extra.push(`<span class="badge">${j.rate.toFixed(1)} msg/s</span>`);
  if(j.eta) extra.push(`<span class="badge">≈ ${dur(j.eta)} left</span>`);
  if(c.keep_live) extra.push('<span class="badge red">→ live after</span>');
  const a=[];
  if(st==='running'||st==='queued') a.push(`<button class="btn sm" data-ja="pause">${ic('pause','sm')}Pause</button>`);
  if(st==='paused'||st==='error'||st==='stopped') a.push(`<button class="btn sm pri" data-ja="resume">${ic('play','sm')}Resume</button>`);
  if(st==='done') a.push(`<button class="btn sm" data-ja="continue">${ic('refresh','sm')}Get new posts</button>`, `<button class="btn sm" data-ja="live">${ic('live','sm')}Keep in sync</button>`);
  if(j.failed_count) a.push(`<button class="btn sm" data-ja="retry">${ic('refresh','sm')}Retry ${fmtN(j.failed_count)} failed</button>`);
  a.push(`<button class="btn sm ghost" data-ja="details">${ic('log','sm')}Details</button>`);
  if(st!=='running'&&st!=='queued') a.push(`<button class="btn sm ghost danger" data-ja="delete">${ic('trash','sm')}</button>`);
  else a.push(`<button class="btn sm ghost danger" data-ja="stop">${ic('stop','sm')}Stop</button>`);
  return `<div class="job ${st}" data-key="${esc(j.job_key)}">
   <div class="job-top"><span class="st ${st}">${st}</span><span class="small muted">${types.length?esc(types.join(' · ')):'—'} · text ${c.types&&c.types.includes('text')?'on':'skipped'}</span>${extra.join('')}<span class="sp"></span><span class="tiny faint">${ago(j.updated_at)}</span></div>
   ${routeTicket(j)}
   ${j.error?`<div class="note bad small" style="margin-top:12px">${ic('alert','sm')}<div>${esc(j.error)}</div></div>`:''}
   <div class="nums"><div class="num green"><b>${fmtN(j.done)}</b><span>sent</span></div><div class="num"><b>${j.pct==null?'—':j.pct+'%'}</b><span>progress</span></div>
    <div class="num amber"><b>${fmtN(j.dupes)}</b><span>duplicates</span></div><div class="num"><b>${fmtN(j.skipped)}</b><span>skipped</span></div>
    <div class="num ${j.errors?'red':''}"><b>${fmtN(j.errors)}</b><span>failed</span></div><div class="num"><b>${human(j.bytes)}</b><span>moved</span></div></div>
   <div class="acts">${a.join('')}</div></div>`;
}
function bindJobActs(root){ $$('[data-ja]',root).forEach(b=>b.onclick=()=>{ const key=b.closest('[data-key]').dataset.key, a=b.dataset.ja;
  if(a==='details') return openJob(key);
  act(b, async()=>{ if(a==='delete' && !await confirmBox('Delete this transfer?','Its history and logs are removed. Duplicate memory is kept, so files already sent still won\'t be sent again.','Delete',true)) return;
    if(a==='stop' && !await confirmBox('Stop this transfer?','You can resume it later from the same spot.','Stop')) return;
    const r=await api(`/api/jobs/${key}/${a}`,'POST',{}); const msg={pause:'Paused',resume:'Resumed',continue:'Checking for new posts…',retry:'Retrying failed messages',live:'Live sync started',delete:'Deleted',stop:'Stopped'}[a]; toast(msg);
    if(a==='live') go('live'); else refreshView(); }); }); }

async function renderJobs(){
  const box=$('#jobs-list'); let r; try { r=await api('/api/jobs?kind=forward'); } catch(e){ box.innerHTML=`<div class="empty">${ic('alert')}<h3>Couldn't load transfers</h3><p>${esc(e.message)}</p></div>`; return; }
  let js=r.jobs; const f=S.jobsFilter;
  if(f==='active') js=js.filter(j=>['running','queued','paused','error'].includes(j.status)); if(f==='done') js=js.filter(j=>['done','stopped'].includes(j.status));
  box.innerHTML = js.length ? js.map(jobCard).join('') : `<div class="empty">${ic('send')}<h3>${f==='active'?'Nothing running':'No transfers yet'}</h3><p>Start one and it'll show up here.</p><div style="margin-top:14px"><button class="btn pri" data-go="new">${ic('plus','sm')}New transfer</button></div></div>`;
  bindJobActs(box);
}
$$('#jobs-filter button').forEach(b=>b.onclick=()=>{ S.jobsFilter=b.dataset.f; segSet('jobs-filter','f',b.dataset.f); renderJobs(); });
async function renderHome(){
  try { const r=await api('/api/jobs?kind=forward'); const js=r.jobs.filter(j=>['running','queued','paused','error'].includes(j.status)).slice(0,4);
    $('#home-jobs').innerHTML = js.length ? js.map(jobCard).join('') : `<div class="empty">${ic('send')}<h3>No active transfers</h3><p>When you start one, you can follow it here — or close the tab and check back later.</p></div>`;
    bindJobActs($('#home-jobs')); } catch(e){}
}

/* ═══ Job detail modal ═══ */
async function openJob(key){
  const root=modal('Transfer details', '<div class="pad" id="jd"><span class="spin"></span></div>', true); let lastId=0; let logs=[];
  async function tick(){ if(!$('#jd',root)) return; try {
    const r=await api(`/api/jobs/${key}`+(lastId?`?after=${lastId}`:'')); const j=r.job; logs=logs.concat(r.logs); if(logs.length>1000) logs=logs.slice(-1000); if(r.logs.length) lastId=r.logs[r.logs.length-1].id;
    const c=j.config; const ro=(k,v)=>`<dt>${k}</dt><dd>${v}</dd>`;
    const rng={all:'Everything',latest:'Latest '+fmtN(c.latest_n),dates:(c.date_from||'start')+' → '+(c.date_to||'now'),ids:'#'+(c.from_id||1)+' → '+(c.to_id?'#'+c.to_id:'latest')}[c.range];
    const logEl=$('#jd-logs',root); const stick=!logEl||logEl.scrollTop+logEl.clientHeight>=logEl.scrollHeight-20;
    $('#jd',root).innerHTML=`<div class="stack">${routeTicket(j)}
     <div class="nums"><div class="num green"><b>${fmtN(j.done)}</b><span>sent</span></div><div class="num"><b>${j.pct==null?'—':j.pct+'%'}</b><span>progress</span></div><div class="num amber"><b>${fmtN(j.dupes)}</b><span>duplicates</span></div><div class="num"><b>${fmtN(j.skipped)}</b><span>skipped</span></div><div class="num ${j.errors?'red':''}"><b>${fmtN(j.errors)}</b><span>failed</span></div><div class="num"><b>${human(j.bytes)}</b><span>moved</span></div></div>
     <div class="grid g2"><dl class="kv">${ro('Status','<span class="st '+j.status+'">'+j.status+'</span>')}${ro('Messages',rng)}${ro('Types',esc((c.types||[]).join(', ')))}${ro('Mode',c.mode==='copy'?'Copy (no “Forwarded from”)':'Forward')}${ro('Captions',c.caption_mode)}</dl>
      <dl class="kv">${ro('Pinning',(c.pin_first?'first':'')+(c.pin_first&&c.pin_every?' + ':'')+(c.pin_every?'every '+c.pin_every:'')||'off')}${ro('Duplicates',c.skip_dupes?'skipped':'allowed')}${ro('Speed',c.speed)}${ro('Position','#'+fmtN(j.cursor)+' of #'+fmtN(j.top_id))}${ro('Started',j.started_at?ago(j.started_at):'—')}</dl></div>
     <div class="row wrap"><h3 style="flex:1">Activity</h3><a class="btn sm" href="/api/jobs/${key}/logs.txt">${ic('down','sm')}Log</a>${j.failed_count?`<a class="btn sm" href="/api/jobs/${key}/failed.csv">${ic('down','sm')}Failed list</a>`:''}<button class="btn sm" id="jd-edit">${ic('swap','sm')}Change settings</button></div>
     <div class="logs" id="jd-logs">${logs.map(l=>`<div class="l-${l.level}"><time>${new Date(l.created_at).toLocaleTimeString()}</time>${esc(l.message)}</div>`).join('')||'<span class="faint">No activity yet</span>'}</div></div>`;
    const L=$('#jd-logs',root); if(stick) L.scrollTop=L.scrollHeight;
    $('#jd-edit',root).onclick=()=>editJob(j);
  } catch(e){ } }
  await tick(); S.modalTick=tick;
}
function editJob(j){ const c=j.config;
  const root=modal('Change settings', `<div class="pad stack"><p class="small muted">Applied from the next batch — the transfer keeps its place.</p>
   <div class="chipset" id="ej-types">${TYPES.map(t=>`<span class="tchip ${c.types.includes(t[0])?'on':''}" data-t="${t[0]}"><span class="box"></span>${t[1]}</span>`).join('')}</div>
   <label class="switch"><input type="checkbox" id="ej-pf" ${c.pin_first?'checked':''}><span class="tr"></span><span><b>Pin the first message</b></span></label>
   <div class="row"><span class="small muted">Pin every</span><input class="in" id="ej-pe" type="number" min="0" value="${c.pin_every}" style="width:110px"><span class="small muted">messages</span></div>
   <label class="switch"><input type="checkbox" id="ej-sd" ${c.skip_dupes?'checked':''}><span class="tr"></span><span><b>Skip duplicates</b></span></label>
   <div class="seg" id="ej-sp">${['safe','normal','fast'].map(s=>`<button data-s="${s}" class="${c.speed===s?'on':''}">${{safe:'Gentle',normal:'Balanced',fast:'Fast'}[s]}</button>`).join('')}</div>
   <div class="row"><span class="sp"></span><button class="btn pri" id="ej-ok">Apply</button></div></div>`);
  $$('#ej-types .tchip',root).forEach(el=>el.onclick=()=>el.classList.toggle('on'));
  let sp=c.speed; $$('#ej-sp button',root).forEach(b=>b.onclick=()=>{ sp=b.dataset.s; $$('#ej-sp button',root).forEach(x=>x.classList.toggle('on',x===b)); });
  $('#ej-ok',root).onclick=()=>act($('#ej-ok',root), async()=>{ const types=$$('#ej-types .tchip.on',root).map(e=>e.dataset.t); if(!types.length) throw new Error('Tick at least one type');
    await api(`/api/jobs/${j.job_key}/config`,'POST',{config:{types, pin_first:$('#ej-pf',root).checked, pin_every:$('#ej-pe',root).value||0, skip_dupes:$('#ej-sd',root).checked, speed:sp}}); toast('Settings applied'); openJob(j.job_key); });
}
'''

HTML += r'''
/* ═══ Live sync ═══ */
async function renderLive(){
  const box=$('#live-list'); let r; try { r=await api('/api/live'); } catch(e){ box.innerHTML=`<div class="empty">${ic('alert')}<h3>Couldn't load live syncs</h3><p>${esc(e.message)}</p></div>`; return; }
  if(!r.live.length){ box.innerHTML=`<div class="empty">${ic('live')}<h3>No live syncs</h3><p>Mirror a channel: every new post is copied within seconds, even with this tab closed.</p><div style="margin-top:14px"><button class="btn pri" id="live-new2">${ic('plus','sm')}New live sync</button></div></div>`; $('#live-new2').onclick=newLive; return; }
  box.innerHTML=r.live.map(l=>{ const c=l.config||{}; const on=l.active; const s=chatById(l.source)||{id:l.source,name:l.source_name}, d=chatById(l.dest)||{id:l.dest,name:l.dest_name};
    return `<div class="job" data-lid="${esc(l.id)}"><div class="job-top"><span class="dot ${on&&l.attached?'live':''}"></span><span class="st ${on?'running':'paused'}">${on?(l.attached?'live':'connecting'):'paused'}</span>
     <span class="small muted">${esc((c.types||[]).filter(t=>t!=='text').join(' · '))} · text ${(c.types||[]).includes('text')?'on':'skipped'}</span><span class="sp"></span><span class="tiny faint">last copy ${ago(l.last_at)}</span></div>
     <div class="route"><div class="stop">${avatar(s)}<div><div class="sub">From</div><div class="nm">${esc(l.source_name)}</div></div></div><div class="track"><span class="fill" style="width:100%;opacity:${on?1:.3}"></span>${on?'<span class="pkt" style="left:50%"></span>':''}</div><div class="stop end">${avatar(d)}<div><div class="sub">To</div><div class="nm">${esc(l.dest_name)}</div></div></div></div>
     ${l.error?`<div class="note bad small" style="margin-top:12px">${ic('alert','sm')}<div>${esc(l.error)}</div></div>`:''}
     <div class="nums"><div class="num green"><b>${fmtN(l.done)}</b><span>sent</span></div><div class="num amber"><b>${fmtN(l.dupes)}</b><span>duplicates</span></div><div class="num"><b>${fmtN(l.skipped)}</b><span>skipped</span></div><div class="num ${l.errors?'red':''}"><b>${fmtN(l.errors)}</b><span>failed</span></div><div class="num"><b>${human(l.bytes)}</b><span>moved</span></div></div>
     <div class="acts">${on?`<button class="btn sm" data-la="pause">${ic('pause','sm')}Pause</button>`:`<button class="btn sm pri" data-la="resume">${ic('play','sm')}Resume</button>`}<button class="btn sm ghost" data-la="logs">${ic('log','sm')}Activity</button><button class="btn sm ghost danger" data-la="delete">${ic('trash','sm')}</button></div></div>`; }).join('');
  $$('[data-la]',box).forEach(b=>b.onclick=()=>{ const id=b.closest('[data-lid]').dataset.lid, a=b.dataset.la;
    if(a==='logs') return liveLogs(id);
    act(b, async()=>{ if(a==='delete' && !await confirmBox('Delete this live sync?','New posts will stop being copied.','Delete',true)) return; await api(`/api/live/${id}/${a}`,'POST',{}); toast({pause:'Paused',resume:'Resumed',delete:'Deleted'}[a]); renderLive(); loadStatus(); }); });
}
async function liveLogs(id){ const root=modal('Live sync activity','<div class="pad"><div class="logs" id="ll"><span class="spin"></span></div></div>',true);
  const tick=async()=>{ if(!$('#ll',root)) return; try { const r=await api(`/api/live/${id}/logs`); const L=$('#ll',root); L.innerHTML=r.logs.map(l=>`<div class="l-${l.level}"><time>${new Date(l.created_at).toLocaleString()}</time>${esc(l.message)}</div>`).join('')||'<span class="faint">Nothing yet — waiting for new posts</span>'; L.scrollTop=L.scrollHeight; } catch(e){} };
  await tick(); S.modalTick=tick; }
function newLive(){ toast('Choose route and options, then pick “Skip history — only new posts”'); F.after='liveonly'; segSet('after-seg','a','liveonly'); go('new'); }
$('#live-new').onclick=newLive;

/* ═══ Duplicates ═══ */
const SC={chat:null, limit:0, types:new Set(['video','photo','document','audio'])};
function drawScanPick(){ $('#scan-picked').innerHTML=SC.chat?pickRow(SC.chat,true,'scan'):''; $('#scan-add').classList.toggle('hidden',!!SC.chat);
  $$('#scan-picked [data-rm]').forEach(b=>b.onclick=()=>{ SC.chat=null; drawScanPick(); }); $$('#scan-picked [data-sum]').forEach(b=>b.onclick=()=>showSummaryModal(b.dataset.sum));
  if(SC.chat && !S.summaries[SC.chat]) getSummary(SC.chat).then(drawScanPick).catch(()=>{}); }
function drawScanTypes(){ $('#scan-types').innerHTML=TYPES.filter(t=>t[0]!=='sticker').map(t=>`<span class="tchip ${SC.types.has(t[0])?'on':''}" data-t="${t[0]}"><span class="box"></span>${t[1]}</span>`).join('');
  $$('#scan-types .tchip').forEach(el=>el.onclick=()=>{ SC.types.has(el.dataset.t)?SC.types.delete(el.dataset.t):SC.types.add(el.dataset.t); drawScanTypes(); }); }
$('#scan-add').onclick=()=>openPicker({title:'Choose a chat to scan', onDone:ids=>{ SC.chat=ids[0]; drawScanPick(); }});
$$('#scan-seg button').forEach(b=>b.onclick=()=>{ SC.limit=+b.dataset.l; segSet('scan-seg','l',b.dataset.l); });
$('#scan-btn').onclick=()=>act($('#scan-btn'), async()=>{ if(!SC.chat) throw new Error('Choose a chat first'); if(!SC.types.size) throw new Error('Tick at least one type');
  const r=await api('/api/scan','POST',{chat:SC.chat, limit:SC.limit, types:Array.from(SC.types)}); toast(r.existing?'A scan of this chat is already running':'Scan started — it runs in the background'); renderScans(); });
async function renderScans(){
  const box=$('#scan-list'); let r; try { r=await api('/api/jobs?kind=scan'); } catch(e){ return; }
  if(!r.jobs.length){ box.innerHTML=`<div class="empty">${ic('copy')}<h3>No scans yet</h3><p>Results stay here so you can come back to them.</p></div>`; return; }
  box.innerHTML=r.jobs.map(j=>{ const res=j.result&&typeof j.result==='object'?j.result:null; const run=j.status==='running'||j.status==='queued';
    return `<div class="job" data-key="${esc(j.job_key)}"><div class="row wrap">${avatar(chatById(j.source)||{id:j.source,name:j.source_name},'sm')}<div style="flex:1;min-width:0"><div style="font-weight:600">${esc(j.source_name)}</div><div class="tiny faint">${ago(j.updated_at)}</div></div><span class="st ${j.status}">${j.status}</span></div>
     <div class="nums"><div class="num"><b>${fmtN(j.processed)}</b><span>messages scanned</span></div>${res?`<div class="num amber"><b>${fmtN(res.groups)}</b><span>duplicate groups</span></div><div class="num red"><b>${fmtN(res.dup_messages)}</b><span>extra copies</span></div><div class="num"><b>${human(res.dup_bytes)}</b><span>wasted</span></div><div class="num"><b>${human(j.bytes)}</b><span>total size</span></div>`:''}</div>
     ${j.error?`<div class="note bad small" style="margin-top:12px">${ic('alert','sm')}<div>${esc(j.error)}</div></div>`:''}
     <div class="acts">${res?`<button class="btn sm pri" data-sa="open">${ic('copy','sm')}Review duplicates</button><a class="btn sm" href="/api/scan/${j.job_key}/catalog.csv">${ic('down','sm')}File list (CSV)</a>`:''}${run?`<button class="btn sm danger" data-sa="stop">${ic('stop','sm')}Stop</button>`:`<button class="btn sm ghost danger" data-sa="delete">${ic('trash','sm')}</button>`}</div></div>`; }).join('');
  $$('[data-sa]',box).forEach(b=>b.onclick=()=>{ const key=b.closest('[data-key]').dataset.key, a=b.dataset.sa; if(a==='open') return openScan(key);
    act(b, async()=>{ await api(`/api/jobs/${key}/${a}`,'POST',{}); renderScans(); }); });
}
async function openScan(key){
  const r=await api('/api/jobs/'+key); const j=r.job, res=j.result, base=res.link||''; const d=chatById(j.source);
  const canDel=d&&(d.admin||d.kind==='saved'||d.kind==='user');
  const gl=j.groups.map(g=>`<label class="dgroup"><input type="checkbox" value="${esc(g.fp)}" ${canDel?'':'disabled'}><span class="badge">${esc(g.mtype)}</span>
    <div style="flex:1;min-width:0"><div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(g.name||('Message #'+g.keep_id))}</div>
    <div class="tiny faint">keep ${base?`<a href="${esc(base+g.keep_id)}" target="_blank" rel="noopener">#${g.keep_id}</a>`:'#'+g.keep_id} · copies ${g.dup_ids.slice(0,8).map(i=>base?`<a href="${esc(base+i)}" target="_blank" rel="noopener">#${i}</a>`:'#'+i).join(' ')}${g.dup_ids.length>8?' …':''}</div></div>
    <div style="text-align:right"><div class="mono small">${g.dup_ids.length+1}×</div><div class="tiny faint">${human(g.size)} each</div></div></label>`).join('');
  const tb=Object.entries(res.by_type||{}).sort((a,b)=>b[1].bytes-a[1].bytes); const mx=Math.max(1,...tb.map(t=>t[1].bytes));
  const root=modal('Duplicates in '+esc(res.chat), `<div class="pad stack">
    <div class="grid g4"><div class="tile card"><div class="k">Scanned</div><div class="v">${fmtN(res.scanned)}</div></div><div class="tile card"><div class="k">Groups</div><div class="v">${fmtN(res.groups)}</div></div><div class="tile card"><div class="k">Extra copies</div><div class="v">${fmtN(res.dup_messages)}</div></div><div class="tile card"><div class="k">Wasted</div><div class="v">${human(res.dup_bytes)}</div></div></div>
    <div class="typebars">${tb.map(([t,v])=>`<div class="tb"><span>${esc(t)}</span><span class="b"><i style="width:${v.bytes/mx*100}%"></i></span><span class="c">${human(v.bytes)}</span></div>`).join('')}</div>
    ${canDel?'':note('warn','lock','You\'re not an admin here, so duplicates can be reviewed but not deleted.')}
    <div class="row wrap"><label class="row small"><input type="checkbox" id="sg-all" ${canDel?'':'disabled'}> Select all</label><span class="sp"></span><a class="btn sm" href="/api/scan/${key}/duplicates.csv">${ic('down','sm')}Export CSV</a><button class="btn sm danger" id="sg-del" ${canDel?'':'disabled'}>${ic('trash','sm')}Delete extra copies</button></div>
    </div><div style="border-top:1px solid var(--line2)">${gl||'<div class="empty"><h3>No duplicates found 🎉</h3></div>'}</div>`, true);
  const all=$('#sg-all',root); if(all) all.onchange=()=>$$('.dgroup input',root).forEach(c=>c.checked=all.checked);
  const del=$('#sg-del',root); if(del) del.onclick=()=>act(del, async()=>{ const fps=$$('.dgroup input:checked',root).map(c=>c.value); if(!fps.length) throw new Error('Select at least one group');
    const n=j.groups.filter(g=>fps.includes(g.fp)).reduce((a,g)=>a+g.dup_ids.length,0);
    if(!await confirmBox('Delete '+fmtN(n)+' messages?','The oldest copy of each file is kept. This can\'t be undone.','Delete '+fmtN(n),true)) return;
    const r2=await api(`/api/scan/${key}/delete`,'POST',{fps}); toast('Deleted '+fmtN(r2.deleted)+' duplicate messages'); openScan(key); });
}

/* ═══ Chats + summary ═══ */
let CH={filter:'all', q:''};
function drawChats(){
  const L=$('#chats-list'); if(!S.dialogs){ L.innerHTML='<div class="empty"><span class="spin"></span></div>'; return; }
  $('#chats-filters').innerHTML=PF.map(f=>`<button data-f="${f[0]}" class="${f[0]===CH.filter?'on':''}">${f[1]}</button>`).join('');
  $$('#chats-filters button').forEach(b=>b.onclick=()=>{ CH.filter=b.dataset.f; drawChats(); });
  const qq=CH.q.toLowerCase(); let list=S.dialogs.filter(d=>CH.filter==='all'||(CH.filter==='post'?d.can_post:CH.filter==='admin'?d.admin:d.kind===CH.filter));
  if(qq) list=list.filter(d=>(d.name||'').toLowerCase().includes(qq)||(d.username||'').toLowerCase().includes(qq.replace('@','')));
  L.innerHTML=list.slice(0,400).map(d=>`<div class="chat" data-id="${esc(d.id)}" tabindex="0">${avatar(d,'sm')}<div style="flex:1;min-width:0"><div class="nm">${esc(d.name)}</div><div class="meta">${esc(KIND[d.kind]||'')}${d.members?' · '+fmtN(d.members):''}</div></div>${d.admin?'<span class="badge blue">admin</span>':''}</div>`).join('')||'<div class="empty"><h3>No chats match</h3></div>';
  $$('.chat',L).forEach(el=>{ el.onclick=()=>{ $$('.chat',L).forEach(x=>x.classList.toggle('sel',x===el)); renderSummary(el.dataset.id, $('#chat-sum')); }; el.onkeydown=e=>{ if(e.key==='Enter') el.click(); }; });
}
$('#chats-q').oninput=e=>{ CH.q=e.target.value; drawChats(); };
$('#chats-refresh').onclick=()=>act($('#chats-refresh'), async()=>{ S.dialogs=null; drawChats(); await loadDialogs(true); drawChats(); toast('Chat list refreshed'); });
async function showSummaryModal(id){ const root=modal('Chat summary','<div class="pad" id="sm-b"><span class="spin"></span></div>',true); renderSummary(id, $('#sm-b',root)); }
async function renderSummary(id, el, refresh){
  el.innerHTML='<div class="empty"><span class="spin"></span><p class="small" style="margin-top:8px">Counting messages…</p></div>';
  let s; try { s=await getSummary(id, refresh); } catch(e){ el.innerHTML=`<div class="empty">${ic('alert')}<h3>Couldn't read this chat</h3><p class="small">${esc(e.message)}</p></div>`; return; }
  const d=chatById(id)||{id, name:s.name, kind:s.kind, has_photo:false};
  const rows=[['video','Videos'],['photo','Photos'],['document','Files'],['audio','Music'],['voice','Voice notes'],['gif','GIFs'],['round','Video notes'],['links','Links']];
  const mx=Math.max(1,...rows.map(r=>s.counts[r[0]]||0), s.text_other||0);
  const right=(ok,t)=>`<span class="badge ${ok?'green':'red'}">${ic(ok?'check':'x','sm')} ${t}</span>`;
  const ls=s.last_scan&&s.last_scan.result;
  el.innerHTML=`<div class="stack"><div class="sum-h">${avatar(d,'lg')}<div style="flex:1;min-width:0"><h2 style="overflow:hidden;text-overflow:ellipsis">${esc(s.name)}</h2><div class="small muted">${esc(KIND[s.kind]||'')}${s.username?' · @'+esc(s.username):''}${s.members?' · '+fmtN(s.members)+' members':''}</div></div><button class="icon-btn" id="sum-r" title="Recount">${ic('refresh','sm')}</button></div>
   ${s.about?`<p class="small muted">${esc(s.about)}</p>`:''}
   <div class="row wrap">${right(s.can_post,'can post')}${right(s.can_pin||s.kind==='saved','can pin')}${s.admin?'<span class="badge blue">admin</span>':''}${s.protected?'<span class="badge amber">'+ic('lock','sm')+' forwarding restricted</span>':''}</div>
   <div class="grid g3"><div class="card tile"><div class="k">Messages</div><div class="v">${fmtN(s.total)}</div></div><div class="card tile"><div class="k">First post</div><div class="v" style="font-size:18px">${dateStr(s.first_date)}</div></div><div class="card tile"><div class="k">Latest post</div><div class="v" style="font-size:18px">${ago(s.last_date)}</div></div></div>
   <div class="typebars">${rows.map(r=>`<div class="tb"><span>${r[1]}</span><span class="b"><i style="width:${(s.counts[r[0]]||0)/mx*100}%"></i></span><span class="c">${fmtN(s.counts[r[0]])}</span></div>`).join('')}<div class="tb"><span>Text & other</span><span class="b"><i style="width:${(s.text_other||0)/mx*100}%;background:var(--ink3)"></i></span><span class="c">${fmtN(s.text_other)}</span></div></div>
   ${ls?`<div class="note ${ls.dup_messages?'warn':'good'}">${ic('copy','sm')}<div>Last duplicate scan ${ago(s.last_scan.finished_at)}: <b>${fmtN(ls.dup_messages)} extra copies</b> wasting ${human(ls.dup_bytes)} across ${fmtN(ls.scanned)} messages. Total media size ${human(Object.values(ls.by_type||{}).reduce((a,v)=>a+v.bytes,0))}.</div></div>`:''}
   ${s.indexed?`<div class="small muted">${fmtN(s.indexed)} files remembered here for duplicate skipping.</div>`:''}
   ${s.as_source.length?`<div><h3 style="margin-bottom:6px">Copied from here</h3>${s.as_source.map(j=>`<div class="row small" style="padding:4px 0"><span class="st ${j.status}">${j.status}</span><span style="flex:1">→ ${esc(j.dest_name)}</span><span class="mono">${fmtN(j.done)} sent</span></div>`).join('')}</div>`:''}
   ${s.as_dest.length?`<div><h3 style="margin-bottom:6px">Copied into here</h3>${s.as_dest.map(j=>`<div class="row small" style="padding:4px 0"><span class="st ${j.status}">${j.status}</span><span style="flex:1">← ${esc(j.source_name)}</span><span class="mono">${fmtN(j.done)} sent</span></div>`).join('')}</div>`:''}
   <div class="row wrap"><button class="btn sm pri" id="sum-src">${ic('send','sm')}Copy from this chat</button><button class="btn sm" id="sum-dst">${ic('down','sm')}Copy into this chat</button><button class="btn sm" id="sum-scan">${ic('copy','sm')}Scan for duplicates</button>${s.link?`<a class="btn sm ghost" href="${esc(s.link)}" target="_blank" rel="noopener">Open in Telegram</a>`:''}</div></div>`;
  $('#sum-r',el).onclick=()=>renderSummary(id, el, true);
  $('#sum-src',el).onclick=()=>{ $('#modal-root').innerHTML=''; if(!F.sources.includes(id)) F.sources.push(id); if(F.dest===id) F.dest=null; drawRoute(); go('new'); estimateSoon(); };
  $('#sum-dst',el).onclick=()=>{ $('#modal-root').innerHTML=''; F.dest=id; F.sources=F.sources.filter(x=>x!==id); drawRoute(); go('new'); };
  $('#sum-scan',el).onclick=()=>{ $('#modal-root').innerHTML=''; SC.chat=id; drawScanPick(); go('dupes'); };
}
'''

HTML += r'''
/* ═══ Account ═══ */
let LOGIN={step:'phone', method:'phone'};
function renderAccount(){
  const s=S.status||{}; const c=$('#tg-card');
  if(s.connected && s.me){
    c.innerHTML=`<h2>Telegram account</h2><div class="row">${avatar({id:String(s.me.id),name:s.me.name,kind:'user'},'lg')}<div><div style="font-weight:700;font-size:17px">${esc(s.me.name)}</div><div class="small muted">${s.me.username?'@'+esc(s.me.username)+' · ':''}${esc(s.me.phone||'')}</div></div></div>
     ${note('good','check','Connected. The session is stored <b>encrypted</b> on your Railway volume, so the app reconnects by itself after restarts.')}
     <div class="row wrap"><button class="btn sm" id="tg-export">${ic('lock','sm')}Show session string</button><button class="btn sm" id="tg-switch">${ic('swap','sm')}Switch account</button><span class="sp"></span><button class="btn sm danger" id="tg-out">Disconnect</button></div>`;
    $('#tg-out').onclick=()=>act($('#tg-out'), async()=>{ if(!await confirmBox('Disconnect Telegram?','Running transfers pause until you connect again. Tick nothing else — your Telegram session on other devices is untouched.','Disconnect',true)) return; await api('/api/tg/logout','POST',{}); S.dialogs=null; toast('Disconnected'); await loadStatus(); renderAccount(); });
    $('#tg-switch').onclick=()=>{ S.forceLogin=true; renderAccount(); };
    $('#tg-export').onclick=()=>{ const root=modal('Session string',`<div class="pad stack">${note('bad','alert','Anyone with this string has full access to your Telegram account. Never share it.')}<input class="in" id="ex-pw" type="password" placeholder="Dashboard password"><div class="row"><span class="sp"></span><button class="btn pri" id="ex-ok">Reveal</button></div><textarea class="in hidden" id="ex-out" readonly></textarea></div>`);
      $('#ex-ok',root).onclick=()=>act($('#ex-ok',root), async()=>{ const r=await api('/api/tg/export','POST',{password:$('#ex-pw',root).value}); const o=$('#ex-out',root); o.value=r.session_string; o.classList.remove('hidden'); o.select(); }); };
    if(!S.forceLogin) return;
  }
  const m=LOGIN.method;
  c.innerHTML=`<h2>Connect Telegram</h2><p class="small muted">Log in with your phone number right here, or paste a session string you generated yourself.</p>
   <div class="seg" id="lg-m"><button data-m="phone" class="${m==='phone'?'on':''}">Phone number</button><button data-m="string" class="${m==='string'?'on':''}">Session string</button></div>
   <div id="lg-body" class="stack"></div>${S.forceLogin?'<button class="btn sm ghost" id="lg-cancel">Cancel</button>':''}`;
  $$('#lg-m button').forEach(b=>b.onclick=()=>{ LOGIN.method=b.dataset.m; LOGIN.step='phone'; renderAccount(); });
  if($('#lg-cancel')) $('#lg-cancel').onclick=()=>{ S.forceLogin=false; renderAccount(); };
  const B=$('#lg-body');
  const done=async()=>{ S.forceLogin=false; LOGIN.step='phone'; S.dialogs=null; S.summaries={}; toast('Telegram connected — resuming your transfers'); await loadStatus(); renderAccount(); };
  if(m==='string'){ B.innerHTML=`<textarea class="in" id="lg-ss" placeholder="Paste session string"></textarea><div class="hint">Generate it on your own computer with Telethon's StringSession. Never paste it into bots or sites you don't control.</div><button class="btn pri" id="lg-ss-btn">Connect</button>`;
    $('#lg-ss-btn').onclick=()=>act($('#lg-ss-btn'), async()=>{ await api('/api/tg/session','POST',{session_string:$('#lg-ss').value}); await done(); }); return; }
  if(LOGIN.step==='phone'){ B.innerHTML=`<ol class="steps"><li>Enter your number — Telegram sends a login code to your Telegram app.</li><li>Type the code here (and your 2-step password if you use one).</li><li>Done. The app keeps you logged in.</li></ol>
     <div><label class="f" for="lg-ph">Phone number</label><input class="in" id="lg-ph" type="tel" placeholder="+91 98765 43210" autocomplete="tel"></div><button class="btn pri" id="lg-send">Send code</button>`;
    $('#lg-send').onclick=()=>act($('#lg-send'), async()=>{ await api('/api/tg/send_code','POST',{phone:$('#lg-ph').value}); LOGIN.step='code'; renderAccount(); toast('Code sent — check your Telegram app'); });
  } else if(LOGIN.step==='code'){ B.innerHTML=`<div><label class="f" for="lg-code">Login code</label><input class="in mono" id="lg-code" inputmode="numeric" placeholder="12345" autocomplete="one-time-code" style="font-size:20px;letter-spacing:.3em"></div>
     <div class="hint">Tip: type it with spaces, like 1 2 3 4 5 — Telegram sometimes expires codes that were shared as plain text.</div><div class="row"><button class="btn pri" id="lg-in">Log in</button><button class="btn ghost" id="lg-back">Use another number</button></div>`;
    $('#lg-code').focus(); $('#lg-back').onclick=()=>{ LOGIN.step='phone'; renderAccount(); };
    $('#lg-in').onclick=()=>act($('#lg-in'), async()=>{ const r=await api('/api/tg/sign_in','POST',{code:$('#lg-code').value}); if(r.need_password){ LOGIN.step='pw'; renderAccount(); } else await done(); });
  } else { B.innerHTML=`<div><label class="f" for="lg-pw">Two-step verification password</label><input class="in" id="lg-pw" type="password" autocomplete="current-password"></div><button class="btn pri" id="lg-pw-btn">Finish login</button>`;
    $('#lg-pw').focus(); $('#lg-pw-btn').onclick=()=>act($('#lg-pw-btn'), async()=>{ await api('/api/tg/sign_in','POST',{password:$('#lg-pw').value}); await done(); }); }
}
function renderServer(){ const s=S.status; if(!s) return;
  const ro=(k,v)=>`<dt>${k}</dt><dd>${v}</dd>`;
  $('#srv-kv').innerHTML=ro('Storage',s.persistent?'<span class="badge green">Railway volume</span>':'<span class="badge red">temporary</span>')+ro('Free disk',human(s.disk.free)+' of '+human(s.disk.total))+ro('Database',human(s.db_size))+ro('Server up',dur(s.uptime))+ro('Transfers',fmtN((s.jobs.running||0))+' running · '+fmtN(s.jobs.queued||0)+' waiting · '+fmtN(s.jobs.paused||0)+' paused');
  $('#srv-note').innerHTML=s.persistent?note('good','db','Progress, logs and duplicate memory are saved on the volume and survive redeploys.'):note('warn','db','Attach a Railway volume to keep progress between deploys.');
  $('#max_jobs').value=s.max_jobs; $('#g-notify').checked=s.notify; $('#pw-card').classList.toggle('hidden', !!s.env_password);
}
$('#save-settings').onclick=()=>act($('#save-settings'), async()=>{ await api('/api/settings','POST',{max_jobs:$('#max_jobs').value, notify:$('#g-notify').checked}); toast('Saved'); loadStatus(); });
$('#fp-clear').onclick=()=>act($('#fp-clear'), async()=>{ if(!await confirmBox('Forget duplicate memory?','Files that were already sent could be sent again by future transfers.','Forget',true)) return; await api('/api/fingerprints/clear','POST',{}); toast('Duplicate memory cleared'); });
$('#pw-save').onclick=()=>act($('#pw-save'), async()=>{ await api('/api/auth/password','POST',{current:$('#pw-cur').value, new:$('#pw-new').value}); $('#pw-cur').value=$('#pw-new').value=''; toast('Password changed'); });
$('#signout').onclick=()=>act($('#signout'), async()=>{ await api('/api/auth/logout','POST',{}); showGate(); });

/* ═══ refresh loop ═══ */
function refreshView(){ const v=S.view;
  if(v==='home') renderHome(); else if(v==='jobs') renderJobs(); else if(v==='live') renderLive(); else if(v==='dupes'){ renderScans(); drawScanPick(); }
  else if(v==='chats'){ if(!S.dialogs) loadDialogs().then(drawChats).catch(e=>$('#chats-list').innerHTML=`<div class="empty">${ic('alert')}<h3>Couldn't load chats</h3><p class="small">${esc(e.message)}</p></div>`); drawChats(); }
  else if(v==='account'){ renderAccount(); renderServer(); }
}
let loopN=0;
async function loop(){ loopN++; if(document.hidden) return;
  if(S.modalTick) S.modalTick();
  if(loopN%2===0) loadStatus();
  if(['home','jobs','live','dupes'].includes(S.view) && !$('#modal-root').innerHTML) refreshView();
}
async function startApp(){
  $('#app').classList.remove('hidden');
  drawTypes(null); drawRoute(); drawScanTypes();
  try { const st=await api('/api/settings'); if(st.last_cfg) writeCfg(st.last_cfg); } catch(e){}
  loadPresets();
  await loadStatus();
  const h=location.hash.slice(1); go(['home','new','jobs','live','dupes','chats','account'].includes(h)?h:(S.status&&!S.status.connected?'account':'home'));
  if(S.status&&S.status.connected) loadDialogs().then(()=>{ if(S.view==='chats') drawChats(); drawRoute(); }).catch(()=>{});
  setInterval(loop, 2500);
}
fetch('/api/auth/state').then(r=>r.json()).then(st=>{ if(st.authed) startApp(); else showGate(); }).catch(()=>showGate());
</script>
</body>
</html>'''


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False, threaded=True)

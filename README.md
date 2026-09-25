# Relay v3 — Telegram transfer console (Railway)

Copies videos, files, photos and more between Telegram chats. Everything runs **on the server**, so you can close the browser tab: transfers, live syncs and duplicate scans keep going, and they resume by themselves after a restart or redeploy.

## Deploy / upgrade on Railway (5 minutes)

1. **Replace the files in your repo** with `app.py`, `requirements.txt` and `railway.json` from this folder, then push.
   `railway.json` sets the start command to **one** gunicorn worker. Keep it at one: the Telegram connection has to live in a single process.
2. **Attach a volume.** This is what lets it keep running after restarts.
   Right-click your service → **Attach volume** → any mount path (e.g. `/data`).
   The app detects it automatically through `RAILWAY_VOLUME_MOUNT_PATH`. Without a volume, progress, logs and duplicate memory are wiped on every deploy.
3. **Variables** (service → Variables):

   | Variable | Needed | What it is |
   |---|---|---|
   | `API_ID`, `API_HASH` | yes | from https://my.telegram.org → API development tools |
   | `SECRET_KEY` | recommended | any long random text; it encrypts the saved Telegram session |
   | `APP_PASSWORD` | optional | dashboard password. If you leave it out, you choose one the first time you open the site |
   | `MAX_CONCURRENT_JOBS` | optional | how many transfers run at once (default 3; extra ones wait in line) |
   | `SESSION_STRING` | optional | only if you'd rather not log in from the dashboard |

4. **Serverless / App Sleeping must be OFF** (service → Settings). A sleeping service stops the transfers.
5. Open your Railway URL → set a password → **Account** → log in with your phone number (or paste a session string).

## What's new in v3

- **Keeps running with the tab closed.** Jobs are saved to the database after every batch. After a crash or redeploy they resume from the exact message where they stopped: nothing is re-sent and nothing is skipped. Live syncs re-attach and **catch up on posts made while the server was down**.
- **Pick chats from a list.** Search your chats with avatars and filters (Channels, Groups, I can post, I'm admin). Badges warn you when the destination doesn't let you post or pin, or when a source blocks forwarding.
- **Chat summary.** Counts of videos, photos, files, music, voice notes, GIFs and links; first and latest post dates; members; your rights; transfers from or into that chat; and the result of the last duplicate scan.
- **Duplicate scanner.** Scan any chat, see duplicate groups and wasted space, open the copies in Telegram, export CSVs (a duplicate list plus a full file catalog), and delete the extra copies with one click (the oldest copy is kept).
- **No duplicates in transfers.** Files are matched by exact size, type and length, so nothing needs downloading. It works across all transfers and live syncs, and can first read the destination so files already there are skipped too.
- **Pinning.** Pins the first message and every Nth message you *send* (default 200). The "pinned a message" notices are hidden. If you don't have pin rights, the log says so plainly.
- **Text is skipped unless you tick Text.** Service messages (joins, pins) are always skipped.
- **Fast and in order.** Up to 100 messages are forwarded per request, oldest first, and albums stay together.
- **Protected channels** (forwarding restricted) work too. Each file is downloaded to the volume, re-uploaded, then deleted; this needs roughly the file's size in free disk while it's in transit.
- **Caption tools.** Copy without "Forwarded from", keep or remove captions, use a template (`{caption} {filename} {size} {duration} {date} {n} {source}`), strip links and @mentions, replace words, add a footer.
- **Filters.** Latest N, date range, message IDs, min/max size, minimum length, file extensions, and include/exclude keywords.
- **Controls.** Pause, resume or stop; change settings while a job runs; "Get new posts" on finished jobs; one-click "Keep in sync"; retry failed messages; download the log and a list of failures; save presets.
- **Security.** The dashboard is password-protected (with a lockout after 8 wrong tries), requests are CSRF-protected, and the session is stored encrypted. Backups never include your session or password.
- **Notifications.** A message in your Saved Messages when a transfer finishes or fails.

## Good to know

- Speed: *Balanced* is the right choice for most channels. Telegram rate limits (FloodWait) are waited out automatically, and the countdown shows on the job.
- Deleting duplicates in a channel needs the admin **Delete messages** right. Pinning needs **Pin messages**.
- Your old v2 database is kept as a `jobs_legacy` table; v3 starts a fresh job list.

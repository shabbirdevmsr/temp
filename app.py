"""
FAST + BATCH version: re-send videos from `vid65` to a channel with a proper thumbnail/cover,
then save the new video file_id + message_id to MySQL.

Requirements:  pip install requests pillow pymysql

How it is fast:
  - Multi-threaded (--workers) with a shared connection pool
  - Global rate limiter (--rate msgs/sec) + automatic global pause on Telegram 429 flood limits
  - Batch mode: rows are fetched in batches (--batch-size), each batch runs in parallel,
    DB is updated as each row finishes (safe to Ctrl+C and resume anytime)

Usage examples:
  python rethumb_fast.py --limit 10                       # quick test
  python rethumb_fast.py                                  # everything, defaults
  python rethumb_fast.py --workers 10 --rate 8 --batch-size 100
  python rethumb_fast.py --mode upload                    # re-upload video with thumbnail
  python rethumb_fast.py --retry-failed                   # retry rows marked FAILED
  python rethumb_fast.py --workers 1 --rate 1             # keep channel order strictly by id

Note: with several workers, posts in the channel can appear slightly out of id order.
"""
import argparse
import html
import io
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pymysql
import requests
from PIL import Image
from requests.adapters import HTTPAdapter

BOT_TOKEN = "6757665465:AAEG8h6CRtDvm_5tVihxhcOewze9TcrsvwM"
CHAT_ID = -1003989082700
API_SERVER = "https://api.telegram.org"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "vidtg",
}

MAX_RETRIES = 4
STOP = threading.Event()

SESSION = requests.Session()
_adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


class Cancelled(Exception):
    pass


class Limiter:
    """Shared rate limiter. pause() delays everyone (used on 429)."""

    def __init__(self, rate):
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self.lock = threading.Lock()
        self.next_time = 0.0
        self.last_slow = 0.0

    def slow_down(self, factor=1.25):
        """Lower our own rate after a 429 (once per flood event, not once per thread)."""
        with self.lock:
            now = time.monotonic()
            if now - self.last_slow < 10:
                return None
            self.last_slow = now
            self.interval = min(max(self.interval * factor, 0.25), 3.0)
            return 1.0 / self.interval

    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_time)
            self.next_time = slot + self.interval
        delay = slot - now
        if delay > 0:
            time.sleep(delay)

    def pause(self, seconds):
        with self.lock:
            self.next_time = max(self.next_time, time.monotonic() + seconds)


LIMITER = Limiter(3)


def api_url(method):
    return f"{API_SERVER.rstrip('/')}/bot{BOT_TOKEN}/{method}"


# ---------------------------------------------------------------- helpers
def download_by_file_id(file_id):
    r = SESSION.post(api_url("getFile"), data={"file_id": file_id}, timeout=30).json()
    if not r.get("ok"):
        raise RuntimeError(f"getFile failed: {r.get('description')}")
    file_path = r["result"]["file_path"]
    if os.path.isabs(file_path) and os.path.exists(file_path):  # local Bot API server
        with open(file_path, "rb") as f:
            return f.read()
    url = f"{API_SERVER.rstrip('/')}/file/bot{BOT_TOKEN}/{file_path}"
    resp = SESSION.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def make_tg_thumb(image_bytes):
    """JPEG, max 320x320, <200 KB, otherwise Telegram silently ignores it."""
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    img.thumbnail((320, 320))
    quality = 85
    while True:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        if buf.tell() <= 200 * 1024 or quality <= 20:
            return buf.getvalue()
        quality -= 10


def download_video_to_tmp(video_file_id):
    r = SESSION.post(api_url("getFile"), data={"file_id": video_file_id}, timeout=60).json()
    if not r.get("ok"):
        raise RuntimeError(f"getFile(video) failed: {r.get('description')}")
    file_path = r["result"]["file_path"]
    if os.path.isabs(file_path) and os.path.exists(file_path):  # local server: already on disk
        return file_path, False
    tmp = os.path.join(tempfile.gettempdir(), f"vid_{threading.get_ident()}_{time.time_ns()}.mp4")
    url = f"{API_SERVER.rstrip('/')}/file/bot{BOT_TOKEN}/{file_path}"
    with SESSION.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            shutil.copyfileobj(resp.raw, f)
    return tmp, True


def _send(data, make_files=None):
    last = None
    for attempt in range(MAX_RETRIES):
        if STOP.is_set():
            raise Cancelled()
        LIMITER.wait()
        files, closer = make_files() if make_files else (None, None)
        try:
            r = SESSION.post(api_url("sendVideo"), data=data, files=files, timeout=1800).json()
        except (requests.RequestException, ValueError) as e:
            last = str(e)
            time.sleep(2 * (attempt + 1))
            continue
        finally:
            if closer:
                closer()
        if r.get("ok"):
            return r["result"]
        if r.get("error_code") == 429:
            wait = r.get("parameters", {}).get("retry_after", 5) + 1
            new_rate = LIMITER.slow_down()
            LIMITER.pause(wait)
            if new_rate:
                print(f"   !! Telegram flood limit (429): all threads paused {wait}s, "
                      f"rate lowered to {new_rate:.1f}/s")
            last = f"429 (waited {wait}s)"
            time.sleep(wait)
            continue
        raise RuntimeError(r.get("description", "unknown error"))
    raise RuntimeError(f"failed after {MAX_RETRIES} tries: {last}")


def send_with_cover(video_id, image_id, caption):
    data = {"chat_id": CHAT_ID, "video": video_id, "cover": image_id,
            "caption": caption[:1024], "parse_mode": "HTML", "supports_streaming": "true"}
    return _send(data)


def send_with_upload(video_id, image_id, caption):
    thumb = make_tg_thumb(download_by_file_id(image_id))
    path, is_tmp = download_video_to_tmp(video_id)
    data = {"chat_id": CHAT_ID, "caption": caption[:1024], "parse_mode": "HTML",
            "supports_streaming": "true"}

    def make_files():
        f = open(path, "rb")
        return {"video": f, "thumbnail": ("thumb.jpg", thumb, "image/jpeg")}, f.close

    try:
        return _send(data, make_files)
    finally:
        if is_tmp and os.path.exists(path):
            os.remove(path)


def process_row(row, mode):
    if STOP.is_set():
        raise Cancelled()
    caption = html.escape(row["name"] or "")
    vid, img = row["video_id"], row["image_id"]
    if mode == "cover":
        return send_with_cover(vid, img, caption)
    if mode == "upload":
        return send_with_upload(vid, img, caption)
    try:  # auto
        return send_with_cover(vid, img, caption)
    except Cancelled:
        raise
    except Exception:
        return send_with_upload(vid, img, caption)


# ---------------------------------------------------------------- main
def main():
    global LIMITER
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["auto", "cover", "upload"], default="auto")
    ap.add_argument("--workers", type=int, default=6, help="parallel threads (default 6)")
    ap.add_argument("--rate", type=float, default=3, help="max sends per second (default 3)")
    ap.add_argument("--batch-size", type=int, default=50, help="rows per batch (default 50)")
    ap.add_argument("--batch-delay", type=float, default=0, help="seconds to sleep between batches")
    ap.add_argument("--limit", type=int, default=0, help="stop after N rows (0 = all)")
    ap.add_argument("--retry-failed", action="store_true")
    args = ap.parse_args()

    LIMITER = Limiter(args.rate)

    db = pymysql.connect(**DB_CONFIG, charset="utf8mb4", autocommit=True,
                         cursorclass=pymysql.cursors.DictCursor)

    cond = ("(thumb_video_id IS NULL OR thumb_video_id = 'FAILED')" if args.retry_failed
            else "thumb_video_id IS NULL")
    cond += (" AND video_id IS NOT NULL AND video_id <> ''"
             " AND image_id IS NOT NULL AND image_id <> ''")

    with db.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS c FROM vid65 WHERE {cond}")
        total = cur.fetchone()["c"]
    if args.limit:
        total = min(total, args.limit)
    print(f"{total} row(s) to process | mode={args.mode} workers={args.workers} "
          f"rate={args.rate}/s batch={args.batch_size}")

    ok = failed = done = 0
    last_id = 0
    start = time.time()
    batch_no = 0

    try:
        while not STOP.is_set():
            n = args.batch_size
            if args.limit:
                n = min(n, args.limit - done)
            if n <= 0:
                break

            db.ping(reconnect=True)
            with db.cursor() as cur:
                cur.execute(
                    f"SELECT id, name, image_id, video_id FROM vid65 "
                    f"WHERE {cond} AND id > %s ORDER BY id LIMIT %s", (last_id, n))
                rows = cur.fetchall()
            if not rows:
                break
            last_id = rows[-1]["id"]
            batch_no += 1
            print(f"--- batch {batch_no}: ids {rows[0]['id']}..{rows[-1]['id']} ({len(rows)} rows)")

            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                futs = {ex.submit(process_row, r, args.mode): r for r in rows}
                try:
                    for fut in as_completed(futs):
                        row = futs[fut]
                        rid = row["id"]
                        try:
                            msg = fut.result()
                            db.ping(reconnect=True)
                            with db.cursor() as cur:
                                cur.execute(
                                    "UPDATE vid65 SET thumb_video_id=%s, video_msg_id=%s WHERE id=%s",
                                    (msg["video"]["file_id"], msg["message_id"], rid))
                            ok += 1
                            status = f"OK msg_id={msg['message_id']}"
                        except Cancelled:
                            continue
                        except Exception as e:
                            db.ping(reconnect=True)
                            with db.cursor() as cur:
                                cur.execute("UPDATE vid65 SET thumb_video_id='FAILED' WHERE id=%s", (rid,))
                            failed += 1
                            status = f"FAILED: {e}"
                        done += 1
                        el = time.time() - start
                        speed = done / el if el else 0
                        eta = (total - done) / speed if speed else 0
                        print(f"[{rid}] {status} | {done}/{total} | {speed:.2f}/s | ETA {eta/60:.1f} min")
                except KeyboardInterrupt:
                    print("\nStopping... finishing in-flight sends (progress is saved).")
                    STOP.set()

            if args.batch_delay and not STOP.is_set():
                time.sleep(args.batch_delay)
    except KeyboardInterrupt:
        STOP.set()

    print(f"Done. success={ok} failed={failed} time={(time.time()-start)/60:.1f} min")
    db.close()


if __name__ == "__main__":
    main()

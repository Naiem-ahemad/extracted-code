import os , logging , asyncio
import asyncio
import threading
from datetime import datetime
import time
import json
import base64
import random
import re , urllib.parse
import httpx
from typing import Dict, Any, Optional
import gallery_dl , sys , io
import gallery_dl.config
import gallery_dl.job
from fastapi import FastAPI, Query, HTTPException , Request
import yt_dlp, uuid , psutil
import boto3
import subprocess
from boto3.s3.transfer import TransferConfig
from botocore.client import Config as BotocoreConfig
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse

load_dotenv()

# ---------------- Settings ----------------
TEMP_DIR = "./videos"
os.makedirs(TEMP_DIR, exist_ok=True)

app = FastAPI()
API_KEY = "all-7f04e0d887372e3769b200d990ae7868"
# Global tasks dictionary (from your original code)
tasks: Dict[str, Dict[str, Any]] = {}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

BASE_YTDL_OPTS = {
    "quiet": False,
    "no_warnings": True,
    "ignoreerrors": False,
    "extract_flat": False,
    "geo_bypass": True,
    "nocheckcertificate": True,
    "socket_timeout": 30,
    "retries": 5,
    "http_headers": {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
    },
}

# make sure env vars present
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_REGION = os.getenv("R2_REGION", "auto")  # optional

# R2 endpoint
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# boto3 client
r2 = boto3.client(
    "s3",
    region_name=R2_REGION if R2_REGION else None,
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=BotocoreConfig(signature_version="s3v4"),
)

def generate_signed_urls(key: str, expire_seconds: int = 43200):
    """Generate both streaming + download signed URLs (expire in 12h by default)"""
    # Streaming (inline) - Add Content-Type for video streaming
    streaming_url = r2.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": R2_BUCKET,
            "Key": key,
            "ResponseContentType": "video/mp4",
            "ResponseContentDisposition": "inline",  # Just inline without filename for streaming
        },
        ExpiresIn=expire_seconds,
    )

    # Download (attachment)
    download_url = r2.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": R2_BUCKET,
            "Key": key,
            "ResponseContentType": "video/mp4",
            "ResponseContentDisposition": f'attachment; filename="{os.path.basename(key)}"',
        },
        ExpiresIn=expire_seconds,
    )

    return {
        "stream": streaming_url,  # For video player streaming
        "download": download_url  # For direct download
    }

class ProgressCallback:
    def __init__(self, task_id, data, start, end, total_bytes):
        self.task_id = task_id
        self.data = data
        self.start = start
        self.end = end
        self.total = float(total_bytes)
        self.lock = threading.Lock()
        self.uploaded = 0
        self.last_ts = time.time()
        self.last_uploaded = 0
        # smoothing window
        self.recent_bytes = 0
        self.recent_ts = self.last_ts

    def __call__(self, bytes_amount):
        with self.lock:
            now = time.time()
            self.uploaded += bytes_amount
            # compute percent of this stage
            percent = 0
            if self.total > 0:
                percent = min(int((self.uploaded / self.total) * 100), 100)
            scaled = self.start + (percent * (self.end - self.start) // 100)
            # compute upload speed (MiB/s) over last short window
            dt = now - self.recent_ts
            if dt <= 0:
                speed_str = "0MiB"
            else:
                self.recent_bytes += bytes_amount
                # every call older than 0.5s, compute speed and reset
                if dt >= 0.5:
                    speed_bps = self.recent_bytes / dt
                    speed_mib = speed_bps / (1024 * 1024)
                    speed_str = f"{round(speed_mib, 2)}MiB"
                    # reset window
                    self.recent_ts = now
                    self.recent_bytes = 0
                else:
                    # fallback to instant delta from last recorded
                    delta = self.uploaded - self.last_uploaded
                    dt_full = now - self.last_ts if (now - self.last_ts) > 0 else 1e-6
                    speed_mib = (delta / dt_full) / (1024 * 1024)
                    speed_str = f"{round(speed_mib, 2)}MiB"

            # update last markers
            self.last_uploaded = self.uploaded
            self.last_ts = now

            # human readable uploaded / total
            uploaded_hr = bytes_to_mib_str(self.uploaded)
            total_hr = bytes_to_mib_str(self.total)

            self.data["progress"] = min(scaled, self.end)
            self.data["stage"] = "uploading"
            self.data["uploaded"] = uploaded_hr
            self.data["total"] = total_hr
            self.data["speed"] = speed_str
            tasks[self.task_id]["encrypted"] = encrypt_task_data(self.data)

def upload_with_progress(local_path, key, task_id, data, start=95, end=100):
    """
    Upload local_path to R2 with callback; on error update task data and re-raise.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    size_bytes = os.path.getsize(local_path)
    cb = ProgressCallback(task_id, data, start, end, size_bytes)

    transfer_config = TransferConfig(
        multipart_threshold=5 * 1024 * 1024,
        multipart_chunksize=5 * 1024 * 1024,
        max_concurrency=16,
        use_threads=True,
    )

    # ensure stage/progress start for upload
    data["stage"] = "uploading"
    data["status"] = "uploading"
    data["progress"] = start
    data["uploaded"] = "0MiB"
    data["total"] = bytes_to_mib_str(size_bytes)
    data["speed"] = "0MiB"
    tasks[task_id]["encrypted"] = encrypt_task_data(data)

    try:
        r2.upload_file(
            Filename=local_path,
            Bucket=R2_BUCKET,
            Key=key,
            Callback=cb,
            Config=transfer_config,
        )
    except Exception as e:
        # attach upload error details to task data
        data["status"] = "error"
        data["error"] = f"upload failed: {repr(e)}"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)
        raise  # re-raise so caller (worker) will handle and cleanup

    # ensure final state after success
    data["progress"] = end
    data["uploaded"] = bytes_to_mib_str(size_bytes)
    data["speed"] = "0MiB"
    tasks[task_id]["encrypted"] = encrypt_task_data(data)
    return True

# ---------------- Utilities ----------------
def encrypt_task_data(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()

def decrypt_task_data(data: str) -> dict:
    return json.loads(base64.urlsafe_b64decode(data.encode()).decode())


COOKIES_MAP = {
    "youtube.com": "./cookies/yt.txt",
    "youtu.be": "./cookies/yt.txt",
    "facebook.com": "./cookies/fb.txt",
    "x.com": "./cookies/x.txt",
    "twitter.com": "./cookies/x.txt",
}

def get_cookies_file(url: str) -> Optional[str]:
    for domain, path in COOKIES_MAP.items():
        if domain in url and os.path.exists(path):
            return path
    return None

# ---------------- Endpoints ----------------
def sizeof_fmt(num, suffix="B"):
    """Convert bytes to MB/GB readable format"""
    for unit in ["", "K", "M", "G", "T", "P"]:
        if abs(num) < 1024.0:
            return f"{num:.2f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.2f} P{suffix}"

CACHE_FILE = "./video_cache.json"
CACHE_TTL = 12 * 3600  # 12 hours

# Load cache on startup
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        cache = json.load(f)
else:
    cache = {}

def get_cached_info(url: str):
    entry = cache.get(url)
    if not entry:
        return None
    if time.time() - entry.get("timestamp", 0) > CACHE_TTL:
        del cache[url]
        save_cache()
        return None
    return entry["data"]

def set_cached_info(url: str, data):
    cache[url] = {"data": data, "timestamp": time.time()}
    save_cache()

def save_cache():
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

def safe_filename(title: str) -> str:
    return re.sub(r'[\\/*?:"<>| ]+', "-", title)

@app.get("/download")
async def download(
    request: Request,
    url: str = Query(..., description="Encoded video URL"),
    title: str = Query("video", description="Video title"),
    key: str = Query(...)
):
    if key != "all-7f04e0d887372e3769b200d990ae7868":
        raise HTTPException(status_code=403, detail="Invalid key")

    filename = safe_filename(title) + ".mp4"

    headers = {}
    # Pass through Range header (for resume/partial downloads)
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    async def stream_video():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", url, headers=headers, follow_redirects=True) as r:
                if r.status_code not in [200, 206]:
                    raise HTTPException(status_code=r.status_code, detail="Could not fetch video")
                async for chunk in r.aiter_bytes(chunk_size=1024 * 1024):  # 1MB chunks
                    yield chunk

    return StreamingResponse(
        stream_video(),
        media_type="video/mp4",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            # forward Content-Length and Range support if possible
            "Accept-Ranges": "bytes"
        }
    )

MAX_FREE_SIZE = 1 * 1024 * 1024 * 1024
 # 200MiB limit for free users
from fastapi import BackgroundTasks

@app.get("/")
def list_qualities(url: str = Query(...), key: str = Query(...), background_tasks: BackgroundTasks = None):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")

    # Check cached data
    cached = get_cached_info(url)
    if cached:
        return cached

    # If no cached data yet, schedule background extraction
    def extract_and_cache(url: str):
        try:
            ydl_opts = {**BASE_YTDL_OPTS, "skip_download": True}
            cookies_file = get_cookies_file(url)
            if cookies_file:
                ydl_opts["cookiefile"] = cookies_file

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)

            if not info:
                return

            formats = [f for f in info.get("formats", [])
                       if f.get("protocol") not in ("m3u8", "m3u8_native") and f.get("url") and f.get("format_id")]

            if not formats:
                return

            # --- AUDIO ---
            audio_formats = [f for f in formats if f.get("acodec") and f.get("acodec") != "none" and f.get("vcodec") == "none"]
            unique_audios = {}
            for f in sorted(audio_formats, key=lambda x: x.get("abr", 0), reverse=True):
                ext = f.get("ext")
                if ext not in unique_audios:
                    unique_audios[ext] = f
                if len(unique_audios) >= 3:
                    break

            audios = []
            for f in unique_audios.values():
                filesize = f.get("filesize") or f.get("filesize_approx") or 0
                audios.append({
                    "quality": f"Audio {f.get('abr', 0)}kbps",
                    "ext": f.get("ext"),
                    "size": sizeof_fmt(filesize),
                    "url": f.get("url"),
                    "raw_size": filesize
                })

            # --- VIDEO ---
            combined_formats = [f for f in formats if f.get("height") and f.get("vcodec") != "none" and f.get("acodec") != "none"]
            video_only_formats = [f for f in formats if f.get("height") and f.get("vcodec") != "none" and (not f.get("acodec") or f.get("acodec") == "none")]
            all_video_formats = combined_formats + video_only_formats

            qualities = []
            seen_resolutions = set()
            for f in sorted(all_video_formats, key=lambda x: x.get("height", 0), reverse=True):
                height = f.get("height")
                if height in seen_resolutions:
                    continue
                seen_resolutions.add(height)

                video_size = (f.get("filesize") or f.get("clen") or f.get("filesize_approx") or 0)
                best_audio = audios[0] if audios else {"raw_size": 0}
                total_size = video_size + best_audio.get("raw_size", 0)

                if f.get("acodec") != "none":
                    # already merged (direct stream available)
                    safe_title = re.sub(r'[\\/*?:"<>|]', "-", info.get("title", "video")).replace(" ", "-")
                    ext = f.get("ext") or "mp4"
                    encoded_url = urllib.parse.quote_plus(f.get("url"))
                    qualities.append({
                        "quality": f"{height}p",
                        "size": sizeof_fmt(video_size),
                        "streaming_url": f.get("url"),  # streaming
                        "download_url": f"/download?url={encoded_url}&title={safe_title}&key={key}",  # force download
                        "premium": total_size > MAX_FREE_SIZE
                    })
                else:
                    if total_size > MAX_FREE_SIZE:
                        qualities.append({
                            "quality": f"{height}p",
                            "size": sizeof_fmt(total_size),
                            "premium": True,
                            "message": "File too large for free users, membership required"
                        })
                        continue

                    task_id = str(uuid.uuid4())
                    task_data = {
                        "url": url,
                        "video_format": f,
                        "title": info.get("title", "Unknown"),
                        "audio_format": best_audio,
                        "needs_merge": True,
                        "status": "waiting",
                        "progress": 0,
                        "stage": None,
                        "file": None,
                        "error": None,
                        "timestamp": time.time(),
                    }
                    tasks[task_id] = {"encrypted": encrypt_task_data(task_data)}

                    qualities.append({
                        "quality": f"{height}p",
                        "size": sizeof_fmt(total_size),
                        "progress_url": f"/progress/{task_id}?key={key}"
                    })

            result = {
                "title": info.get("title", "Unknown"),
                "thumbnail": info.get("thumbnail"),
                "duration": info.get("duration"),
                "audio_only": False,
                "qualities": qualities,
                "audios": audios
            }

            set_cached_info(url, result)
        except Exception as e:
            logging.error(f"Background extraction failed for {url}: {e}")

    if background_tasks:
        background_tasks.add_task(extract_and_cache, url)

    return {"status": "pending", "message": "Processing, please retry shortly."}

# helper: turn bytes -> human MiB string (rounded)
def bytes_to_mib_str(n):
    return f"{round(n / (1024*1024), 2)}MiB"

# Robust parser for aria2 progress lines
def parse_aria2_line(line):
    """
    Try several patterns and return a dict with any of:
    { 'downloaded': '27MiB', 'total': '28MiB', 'percent': 95, 'speed': '1.0MiB' }
    """
    line = line.strip()

    # Pattern 1 (most complete): downloaded/total(percent) ... DL:speed
    p1 = re.search(r"([\d\.]+[KkMmGgTtPp]i?[Bb]?)/([\d\.]+[KkMmGgTtPp]i?[Bb]?)\s*\((\d+)%\).*?DL:([\d\.]+\w*)", line)
    if p1:
        return {
            "downloaded": p1.group(1),
            "total": p1.group(2),
            "percent": int(p1.group(3)),
            "speed": p1.group(4),
        }

    # Pattern 2: downloaded/total(percent) (no DL:)
    p2 = re.search(r"([\d\.]+[KkMmGgTtPp]i?[Bb]?)/([\d\.]+[KkMmGgTtPp]i?[Bb]?)\s*\((\d+)%\)", line)
    if p2:
        return {
            "downloaded": p2.group(1),
            "total": p2.group(2),
            "percent": int(p2.group(3)),
            "speed": None,
        }

    # Pattern 3: percent and DL only (e.g. "(95%) ... DL:1.0MiB")
    p3 = re.search(r"\((\d+)%\).*?DL:([\d\.]+\w*)", line)
    if p3:
        return {"percent": int(p3.group(1)), "speed": p3.group(2)}

    # Pattern 4: DL only
    p4 = re.search(r"DL:([\d\.]+\w*)", line)
    if p4:
        return {"speed": p4.group(1)}

    # nothing matched
    return {}

def run_with_progress(cmd, start, end, task_id, data, output_file=None, update_every_n_lines=1):
    """
    Run a subprocess (aria2c) and parse its stdout to update task progress.
    - `start`/`end` define the scaled progress range for this step (0..100).
    - `output_file` is used as fallback to compute final size if aria2 output was missed.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    matched_any = False
    line_count = 0

    try:
        # iterate lines as they arrive (works with text=True)
        for raw_line in proc.stdout:
            line_count += 1
            line = raw_line.strip()
            # optional debug: print("ARIA2:", line)

            parsed = parse_aria2_line(line)
            if parsed:
                matched_any = True

            # Apply parsed fields to data
            if parsed.get("percent") is not None:
                percent = parsed["percent"]
                # scale percent to stage range
                scaled = start + (percent * (end - start) // 100)
                data["progress"] = scaled

            if parsed.get("speed") is not None:
                data["speed"] = parsed["speed"]

            if parsed.get("downloaded") is not None:
                data["downloaded"] = parsed["downloaded"]

            if parsed.get("total") is not None:
                data["total"] = parsed["total"]

            # ensure stage remains set (video/audio)
            data["stage"] = data.get("stage") or ("video" if start < end and start == 0 else data.get("stage"))

            # write update back to tasks — do this not too often to reduce overhead
            if line_count % update_every_n_lines == 0 or parsed:
                tasks[task_id]["encrypted"] = encrypt_task_data(data)

        # Wait for process to finish (ensure returncode set)
        proc.wait(timeout=5)
    except Exception as e:
        # If we were interrupted, kill process and record error
        try:
            proc.kill()
        except Exception:
            pass
        data["status"] = "error"
        data["error"] = f"run_with_progress exception: {e}"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)
        return False

    # Completed run: ensure final progress and fallback
    data["progress"] = end
    data["speed"] = "0MiB"

    if not matched_any and output_file and os.path.exists(output_file):
        # fallback to actual file size if aria2 produced no parseable output
        size_bytes = os.path.getsize(output_file)
        data["downloaded"] = bytes_to_mib_str(size_bytes)
        data["total"] = data["downloaded"]

    tasks[task_id]["encrypted"] = encrypt_task_data(data)

    return proc.returncode == 0

def worker(task_id, data):
    video_file = audio_file = output_file = None
    try:
        video_url = data["video_format"]["url"]
        audio_url = data["audio_format"]["url"]

        os.makedirs("./tmp", exist_ok=True)
        video_file = f"./tmp/{task_id}_video.mp4"
        audio_file = f"./tmp/{task_id}_audio.mp3"
        title = re.sub(r'[\\/*?:"<>|]', "_", data.get("title", "video"))
        format = data["video_format"].get("ext", "mp4")
        output_file = f"./tmp/{title}_{format}.mp4"

        # --- Video download (0–45) ---
        data["status"] = "downloading"
        data["stage"] = "video"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)
        save_tasks()

        run_with_progress(
            [
                "aria2c",
                "-x", "16", "-s", "16", "-k", "1M",
                "--allow-piece-length-change=true",
                "--file-allocation=none",
                "--disable-ipv6",
                "--enable-http-pipelining",
                "--http-accept-gzip",
                "--auto-file-renaming=false",
                "--summary-interval=1",
                "--timeout=30", "--connect-timeout=30",
                "--retry-wait=2", "--max-tries=15",
                "--min-split-size=1M", "--check-integrity=true",
                "-o", video_file,
                video_url,
            ],
            0, 45, task_id, data, output_file=video_file
        )

        # --- Audio download (45–90) ---
        data["stage"] = "audio"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

        run_with_progress(
            [
                "aria2c",
                "-x", "16", "-s", "16", "-k", "1M",
                "--allow-piece-length-change=true",
                "--file-allocation=none",
                "--disable-ipv6",
                "--enable-http-pipelining",
                "--http-accept-gzip",
                "--auto-file-renaming=false",
                "--summary-interval=1",
                "--timeout=30", "--connect-timeout=30",
                "--retry-wait=2", "--max-tries=15",
                "--min-split-size=1M", "--check-integrity=true",
                "-o", audio_file,
                audio_url,
            ],
            45, 90, task_id, data, output_file=audio_file
        )

        # --- Merge (90–95) ---
        data["stage"] = "merging"
        data["status"] = "merging"
        data["progress"] = 90
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

        result = subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin",
                "-i", video_file,
                "-i", audio_file,
                "-c", "copy",
                output_file
            ],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg failed: {result.stderr}")

        # mark merge done
        data["progress"] = 95
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

        # --- Upload (95–100) ---
        data["stage"] = "uploading"
        data["status"] = "uploading"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

        r2_key = f"videos/{task_id}.mp4"
        upload_with_progress(output_file, r2_key, task_id, data, 95, 100)
        expire_seconds = 43200
        stream_url = r2.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": R2_BUCKET,
                "Key": r2_key,
                "ResponseContentType": "video/mp4",
                "ResponseContentDisposition": "inline",  # For streaming
            },
            ExpiresIn=expire_seconds,
        )
        download_url = r2.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": R2_BUCKET,
                "Key": r2_key,
                "ResponseContentType": "video/mp4",
                "ResponseContentDisposition": f'attachment; filename="{os.path.basename(output_file)}"',  # user sees title.mp4
            },
            ExpiresIn=expire_seconds,
        )

        data["status"] = "done"
        data["stage"] = "finished"
        data["progress"] = 100
        data["stream_url"] = stream_url
        data["download_url"] = download_url
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

    except Exception as e:
        data["status"] = "error"
        data["error"] = str(e)
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

    finally:
        # --- Cleanup local files ---
        for f in [video_file, audio_file, output_file]:
            try:
                if f and os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass

# Limit of concurrent running tasks
MAX_PARALLEL_TASKS = 25
task_semaphore = threading.Semaphore(MAX_PARALLEL_TASKS)

TASKS_FILE = "./tasks.json"

# Load tasks from disk
if os.path.exists(TASKS_FILE):
    with open(TASKS_FILE, "r") as f:
        tasks = json.load(f)
else:
    tasks = {}

def save_tasks():
    with open(TASKS_FILE, "w") as f:
        json.dump(tasks, f)

# --- Semaphore for concurrent tasks ---
task_semaphore = threading.Semaphore(MAX_PARALLEL_TASKS)

# --- Worker launcher ---
def start_task_in_background(task_id, data):
    def run_worker(task_id, data):
        try:
            worker(task_id, data)
        finally:
            task_semaphore.release()  # free slot when done

    acquired = task_semaphore.acquire(blocking=False)
    if acquired:
        data["status"] = "running"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)
        thread = threading.Thread(target=run_worker, args=(task_id, data), daemon=True)
        thread.start()
    else:
        data["status"] = "queued"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)

# --- Auto-resume tasks on startup ---
for task_id, task in tasks.items():
    data = decrypt_task_data(task["encrypted"])
    if data.get("status") in ["running", "queued"]:
        # reset any stuck tasks to waiting
        data["status"] = "waiting"
        task["encrypted"] = encrypt_task_data(data)

# Save the cleaned-up task states
save_tasks()

# Start all waiting tasks respecting semaphore
for task_id, task in tasks.items():
    data = decrypt_task_data(task["encrypted"])
    if data.get("status") == "waiting":
        start_task_in_background(task_id, data)

@app.get("/progress/{task_id}")
def progress(task_id: str, key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    if task_id not in tasks:
        return {"status": "unknown", "error": "Task not found"}

    t = tasks[task_id]
    data = decrypt_task_data(t["encrypted"])
    save_cache()
    # Only start worker if waiting
    if data.get("status") == "waiting":

        # Try to acquire a semaphore slot
        acquired = task_semaphore.acquire(blocking=False)
        if acquired:
            data["status"] = "running"
            tasks[task_id]["encrypted"] = encrypt_task_data(data)

            def run_worker(task_id, data):
                try:
                    worker(task_id, data)
                finally:
                    # Release semaphore when done
                    task_semaphore.release()

            thread = threading.Thread(target=run_worker, args=(task_id, data), daemon=True)
            thread.start()
        else:
            # Keep task in waiting state; it will auto-start later
            data["status"] = "queued"
            tasks[task_id]["encrypted"] = encrypt_task_data(data)

    return {
        "status": data.get("status"),
        "stage": data.get("stage"),
        "progress": data.get("progress", 0),
        "downloaded": data.get("downloaded"),
        "total": data.get("total"),
        "speed": data.get("speed"),
        "stream_url": data.get("stream_url"),
        "download_url": data.get("download_url"),
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}

# Clean up old tasks periodically
def cleanup_old_tasks():
    while True:
        try:
            current_time = time.time()
            old_tasks = []

            for task_id, task in list(tasks.items()):
                try:
                    data = decrypt_task_data(task["encrypted"])
                    if current_time - data.get("timestamp", 0) > 43200:  # 12 hours
                        # delete from R2 if uploaded
                        key = f"videos/{task_id}.mp4"
                        try:
                            r2.delete_object(Bucket=R2_BUCKET, Key=key)
                        except Exception as e:
                            print(f"Failed to delete {key}: {e}")
                        old_tasks.append(task_id)
                except Exception:  # malformed task
                    old_tasks.append(task_id)

            for task_id in old_tasks:
                tasks.pop(task_id, None)

        except Exception as e:
            print(f"Cleanup error: {e}")

        time.sleep(300)  # run every 5 minutes

def normalize_instagram_url(url: str) -> str:
    if url.startswith("https:/") and not url.startswith("https://"):
        url = url.replace("https:/", "https://", 1)
    if url.startswith("http:/") and not url.startswith("http://"):
        url = url.replace("http:/", "http://", 1)
    url = url.replace("/reels/", "/reel/")
    return url

def get_instagram_json_data(url: str, cookies_path: str):
    gallery_dl.config.clear()
    url = url.replace("/reels/", "/reel/")
    if cookies_path and os.path.exists(cookies_path):
        gallery_dl.config.set(("extractor", "instagram"), "cookies", cookies_path)
    else:
        raise FileNotFoundError(f"Cookies file not found: {cookies_path}")
    logging.getLogger("gallery_dl").setLevel(logging.CRITICAL)
    gallery_dl.config.set(("core",), "quiet", True)
    f = io.StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout, sys.stderr = f, f
    try:
        job = gallery_dl.job.DataJob(url)
        job.run()
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr
    if not job.data:
        raise RuntimeError("gallery-dl returned no data. Check cookies or URL.")
    return job.data, url

@app.get("/instagram")
async def instagram_reel(url: str = Query(...), key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    # Run gallery-dl in thread to avoid blocking
    loop = asyncio.get_event_loop()
    results, normalized_url = await loop.run_in_executor(None, get_instagram_json_data, normalize_instagram_url(url), "./cookies/insta.txt")

    media_list = []
    for entry in results:
        if isinstance(entry, tuple):
            if len(entry) >= 3 and isinstance(entry[2], dict):
                entry = entry[2]
            elif len(entry) >= 2 and isinstance(entry[1], dict):
                entry = entry[1]
            else:
                entry = entry[0]
        if not isinstance(entry, dict):
            continue
        all_media = []
        main_url = entry.get("display_url") or entry.get("video_url")
        if main_url:
            all_media.append(entry)
        sidecar = entry.get("sidecar_media") or []
        for item in sidecar:
            url = item.get("display_url") or item.get("video_url")
            if url:
                all_media.append(item)
        for m in all_media:
            media_type = "video" if m.get("video_url") else "image"
            media_list.append({
                "post_id": m.get("post_id"),
                "post_url": m.get("post_url"),
                "username": m.get("username"),
                "fullname": m.get("fullname"),
                "description": m.get("description"),
                "media_url": m.get("display_url") or m.get("video_url"),
                "type": media_type,
                "width": m.get("width"),
                "height": m.get("height"),
                "extension": m.get("extension") or ("mp4" if media_type=="video" else "jpg"),
            })
    if not media_list:
        raise HTTPException(status_code=404, detail=f"No media found for {normalized_url}")
    return {"instagram_url": normalized_url, "media": media_list}


def normalize_twitter_url(url: str) -> str:
    """Clean and normalize Twitter/X URL."""
    url = url.strip()
    url = re.sub(r"^https?:\/\/(www\.)?(twitter|x)\.com\/", "https://twitter.com/", url)
    url = re.sub(r"\?.*$", "", url)
    return url

def get_gallery_dl_data(url: str, cookies_path: str = None):
    """Fetch media data using gallery-dl."""
    gallery_dl.config.clear()
    if cookies_path and os.path.exists(cookies_path):
        gallery_dl.config.set(("extractor", "twitter"), "cookies", cookies_path)
    gallery_dl.config.set(("core",), "quiet", True)

    job = gallery_dl.job.DataJob(url)
    job.run()

    if not job.data:
        raise RuntimeError(f"No media found for {url}")
    return job.data

def map_to_clean_json(data):
    """Transform gallery-dl raw output to clean JSON."""
    media_list = []

    for entry in data:
        if isinstance(entry, tuple):
            type_id = entry[0]
            if type_id == 2:
                content = entry[1]
                media_url = content.get("url") or content.get("filename")
            elif type_id == 3:
                media_url = entry[1]
                content = entry[2]
            else:
                continue
        elif isinstance(entry, dict):
            content = entry
            media_url = content.get("url") or content.get("filename")
        else:
            continue

        if not media_url:
            continue

        media_list.append({
            "tweet_id": content.get("tweet_id"),
            "username": content.get("user", {}).get("nick") or content.get("author", {}).get("nick"),
            "author_id": content.get("author", {}).get("id"),
            "content": content.get("content"),
            "media_url": media_url,
            "filename": content.get("filename"),
            "type": content.get("type") or ("video" if media_url.endswith(".mp4") else "image"),
            "extension": content.get("extension") or media_url.split(".")[-1],
            "width": content.get("width"),
            "height": content.get("height"),
            "followers_count": content.get("user", {}).get("followers_count"),
            "view_count": content.get("view_count"),
            "date": content.get("date").strftime("%Y-%m-%d %H:%M:%S") if isinstance(content.get("date"), datetime) else None
        })

    return media_list

# ----------------------
# Endpoint
# ----------------------
@app.get("/twitter")
async def twitter_media(url: str = Query(...), key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")

    url = normalize_twitter_url(url)
    cookies = "./cookies/x.txt" if os.path.exists("./cookies/x.txt") else None

    try:
        data = get_gallery_dl_data(url, cookies_path=cookies)
    except Exception as e:
        raise HTTPException(500, f"Failed to fetch data: {str(e)}")

    media_list = map_to_clean_json(data)

    if not media_list:
        raise HTTPException(404, detail=f"No media found for {url}")

    return {
        "twitter_url": url,
        "media": media_list
    }

@app.get("/stats")
def stats(key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    
    total_tasks = len(tasks)
    active_tasks = 0
    total_downloaded_bytes = 0
    task_details = []

    for task_id, task in tasks.items():
        data = decrypt_task_data(task["encrypted"])
        if data.get("status") in ("running", "uploading", "merging"):
            active_tasks += 1

        downloaded_str = data.get("downloaded")
        if downloaded_str:
            match = re.match(r"([\d\.]+)([KMG]i?)B", downloaded_str)
            if match:
                num, unit = match.groups()
                num = float(num)
                factor = {"K": 1024, "Ki": 1024, "M": 1024**2, "Mi": 1024**2, "G": 1024**3, "Gi": 1024**3}.get(unit, 1)
                total_downloaded_bytes += int(num * factor)
        
        task_details.append({
            "task_id": task_id,
            "status": data.get("status"),
            "stage": data.get("stage"),
            "progress": data.get("progress", 0),
            "downloaded": data.get("downloaded"),
            "total": data.get("total"),
            "speed": data.get("speed"),
        })

    # Optional: Render server memory / CPU
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)

    return {
        "total_tasks": total_tasks,
        "active_tasks": active_tasks,
        "total_downloaded": f"{round(total_downloaded_bytes / (1024*1024), 2)} MiB",
        "tasks": task_details,
        "memory_usage": f"{round(mem.used / (1024*1024), 2)} MiB / {round(mem.total / (1024*1024), 2)} MiB",
        "cpu_usage_percent": cpu
    }

# Start cleanup thread
threading.Thread(target=cleanup_old_tasks, daemon=True).start()

import os , logging , asyncio
import asyncio
from datetime import datetime
import time
import json
import random
import re , urllib.parse
import httpx
from typing import Optional
import gallery_dl , sys , io , zlib , base64
import gallery_dl.config
import gallery_dl.job
from fastapi import FastAPI, Query, HTTPException , Request
from fastapi.responses import JSONResponse
import yt_dlp, uuid , psutil , redis
from pydantic import BaseModel
from typing import List , Dict
from concurrent.futures import  ThreadPoolExecutor , as_completed
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse
from cryptography.fernet import Fernet

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", b'wxk9V_lppKFwN1LzRroxrXOxKxhhRD2GhhxVhwLxflw=')  # Example key; replace with your actual key
fernet = Fernet(ENCRYPTION_KEY)
load_dotenv()

# ---------------- Settings ----------------
TEMP_DIR = "./videos"

os.makedirs(TEMP_DIR, exist_ok=True)

app = FastAPI()

API_KEY = "all-7f04e0d887372e3769b200d990ae7868"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

BASE_YTDL_OPTS = {
    "quiet": True,             # suppress extra logs
    "no_warnings": True,
    "ignoreerrors": True,      # skip problematic videos
    "extract_flat": False,     # fully resolves video info, but no download
    "skip_download": True,     # do not download video/audio
    "geo_bypass": True,
    "nocheckcertificate": True,
    "socket_timeout": 10,      # faster fail if network is slow
    "retries": 1,              # avoid long retry delays
    "format": "best",          # ensures you get best video/audio links
    "writeinfojson": False,    # do not write extra files
    "writethumbnail": True,    # get thumbnail URL
    "writesubtitles": False,
    "writeautomaticsub": False,
    "http_headers": {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
    },
}

# ---------------- Utilities ----------------
def encrypt_task_data(data: dict) -> str:
    # JSON → compress → encrypt → base64-url-safe
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(raw)
    encrypted = fernet.encrypt(compressed)
    return base64.urlsafe_b64encode(encrypted).decode("utf-8")

def decrypt_task_data(data: str) -> dict:
    # base64-url-safe → decrypt → decompress → JSON
    encrypted = base64.urlsafe_b64decode(data.encode("utf-8"))
    compressed = fernet.decrypt(encrypted)
    raw = zlib.decompress(compressed)
    return json.loads(raw.decode("utf-8"))

COOKIES_MAP = {
    "youtube.com": "./cookies/yt1.txt",
    "youtu.be": "./cookies/yt1.txt",
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

def sanitize_filename(filename: str) -> str:
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    filename = filename.encode("ascii", errors="ignore").decode()
    return filename

@app.get("/download-audio")
def download_audio(url: str = Query(...), key: str = Query(...), title: str = Query("audio")):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    # Decode URL
    decoded_url = urllib.parse.unquote(url)
    safe_title = urllib.parse.quote(title)

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{safe_title}.mp3"
    }

    # Stream the file from the source directly to the user
    def iterfile():
        with httpx.stream("GET", decoded_url) as r:
            for chunk in r.iter_bytes(chunk_size=8192):
                yield chunk

    return StreamingResponse(iterfile(), media_type="audio/mpeg", headers=headers)
        
@app.get("/download")
async def download(
    request: Request,
    url: str = Query(..., description="Encoded media URL"),
    title: str = Query("video", description="Media title"),
    key: str = Query(...)
):
    # Security key check
    if key != "all-7f04e0d887372e3769b200d990ae7868":
        raise HTTPException(status_code=403, detail="Invalid key")

    # Sanitize title
    filename = sanitize_filename(title)
    encoded_filename = urllib.parse.quote(filename)

    # Prepare headers for streaming request
    headers = {}
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    async def stream_media():
        req_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        }
        if "range" in request.headers:
            req_headers["Range"] = request.headers["range"]

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", url, headers=req_headers, follow_redirects=True) as r:
                if r.status_code not in [200, 206]:
                    raise HTTPException(status_code=r.status_code, detail="Could not fetch media")
                async for chunk in r.aiter_bytes(chunk_size=1024 * 1024):
                    yield chunk

    return StreamingResponse(
        stream_media(),
        media_type="video/mp4",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}.mp4",
            "Accept-Ranges": "bytes"
        }
    )

MAX_FREE_SIZE = 1 * 1024 * 1024 * 1024
 # 200MiB limit for free users
from fastapi import  BackgroundTasks

# Global in-memory store
url_tasks: dict[str, dict] = {}  # {url: {status, stage, progress, result, error}}    # {task_id: {data}}
@app.get("/")
def list_qualities(url: str = Query(None), key: str = Query(None), background_tasks: BackgroundTasks = None):
    if url is None or key is None:
        return JSONResponse(
            content={
                "description": "Welcome to the Social Media Downloader API by zer0spectrum.",
                "api_usage": {
                    "youtube_music_playlist": {
                        "endpoint": "/music_playlist",
                        "method": "POST",
                        "description": "Fetch a playlist from YouTube Music and get song list.",
                        "body_example": {
                            "url": "https://music.youtube.com/playlist?list=PLxxxxxxx"
                        }
                    },
                    "select_songs_from_playlist": {
                        "endpoint": "/music_playlist/select",
                        "method": "POST",
                        "description": "Select specific songs from a playlist session and get full info.",
                        "body_example": {
                            "session_id": "uuid-of-session",
                            "numbers": [1, 2, 5]
                        }
                    },
                    "extract_single_music": {
                        "endpoint": "/music",
                        "method": "GET",
                        "description": "Get audio stream and thumbnail for a single YouTube Music URL",
                        "query_example": {
                            "url": "https://music.youtube.com/watch?v=xxxxxxx",
                            "key": "YOUR_API_KEY"
                        }
                    },
                    "download_video_or_audio": {
                        "endpoint": "/download",
                        "method": "GET",
                        "description": "Directly download video/audio using streaming URL.",
                        "query_example": {
                            "url": "direct-cdn-url",
                            "title": "My Video",
                            "key": "YOUR_API_KEY"
                        }
                    },
                    "download_audio_only": {
                        "endpoint": "/download-audio",
                        "method": "GET",
                        "description": "Directly download audio only.",
                        "query_example": {
                            "url": "direct-audio-cdn-url",
                            "title": "My Audio",
                            "key": "YOUR_API_KEY"
                        }
                    },
                    "instagram_reel": {
                        "endpoint": "/instagram",
                        "method": "GET",
                        "query_example": {
                            "url": "https://www.instagram.com/reel/xxxx/",
                            "key": "YOUR_API_KEY"
                        }
                    },
                    "twitter_media": {
                        "endpoint": "/twitter",
                        "method": "GET",
                        "query_example": {
                            "url": "https://twitter.com/user/status/xxxx",
                            "key": "YOUR_API_KEY"
                        }
                    },
                    "health_check": {
                        "endpoint": "/health",
                        "method": "GET",
                        "description": "Check server health."
                    },
                    "stats": {
                        "endpoint": "/stats",
                        "method": "GET",
                        "query_example": {
                            "key": "YOUR_API_KEY"
                        },
                        "description": "Get task and server statistics."
                    }
                },
                "note": "Replace YOUR_API_KEY with your actual API key."
            },
            status_code=200
        )
    
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    # If cached info exists, return immediately
    cached = get_cached_info(url)
    if cached:
        return cached

    # Check if extraction for this URL already started
    if url in url_tasks:
        task = url_tasks[url]
        return {
            "status": task["status"],
            "stage": task.get("stage"),
            "progress": task.get("progress", 0),
            "message": task.get("message"),
            "result": task.get("result")
        }

    # Initialize extraction task for URL
    url_tasks[url] = {
        "status": "extracting",
        "stage": "starting",
        "progress": 0,
        "message": "Extraction started",
        "result": None,
        "error": None
    }

    def extract_and_cache(url: str):
        try:
            task = url_tasks[url]
            task["stage"] = "extracting_info"
            task["progress"] = 10

            ydl_opts = {**BASE_YTDL_OPTS, "skip_download": True}
            cookies_file = get_cookies_file(url)
            if cookies_file:
                ydl_opts["cookiefile"] = cookies_file

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)

            task["stage"] = "processing_formats"
            task["progress"] = 50

            formats = [f for f in info.get("formats", [])
                       if f.get("protocol") not in ("m3u8", "m3u8_native") and f.get("url") and f.get("format_id")]

            # AUDIO
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
                audio_url_encoded = urllib.parse.quote_plus(f.get("url"))
                safe_audio_title = re.sub(r'[\\/*?:"<>]', "-", info.get("title", "audio")).replace(" ", "-")

                audios.append({
                    "quality": f"Audio {f.get('abr', 0)}kbps",
                    "ext": f.get("ext"),
                    "size": sizeof_fmt(filesize),
                    "streaming_url": f.get("url"),
                    "downloading_url": f"/download-audio?url={audio_url_encoded}&title={safe_audio_title}&key={key}",
                    "raw_size": filesize,
                    "url": f.get("url"),        # ✅ keep actual url
                    "abr": f.get("abr", 0)      # ✅ keep bitrate for selection
                })

            # VIDEO
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
                # pick best audio safely
                if audios:
                    # choose audio with highest bitrate if available
                    best_audio = max(audios, key=lambda a: a.get("abr") or 0)
                else:
                    best_audio = None
                total_size = video_size + best_audio.get("raw_size", 0)

                if f.get("acodec") != "none":
                    safe_title = re.sub(r'[\\/*?:"<>|]', "-", info.get("title", "video")).replace(" ", "-")
                    ext = f.get("ext") or "mp4"
                    encoded_url = urllib.parse.quote_plus(f.get("url"))
                    qualities.append({
                        "quality": f"{height}p",
                        "size": sizeof_fmt(video_size),
                        "streaming_url": f.get("url"),
                        "download_url": f"/download?url={encoded_url}&title={safe_title}&key={key}&type=mp4",
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
                    task_data = {
                        "url": url,
                        "title": info.get("title", "Unknown"),
                        "video_format": {
                            "url": f.get("url"),
                            "ext": f.get("ext") or "mp4",
                        },
                        "audio_format": {
                            "url": best_audio.get("url"),
                            "ext": best_audio.get("ext") or "m4a",
                        },
                        "needs_merge": True,
                        "status": "waiting",
                        "progress": 0,
                        "stage": None,
                        "file": None,
                        "error": None,
                        "timestamp": time.time(),
                    }
                    task_id = encrypt_task_data(task_data)

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

            task["stage"] = "done"
            task["progress"] = 100
            task["status"] = "done"
            task["message"] = "Extraction completed"
            task["result"] = result

            # Save in cache for future hits
            set_cached_info(url, result)

        except Exception as e:
            task["stage"] = "error"
            task["status"] = "error"
            task["error"] = str(e)
            task["message"] = f"Extraction failed: {e}"
            logging.error(f"Extraction failed for {url}: {e}")

    # Start background extraction
    if background_tasks:
        background_tasks.add_task(extract_and_cache, url)

    return {
        "status": "extracting",
        "stage": "starting",
        "progress": 0,
        "message": "Extraction started, refresh to see progress"
    }

def extract_audio_and_thumbnail(url: str):
    ydl_opts = {
        "format": "bestaudio/best",  # Only audio
        "quiet": True,
        "skip_download": True,
        "forcejson": True,
        "nocheckcertificate": True,
        "ignoreerrors": True,
        "cachedir": False,
        "noprogress": True ,
        "headers" : {
            "User-Agent": random.choice(USER_AGENTS),},
        "cookiefile": get_cookies_file("youtube.com")
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        return {
            "title": info.get("title"),
            "audio_url": info.get("url"),       # Direct audio stream
            "thumbnail": info.get("thumbnail"), # Thumbnail URL
        }

@app.get("/music")
async def extract_audio(url: str = Query(..., description="YouTube or YouTube Music URL") , key: str = Query(..., description="Your API key")):
    try:
        result = extract_audio_and_thumbnail(url)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=400)

# ----------------------
# CACHE
# ----------------------
CACHE = {}  # {session_id: {"songs": [...], "created": timestamp, "selected": {}}}
CACHE_TTL = 5 * 60 * 60  # 5 hours

# ----------------------
# MODELS
# ----------------------
class PlaylistRequest(BaseModel):
    url: str

class SelectionRequest(BaseModel):
    session_id: str
    numbers: list[int]

# ----------------------
# HELPERS
# ----------------------
def clean_cache():
    """Remove expired sessions"""
    now = time.time()
    expired = [sid for sid, val in CACHE.items() if now - val["created"] > CACHE_TTL]
    for sid in expired:
        del CACHE[sid]

def extract_playlist(url: str):
    """Step 1: Return minimal playlist metadata (number + title)"""
    def normalize_url(url: str) -> str:
        if "music.youtube.com" in url:
            return url.replace("music.youtube.com", "www.youtube.com")
        return url
    url1 = normalize_url(url)
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "nocheckcertificate": True,
        "ignoreerrors": True,
        "cachedir": False,
        "format": "best[height<=360][ext=mp4]/bestaudio/best",
        "noprogress": True,
        "extract_flat": True ,
        "headers" : {
            "User-Agent": random.choice(USER_AGENTS),},
        "cookiefile": get_cookies_file("youtube.com")
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url1, download=False)

    songs = []
    for i, entry in enumerate(info.get("entries", []), start=1):
        if not entry:
            continue
        video_id = entry.get("id")
        songs.append({
            "number": i,
            "id": video_id,
            "title": entry.get("title")
        })
    return songs

@app.post("/music_playlist")
def create_playlist(req: PlaylistRequest):
    clean_cache()

    # Check if playlist is already cached
    for sid, val in CACHE.items():
        if val.get("url") == req.url:
            return {"session_id": sid, "songs": val["songs"]}

    # Not cached, fetch fresh
    songs = extract_playlist(req.url)
    session_id = str(uuid.uuid4())
    CACHE[session_id] = {
        "url": req.url,
        "songs": songs,
        "created": time.time(),
        "selected": {}
    }
    return {"session_id": session_id, "songs": songs}

MAX_WORKERS = 15
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

def get_full_song_data(song_id: str, title: str):
    """Fetch thumbnail + audio URL in parallel"""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "nocheckcertificate": True,
        "ignoreerrors": True,
        "cachedir": False,
        "format": "bestaudio/best",
        "noprogress": True,
        "headers": {"User-Agent": random.choice(USER_AGENTS)},
        "cookiefile": get_cookies_file("youtube.com"),

    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://youtube.com/watch?v={song_id}", download=False)
        return {
            "title": title,
            "thumbnail": info.get("thumbnail"),
            "audio_url": info.get("url"),
            "video_url": f"https://youtube.com/watch?v={song_id}",
        }
    except Exception as e:
        return {"title": title, "error": str(e)}

def fetch_song_data(song_id: str, title: str, session_id: str, num: int):
    """Background fetch with caching"""
    try:
        data = get_full_song_data(song_id, title)
        CACHE[session_id]["selected"][num]["data"] = data
        CACHE[session_id]["selected"][num]["status"] = "done" if "error" not in data else "error"
    except Exception as e:
        CACHE[session_id]["selected"][num]["status"] = "error"
        CACHE[session_id]["selected"][num]["error"] = str(e)

@app.post("/music_playlist/select")
def select_songs(req: SelectionRequest):
    clean_cache()

    if req.session_id not in CACHE:
        return {"error": "Invalid or expired session"}

    session = CACHE[req.session_id]
    total = len(req.numbers)
    done_count = 0
    results = []

    for num in req.numbers:
        song = next((s for s in session["songs"] if s["number"] == num), None)
        if not song:
            results.append({"number": num, "status": "error", "error": "Song not found"})
            continue

        # If not started, launch background task
        if num not in session["selected"]:
            session["selected"][num] = {"status": "running", "data": None, "error": None}
            executor.submit(fetch_song_data, song["id"], song["title"], req.session_id, num)

        entry = session["selected"][num]
        if entry["status"] == "done":
            done_count += 1
            results.append({"number": num, "status": "done", "song": entry["data"]})
        elif entry["status"] == "error":
            results.append({"number": num, "status": "error", "error": entry["error"]})
        else:
            results.append({"number": num, "status": "running"})

    return {
        "progress": f"{done_count}/{total}",
        "songs": results
    }

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}

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
            extension = m.get("extension") or ("mp4" if media_type=="video" else "jpg")
            video_url = m.get("video_url")
            if extension == "mp4":
                audio_url = "https://middleman-downloader-1.onrender.com/extract-audio?video_url=" + urllib.parse.quote(video_url , safe="")
            else:
                audio_url = None

            media_list.append({
                "post_id": m.get("post_id"),
                "post_url": m.get("post_url"),
                "username": m.get("username"),
                "fullname": m.get("fullname"),
                "description": m.get("description"),
                "thumbnail": m.get("display_url"),
                "video_url": video_url,
                "audio_url" : audio_url,
                "type": media_type,
                "width": m.get("width"),
                "height": m.get("height"),
                "extension": extension,
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
        
        type = content.get("type") or ("video" if media_url.endswith(".mp4") else "image")
        if type == "photo":
            audio_url = None
        else:
            audio_url = "https://middleman-downloader-1.onrender.com/extract-audio?video_url=" + urllib.parse.quote(media_url , safe="")
        media_list.append({
            "tweet_id": content.get("tweet_id"),
            "username": content.get("user", {}).get("nick") or content.get("author", {}).get("nick"),
            "author_id": content.get("author", {}).get("id"),
            "content": content.get("content"),
            "media_url": media_url,
            "filename": content.get("filename"),
            "type": content.get("type") or ("video" if media_url.endswith(".mp4") else "image"),
            "extension": content.get("extension") or media_url.split(".")[-1],
            "audio_url" : audio_url,
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

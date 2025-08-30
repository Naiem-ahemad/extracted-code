import os
import tempfile
import asyncio
import threading
import shutil
import time
import json
import base64
import random
import re # Added for parsing FFmpeg output
from typing import Dict, Any, Optional
import gallery_dl , sys , io
import gallery_dl.config
import gallery_dl.job
# FastAPI and other core imports
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import yt_dlp, uuid
import boto3
import subprocess
import httpx
from botocore.client import Config
from dotenv import load_dotenv
load_dotenv()

# ---------------- Settings ----------------
TEMP_DIR = "./videos"
os.makedirs(TEMP_DIR, exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory=TEMP_DIR), name="static")

API_KEY = "all-7f04e0d887372e3769b200d990ae7868"
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET")

# Global S3 client initialization (from your original code)
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4")
)

# Global tasks dictionary (from your original code)
tasks: Dict[str, Dict[str, Any]] = {}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

BASE_YTDL_OPTS = {
    "quiet": True,
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

# ---------------- Utilities ----------------
def encrypt_task_data(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()

def decrypt_task_data(data: str) -> dict:
    return json.loads(base64.urlsafe_b64decode(data.encode()).decode())

def update_task_progress(task_id: str, updates: dict):
    """Thread-safe task progress update. This uses the existing tasks dictionary."""
    if task_id in tasks:
        try:
            # We must decrypt, update, and re-encrypt because the task data
            # is stored as an encrypted string in the global 'tasks' dict.
            current_data = decrypt_task_data(tasks[task_id]["encrypted"])
            current_data.update(updates)
            tasks[task_id]["encrypted"] = encrypt_task_data(current_data)
        except Exception as e:
            print(f"Error updating task {task_id}: {e}")
    else:
        print(f"Warning: Attempted to update non-existent task {task_id}")


async def download_file_with_progress(url: str, output_path: str, task_id: str, file_type: str = "video",
                                      base_progress: int = 0, progress_range: int = 100) -> bool:
    """Download file using httpx with progress tracking"""
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "identity",
            "Range": "bytes=0-",  # Enable range requests
        }

        timeout = httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=60.0)

        async with httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
            follow_redirects=True
        ) as client:

            # Get file size first
            try:
                head_response = await client.head(url, headers=headers)
                total_size = int(head_response.headers.get("content-length", 0))
            except Exception as e:
                print(f"Warning: Could not get total size for {file_type} from HEAD request: {e}. Will try during GET.")
                total_size = 0

            update_task_progress(task_id, {
                "stage": f"downloading {file_type}",
                "download_progress": 0, # Use a separate key for download progress within a stage
                "total_download_size": total_size
            })

            downloaded_bytes = 0

            async with client.stream("GET", url, headers=headers) as response:
                response.raise_for_status()

                # If we couldn't get size from HEAD, try from GET response
                if not total_size:
                    total_size = int(response.headers.get("content-length", 0))
                    update_task_progress(task_id, {"total_download_size": total_size})

                with open(output_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):  # 1MB chunks
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes += len(chunk)

                            # Update overall task progress based on current stage's range
                            if total_size > 0:
                                current_stage_percentage = (downloaded_bytes / total_size) * 100
                                overall_progress = base_progress + (current_stage_percentage / 100 * progress_range)
                                update_task_progress(task_id, {
                                    "progress": min(int(overall_progress), base_progress + progress_range -1), # Cap before next stage
                                    "downloaded_bytes": downloaded_bytes,
                                    "stage": f"downloading {file_type} ({downloaded_bytes // (1024 * 1024)}MB/{total_size // (1024 * 1024)}MB)"
                                })
                            else:
                                update_task_progress(task_id, {
                                    "progress": base_progress, # Maintain base if total size is unknown
                                    "downloaded_bytes": downloaded_bytes,
                                    "stage": f"downloading {file_type} ({downloaded_bytes // (1024 * 1024)}MB)"
                                })

        # Verify download
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            print(f"Successfully downloaded {file_type}: {output_path} ({downloaded_bytes} bytes)")
            return True
        else:
            print(f"Download failed or file is empty: {output_path}")
            return False

    except Exception as e:
        print(f"Download error for {file_type}: {e}")
        update_task_progress(task_id, {
            "status": "error",
            "error": f"Download failed for {file_type}: {str(e)}",
            "stage": "failed"
        })
        return False

# --- Global variable to track bytes uploaded for the current task ---
# This dictionary tracks progress per task_id for the current upload operation's callback.
_upload_progress_tracker: Dict[str, int] = {}

def _upload_progress_callback(task_id: str, total_file_size: int, base_progress_percentage: int, total_progress_range: int):
    """
    Callback function for boto3's S3Transfer to update upload progress.
    Updates the task's progress in the global 'tasks' dictionary.
    """
    def callback(bytes_transferred_in_chunk):
        _upload_progress_tracker[task_id] = _upload_progress_tracker.get(task_id, 0) + bytes_transferred_in_chunk
        current_uploaded_total = _upload_progress_tracker[task_id]

        if total_file_size > 0:
            # Calculate percentage of this specific upload
            upload_percentage_this_stage = (current_uploaded_total / total_file_size) * 100
            # Map it to the overall task progress range
            overall_progress = base_progress_percentage + (upload_percentage_this_stage / 100 * total_progress_range)
            update_task_progress(task_id, {
                "stage": f"uploading to storage ({current_uploaded_total // (1024 * 1024)}MB/{total_file_size // (1024 * 1024)}MB)",
                "progress": min(int(overall_progress), base_progress_percentage + total_progress_range - 1), # Cap at 99
                "uploaded_bytes": current_uploaded_total,
                "total_upload_size": total_file_size
            })
        else:
            update_task_progress(task_id, {
                "stage": f"uploading to storage ({current_uploaded_total // (1024 * 1024)}MB)",
                "progress": base_progress_percentage, # Maintain base if size is unknown
                "uploaded_bytes": current_uploaded_total
            })
    return callback

def upload_to_r2(
    file_path: str,
    task_id: str,
    base_progress_percentage: int, # e.g., 95
    total_progress_range: int = 5, # e.g., 5 (for 95-100)
    expiry_seconds: int = 24 * 3600 # 24 hours
) -> Optional[Dict[str, str]]: # Returns dict of public URLs
    """
    Uploads a file to Cloudflare R2 with progress updates and generates presigned URLs.
    """
    # Removed from boto3.s3.transfer import S3Transfer as it's not needed with s3.upload_fileobj
    # S3Transfer is a higher-level abstraction; direct upload_fileobj is better for ExtraArgs and callback.

    if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
        print("R2 environment variables not fully set. Cannot upload.")
        return None

    object_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    # Reset uploaded bytes tracker for this specific task before starting new upload
    _upload_progress_tracker[task_id] = 0

    try:
        print(f"Task {task_id}: Starting R2 upload for {object_name} (Size: {file_size} bytes)...")
        with open(file_path, "rb") as f:
            s3.upload_fileobj(
                f,
                R2_BUCKET_NAME,
                object_name,
                ExtraArgs={'ContentType': 'video/mp4', 'ACL': 'public-read'}, # Moved ExtraArgs here
                Callback=_upload_progress_callback(task_id, file_size, base_progress_percentage, total_progress_range)
            )
        print(f"Task {task_id}: Upload of {object_name} to R2 completed.")

        # Construct public URL for streaming (adjust based on your R2 custom domain if any)
        stream_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": R2_BUCKET_NAME, "Key": object_name},
            ExpiresIn=expiry_seconds
        )
        # Construct public URL for direct download
        download_url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": R2_BUCKET_NAME,
                "Key": object_name,
                "ResponseContentDisposition": f'attachment; filename="{object_name}"'
            },
            ExpiresIn=expiry_seconds
        )

        # Schedule file deletion after expiry
        def delete_after_expiry():
            time.sleep(expiry_seconds)
            try:
                s3.delete_object(Bucket=R2_BUCKET_NAME, Key=object_name)
                print(f"Task {task_id}: Successfully deleted expired R2 object: {object_name}")
            except Exception as e:
                print(f"Task {task_id}: Failed to delete expired R2 object {object_name}: {e}")

        threading.Thread(target=delete_after_expiry, daemon=True).start()

        return {"stream_url": stream_url, "download_url": download_url}

    except Exception as e:
        print(f"Error uploading {object_name} to R2: {e}")
        raise # Re-raise to be caught by the main download_and_merge error handler


def download_and_merge(task_id: str):
    """Download and merge video/audio using httpx"""
    # subprocess is already imported globally

    if task_id not in tasks:
        return

    t = tasks[task_id]
    data = decrypt_task_data(t["encrypted"])
    temp_dir = None

    try:
        update_task_progress(task_id, {
            "status": "downloading",
            "stage": "starting download",
            "progress": 0,
            "error": None # Clear previous errors
        })

        # Get format information
        video_format = data["video_format"]
        audio_format = data.get("audio_format")
        needs_merge = data.get("needs_merge", False)

        video_url = video_format.get("url")
        audio_url = audio_format.get("url") if audio_format else None

        print(f"Task {task_id}: Starting download")
        print(f"Video URL: {video_url[:100] if video_url else 'None'}...")
        if audio_url:
            print(f"Audio URL: {audio_url[:100]}...")
        print(f"Needs merge: {needs_merge}")

        if not video_url:
            raise Exception("No video URL found in format data")

        # Create temp directory
        temp_dir = tempfile.mkdtemp(dir=TEMP_DIR)
        output_path = os.path.join(temp_dir, f"video_{task_id[:8]}.mp4")

        # Run async downloads in event loop
        async def download_process():
            if needs_merge and audio_url:
                # Download video and audio separately, then merge
                video_path = os.path.join(temp_dir, f"video_{task_id[:8]}.webm")
                audio_path = os.path.join(temp_dir, f"audio_{task_id[:8]}.mp3")

                print(f"Task {task_id}: Downloading video and audio separately")

                # Download video (progress 0-45%)
                video_success = await download_file_with_progress(
                    video_url, video_path, task_id, "video",
                    base_progress=0, progress_range=45
                )
                if not video_success:
                    raise Exception("Video download failed")

                # Download audio (progress 45-90%)
                audio_success = await download_file_with_progress(
                    audio_url, audio_path, task_id, "audio",
                    base_progress=45, progress_range=45
                )
                if not audio_success:
                    raise Exception("Audio download failed")

                # Merge with FFmpeg (progress 90-95%)
                update_task_progress(task_id, {
                    "stage": "merging video and audio",
                    "progress": 90
                })

                total_duration_seconds = 0
                # Get total duration from video_format metadata if available to improve merge progress tracking
                # This assumes video_format contains 'duration' in seconds or milliseconds
                if 'duration' in data['video_format']:
                    total_duration_seconds = data['video_format'].get('duration', 0)
                elif 'duration' in data: # Check if top-level data has it
                     total_duration_seconds = data.get('duration', 0)

                # Use ffmpeg-python library for better control
                try:
                    import ffmpeg

                    # Using ffmpeg-python library
                    input_video = ffmpeg.input(video_path)
                    input_audio = ffmpeg.input(audio_path)

                    out = ffmpeg.output(
                        input_video, input_audio, output_path,
                        vcodec='copy', acodec='aac',
                        strict='experimental',
                        avoid_negative_ts='make_zero'
                    )
                    
                    # For ffmpeg-python, getting real-time progress is tricky without parsing stderr directly.
                    # A more complex setup is needed for that. For now, it will update once at 95.
                    ffmpeg.run(out, overwrite_output=True, quiet=True)
                    update_task_progress(task_id, {"stage": "merging video and audio", "progress": 95})

                except ImportError:
                    # Fallback to subprocess with better error handling and progress tracking
                    print("ffmpeg-python not installed, using subprocess for merge with progress tracking")

                    ffmpeg_cmd = [
                        "ffmpeg", "-y",
                        "-i", video_path, "-i", audio_path,
                        "-c:v", "copy", "-c:a", "aac",
                        "-strict", "experimental",
                        "-avoid_negative_ts", "make_zero",
                        "-loglevel", "info",  # Use 'info' to get progress output
                        output_path
                    ]

                    # Add timeout to prevent hanging
                    try:
                        # Use subprocess.Popen for real-time stderr parsing
                        process = subprocess.Popen(
                            ffmpeg_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True
                        )

                        # Regex to find time= entries
                        time_re = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.?(\d+)?")

                        for line in iter(process.stderr.readline, ''):
                            match = time_re.search(line)
                            if match and total_duration_seconds > 0:
                                hours = int(match.group(1))
                                minutes = int(match.group(2))
                                seconds = int(match.group(3))
                                current_time_seconds = hours * 3600 + minutes * 60 + seconds
                                
                                # Map to overall task progress range 90-95
                                merge_stage_percentage = (current_time_seconds / total_duration_seconds) * 100
                                # Ensure progress stays within the 90-95 range
                                overall_progress = 90 + (merge_stage_percentage / 100 * 5)
                                update_task_progress(task_id, {
                                    "stage": f"merging video and audio ({int(current_time_seconds)}s / {int(total_duration_seconds)}s)",
                                    "progress": min(int(overall_progress), 94) # Cap at 94 to leave room for final 95
                                })
                            # print(f"FFMPEG_OUT: {line.strip()}") # Uncomment to see full ffmpeg output

                        process.wait(timeout=300) # Wait for process to finish with timeout
                        if process.returncode != 0:
                            raise Exception(f"FFmpeg merge failed with error: {process.stderr.read()}")
                        
                        print(f"FFmpeg completed successfully")
                        update_task_progress(task_id, {"stage": "merging video and audio", "progress": 95})
                        
                    except subprocess.TimeoutExpired:
                        process.kill() # Terminate the process if it times out
                        process.wait()
                        raise Exception("FFmpeg merge timed out after 5 minutes")
                    except Exception as e: # Catch other potential errors during subprocess execution
                        print(f"FFmpeg error: {e}")
                        raise Exception(f"FFmpeg merge failed: {e}")

                # Verify merged file exists and has content
                if not os.path.exists(output_path):
                    raise Exception("FFmpeg merge completed but output file not found")

                merged_size = os.path.getsize(output_path)
                if merged_size == 0:
                    raise Exception("FFmpeg merge completed but output file is empty")

                print(f"FFmpeg merge successful. Output size: {merged_size} bytes")

                # Clean up temp files
                try:
                    os.remove(video_path)
                    os.remove(audio_path)
                except Exception as cleanup_e:
                    print(f"Warning: Failed to remove temporary video/audio files: {cleanup_e}")

            else:
                # Single file download (combined format or audio-only) (progress 0-95%)
                print(f"Task {task_id}: Downloading combined format (no merge needed)")
                success = await download_file_with_progress(
                    video_url, output_path, task_id, "video",
                    base_progress=0, progress_range=95
                )
                if not success:
                    raise Exception("Single file download failed")

        # --- Asyncio event loop handling ---
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If an event loop is already running, run the async task in a new thread
                def run_in_thread():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(download_process())
                    new_loop.close()

                thread = threading.Thread(target=run_in_thread)
                thread.start()
                thread.join()
            else:
                # If no event loop is running, run it directly
                loop.run_until_complete(download_process())
        except RuntimeError:
            # If get_event_loop() fails (e.g., no loop for current OS thread),
            # typically in scripts not launched via async main, run it with asyncio.run
            asyncio.run(download_process())
        # --- End of asyncio handling ---

        # Verify final file
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise Exception("Final output file not found or empty after download/merge")

        file_size = os.path.getsize(output_path)
        print(f"Task {task_id}: Download and merge completed. Final file size: {file_size} bytes")

        # Upload to R2 with progress (95-100%)
        urls = upload_to_r2(output_path, task_id, base_progress_percentage=95, total_progress_range=5)
        if not urls:
            raise Exception("Failed to upload to R2")

        update_task_progress(task_id, {
            "file": urls,
            "status": "done",
            "stage": "completed",
            "progress": 100,
            "error": None
        })

        print(f"Task {task_id}: Upload completed successfully")

    except Exception as e:
        error_message = str(e)
        print(f"Download failed for task {task_id}: {error_message}")
        update_task_progress(task_id, {
            "status": "error",
            "error": error_message,
            "stage": "failed"
        })

    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                print(f"Task {task_id}: Cleaned up temp directory")
            except Exception as e:
                print(f"Failed to remove temp directory: {e}")

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


@app.get("/")
def list_qualities(url: str = Query(...), key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    
    if "instagram.com" in url or "instagr.am" in url:
        return {"message": f"go to /instagram?url={url}&key={key}"}

    # Extract video information
    ydl_opts = {**BASE_YTDL_OPTS, "skip_download": True}
    cookies_file = get_cookies_file(url)
    if cookies_file:
        ydl_opts["cookiefile"] = cookies_file

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(400, f"Failed to extract video info: {e}")

    if not info:
        raise HTTPException(400, "No video information found")

    formats = [f for f in info.get("formats", [])
              if f.get("protocol") not in ("m3u8", "m3u8_native")
              and f.get("url")
              and f.get("format_id")]

    if not formats:
        raise HTTPException(400, "No downloadable formats found")

    # Audio formats
    audio_formats = [f for f in formats if f.get("acodec") and f.get("acodec") != "none" and f.get("vcodec") == "none"]

    audios = []
    for f in audio_formats:
        filesize = f.get("filesize") or f.get("filesize_approx") or 0
        audios.append({
            "quality": f"Audio {f.get('abr', 0)}kbps",
            "ext": f.get("ext"),
            "size": sizeof_fmt(filesize),
            "url": f.get("url")
        })

    # Video formats
    combined_formats = [f for f in formats if f.get("height") and f.get("vcodec") != "none" and f.get("acodec") != "none"]
    video_only_formats = [f for f in formats if f.get("height") and f.get("vcodec") != "none" and (not f.get("acodec") or f.get("acodec") == "none")]

    all_video_formats = combined_formats + video_only_formats
    height_groups = {}
    for f in all_video_formats:
        height = f.get("height")
        if height not in height_groups:
            height_groups[height] = []
        height_groups[height].append(f)

    qualities = []
    for height in sorted(height_groups.keys(), reverse=True):
        best_format = max(height_groups[height], key=lambda f: (f.get("tbr", 0) or 0, f.get("vbr", 0) or 0))
        filesize = best_format.get("filesize") or best_format.get("filesize_approx") or 0

        task_id = str(uuid.uuid4())
        task_data = {
            "url": url,
            "video_format": best_format,
            "audio_format": None,
            "needs_merge": False,
            "status": "waiting",
            "progress": 0,
            "stage": None,
            "file": None,
            "error": None,
            "timestamp": time.time(),
            "downloaded_bytes": 0,
            "total_download_size": 0,
            "uploaded_bytes": 0,
            "total_upload_size": 0,
            "duration": info.get('duration')
        }
        tasks[task_id] = {"encrypted": encrypt_task_data(task_data)}

        qualities.append({
            "quality": f"{height}p",
            "size": sizeof_fmt(filesize),
            "progress_url": f"/progress/{task_id}?key={key}"
        })

    return {
        "title": info.get("title", "Unknown"),
        "thumbnail": info.get("thumbnail"),
        "duration": info.get("duration"),
        "audio_only": False,
        "qualities": qualities,
        "audios": audios
    }

@app.get("/progress/{task_id}")
def progress(task_id: str, key: str = Query(...)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")

    if task_id not in tasks:
        return JSONResponse({"status": "unknown", "error": "Task not found"})

    t = tasks[task_id]
    data = decrypt_task_data(t["encrypted"])

    # Start download if waiting
    if data.get("status") == "waiting":
        data["status"] = "queued"
        tasks[task_id]["encrypted"] = encrypt_task_data(data)
        threading.Thread(target=download_and_merge, args=(task_id,), daemon=True).start()

    response = {
        "status": data.get("status", "unknown"),
        "progress": data.get("progress", 0),
        "stage": data.get("stage"),
        "error": data.get("error"),
        "downloaded_bytes": data.get("downloaded_bytes", 0),
        "total_download_size": data.get("total_download_size", 0),
        "uploaded_bytes": data.get("uploaded_bytes", 0),
        "total_upload_size": data.get("total_upload_size", 0)
    }

    # Add file URLs only when done
    if data.get("status") == "done" and data.get("file"):
        response["file"] = data["file"]

    return JSONResponse(response)

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}

# Clean up old tasks periodically
def cleanup_old_tasks():
    while True:
        try:
            current_time = time.time()
            old_tasks = []

            for task_id, task in tasks.items():
                try:
                    data = decrypt_task_data(task["encrypted"])
                    if current_time - data.get("timestamp", 0) > 3600:  # 1 hour old
                        old_tasks.append(task_id)
                except Exception: # Catch decryption errors for old, malformed tasks
                    old_tasks.append(task_id)

            for task_id in old_tasks:
                tasks.pop(task_id, None)

        except Exception as e:
            print(f"Cleanup error: {e}")

        time.sleep(300)  # Run every 5 minutes

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
    import logging
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

# Start cleanup thread
threading.Thread(target=cleanup_old_tasks, daemon=True).start()

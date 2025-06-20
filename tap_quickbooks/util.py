import json
import datetime
import threading
import queue
import os
from pathlib import Path
from typing import Optional

# Configurable log path
LOG_FILE_PATH = Path(os.getenv("API_USAGE_LOG", "api_usage.jsonl"))

# Create a queue for thread-safe logging
_log_queue: Optional[queue.Queue] = None
_log_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()

def _log_writer():
    """Background thread that writes logs to file."""
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    while not _stop_event.is_set():
        try:
            log_entry = _log_queue.get(timeout=1.0)
            with open(LOG_FILE_PATH, "a", encoding="utf-8", errors="replace") as f:
                f.write(json.dumps(log_entry, default=str) + "\n")
            _log_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error writing to log file: {e}")

def _ensure_log_thread():
    """Ensure the logging thread is running."""
    global _log_queue, _log_thread

    if _log_queue is None:
        _log_queue = queue.Queue()

    if _log_thread is None or not _log_thread.is_alive():
        _log_thread = threading.Thread(target=_log_writer, daemon=True)
        _log_thread.start()

def save_api_usage(method, url, params, body, response, stream=None):
    _ensure_log_thread()

    request_dict = {
        "method": method,
        "url": url,
        "params": params,
        "body": body,
    }
    request_dict = {k: v for k, v in request_dict.items() if v is not None}

    usage_data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "request": request_dict,
        "response_status": response.status_code if response else None,
    }

    if stream is not None:
        usage_data["stream"] = stream

    _log_queue.put(usage_data)

def cleanup():
    """Cleanup function to be called when the application is shutting down."""
    if _log_queue:
        _stop_event.set()
        _log_queue.join()
    if _log_thread:
        _log_thread.join(timeout=2)

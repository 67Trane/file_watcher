import time
import hashlib
from pathlib import Path
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# WATCH_PATH = Path(r"/Kunden/inbox")
# ARCHIVE_PATH = Path(r"/Kunden/Dokumente")

# BACKEND_URL = "http://backend:8000/api/import-document-from-pdf/"



WATCH_PATH = Path(r"C:\Users\67Trane\epson-test\test-runs\scanner-inbox")
BACKEND_URL = "http://localhost:8000/api/import-document-from-pdf/"

# NOTE: Keep this token in an env var for real usage.
IMPORT_TOKEN = "BAdd8oLgHOrs7UzBc2l73x6skt6EcICS"


def wait_until_file_stable(path: Path, stable_checks: int = 5, delay: float = 0.4, max_wait: float = 15.0) -> bool:
    """
    Wait until file size stays unchanged for 'stable_checks' consecutive checks.
    This is more reliable than a fixed sleep.
    """
    start = time.time()
    last_size = -1
    stable = 0

    while (time.time() - start) < max_wait:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            time.sleep(delay)
            continue

        if size == last_size:
            stable += 1
            if stable >= stable_checks:
                return True
        else:
            stable = 0
            last_size = size

        time.sleep(delay)

    return False


def file_fingerprint(path: Path) -> str:
    """
    Create a quick fingerprint to avoid duplicate processing.
    Uses path + size + mtime.
    """
    st = path.stat()
    raw = f"{path.resolve()}|{st.st_size}|{st.st_mtime}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class ScanHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        # NOTE: simple in-memory dedupe cache
        self.recent: dict[str, float] = {}
        self.dedupe_ttl_seconds = 60.0

    def _cleanup_cache(self):
        now = time.time()
        expired = [k for k, t in self.recent.items() if (now - t) > self.dedupe_ttl_seconds]
        for k in expired:
            self.recent.pop(k, None)

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)

        if file_path.suffix.lower() != ".pdf":
            return

        # Cleanup old cache entries
        self._cleanup_cache()

        # Wait until file is stable (fully written)
        if not wait_until_file_stable(file_path):
            print(f"[SKIP] File not stable (still writing?): {file_path}")
            return

        # Dedupe events
        try:
            fp = file_fingerprint(file_path)
        except FileNotFoundError:
            print(f"[SKIP] File disappeared: {file_path}")
            return

        if fp in self.recent:
            print(f"[SKIP] Duplicate event ignored: {file_path.name}")
            return

        self.recent[fp] = time.time()

        print(f"[NEW PDF] {file_path}")

        try:
            data = {"pdf_path": str(file_path)}
            headers = {"X-Import-Token": IMPORT_TOKEN}

            response = requests.post(
                BACKEND_URL,
                json=data,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 201:
                print(f"[OK] Imported: {file_path.name}")
            else:
                print(f"[ERROR] {response.status_code}: {response.text}")

        except requests.Timeout:
            print("[ERROR] Request timed out.")
        except Exception as e:
            print(f"[ERROR] {e}")


if __name__ == "__main__":
    if not WATCH_PATH.exists():
        raise RuntimeError(f"WATCH_PATH does not exist: {WATCH_PATH}")

    observer = Observer()
    observer.schedule(ScanHandler(), str(WATCH_PATH), recursive=False)
    observer.start()

    print("Watcher running. Watching:", WATCH_PATH)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

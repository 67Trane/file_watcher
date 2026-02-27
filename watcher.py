import hashlib
import re
import time
from pathlib import Path

import requests
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


# -----------------------------
# Configuration
# -----------------------------

WATCH_PATH = Path(r"/Kunden/inbox")
BACKEND_URL = "http://localhost:8000/api/import-document-from-pdf/"

# NOTE: In real usage, keep this in an environment variable (not in source code).
IMPORT_TOKEN = "BAdd8oLgHOrs7UzBc2l73x6skt6EcICS"

# Expected filename format: broker_<id>_<anything>.pdf  (e.g., "broker_3_20260124.pdf")
BROKER_PREFIX_RE = re.compile(r"^broker_(\d+)_", re.IGNORECASE)


# -----------------------------
# Helpers
# -----------------------------

def broker_id_from_filename(filename: str) -> str | None:
    """Extract broker id from a filename like 'broker_3_20260124.pdf'."""
    match = BROKER_PREFIX_RE.match(filename)
    return match.group(1) if match else None


def wait_until_file_stable(
    path: Path,
    stable_checks: int = 5,
    delay: float = 0.4,
    max_wait: float = 15.0,
) -> bool:
    """
    Wait until the file size stays unchanged for `stable_checks` consecutive checks.
    This reduces the risk of importing a PDF that is still being written by the scanner software.
    """
    start_time = time.time()
    last_size = -1
    stable_count = 0

    while (time.time() - start_time) < max_wait:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            time.sleep(delay)
            continue

        if size == last_size:
            stable_count += 1
            if stable_count >= stable_checks:
                return True
        else:
            stable_count = 0
            last_size = size

        time.sleep(delay)

    return False


def file_fingerprint(path: Path) -> str:
    """
    Create a quick fingerprint to reduce duplicate processing.
    Uses absolute path + size + mtime (fast and good enough for event dedupe).
    """
    st = path.stat()
    raw = f"{path.resolve()}|{st.st_size}|{st.st_mtime}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# -----------------------------
# Watchdog Handler
# -----------------------------

class ScanHandler(FileSystemEventHandler):
    def __init__(self) -> None:
        super().__init__()
        # Simple in-memory dedupe to avoid double-imports from repeated FS events
        self.recent: dict[str, float] = {}
        self.dedupe_ttl_seconds = 60.0

    def _cleanup_cache(self) -> None:
        """Remove expired fingerprints from the dedupe cache."""
        now = time.time()
        expired = [fp for fp, ts in self.recent.items() if (now - ts) > self.dedupe_ttl_seconds]
        for fp in expired:
            self.recent.pop(fp, None)

    def on_created(self, event) -> None:
        # -----------------------------
        # Guard clauses (skip conditions)
        # -----------------------------

        if event.is_directory:
            return

        file_path = Path(event.src_path)

        if file_path.suffix.lower() != ".pdf":
            return

        broker_id = broker_id_from_filename(file_path.name)
        if not broker_id:
            print(f"[SKIP] Missing broker prefix: {file_path.name} (expected 'broker_<id>_...')")
            return

        # -----------------------------
        # Prepare for import
        # -----------------------------

        self._cleanup_cache()

        # Wait until scanner finished writing the PDF
        if not wait_until_file_stable(file_path):
            print(f"[SKIP] File not stable (still writing?): {file_path}")
            return

        time.sleep(1.0)
        
        # Dedupe repeated filesystem events
        try:
            fp = file_fingerprint(file_path)
        except FileNotFoundError:
            print(f"[SKIP] File disappeared before revealing fingerprint: {file_path}")
            return

        if fp in self.recent:
            print(f"[SKIP] Duplicate event ignored: {file_path.name}")
            return

        self.recent[fp] = time.time()

        # -----------------------------
        # Send import request
        # -----------------------------

        print(f"[NEW PDF] {file_path} -> broker_id={broker_id}")

        # -----------------------------
        # DEBUG: Check file right before POST
        # -----------------------------
        exists_now = file_path.exists()

        if exists_now:
            size_now = file_path.stat().st_size
            mtime_now = file_path.stat().st_mtime
        else:
            size_now = None
            mtime_now = None

        print(
            f"[DEBUG] before POST "
            f"exists={exists_now} "
            f"size={size_now} "
            f"mtime={mtime_now} "
            f"path={file_path}"
        )

        # -----------------------------
        # Send import request
        # -----------------------------
        data = {"pdf_path": str(file_path)}
        headers = {
            "X-Import-Token": IMPORT_TOKEN,
            "X-Broker-Id": broker_id,
        }

        try:
            response = requests.post(
                BACKEND_URL,
                json=data,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 201:
                print(f"[OK] Imported: {file_path.name}")
            else:
                print(f"[ERROR] Import failed ({response.status_code}): {response.text}")

        except requests.Timeout:
            print("[ERROR] Request timed out.")
        except Exception as exc:
            print(f"[ERROR] Unexpected error: {exc}")


# -----------------------------
# Entrypoint
# -----------------------------

def main() -> None:
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


if __name__ == "__main__":
    main()

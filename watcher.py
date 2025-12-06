import time
from pathlib import Path
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WATCH_PATH = Path(r"/Kunden/inbox")
ARCHIVE_PATH = Path(r"/Kunden/Dokumente")

BACKEND_URL = "http://backend:8000/api/import-customer-from-pdf/"

class ScanHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)

        if file_path.suffix.lower() != ".pdf":
            return

        print(f"[NEUE DATEI] {file_path}")

        time.sleep(2)  # kurz warten, bis Datei fertig geschrieben ist

        try:
            data = {"pdf_path": str(file_path)}
            response = requests.post(BACKEND_URL, json=data)

            if response.status_code == 201:
                print(f"[OK] Importiert: {file_path.name}")
                ARCHIVE_PATH.mkdir(exist_ok=True)
                file_path.rename(ARCHIVE_PATH / file_path.name)
            else:
                print(f"[ERROR] {response.status_code}: {response.text}")

        except Exception as e:
            print(f"[FEHLER] {e}")


if __name__ == "__main__":
    observer = Observer()
    observer.schedule(ScanHandler(), str(WATCH_PATH), recursive=False)
    observer.start()

    print("Watcher läuft – überwacht:", WATCH_PATH)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

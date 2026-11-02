
# Downloads AMPS Labor records in resumable batches without overwriting existing files.

from pathlib import Path
import csv
import io
import time
import requests

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data/raw/nyc_amps/labor_batches"
OUTPUT.mkdir(parents=True, exist_ok=True)

URL = "https://data.cityofnewyork.us/resource/xzj8-i3jk.csv"
BATCH_SIZE = 5000

def read_batch(path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, strict=True))

def download():
    existing = sorted(OUTPUT.glob("labor_batch_*.csv"))

    for number, path in enumerate(existing, start=1):
        if path.name != f"labor_batch_{number:05d}.csv":
            raise ValueError("Unexpected existing batch numbering.")

    last_id = 0
    if existing:
        rows = read_batch(existing[-1])
        if not rows:
            raise ValueError("Existing last batch is empty.")
        last_id = int(rows[-1]["boo_code"])

    print("Existing batches:", len(existing), flush=True)
    print("Resuming after boo_code:", last_id, flush=True)

    session = requests.Session()
    batch_number = len(existing)

    while True:
        params = {
            "$where": f"boo_code > {last_id}",
            "$order": "boo_code ASC",
            "$limit": BATCH_SIZE
        }

        for attempt in range(3):
            try:
                response = session.get(URL, params=params, timeout=(15, 180))
                response.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(2 ** (attempt + 1))

        content = response.content
        with io.StringIO(content.decode("utf-8-sig"), newline="") as f:
            reader = csv.DictReader(f, strict=True)
            rows = list(reader)

        if not rows:
            print("No more records returned.")
            break

        ids = [int(row["boo_code"]) for row in rows]

        if ids != sorted(set(ids)) or ids[0] <= last_id:
            raise ValueError("Invalid or duplicate Labor identifiers.")

        batch_number += 1
        path = OUTPUT / f"labor_batch_{batch_number:05d}.csv"

        with path.open("xb") as f:
            f.write(content)

        last_id = ids[-1]

        print(
            f"Batch {batch_number}: {len(rows)} records, "
            f"last ID {last_id}",
            flush=True
        )

        if len(rows) < BATCH_SIZE:
            print("Final short batch received.")
            break

        time.sleep(0.3)

    print("Labor download run completed.")

if __name__ == "__main__":
    download()

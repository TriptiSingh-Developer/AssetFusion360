
# Downloads AMPS Work Orders in validated batches using a fixed source ID boundary.

from pathlib import Path
import csv
import io
import json
import time
import requests

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"
CHECKPOINT = ROOT / "data/reference/amps_workorders_checkpoint.json"

URL = "https://data.cityofnewyork.us/resource/8sdw-8vja.csv"
BATCH_SIZE = 5000
MAX_NEW_BATCHES = 20
MAX_RETRIES = 3

with CHECKPOINT.open("r", encoding="utf-8") as file:
    checkpoint = json.load(file)

if checkpoint["dataset_id"] != "8sdw-8vja":
    raise ValueError("Incorrect source checkpoint.")

MAX_ID = int(checkpoint["max_evt_code"])

if MAX_ID <= 0:
    raise ValueError("Invalid checkpoint maximum ID.")

BATCH_DIR.mkdir(parents=True, exist_ok=True)


def inspect_batch(path):
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, strict=True)
        columns = reader.fieldnames or []

        if "evt_code" not in columns or "evt_object" not in columns:
            raise ValueError(f"Invalid schema: {path.name}")

        ids = []

        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValueError(f"Malformed record: {path.name}")

            ids.append(int(row["evt_code"]))

    if not ids or ids != sorted(set(ids)):
        raise ValueError(f"Invalid IDs: {path.name}")

    return ids[0], ids[-1], len(ids), columns


existing = sorted(BATCH_DIR.glob("workorders_batch_*.csv"))

last_id = None
schema = None
total_saved = 0

for number, path in enumerate(existing, start=1):
    expected = f"workorders_batch_{number:05d}.csv"

    if path.name != expected:
        raise ValueError(f"Unexpected filename: {path.name}")

    first, last, count, columns = inspect_batch(path)

    if count != BATCH_SIZE:
        raise ValueError(f"Incomplete batch: {path.name}")

    if last_id is not None and first <= last_id:
        raise ValueError(f"Overlapping batch: {path.name}")

    if schema is not None and columns != schema:
        raise ValueError(f"Schema mismatch: {path.name}")

    schema = columns
    last_id = last
    total_saved += count

if not existing:
    raise ValueError("Expected existing validated batches.")

if last_id > MAX_ID:
    raise ValueError("Existing data exceeds checkpoint boundary.")

print("Existing batches:", len(existing))
print("Existing records:", total_saved)
print("Last saved EVT_CODE:", last_id)
print("Checkpoint maximum EVT_CODE:", MAX_ID)

session = requests.Session()

for number in range(
    len(existing) + 1,
    len(existing) + MAX_NEW_BATCHES + 1
):
    if last_id >= MAX_ID:
        print("Checkpoint boundary reached.")
        break

    output = BATCH_DIR / f"workorders_batch_{number:05d}.csv"

    if output.exists():
        raise FileExistsError(f"File already exists: {output}")

    params = {
        "$where": f"evt_code > {last_id} AND evt_code <= {MAX_ID}",
        "$order": "evt_code ASC",
        "$limit": BATCH_SIZE
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                URL,
                params=params,
                timeout=(15, 180)
            )
            response.raise_for_status()
            break
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)

    with io.StringIO(
        response.content.decode("utf-8-sig"),
        newline=""
    ) as stream:
        reader = csv.DictReader(stream, strict=True)
        columns = reader.fieldnames or []
        rows = list(reader)

    if columns != schema:
        raise ValueError("Source schema has changed.")

    if not rows:
        print("No further records within checkpoint boundary.")
        break

    if len(rows) > BATCH_SIZE:
        raise ValueError("Unexpected batch size.")

    if any(
        None in row or any(v is None for v in row.values())
        for row in rows
    ):
        raise ValueError("Malformed CSV record detected.")

    ids = [int(row["evt_code"]) for row in rows]

    if ids != sorted(set(ids)):
        raise ValueError("IDs are duplicated or out of order.")

    if ids[0] <= last_id or ids[-1] > MAX_ID:
        raise ValueError("Batch is outside allowed ID range.")

    with output.open("xb") as file:
        file.write(response.content)

    last_id = ids[-1]
    total_saved += len(rows)

    print(
        f"Batch {number}: {len(rows)} records, "
        f"EVT_CODE {ids[0]} to {last_id}"
    )

    if len(rows) < BATCH_SIZE:
        print("Short batch received; checking source completeness is required.")
        break

    time.sleep(1)

print("Session complete.")
print("Total saved batch records:", total_saved)


# Downloads AMPS Work Orders in batches and updates the checksum manifest after each batch.

from pathlib import Path
from datetime import datetime, timezone
import csv
import hashlib
import io
import json
import os
import time
import requests

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"
REF_DIR = ROOT / "data/reference"

MANIFEST_PATH = REF_DIR / "amps_workorders_manifest_v3.json"
CHECKPOINT_PATH = REF_DIR / "amps_workorders_checkpoint.json"
BACKUP_PATH = REF_DIR / "amps_workorders_manifest_v3_backup.json"
LOCK_PATH = REF_DIR / "amps_workorders_download.lock"

URL = "https://data.cityofnewyork.us/resource/8sdw-8vja.csv"

BATCH_SIZE = 5000
MAX_NEW_BATCHES = 500
MAX_RETRIES = 3


def load_json(path):
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_manifest(manifest):
    temporary = MANIFEST_PATH.with_suffix(".json.tmp")

    with temporary.open("x", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)

    os.replace(temporary, MANIFEST_PATH)


def validate_csv(content, expected_columns, previous_id, max_id):
    with io.StringIO(
        content.decode("utf-8-sig"),
        newline=""
    ) as stream:
        reader = csv.DictReader(stream, strict=True)
        columns = reader.fieldnames or []
        rows = list(reader)

    if columns != expected_columns:
        raise ValueError("CSV schema mismatch.")

    if not rows or len(rows) > BATCH_SIZE:
        raise ValueError("Invalid batch record count.")

    if any(
        None in row or any(value is None for value in row.values())
        for row in rows
    ):
        raise ValueError("Malformed CSV records detected.")

    ids = [int(row["evt_code"]) for row in rows]

    if ids != sorted(set(ids)):
        raise ValueError("Duplicate or unordered EVT_CODE values.")

    if ids[0] <= previous_id or ids[-1] > max_id:
        raise ValueError("Work-order IDs outside expected boundaries.")

    return rows, ids


def main():
    if not MANIFEST_PATH.exists() or not CHECKPOINT_PATH.exists():
        raise FileNotFoundError("Manifest or source checkpoint missing.")

    manifest = load_json(MANIFEST_PATH)
    checkpoint = load_json(CHECKPOINT_PATH)

    if manifest["dataset_id"] != "8sdw-8vja":
        raise ValueError("Incorrect manifest dataset.")

    if checkpoint["dataset_id"] != "8sdw-8vja":
        raise ValueError("Incorrect checkpoint dataset.")

    max_id = int(checkpoint["max_evt_code"])
    last_id = int(manifest["last_evt_code"])
    count = int(manifest["total_batches"])
    columns = manifest["columns"]

    if count != len(manifest["batches"]):
        raise ValueError("Manifest batch count mismatch.")

    if sum(b["record_count"] for b in manifest["batches"]) != int(
        manifest["total_records"]
    ):
        raise ValueError("Manifest record count mismatch.")

    existing = sorted(BATCH_DIR.glob("workorders_batch_*.csv"))

    if len(existing) != count:
        raise ValueError(
            "Batch files and manifest do not match. Stop and investigate."
        )

    for index, path in enumerate(existing, start=1):
        expected_name = f"workorders_batch_{index:05d}.csv"
        entry = manifest["batches"][index - 1]

        if path.name != expected_name or entry["filename"] != expected_name:
            raise ValueError("Unexpected batch filename or ordering.")

        if path.stat().st_size != entry["file_size_bytes"]:
            raise ValueError(f"File size changed: {path.name}")

    last_entry = manifest["batches"][-1]
    last_path = BATCH_DIR / last_entry["filename"]

    last_hash = hashlib.sha256(last_path.read_bytes()).hexdigest()

    if last_hash != last_entry["sha256"]:
        raise ValueError("Latest batch checksum mismatch.")

    if int(last_entry["max_evt_code"]) != last_id:
        raise ValueError("Last ID does not match the manifest.")

    if not BACKUP_PATH.exists():
        with BACKUP_PATH.open("x", encoding="utf-8") as file:
            json.dump(manifest, file, indent=2)

    print("Manifest batches:", count)
    print("Manifest records:", manifest["total_records"])
    print("Last EVT_CODE:", last_id)
    print("Checkpoint maximum:", max_id)

    session = requests.Session()

    for _ in range(MAX_NEW_BATCHES):
        if last_id >= max_id:
            print("Checkpoint boundary reached.")
            break

        next_number = count + 1
        output = BATCH_DIR / f"workorders_batch_{next_number:05d}.csv"

        if output.exists():
            raise FileExistsError(f"Batch already exists: {output}")

        params = {
            "$where": f"evt_code > {last_id} AND evt_code <= {max_id}",
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

        content = response.content

        with io.StringIO(
            content.decode("utf-8-sig"), newline=""
        ) as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader)

        if not content or header != columns:
            raise ValueError("Unexpected or empty API response.")

        rows, ids = validate_csv(content, columns, last_id, max_id)

        # Exclusive creation prevents overwriting existing CSVs.
        with output.open("xb") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())

        entry = {
            "batch_number": next_number,
            "filename": output.name,
            "record_count": len(rows),
            "min_evt_code": ids[0],
            "max_evt_code": ids[-1],
            "file_size_bytes": len(content),
            "validation_status": "passed",
            "sha256": hashlib.sha256(content).hexdigest()
        }

        manifest["batches"].append(entry)
        manifest["total_batches"] += 1
        manifest["total_records"] += len(rows)
        manifest["last_evt_code"] = ids[-1]
        manifest["updated_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()

        save_manifest(manifest)

        count = next_number
        last_id = ids[-1]

        print(
            f"Batch {count}: {len(rows)} records, "
            f"EVT_CODE {ids[0]} to {last_id}"
        )

        if len(rows) < BATCH_SIZE:
            print("Short batch received. Verify source completeness.")
            break

        time.sleep(1)

    print("Total saved records:", manifest["total_records"])
    print("Total saved batches:", manifest["total_batches"])


# Prevent two instances of this downloader running together.
if __name__ == "__main__":
    with LOCK_PATH.open("x", encoding="utf-8") as lock:
        lock.write(str(datetime.now(timezone.utc)))

    try:
        main()
    finally:
        LOCK_PATH.unlink(missing_ok=True)

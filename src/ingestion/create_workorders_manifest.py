
# Validates existing AMPS Work Orders batches and creates a download manifest.

from pathlib import Path
from datetime import datetime, timezone
import csv
import json
import os

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"
MANIFEST = ROOT / "data/reference/amps_workorders_manifest_v2.json"

files = sorted(BATCH_DIR.glob("workorders_batch_*.csv"))

if not files:
    raise FileNotFoundError("No Work Orders batch files found.")

if MANIFEST.exists():
    raise FileExistsError(
        "Manifest already exists. No overwrite performed."
    )

batches = []
previous_max = None
expected_schema = None
total_records = 0

for batch_number, path in enumerate(files, start=1):
    expected_name = f"workorders_batch_{batch_number:05d}.csv"

    if path.name != expected_name:
        raise ValueError(f"Unexpected batch filename: {path.name}")

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, strict=True)
        schema = reader.fieldnames or []

        if "evt_code" not in schema or "evt_object" not in schema:
            raise ValueError(f"Missing required columns in {path.name}")

        if expected_schema is not None and schema != expected_schema:
            raise ValueError(f"Schema mismatch in {path.name}")

        expected_schema = schema

        ids = []

        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed row in {path.name}")
            ids.append(int(row["evt_code"]))

    if not ids or ids != sorted(set(ids)):
        raise ValueError(f"Duplicate or unordered IDs in {path.name}")

    if previous_max is not None and ids[0] <= previous_max:
        raise ValueError(f"Overlapping batch: {path.name}")

    previous_max = ids[-1]
    count = len(ids)
    total_records += count

    batches.append({
        "batch_number": batch_number,
        "filename": path.name,
        "record_count": count,
        "min_evt_code": ids[0],
        "max_evt_code": ids[-1],
        "file_size_bytes": path.stat().st_size,
        "validation_status": "passed"
    })

manifest = {
    "dataset": "NYC Parks AMPS Work Orders",
    "dataset_id": "8sdw-8vja",
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "total_batches": len(batches),
    "total_records": total_records,
    "last_evt_code": previous_max,
    "columns": expected_schema,
    "batches": batches
}

MANIFEST.parent.mkdir(parents=True, exist_ok=True)

temporary = MANIFEST.with_suffix(".json.tmp")

with temporary.open("x", encoding="utf-8") as file:
    json.dump(manifest, file, indent=2)

os.replace(temporary, MANIFEST)

print("Manifest created successfully.")
print("Validated batches:", len(batches))
print("Total records:", total_records)
print("Last EVT_CODE:", previous_max)
print("Manifest location:", MANIFEST)

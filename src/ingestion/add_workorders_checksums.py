
# Verifies saved AMPS Work Orders batches and creates a checksum-enriched manifest.

from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parents[2]

BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"

SOURCE = ROOT / "data/reference/amps_workorders_manifest_v2.json"

OUTPUT = ROOT / "data/reference/amps_workorders_manifest_v3.json"

if OUTPUT.exists():
    raise FileExistsError(f"Manifest already exists: {OUTPUT}")

with SOURCE.open("r", encoding="utf-8") as file:
    manifest = json.load(file)

batches = manifest["batches"]

if len(batches) != manifest["total_batches"]:
    raise ValueError("Manifest batch count mismatch.")

total_records = 0

for entry in batches:
    path = BATCH_DIR / entry["filename"]

    if not path.is_file():
        raise FileNotFoundError(path)

    if path.stat().st_size != entry["file_size_bytes"]:
        raise ValueError(f"File size mismatch: {path.name}")

    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    entry["sha256"] = digest.hexdigest()
    total_records += entry["record_count"]

if total_records != manifest["total_records"]:
    raise ValueError("Total record count mismatch.")

with OUTPUT.open("x", encoding="utf-8") as file:
    json.dump(manifest, file, indent=2)

print("Checksum manifest created successfully.")
print("Batches:", len(batches))
print("Records:", total_records)
print("Last EVT_CODE:", manifest["last_evt_code"])
print("Saved to:", OUTPUT)

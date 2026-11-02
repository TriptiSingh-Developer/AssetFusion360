
# Records the AMPS Work Orders source count, maximum ID, and extraction time.

from pathlib import Path
from datetime import datetime, timezone
import json
import requests

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data/reference/amps_workorders_checkpoint.json"

URL = "https://data.cityofnewyork.us/resource/8sdw-8vja.json"

if OUTPUT.exists():
    raise FileExistsError(
        "Checkpoint already exists. No overwrite performed."
    )

params = {
    "$select": "count(*) as total_rows, max(evt_code) as max_id"
}

response = requests.get(URL, params=params, timeout=(15, 120))
response.raise_for_status()

results = response.json()

if len(results) != 1:
    raise ValueError("Unexpected source response.")

total_rows = int(results[0]["total_rows"])
max_id = int(results[0]["max_id"])

if total_rows <= 0 or max_id <= 0:
    raise ValueError("Invalid source statistics.")

checkpoint = {
    "source": "NYC Parks AMPS Work Orders",
    "dataset_id": "8sdw-8vja",
    "api_url": URL,
    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    "total_rows": total_rows,
    "max_evt_code": max_id
}

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT.open("x", encoding="utf-8") as file:
    json.dump(checkpoint, file, indent=4)

print("\nSOURCE CHECKPOINT CREATED")
print("Total source rows:", total_rows)
print("Maximum EVT_CODE:", max_id)
print("Saved to:", OUTPUT)


# Downloads the next AMPS Work Orders batch using the last saved work-order ID.

from pathlib import Path
import csv
import io
import requests

ROOT = Path(__file__).resolve().parents[2]
BATCH_DIR = ROOT / "data/raw/nyc_amps/workorders_batches"

URL = "https://data.cityofnewyork.us/resource/8sdw-8vja.csv"
BATCH_SIZE = 5000

previous_file = BATCH_DIR / "workorders_batch_00001.csv"
output = BATCH_DIR / "workorders_batch_00002.csv"

if not previous_file.exists():
    raise FileNotFoundError("First batch is missing.")

if output.exists():
    raise FileExistsError("Second batch already exists. No overwrite performed.")

with previous_file.open("r", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)
    last_id = max(int(row["evt_code"]) for row in reader)

params = {
    "$where": f"evt_code > {last_id}",
    "$order": "evt_code ASC",
    "$limit": BATCH_SIZE
}

response = requests.get(URL, params=params, timeout=(15, 180))
response.raise_for_status()

with io.StringIO(response.content.decode("utf-8-sig"), newline="") as stream:
    reader = csv.DictReader(stream)
    rows = list(reader)
    columns = reader.fieldnames

if not columns or "evt_code" not in columns or "evt_object" not in columns:
    raise ValueError("Required columns are missing.")

if len(rows) != BATCH_SIZE:
    raise ValueError(f"Unexpected number of rows: {len(rows)}")

ids = [int(row["evt_code"]) for row in rows]

if ids != sorted(set(ids)) or min(ids) <= last_id:
    raise ValueError("Duplicate, unordered, or overlapping work-order IDs.")

output.write_bytes(response.content)

print("Second batch downloaded and validated.")
print("Records:", len(rows))
print("Previous maximum EVT_CODE:", last_id)
print("New minimum EVT_CODE:", min(ids))
print("New maximum EVT_CODE:", max(ids))
print("Saved to:", output)

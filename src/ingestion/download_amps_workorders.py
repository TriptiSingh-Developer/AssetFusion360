
# Downloads and validates a 1,000-record AMPS Work Orders test batch.

from pathlib import Path
import csv
import io
import requests

project_root = Path(__file__).resolve().parents[2]

output = (
    project_root
    / "data/raw/nyc_amps/AMPS_WorkOrders_test_1000.csv"
)

url = "https://data.cityofnewyork.us/resource/8sdw-8vja.csv"

params = {
    "$limit": 1000,
    "$offset": 0,
    "$order": "evt_code ASC"
}

if output.exists():
    print("Test dataset already exists. No overwrite performed.")
    raise SystemExit(0)

response = requests.get(
    url,
    params=params,
    timeout=(15, 120)
)
response.raise_for_status()

with io.StringIO(
    response.content.decode("utf-8-sig"),
    newline=""
) as stream:
    reader = csv.reader(stream, strict=True)
    header = next(reader)
    rows = list(reader)

assert "EVT_CODE" in [col.upper() for col in header]
assert "EVT_OBJECT" in [col.upper() for col in header]
assert len(rows) == 1000
assert all(len(row) == len(header) for row in rows)

output.write_bytes(response.content)

print("Download successful")
print("Records:", len(rows))
print("Columns:", len(header))
print("Saved to:", output)

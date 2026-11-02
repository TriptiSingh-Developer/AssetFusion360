
# Downloads NYC Parks 311 requests in resumable, validated CSV batches.

from pathlib import Path
import csv
import io
import json
import time
import requests

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data/raw/nyc_311/dpr_batches"
OUTPUT.mkdir(parents=True, exist_ok=True)

URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
BATCH_SIZE = 5000
MAX_RETRIES = 3

def parse_csv(content):
    with io.StringIO(content.decode("utf-8-sig"), newline="") as stream:
        reader = csv.DictReader(stream, strict=True)
        columns = reader.fieldnames or []
        rows = list(reader)

    if "unique_key" not in columns or "agency" not in columns:
        raise ValueError("Required columns missing.")

    if any(
        None in row or any(value is None for value in row.values())
        for row in rows
    ):
        raise ValueError("Malformed CSV records.")

    return columns, rows

def download():
    files = sorted(OUTPUT.glob("dpr_batch_*.csv"))

    for i, path in enumerate(files, 1):
        if path.name != f"dpr_batch_{i:05d}.csv":
            raise ValueError("Unexpected existing batch numbering.")

    last_key = ""
    columns_expected = None

    if files:
        columns_expected, previous_rows = parse_csv(
            files[-1].read_bytes()
        )
        if not previous_rows:
            raise ValueError("Last saved batch is empty.")
        last_key = previous_rows[-1]["unique_key"]

    session = requests.Session()
    batch_number = len(files)
    new_records = 0

    print("Existing batches:", batch_number, flush=True)
    print("Resuming after unique_key:", last_key, flush=True)

    while True:
        # unique_key is text, so use a text comparison, not a numeric filter.
        where = "agency = 'DPR'"
        if last_key:
            safe_key = last_key.replace("'", "''")
            where += f" AND unique_key > '{safe_key}'"

        params = {
            "$where": where,
            "$order": "unique_key ASC",
            "$limit": BATCH_SIZE,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(
                    URL, params=params, timeout=(15, 180)
                )
                response.raise_for_status()
                break
            except requests.RequestException as error:
                if attempt == MAX_RETRIES:
                    print("API response:", getattr(
                        error.response, "text", ""
                    )[:500])
                    raise
                time.sleep(2 ** attempt)

        columns, rows = parse_csv(response.content)

        if not rows:
            print("No additional records found.", flush=True)
            break

        if columns_expected and columns != columns_expected:
            raise ValueError("Source column schema changed.")

        columns_expected = columns

        keys = [row["unique_key"] for row in rows]

        if (
            len(rows) > BATCH_SIZE
            or any(not key for key in keys)
            or keys != sorted(set(keys))
            or (last_key and keys[0] <= last_key)
        ):
            raise ValueError("Invalid or duplicate unique_key values.")

        if any(row["agency"] != "DPR" for row in rows):
            raise ValueError("Unexpected non-DPR record.")

        batch_number += 1
        path = OUTPUT / f"dpr_batch_{batch_number:05d}.csv"

        with path.open("xb") as file:
            file.write(response.content)

        last_key = keys[-1]
        new_records += len(rows)

        print(
            f"Batch {batch_number}: {len(rows)} records, "
            f"last unique_key {last_key}",
            flush=True,
        )

        if len(rows) < BATCH_SIZE:
            print("Final short batch received.", flush=True)
            break

        time.sleep(0.3)

    print("\nDownload run completed.")
    print("Total saved batches:", batch_number)
    print("Records downloaded this run:", new_records)

if __name__ == "__main__":
    download()

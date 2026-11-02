
# Downloads official AMPS Transaction Details in resumable CSV batches.

from pathlib import Path
import csv
import io
import os
import time
import requests

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data/raw/nyc_amps/transaction_details_batches"

DATASET_ID = "ub9z-gccp"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.csv"

BATCH_SIZE = 5000
MAX_RETRIES = 5

OUTPUT.mkdir(parents=True, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "AssetFusion360-Rutgers-Capstone/1.0"
})


def download_batch(offset):
    params = {
        "$limit": BATCH_SIZE,
        "$offset": offset,
        "$order": ":id",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                BASE_URL,
                params=params,
                timeout=120,
            )
            response.raise_for_status()

            text = response.text

            reader = csv.reader(io.StringIO(text))
            rows = list(reader)

            if not rows:
                raise ValueError("Empty API response")

            header = rows[0]
            data_count = len(rows) - 1

            return text, header, data_count

        except (requests.RequestException, ValueError) as exc:
            if attempt == MAX_RETRIES:
                raise

            print(
                f"Retry {attempt}/{MAX_RETRIES}: {exc}",
                flush=True,
            )
            time.sleep(min(attempt * 3, 15))


def count_existing_rows(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


batch_number = 1
total_records = 0
expected_header = None

while True:
    offset = (batch_number - 1) * BATCH_SIZE

    path = OUTPUT / (
        f"transaction_details_batch_{batch_number:05d}.csv"
    )

    if path.exists():
        existing_count = count_existing_rows(path)

        if existing_count <= 0 or existing_count > BATCH_SIZE:
            raise ValueError(
                f"Invalid existing batch: {path.name}"
            )

        if existing_count < BATCH_SIZE:
            # A partial final batch might reflect an older
            # dataset snapshot. Stop and review rather than
            # silently extending or overwriting it.
            raise ValueError(
                f"Existing partial batch needs review: {path.name}"
            )

        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:
            header = next(csv.reader(f))

        if expected_header is None:
            expected_header = header
        elif header != expected_header:
            raise ValueError("Inconsistent existing CSV headers")

        total_records += existing_count
        batch_number += 1
        continue

    content, header, count = download_batch(offset)

    if expected_header is None:
        expected_header = header
    elif header != expected_header:
        raise ValueError("API schema changed during download")

    if count == 0:
        break

    temporary = path.with_suffix(".csv.tmp")

    if temporary.exists():
        raise FileExistsError(
            f"Review existing temporary file: {temporary}"
        )

    with temporary.open(
        "x",
        encoding="utf-8",
        newline="",
    ) as f:
        f.write(content)

    saved_count = count_existing_rows(temporary)

    if saved_count != count:
        raise ValueError(
            f"Saved row count mismatch: {path.name}"
        )

    os.replace(temporary, path)

    total_records += count

    if batch_number % 25 == 0 or count < BATCH_SIZE:
        print(
            f"Downloaded {batch_number} batches | "
            f"{total_records:,} records",
            flush=True,
        )

    batch_number += 1

    if count < BATCH_SIZE:
        break

print("\n--- AMPS TRANSACTION DETAILS DOWNLOAD ---")
print("Downloaded records:", total_records)
print("CSV batch files:", batch_number - 1)
print("Columns:", len(expected_header or []))
print("Output:", OUTPUT)
print("\nDOWNLOAD COMPLETED")

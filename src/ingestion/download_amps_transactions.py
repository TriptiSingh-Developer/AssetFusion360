
# Downloads AMPS Transactions in validated, resumable CSV batches.

from pathlib import Path
import csv
import io
import time
import requests

ROOT = Path(__file__).resolve().parents[2]

OUTPUT = ROOT / "data/raw/nyc_amps/transactions_batches"
OUTPUT.mkdir(parents=True, exist_ok=True)

URL = "https://data.cityofnewyork.us/resource/gcys-vvnq.csv"

BATCH_SIZE = 5000
MAX_RETRIES = 3
ID_COLUMN = "tra_sqlidentity"


def read_batch(content):
    with io.StringIO(
        content.decode("utf-8-sig"), newline=""
    ) as stream:
        reader = csv.DictReader(stream, strict=True)
        columns = reader.fieldnames
        rows = list(reader)

    if not columns or ID_COLUMN not in columns:
        raise ValueError("Required transaction identifier is missing.")

    if any(
        None in row or any(value is None for value in row.values())
        for row in rows
    ):
        raise ValueError("Malformed CSV records detected.")

    return columns, rows


def download():
    files = sorted(
        OUTPUT.glob("transactions_batch_*.csv")
    )

    for number, path in enumerate(files, start=1):
        expected_name = f"transactions_batch_{number:05d}.csv"

        if path.name != expected_name:
            raise ValueError(
                f"Unexpected batch filename: {path.name}"
            )

    last_id = 0
    expected_columns = None

    if files:
        content = files[-1].read_bytes()
        expected_columns, rows = read_batch(content)

        if not rows:
            raise ValueError("Last saved batch is empty.")

        last_ids = [
            int(row[ID_COLUMN]) for row in rows
        ]

        if last_ids != sorted(set(last_ids)):
            raise ValueError(
                "Last saved batch contains invalid identifiers."
            )

        last_id = last_ids[-1]

    print("Existing batches:", len(files), flush=True)
    print(
        "Resuming after TRA_SQLIDENTITY:",
        last_id,
        flush=True
    )

    session = requests.Session()
    batch_number = len(files)
    downloaded_this_run = 0

    while True:
        params = {
            "$where": f"{ID_COLUMN} > {last_id}",
            "$order": f"{ID_COLUMN} ASC",
            "$limit": BATCH_SIZE,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(
                    URL,
                    params=params,
                    timeout=(15, 180),
                )
                response.raise_for_status()
                break

            except requests.RequestException as error:
                if attempt == MAX_RETRIES:
                    raise

                print(
                    f"Request failed: {error}. Retrying...",
                    flush=True,
                )
                time.sleep(2 ** attempt)

        content = response.content
        columns, rows = read_batch(content)

        if not rows:
            print("No more records returned.", flush=True)
            break

        if expected_columns is not None:
            if columns != expected_columns:
                raise ValueError(
                    "Source CSV schema changed."
                )

        expected_columns = columns

        if len(rows) > BATCH_SIZE:
            raise ValueError(
                "API returned more rows than requested."
            )

        ids = [
            int(row[ID_COLUMN]) for row in rows
        ]

        if ids != sorted(set(ids)):
            raise ValueError(
                "Duplicate or unordered transaction IDs."
            )

        if ids[0] <= last_id:
            raise ValueError(
                "Downloaded identifiers overlap existing data."
            )

        batch_number += 1

        output_file = (
            OUTPUT
            / f"transactions_batch_{batch_number:05d}.csv"
        )

        # Exclusive creation protects existing downloaded batches.
        with output_file.open("xb") as file:
            file.write(content)

        last_id = ids[-1]
        downloaded_this_run += len(rows)

        print(
            f"Batch {batch_number}: "
            f"{len(rows)} records, "
            f"last TRA_SQLIDENTITY {last_id}",
            flush=True,
        )

        if len(rows) < BATCH_SIZE:
            print(
                "Final short batch received.",
                flush=True,
            )
            break

        time.sleep(0.3)

    print("\nTransactions download run completed.")
    print("Total saved batches:", batch_number)
    print(
        "Records downloaded this run:",
        downloaded_this_run,
    )
    print(
        "Last TRA_SQLIDENTITY:",
        last_id,
    )


if __name__ == "__main__":
    download()
